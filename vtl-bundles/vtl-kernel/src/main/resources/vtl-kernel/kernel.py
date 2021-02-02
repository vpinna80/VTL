# -*- coding: utf-8 -*-
#
# Copyright © 2020 Banca D'Italia
#
# Licensed under the EUPL, Version 1.2 (the "License");
# You may not use this work except in compliance with the
# License.
# You may obtain a copy of the License at:
#
# https://joinup.ec.europa.eu/sites/default/files/custom-page/attachment/2020-03/EUPL-1.2%20EN.txt
#
# Unless required by applicable law or agreed to in
# writing, software distributed under the License is
# distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
#
# See the License for the specific language governing
# permissions and limitations under the License.
#

'''
Created on 24-ago-2020

@author: Valentino Pinna
'''

import os, sys, re, jpype
import jpype.imports
from ipykernel.kernelbase import Kernel
from pandas import DataFrame
from jpype import JPackage
from jpype.types import *

def startJVM():
    jars = os.path.join(os.path.dirname(os.path.realpath(__file__)), "libs")
    classpath = f'{str.join(os.pathsep, [os.path.join(jars, entry) for entry in os.listdir(jars)])}'
    if not jpype.isJVMStarted():
        jpype.startJVM(classpath = classpath)
    else:
        jpype.addClassPath(classpath)


def convertDataSet(dataset):
    from java.util.stream import Collectors
    stream = dataset.stream()
    data = [ { str(entry.getKey().toString()): parseValue(entry.getValue().get()) for entry in dp.entrySet() } for dp in stream.collect(Collectors.toList()) ]
    stream.close()
    it = JPackage('it')
    from it.bancaditalia.oss.vtl.model.data.ComponentRole import Identifier, Measure, Attribute
    identifiers = [ str(c.toString()) for c in dataset.getComponents(Identifier)]
    measures = [ str(c.toString()) for c in dataset.getComponents(Measure)]
    attributes = [ str(c.toString()) for c in dataset.getComponents(Attribute)]
    return DataFrame(data = data)[identifiers + measures + attributes]                       \
            .style.set_properties(subset = identifiers, **{ 'background-color': '#FFE0E0' }) \
            .set_properties(subset = measures, **{ 'background-color': '#FFFFC0' })          \
            .render()


def convertScalar(alias, value):
    return DataFrame(data = [ parseValue(value.get()) ], index = alias, columns = ['scalar(' + str(value.getDomain().toString()) + ')']).to_html()


def parseValue(value):
    import jpype.imports
    from java.lang import String, Long, Double, Boolean
    it = JPackage('it')
    if value == None:
        return None
    elif isinstance(value, String):
        return str(value)
    elif isinstance(value, Long):
        return value.longValue()
    elif isinstance(value, Double):
        return value.doubleValue()
    elif isinstance(value, Boolean):
        return value.booleanValue()
    else:
        return str(value.toString())


def mapDomain(domain):
    import jpype.imports
    it = JPackage('it')
    from it.bancaditalia.oss.vtl.impl.types.domain import Domains
    if Domains.INTEGER.isAssignableFrom(domain):
        return 'int'
    elif Domains.NUMBER.isAssignableFrom(domain):
        return 'float'
    elif Domains.BOOLEAN.isAssignableFrom(domain):
        return 'bool'
    else:
        return 'string'


class VTLKernel(Kernel):
    implementation = 'VTL Engine'
    implementation_version = '${python.package.version}'
    language = 'VTL'
    language_version = '2.1'
    language_info = {
        'name': 'VTL',
        'mimetype': 'text/x-vtl',
        'file_extension': '.vtl',
    }
    banner = "${project.description}"
    queryPattern = re.compile(r"^\s*\?\s*(\S+)\s*$")


    def __init__(self, *args, **kwargs):
        super(VTLKernel, self).__init__(*args, **kwargs)
        startJVM()
        from java.lang import System
        from java.io import ByteArrayOutputStream
        from java.io import PrintStream
        self.baos = ByteArrayOutputStream()
        ps = PrintStream(self.baos)
        System.setOut(ps)
        System.setErr(ps)
        it = JPackage('it')
        self.VTLSession = it.bancaditalia.oss.vtl.config.ConfigurationManager.getDefault().createSession()

   
    def sendLog(self):
        self.send_response(self.iopub_socket, 'stream', {
            'name':'Log', 
            'text':str(self.baos.toString())
        })
        self.baos.reset()
            

    def sendData(self, alias, value):
        if isinstance(value, JPackage('it').bancaditalia.oss.vtl.model.data.DataSet):
            html = convertDataSet(value)
        else:
            html = convertScalar(alias, value)
        
        self.send_response(self.iopub_socket, 'display_data', {
            'data': { 
                'text/html': html 
            }, 
            'metadata': {},
            'transient': {}
        })
    
    
    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        from java.lang import Exception
        matcher = self.queryPattern.fullmatch(code)
        try:
            user_expressions = {}
            if matcher is not None:
                name = matcher.group(1)
                self.sendData(name, self.VTLSession.resolve(name))
                self.sendLog()
            else:
                statements = self.VTLSession.addStatements(code).getWorkspace().getRules()
                self.sendLog()
                self.VTLSession.compile()
                self.sendLog()
                for statement in statements:
                    name = statement.getId()
                    self.sendData(name, self.VTLSession.resolve(name))
                self.sendLog()
                user_expressions = { str(statement.getId()): str(statement.getExpression().toString()) for statement in statements }

            response = {
                'status': 'ok',
                'execution_count': self.execution_count,
                'user_expressions': user_expressions
            }
        except Exception as ex:
            ex.printStackTrace()
            self.sendLog()

            traceback = [str(tr.toString()) for tr in ex.getStackTrace()]
            while ex.getCause() != None:
                ex = ex.getCause()
                traceback.append("Caused by " + str(ex.getClass().getName()) + ": " + str(ex.getMessage()))
                traceback.append([str(tr.toString()) for tr in ex.getStackTrace()])

            response = {
               'status' : 'error',
               'ename' : str(ex.getClass().getName()),
               'evalue' : str(ex.getMessage()),
               'traceback' : traceback 
            }
                
        return response
    
    def do_shutdown(self, restart):
        if restart:
            if not jpype.isJVMStarted():
                startJVM()
            self.VTLSession = JPackage('it').bancaditalia.oss.vtl.config.ConfigurationManager.getDefault().createSession()
        else:
            jpype.shutdownJVM()

if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=VTLKernel)
    
