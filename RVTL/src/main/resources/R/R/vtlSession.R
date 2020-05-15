#####################################################################################
#
# Copyright 2020, Bank Of Italy
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
###############################################################################

# S4 Class for managing a VTLSession instance with editor code
#
###############################################################################
VTLSession <- 
  setRefClass("VTLSession", 
              fields = list(name = "character", text = "character", instance = "jobjRef"),
              methods = list(
                initialize = function (name = character(0), code = "") {
                                if (!is.character(name) || length(name) != 1 || nchar(name) == 0)
                                  stop("name must be a non-empty character vector with 1 element")
                                if (!is.character(code) || length(code) != 1)
                                  stop("name must be a character vector with 1 element")
                                .self[['name']] = name
                                .self[['text']] = code
                                .self[['instance']] = J("it.bancaditalia.oss.vtl.impl.session.VTLSessionHandler")$getSession(name)
                                if (nchar(code) > 0)
                                  addStatements(code)
                              }, 
                show = function() { print(name) },
                setText = function(code) { text <<- code },
                addStatements = function(statements) { instance$addStatements(statements) },
                compile = function () { instance$compile() },
                resolve = function(node) { instance$resolve(node) },
                getStatements = function () { instance$getStatements() },
                getNodes = function () { 
                              jnodes = instance$getNodes()
                              return(convertToR(jnodes))
                            },
                getTopology = function(distance =100, charge = -100) {
                    jedges = J('it.bancaditalia.oss.vtl.impl.session.VTLSessionHandler')$getTopology(name)
                    edges = .jcall(jedges, "[Ljava/lang/Object;","toArray")
                    inNodes = sapply(edges[[1]], .jstrVal)
                    outNodes = sapply(edges[[2]], .jstrVal)
                    allNodes = unique(c(inNodes, outNodes))
                    
                    statements = sapply(instance$getStatements()$entrySet(), 
                                        function (x) setNames(list(x$getValue()), x$getKey()))
                    primitiveNodes = allNodes[which(!allNodes %in% names(statements))]
                    primitives = rep('PRIMITIVE NODE', times=length(primitiveNodes))
                    names(primitives) = primitiveNodes
                    statements = append(statements, primitives)
                    
                    return(tryCatch({
                      net = igraph_to_networkD3(make_graph(c(rbind(outNodes, inNodes))))
                      net$links$value=rep(3, length(inNodes))
                      net$nodes$statement=as.character(statements[as.character(net$nodes$name)])
                      return(forceNetwork(Links = net$links, 
                                   Nodes = net$nodes, 
                                   Source = 'source',
                                   Target = 'target',
                                   NodeID = 'name',
                                   Group = 'statement',
                                   Value = 'value',
                                   linkDistance = distance,
                                   charge = charge,
                                   fontSize = 20,
                                   opacity = 1,
                                   zoom =T,
                                   arrows = T,
                                   opacityNoHover = 1,
                                   clickAction = 'alert(d.group);',
                                   bounded = T
                      ))
                    }, error = function(e) {
                      return(NULL)
                    }))
                  }
              ))
