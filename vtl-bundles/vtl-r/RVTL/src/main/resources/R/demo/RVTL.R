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

# Main class for consuming SDMX web services
#
# Author: Attilio Mattiocco
###############################################################################

vtlAddStatements(session = 'test', restartSession = T,
                 
                  statements = 'a := 2;
                                b := 3;
                                c := abs(sqrt(14 + a));
                                d := a + b + c;
                                e := c + d / a;
                                f := e - d;
                                g := -f;
                                test := a * b + c / a;')

vtlAddStatements(session = 'test2', restartSession = T,
                 
                 statements = 'a := 2;
                                b := 3;
                                c := abs(sqrt(14 + a));
                                d := a + b + c;')

vtlEvalNode(session = 'test', node = 'test')

vtlListStatements(session = 'test')

vtlTopology(session = 'test')

vtlListSessions()