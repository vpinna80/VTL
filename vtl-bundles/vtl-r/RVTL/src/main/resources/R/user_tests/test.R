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

library(RVTL)
library(testthat)

runTest=function(test_name){
  vtlCode = readChar(x, file.info(x)$size)
  vtlAddStatements(sessionID = test_name, statements = vtlCode)
  vtlCompile(sessionID = test_name)
  return(vtlEvalNodes(sessionID = test_name, nodes = 'test_result')[[1]])
}

for(x in dir(path = 'vtl_scripts', pattern = '*.vtl', full.names = T)){
  print(paste0('Run test:', x))
  test_that(x,
  {
    result = runTest(x)
    for(m in attr(result, 'measures')){
      expect_true(object = all(result[,m]), label = paste(x, m))
    }
  })
}

