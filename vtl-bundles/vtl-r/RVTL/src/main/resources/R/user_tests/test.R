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
  expect_true(object = vtlAddStatements(sessionID = test_name, statements = vtlCode), label = paste(test_name, 'syntax'))
  expect_true(object = vtlCompile(sessionID = test_name), label = paste(test_name, 'compilation'))
  return(vtlEvalNodes(sessionID = test_name, nodes = 'test_result')[[1]])
}

# test all vtl scripts
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

# run scalar tests row by row
scalar_src = file('vtl_scripts_scalars/tests.src', "r")
tests = readLines(scalar_src)
close(scalar_src)
n=0
for(x in tests){
  n=n+1
  session = paste('scalar', n)
  test_that(session,
  {
    print(session)
    expect_true(object = vtlAddStatements(sessionID = session, statements = x), label = paste(session, 'syntax'))
    expect_true(object = vtlCompile(sessionID = session), label = paste(session, 'compilation'))
    result = vtlEvalNodes(sessionID = session, nodes = 'test_result')[[1]]
    expect_true(object = result$Scalar, label = session)
    print('Done')
  })
}
