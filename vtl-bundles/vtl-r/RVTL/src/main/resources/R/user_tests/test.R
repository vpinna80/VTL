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

# Configure the CSVPathEnvironment: if we are in interactive mode, rely on the wd(). 
# Otherwise the environment variable has to be set in the calling shell
if(interactive()) Sys.setenv(VTL_PATH=paste0(getwd(),'/input_data',';',getwd(),'/output_data'))

# force read sdmx configuration
RJSDMX::getFlows('ECB')

# Configure the SDMX Metadata Repo
vtlProperties <- J("it.bancaditalia.oss.vtl.config.VTLGeneralProperties")
vtlProperties$METADATA_REPOSITORY$setValue('it.bancaditalia.oss.vtl.impl.domains.SDMXMetadataRepository')

configManager <- J("it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory")
supportedProperties <- configManager$getSupportedProperties(J('it.bancaditalia.oss.vtl.impl.domains.SDMXMetadataRepository')@jobj)
supportedProperties <- .jcast(supportedProperties, 'java/util/Collection')
for (property in as.list(supportedProperties)){
  if(property$getName() == 'vtl.metadata.sdmx.provider.endpoint'){
    property$setValue('https://sdw-wsrest.ecb.europa.eu/service')
  } 
}

#Configure SDMX Env regarding identifiers policy
supportedProperties <- configManager$getSupportedProperties(J('it.bancaditalia.oss.vtl.impl.environment.SDMXEnvironment')@jobj)
supportedProperties <- .jcast(supportedProperties, 'java/util/Collection')
for (property in as.list(supportedProperties)){
  if(property$getName() == 'vtl.sdmx.keep.identifiers'){
    property$setValue('true')
  } 
}

#prepare r env test
r_env_input_1 = data.frame(id1=c('1999', '2000', '2001', '2002', '2003', '2004'),
                           id2=c(1.06, 0.92, 0.89, 0.94, 1.13, 1.24),
                           m1=c('A1', 'A2', 'A3', NA, 'A5', 'A6'),
                           m2=c(1.2, 2.3, 3.4, 4.5, NA, 6.7),
                           m3=c(T,F,T,F,T,NA),
                           attr1=c('att1', 'att2','att3',NA,'att5','att6'),
                           attr2=c(T,F,T,F,NA,F),
                           attr3=c(1:5,NA))
attr(r_env_input_1, 'measures')=c('m1','m2','m3')
attr(r_env_input_1, 'identifiers')=c('id1','id2')
                           
runTest=function(test_name){
  vtlCode = readChar(x, file.info(x)$size)
  expect_true(object = vtlAddStatements(sessionID = test_name, statements = vtlCode), label = paste(test_name, 'syntax'))
  expect_true(object = vtlCompile(sessionID = test_name), label = paste(test_name, 'compilation'))
  return(vtlEvalNodes(sessionID = test_name, nodes = 'test_result')[[1]])
}

# test all vtl scripts
for(x in dir(path = 'vtl_scripts', pattern = '*.vtl', full.names = T, recursive = T)){
  print(paste0('Run test:', x))
  test_that(x,
  {
    result = runTest(x)
    for(m in attr(result, 'measures')){
      expect_true(object = all(result[,m], na.rm = T), label = paste(x, m))
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

