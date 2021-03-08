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

library(dplyr)
library(RVTL)
library(RJSDMX)
library(testthat)
context("Arithmetic")

# SCALARS
test_that('scalar assignment works',
  {
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                    statements = 'tmp := 1.23;                                                                         
                                                  result:= tmp;', 
                                    restartSession = T), label = 'Assignment syntax')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Assignment compile')
  expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'tmp')$tmp[1,1], expected = 1.23, label = 'Assignment value correct')
  expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result[1,1], expected = 1.23, label = 'Assignment value correct')
})

test_that('scalar addition works',
  {
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                    statements = 'tmp1 := 1.23;                                                                         
                                                  tmp2 := 1.23;
                                                  result := tmp1 + tmp2;', 
                                    restartSession = T), label = 'Plus failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Plus compile failed')
  expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result[1,1], expected = 2.46, label = 'Plus value correct')
})

test_that('scalar subtraction works', {
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                    statements = 'tmp1 := 1.23;                                                                         
                                                  tmp2 := 1.23;
                                                  result := tmp1 - tmp2;', 
                                    restartSession = T), label = 'Minus failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Minus compile failed')
  expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result[1,1], expected = 0.0, label = 'Plus value correct')
})

test_that('scalar division works', {
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                    statements = 'tmp1 := 1.23;                                                                         
                                                  tmp2 := 1.23;
                                                  result := tmp1 / tmp2;', 
                                    restartSession = T), label = 'Division failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Division compile failed')
  expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result[1,1], expected = 1.0, label = 'Division value correct')
})

test_that('scalar multiplication works', {
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                    statements = 'tmp1 := 1.23;                                                                         
                                                  tmp2 := 1.23;
                                                  result := tmp1 * tmp2;', 
                                    restartSession = T), label = 'multiplication failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'multiplication compile failed')
  expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result[1,1], expected = 1.5129, label = 'multiplication value correct')
})

###
# R Environment
###

test_that('dataset assignment works', {
  r_input <<- data.frame(id1 = c('a', 'b', 'c'), m1 = c(1.23, -2.34, 3.45), m2 = c(1.23, -2.34, 3.45), stringsAsFactors = F)
  attr(r_input, 'identifiers') <<- c('id1')
  attr(r_input, 'measures') <<- c('m1', 'm2')
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                    statements = 'tmp := r_input;                                                                         
                                    result:= tmp;', 
                                    restartSession = T), label = 'Assignment failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Assignment compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'tmp')$tmp, target = r_input), label = 'Assignment value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result')$result, target = r_input), label = 'Assignment value correct')
})

test_that('dataset addition works', {
  r_input <<- data.frame(id1 = c('a', 'b', 'c'), m1 = c(1.23, -2.34, 3.45), m2 = c(1.23, -2.34, 3.45), stringsAsFactors = F)
  attr(r_input, 'identifiers') <<- c('id1')
  attr(r_input, 'measures') <<- c('m1', 'm2')
  result = r_input
  result$m1=result$m1 + result$m1
  result$m2=result$m2 + result$m2
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                    statements = 'tmp1 := r_input;                                                                         
                                    tmp2 := r_input;
                                    result := tmp1 + tmp2;', 
                                    restartSession = T), label = 'Plus failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Plus compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result, target = result), label = 'Plus value correct')
})

test_that('dataset subtraction works', {
  r_input <<- data.frame(id1 = c('a', 'b', 'c'), m1 = c(1.23, -2.34, 3.45), m2 = c(1.23, -2.34, 3.45), stringsAsFactors = F)
  attr(r_input, 'identifiers') <<- c('id1')
  attr(r_input, 'measures') <<- c('m1', 'm2')
  result = r_input
  result$m1=result$m1 - result$m1
  result$m2=result$m2 - result$m2
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                    statements = 'tmp1 := r_input;                                                                         
                                    tmp2 := r_input;
                                    result := tmp1 - tmp2;', 
                                    restartSession = T), label = 'Minus failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Minus compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result, target = result), label = 'Minus value correct')
})

test_that('dataset division works', {
  r_input <<- data.frame(id1 = c('a', 'b', 'c'), m1 = c(1.23, -2.34, 3.45), m2 = c(1.23, -2.34, 3.45), stringsAsFactors = F)
  attr(r_input, 'identifiers') <<- c('id1')
  attr(r_input, 'measures') <<- c('m1', 'm2')
  result = r_input
  result$m1=result$m1 / result$m1
  result$m2=result$m2 / result$m2
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                    statements = 'tmp1 := r_input;                                                                         
                                                  tmp2 := r_input;
                                                  result := tmp1 / tmp2;', 
                                    restartSession = T), label = 'Division failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Division compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result, target = result), 
                                                          label = 'Division value correct')
})

test_that('dataset multiplication works', {
  r_input <<- data.frame(id1 = c('a', 'b', 'c'), m1 = c(1.23, -2.34, 3.45), m2 = c(1.23, -2.34, 3.45), stringsAsFactors = F)
  attr(r_input, 'identifiers') <<- c('id1')
  attr(r_input, 'measures') <<- c('m1', 'm2')
  result = r_input
  result$m1=result$m1 * result$m1
  result$m2=result$m2 * result$m2
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                    statements = 'tmp1 := r_input;                                                                         
                                                  tmp2 := r_input;
                                                  result := tmp1 * tmp2;', 
                                    restartSession = T), label = 'multiplication failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'multiplication compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result, target = result), 
                                                          label = 'multiplication value correct')
})


###
# SDMX Environment
###

test_that('SDMX dataset assignment works', {
  result = getTimeSeriesTable('ECB', 'EXR.A.USD.EUR.SP00.A')
  result = within(result, rm(action, validFromDate, CONNECTORS_AUTONAME, ID))
  result$PUBL_PUBLIC = rep(NA, nrow(result))
  result$COVERAGE = rep(NA, nrow(result))
  result$SOURCE_PUB = rep(NA, nrow(result))
  result$NAT_TITLE = rep(NA, nrow(result))
  result$BREAKS = rep(NA, nrow(result))
  result$COMPILING_ORG = rep(NA, nrow(result))
  result$OBS_PRE_BREAK = rep(NA, nrow(result))
  result$PUBL_MU = rep(NA, nrow(result))
  result$DOM_SER_IDS = rep(NA, nrow(result))
  result$COMPILATION = rep(NA, nrow(result))
  result$OBS_COM = rep(NA, nrow(result))
  result$OBS_CONF = rep(NA, nrow(result))
  result$UNIT_INDEX_BASE = rep(NA, nrow(result))
  result$DISS_ORG = rep(NA, nrow(result))
  result$PUBL_ECB = rep(NA, nrow(result))
  attr(result, 'identifiers') = c('TIME_PERIOD')
  attr(result, 'measures') = c('OBS_VALUE')

  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = "tmp := 'ECB:EXR/A.USD.EUR.SP00.A';                                                                         
                                                      result:= tmp;", 
                                        restartSession = T), label = 'Assignment failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Assignment compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'tmp')$tmp, target = result), label = 'Assignment value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result, target = result), label = 'Assignment value correct')
})

test_that('SDMX dataset addition works', {
  result = getTimeSeriesTable('ECB', 'EXR.A.USD.EUR.SP00.A')
  result = within(result, rm(action, validFromDate, CONNECTORS_AUTONAME, ID, EXR_TYPE, 
                               COLLECTION, SOURCE_AGENCY, UNIT_MULT, TITLE_COMPL, FREQ, 
                               OBS_STATUS, CURRENCY_DENOM, EXR_SUFFIX, UNIT, DECIMALS, 
                               TIME_FORMAT, TITLE, CURRENCY))
  attr(result, 'identifiers') = c('TIME_PERIOD')
  attr(result, 'measures') = c('OBS_VALUE')
  result$OBS_VALUE = result$OBS_VALUE + result$OBS_VALUE

  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = "tmp1 := 'ECB:EXR/A.USD.EUR.SP00.A';                                                                         
                                        tmp2 := 'ECB:EXR/A.USD.EUR.SP00.A';
                                        result := (tmp1 + tmp2)[rename number_var to OBS_VALUE];
                                        ", 
                                        restartSession = T), label = 'Plus failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Plus compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result, target = result), label = 'Plus value correct')
})

test_that('SDMX dataset subtraction works', {
  result = getTimeSeriesTable('ECB', 'EXR.A.USD.EUR.SP00.A')
  result = within(result, rm(action, validFromDate, CONNECTORS_AUTONAME, ID, EXR_TYPE, 
                               COLLECTION, SOURCE_AGENCY, UNIT_MULT, TITLE_COMPL, FREQ, 
                               OBS_STATUS, CURRENCY_DENOM, EXR_SUFFIX, UNIT, DECIMALS, 
                               TIME_FORMAT, TITLE, CURRENCY))
  attr(result, 'identifiers') = c('TIME_PERIOD')
  attr(result, 'measures') = c('OBS_VALUE')
  result$OBS_VALUE = result$OBS_VALUE - result$OBS_VALUE

  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = "tmp1 := 'ECB:EXR/A.USD.EUR.SP00.A';                                                                         
                                        tmp2 := 'ECB:EXR/A.USD.EUR.SP00.A';
                                        result := (tmp1 - tmp2)[rename number_var to OBS_VALUE];", 
                                        restartSession = T), label = 'Minus failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Minus compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result, target = result), label = 'Minus value correct')
})

test_that('SDMX dataset division works', {
  result = getTimeSeriesTable('ECB', 'EXR.A.USD.EUR.SP00.A')
  result = within(result, rm(action, validFromDate, CONNECTORS_AUTONAME, ID, EXR_TYPE, 
                               COLLECTION, SOURCE_AGENCY, UNIT_MULT, TITLE_COMPL, FREQ, 
                               OBS_STATUS, CURRENCY_DENOM, EXR_SUFFIX, UNIT, DECIMALS, 
                               TIME_FORMAT, TITLE, CURRENCY))
  attr(result, 'identifiers') = c('TIME_PERIOD')
  attr(result, 'measures') = c('OBS_VALUE')
  result$OBS_VALUE = result$OBS_VALUE / result$OBS_VALUE

  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = "tmp1 := 'ECB:EXR/A.USD.EUR.SP00.A';                                                                         
                                        tmp2 := 'ECB:EXR/A.USD.EUR.SP00.A';
                                        result := (tmp1 / tmp2)[rename number_var to OBS_VALUE];", 
                                    restartSession = T), label = 'Division failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Division compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result, target = result), 
                                                          label = 'Division value correct')
})

test_that('SDMX dataset multiplication works', {
  result = getTimeSeriesTable('ECB', 'EXR.A.USD.EUR.SP00.A')
  result = within(result, rm(action, validFromDate, CONNECTORS_AUTONAME, ID, EXR_TYPE, 
                               COLLECTION, SOURCE_AGENCY, UNIT_MULT, TITLE_COMPL, FREQ, 
                               OBS_STATUS, CURRENCY_DENOM, EXR_SUFFIX, UNIT, DECIMALS, 
                               TIME_FORMAT, TITLE, CURRENCY))
  attr(result, 'identifiers') = c('TIME_PERIOD')
  attr(result, 'measures') = c('OBS_VALUE')
  result$OBS_VALUE = result$OBS_VALUE * result$OBS_VALUE

  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                    statements = "tmp1 := 'ECB:EXR/A.USD.EUR.SP00.A';                                                                         
                                    tmp2 := 'ECB:EXR/A.USD.EUR.SP00.A';
                                    result := (tmp1 * tmp2)[rename number_var to OBS_VALUE];", 
                                    restartSession = T), label = 'multiplication failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'multiplication compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result, target = result), 
                                                          label = 'multiplication value correct')
})

###
# CSV Environment
###

test_that('CSV dataset assignment works', {
  result = read.csv(paste0(find.package(package = 'RVTL'), '/vtlStudio2/test_data/ecbexrusd_plain.csv'), stringsAsFactors = F, colClasses='character')
  result$OBS_VALUE = as.numeric(result$OBS_VALUE)
  attr(result, 'identifiers') = c("CURRENCY", "CURRENCY_DENOM", "EXR_SUFFIX",
                                    "EXR_TYPE", "FREQ", "TIME_PERIOD" )
  attr(result, 'measures') = c('OBS_VALUE')
  
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = paste0("tmp := 'csv:", 
                                        find.package(package = 'RVTL'), 
                                            "/vtlStudio2/test_data/ecbexrusd_vtl.csv" , "';                                                                        
                                        result:= tmp;"), 
                                        restartSession = T), label = 'Assignment failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Assignment compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'tmp')$tmp, target = result), label = 'Assignment value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result')$result, target = result), label = 'Assignment value correct')
})

test_that('CSV dataset addition works', {
  result = read.csv(paste0(find.package(package = 'RVTL'), '/vtlStudio2/test_data/ecbexrusd_plain.csv'), stringsAsFactors = F, colClasses='character')
  result = within(result, rm( ID, 
                             COLLECTION, SOURCE_AGENCY, UNIT_MULT,   
                             OBS_STATUS, UNIT, DECIMALS, 
                             TIME_FORMAT, TITLE))
  result$OBS_VALUE = as.numeric(result$OBS_VALUE)
  result$OBS_VALUE = result$OBS_VALUE + result$OBS_VALUE
  attr(result, 'identifiers') = c("CURRENCY", "CURRENCY_DENOM", "EXR_SUFFIX",
                                    "EXR_TYPE", "FREQ", "TIME_PERIOD" )
  attr(result, 'measures') = c('OBS_VALUE')
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = paste0("tmp1 := 'csv:", 
                                                            find.package(package = 'RVTL'), 
                                                            "/vtlStudio2/test_data/ecbexrusd_vtl.csv" , "';                                                                        
                                        tmp2 := tmp1;
                                        result := (tmp1 + tmp2)[rename number_var to OBS_VALUE];"), 
                                        restartSession = T), label = 'Plus failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Plus compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result, target = result), label = 'Plus value correct')
})

test_that('CSV dataset subtraction works', {
  result = read.csv(paste0(find.package(package = 'RVTL'), '/vtlStudio2/test_data/ecbexrusd_plain.csv'), stringsAsFactors = F, colClasses='character')
  result = within(result, rm( ID, 
                             COLLECTION, SOURCE_AGENCY, UNIT_MULT,   
                             OBS_STATUS, UNIT, DECIMALS, 
                             TIME_FORMAT, TITLE))
  
  result$OBS_VALUE = as.numeric(result$OBS_VALUE)
  result$OBS_VALUE = result$OBS_VALUE - result$OBS_VALUE
  attr(result, 'identifiers') = c("CURRENCY", "CURRENCY_DENOM", "EXR_SUFFIX",
                                  "EXR_TYPE", "FREQ", "TIME_PERIOD" )
  attr(result, 'measures') = c('OBS_VALUE')
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = paste0("tmp1 := 'csv:", 
                                                            find.package(package = 'RVTL'), 
                                                            "/vtlStudio2/test_data/ecbexrusd_vtl.csv" , "';                                                                        
                                        tmp2 := tmp1;
                                        result := (tmp1 - tmp2)[rename number_var to OBS_VALUE];"), 
                                        restartSession = T), label = 'Minus failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Minus compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result, target = result), label = 'Minus value correct')
})

test_that('CSV dataset division works', {
  result = read.csv(paste0(find.package(package = 'RVTL'), '/vtlStudio2/test_data/ecbexrusd_plain.csv'), stringsAsFactors = F, colClasses='character')
  result = within(result, rm(   ID, 
                             COLLECTION, SOURCE_AGENCY, UNIT_MULT,   
                             OBS_STATUS, UNIT, DECIMALS, 
                             TIME_FORMAT, TITLE))
  
  result$OBS_VALUE = as.numeric(result$OBS_VALUE)
  result$OBS_VALUE = result$OBS_VALUE / result$OBS_VALUE
  attr(result, 'identifiers') = c("CURRENCY", "CURRENCY_DENOM", "EXR_SUFFIX",
                                  "EXR_TYPE", "FREQ", "TIME_PERIOD" )
  attr(result, 'measures') = c('OBS_VALUE')
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = paste0("tmp1 := 'csv:", 
                                                            find.package(package = 'RVTL'), 
                                                            "/vtlStudio2/test_data/ecbexrusd_vtl.csv" , "';                                                                        
                                        tmp2 := tmp1;
                                        result := (tmp1 / tmp2)[rename number_var to OBS_VALUE];"), 
                                        restartSession = T), label = 'Division failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Division compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result, target = result), 
              label = 'Division value correct')
})

test_that('CSV dataset multiplication works', {
  result = read.csv(paste0(find.package(package = 'RVTL'), '/vtlStudio2/test_data/ecbexrusd_plain.csv'), stringsAsFactors = F, colClasses='character')
  result = within(result, rm(   ID, 
                             COLLECTION, SOURCE_AGENCY, UNIT_MULT,   
                             OBS_STATUS, UNIT, DECIMALS, 
                             TIME_FORMAT, TITLE))
  
  result$OBS_VALUE = as.numeric(result$OBS_VALUE)
  result$OBS_VALUE = result$OBS_VALUE * result$OBS_VALUE
  attr(result, 'identifiers') = c("CURRENCY", "CURRENCY_DENOM", "EXR_SUFFIX",
                                  "EXR_TYPE", "FREQ", "TIME_PERIOD" )
  attr(result, 'measures') = c('OBS_VALUE')
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = paste0("tmp1 := 'csv:", 
                                                            find.package(package = 'RVTL'), 
                                                            "/vtlStudio2/test_data/ecbexrusd_vtl.csv" , "';                                                                        
                                        tmp2 := tmp1;
                                        result := (tmp1 * tmp2)[rename number_var to OBS_VALUE];"), 
                                        restartSession = T), label = 'multiplication failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'multiplication compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result, target = result), 
              label = 'multiplication value correct')
})

###
#  Dataset and scalar arithmetic
###
test_that('Dataset and scalar addition works', {
  r_input <<- data.frame(id1 = c('a', 'b', 'c'), m1 = c(1.23, -2.34, 3.45), m2 = c(1.23, -2.34, 3.45), stringsAsFactors = F)
  attr(r_input, 'identifiers') <<- c('id1')
  attr(r_input, 'measures') <<- c('m1', 'm2')
  result = r_input
  result$m1=result$m1 + 1.23
  result$m2=result$m2 + 1.23
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = 'tmp1 := r_input;                                                                         
                                          result := tmp1 + 1.23;', 
                                        restartSession = T), label = 'Plus failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Plus compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result, target = result), label = 'Plus value correct')
})

test_that('Dataset and scalar subtraction works', {
  r_input <<- data.frame(id1 = c('a', 'b', 'c'), m1 = c(1.23, -2.34, 3.45), m2 = c(1.23, -2.34, 3.45), stringsAsFactors = F)
  attr(r_input, 'identifiers') <<- c('id1')
  attr(r_input, 'measures') <<- c('m1', 'm2')
  result = r_input
  result$m1=result$m1 - 1.23
  result$m2=result$m2 - 1.23
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = 'tmp1 := r_input;
                                          result := tmp1 - 1.23;', 
                                        restartSession = T), label = 'Minus failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Minus compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result, target = result), label = 'Minus value correct')
})

test_that('Dataset and scalar division works', {
  r_input <<- data.frame(id1 = c('a', 'b', 'c'), m1 = c(1.23, -2.34, 3.45), m2 = c(1.23, -2.34, 3.45), stringsAsFactors = F)
  attr(r_input, 'identifiers') <<- c('id1')
  attr(r_input, 'measures') <<- c('m1', 'm2')
  result = r_input
  result$m1=result$m1 / 1.23
  result$m2=result$m2 / 1.23
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = 'tmp1 := r_input;
                                                  result := tmp1 / 1.23;', 
                                        restartSession = T), label = 'Division failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Division compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result, target = result), 
              label = 'Division value correct')
})

test_that('Dataset and scalar multiplication works', {
  r_input <<- data.frame(id1 = c('a', 'b', 'c'), m1 = c(1.23, -2.34, 3.45), m2 = c(1.23, -2.34, 3.45), stringsAsFactors = F)
  attr(r_input, 'identifiers') <<- c('id1')
  attr(r_input, 'measures') <<- c('m1', 'm2')
  result = r_input
  result$m1=result$m1 * 1.23
  result$m2=result$m2 * 1.23
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = 'tmp1 := r_input;
                                                  result := tmp1 * 1.23;', 
                                        restartSession = T), label = 'multiplication failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'multiplication compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'result')$result, target = result), 
              label = 'multiplication value correct')
})

######
# Other operators will be tested only on R input
######
test_that('Other operators work', {
  r_input <<- data.frame(id1 = c('a', 'b', 'c'), m1 = c(1.23, -2.34, 3.45), m2 = c(1.23, -2.34, 3.45), stringsAsFactors = F)
  attr(r_input, 'identifiers') <<- c('id1')
  attr(r_input, 'measures') <<- c('m1', 'm2')
  r_input2 <<- data.frame(id1 = c('a', 'b', 'c'), m1 = c(2, 2, 2), m2 = c(2, 2, 2), stringsAsFactors = F)
  attr(r_input2, 'identifiers') <<- c('id1')
  attr(r_input2, 'measures') <<- c('m1', 'm2')
  resultUplus = r_input
  resultUminus = r_input
  resultUminus$m1=-r_input$m1
  resultUminus$m2=-r_input$m2
  resultMod = r_input
  resultMod$m1 = r_input$m1 %% 2
  resultMod$m2 = r_input$m2 %% 2
  # workaround for misalignment
  resultMod$m1[2]= resultMod$m1[2] -2
  resultMod$m2[2]= resultMod$m2[2] -2
  resultRound = r_input
  resultRound$m1 = round(r_input$m1, digits = 1)
  resultRound$m2 = round(r_input$m2, digits = 1)
  resultCeil = r_input
  resultCeil$m1 = ceiling(resultCeil$m1)
  resultCeil$m2 = ceiling(resultCeil$m2)
  resultFloor = r_input
  resultFloor$m1 = floor(resultFloor$m1)
  resultFloor$m2 = floor(resultFloor$m2)
  resultAbs = r_input
  resultAbs$m1 = abs(resultAbs$m1)
  resultAbs$m2 = abs(resultAbs$m2)
  resultExp = r_input
  resultExp$m1 = exp(resultExp$m1)
  resultExp$m2 = exp(resultExp$m2)
  resultLn = r_input
  resultLn$m1 = log(resultLn$m1)
  resultLn$m2 = log(resultLn$m2)
  resultPower = r_input
  resultPower$m1 = resultPower$m1 ^ 2
  resultPower$m2 = resultPower$m2 ^ 2
  resultSqrt = r_input
  resultSqrt$m1 = sqrt(resultSqrt$m1)
  resultSqrt$m2 = sqrt(resultSqrt$m2)
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = 'tmp1 := r_input;
                                                  tmp2 := r_input2;
                                                  resultUplus := +tmp1;
                                                  resultUminus := -tmp1;
                                                  resultMod := mod(tmp1, tmp2);
                                                  resultRound := round(tmp1, 1);
                                                  resultCeil := ceil(tmp1);
                                                  resultFloor := floor(tmp1);
                                                  resultAbs := abs(tmp1);
                                                  resultExp := exp(tmp1);
                                                  resultLn := ln(tmp1);
                                                  resultPower := power(tmp1, 2);
                                                  resultSqrt := sqrt(tmp1);', 
                                        restartSession = T), label = 'syntax failed')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'compile failed')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultUplus')$resultUplus, target = resultUplus), 
              label = 'unary plus value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultUminus')$resultUminus, target = resultUminus), 
              label = 'unary minus value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultMod')$resultMod, target = resultMod), 
              label = 'module value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultRound')$resultRound, target = resultRound),
             label = 'round value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultCeil')$resultCeil, target = resultCeil), 
              label = 'Ceiling value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultFloor')$resultFloor, target = resultFloor), 
              label = 'Floor value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultAbs')$resultAbs, target = resultAbs), 
              label = 'Abs value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultExp')$resultExp, target = resultExp), 
              label = 'Exp value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultLn')$resultLn, target = resultLn), 
              label = 'Ln value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultPower')$resultPower, target = resultPower), 
              label = 'Power value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultSqrt')$resultSqrt, target = resultSqrt), 
              label = 'Sqrt value correct')
  
})
