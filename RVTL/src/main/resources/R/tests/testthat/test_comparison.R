/**
 * Copyright © 2020 Banca D'Italia
 *
 * Licensed under the EUPL, Version 1.2 (the "License");
 * You may not use this work except in compliance with the
 * License.
 * You may obtain a copy of the License at:
 *
 * https://joinup.ec.europa.eu/sites/default/files/custom-page/attachment/2020-03/EUPL-1.2%20EN.txt
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the License is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 *
 * See the License for the specific language governing
 * permissions and limitations under the License.
 */
library(dplyr)
library(RVTL)
library(RJSDMX)
library(testthat)
context("Comparison")

# SCALARS
test_that('scalar comparisons work',
  {
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                    statements = 'tmp := 1.23;                                                                         
                                                  equal := tmp = 1.23;
                                                  not_equal := tmp <> 1.23;
                                                  greater := tmp > 1.23;
                                                  greater_eq := tmp >=1.23;  
                                                  less := tmp < 1.23;
                                                  less_eq := tmp <= 1.23;', 
                                    restartSession = T), label = 'scalar comparison syntax')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'scalar comparison compile')
  expect_true(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'equal')$equal[1,1],  
              label = 'equal value correct')
  expect_false(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'not_equal')$not_equal[1,1],  
              label = 'not_equal value correct')
  expect_false(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'greater')$greater[1,1],  
              label = 'greater value correct')
  expect_true(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'greater_eq')$greater_eq[1,1],  
              label = 'greater_eq value correct')
  expect_false(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'less')$less[1,1],  
              label = 'less value correct')
  expect_true(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'less_eq')$less_eq[1,1],  
              label = 'equal value correct')
})


###
# R Environment
###

test_that('dataset comparisons work', {
  r_input <<- data.frame(id1 = c('a', 'b', 'c'), m1 = c(1.23, 2.34, 3.45), stringsAsFactors = F)
  attr(r_input, 'identifiers') <<- c('id1')
  attr(r_input, 'measures') <<- c('m1')
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session',
                                        statements = 'tmp1 := r_input;
                                        tmp2 := r_input;
                                        equal := tmp1 = tmp2;
                                        not_equal := tmp1 <> tmp2;
                                        greater := tmp1 > tmp2;
                                        greater_eq := tmp1 >=tmp2;
                                        less := tmp1 < tmp2;
                                        less_eq := tmp1 <= tmp2;',
                                    restartSession = T), label = 'dataset comparison syntax')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'dataset comparison compile')
  expect_true(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'equal')$equal$bool_var),
              label = 'equal value correct')
  expect_false(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'not_equal')$not_equal$bool_var),
              label = 'not_equal value correct')
  expect_false(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'greater')$greater$bool_var),
              label = 'greater value correct')
  expect_true(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'greater_eq')$greater_eq$bool_var),
              label = 'greater_eq value correct')
  expect_false(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'less')$less$bool_var),
              label = 'less value correct')
  expect_true(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'less_eq')$less_eq$bool_var),
              label = 'equal value correct')
})


###
# SDMX Environment
###

test_that('SDMX dataset comparisons works', {
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = "tmp1 := 'ECB:EXR/A.USD.EUR.SP00.A';   
                                        tmp2 := tmp1;
                                        equal := tmp1 = tmp2;
                                        not_equal := tmp1 <> tmp2;
                                        greater := tmp1 > tmp2;
                                        greater_eq := tmp1 >=tmp2;  
                                        less := tmp1 < tmp2;
                                        less_eq := tmp1 <= tmp2;", 
                                    restartSession = T), label = 'SDMX comparison syntax')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'SDMX comparison compile')
  expect_true(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'equal')$equal$bool_var),  
              label = 'equal value correct')
  expect_false(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'not_equal')$not_equal$bool_var),
              label = 'not_equal value correct')
  expect_false(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'greater')$greater$bool_var),
              label = 'greater value correct')
  expect_true(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'greater_eq')$greater_eq$bool_var),
              label = 'greater_eq value correct')
  expect_false(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'less')$less$bool_var),
              label = 'less value correct')
  expect_true(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'less_eq')$less_eq$bool_var),
              label = 'equal value correct')
})

###
# CSV Environment
###

test_that('CSV dataset comparisons works', {
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = paste0("tmp1 := 'csv:", 
                                                            find.package(package = 'RVTL'), 
                                                            "/vtlStudio2/test_data/ecbexrusd_vtl.csv" , "';    
                                        tmp2 := tmp1;
                                        equal := tmp1 = tmp2;
                                        not_equal := tmp1 <> tmp2;
                                        greater := tmp1 > tmp2;
                                        greater_eq := tmp1 >=tmp2;  
                                        less := tmp1 < tmp2;
                                        less_eq := tmp1 <= tmp2;"), 
                                    restartSession = T), label = 'CSV comparison syntax')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'CSV comparison compile')
  expect_true(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'equal')$equal$bool_var),  
              label = 'equal value correct')
  expect_false(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'not_equal')$not_equal$bool_var),
              label = 'not_equal value correct')
  expect_false(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'greater')$greater$bool_var),
              label = 'greater value correct')
  expect_true(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'greater_eq')$greater_eq$bool_var),
              label = 'greater_eq value correct')
  expect_false(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'less')$less$bool_var),
              label = 'less value correct')
  expect_true(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'less_eq')$less_eq$bool_var),
              label = 'equal value correct')
})

###
# Dataset and scalar
###

test_that('Dataset and scalar comparisons work', {
  r_input <<- data.frame(id1 = c('a', 'b', 'c'), m1 = c(1.23, 1.23, 1.23), stringsAsFactors = F)
  attr(r_input, 'identifiers') <<- c('id1')
  attr(r_input, 'measures') <<- c('m1')
  r_input2 <<- data.frame(id1 = c('a', 'b'), m1 = c(1.23, 2.34), stringsAsFactors = F)
  attr(r_input2, 'identifiers') <<- c('id1')
  attr(r_input2, 'measures') <<- c('m1')
  between_result = r_input2
  between_result$m1=c(T,F)
  exists_result = r_input2
  exists_result$bool_var=c(T,T);
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = 'tmp1 := r_input;   
                                        tmp2 := r_input2;
                                        equal := tmp1 = 1.23;
                                        not_equal := tmp1 <> 1.23;
                                        greater := tmp1 > 1.23;
                                        greater_eq := tmp1 >=1.23;  
                                        less := tmp1 < 1.23;
                                        less_eq := tmp1 <= 1.23;
                                        between_result := between(tmp1, 1, 2);
                                        /*exists_result := exists_in(tmp1, tmp2); */
                                        ', 
                                        restartSession = T), label = 'dataset comparison syntax')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'dataset comparison compile')
  expect_true(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'equal')$equal$bool_var),  
              label = 'equal value correct')
  expect_false(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'not_equal')$not_equal$bool_var),
               label = 'not_equal value correct')
  expect_false(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'greater')$greater$bool_var),
               label = 'greater value correct')
  expect_true(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'greater_eq')$greater_eq$bool_var),
              label = 'greater_eq value correct')
  expect_false(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'less')$less$bool_var),
               label = 'less value correct')
  expect_true(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'less_eq')$less_eq$bool_var),
              label = 'Less equal value correct')
  expect_true(object = all(vtlEvalNodes(sessionID = 'test_session', nodes = 'between_result')$between_result$bool_var),
              label = 'Between value correct')
  # expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', nodes = 'exists_result')$exists_result$bool_var), target = exists_result, 
  #             label = 'Exists_in value correct')
})

