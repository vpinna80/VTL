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
context("Clauses")

###
# R Environment
###

test_that('R dataset clauses work', {
  r_input <<- data.frame(id1 = c('a', 'b', 'c'), m1 = c(1.23, 2.34, 3.45), att1=c('a1', 'b1', 'c1'), stringsAsFactors = F)
  attr(r_input, 'identifiers') <<- c('id1')
  attr(r_input, 'measures') <<- c('m1')
  result1 = r_input[r_input$id1=='b', c(2,3)]
  result2 = r_input[r_input$id1=='b',]
  result3 = r_input[r_input$m1==2.34,]
  result4 = r_input[r_input$att1=='b1',]
  result5 = r_input
  result5$m2 = result5$m1*2
  result6 = aggregate(r_input$m1, by = list(r_input$id1), FUN = 'sum')
  names(result6) = c('id1','m2')
  result7 = within(r_input, rm(m1))
  result8 = within(r_input, rm(att1))
  result9 = within(r_input, rm(att1))
  result10 = within(r_input, rm(m1))
  result11 = r_input
  names(result11) = c('id2','m2', 'att2')

  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = 'tmp := r_input;
                                        result1:= tmp[sub id1 = "b"];
                                        result2:= tmp[filter id1 = "b"];
                                        result3:= tmp[filter m1 = 2.34];
                                        result4:= tmp[filter att1 = "b1"];
                                        result5 := tmp[calc m2 := m1 * 2];
                                        result6 := tmp[aggr m2 := sum(m1) group by id1];
                                        result7 := tmp[drop m1];
                                        result8 := tmp[drop att1];
                                        result9 := tmp[keep m1];
                                        result10 := tmp[keep att1];
                                        result11 := tmp[rename att1 to att2, m1 to m2, id1 to id2];', 
                                        restartSession = T), label = 'clauses syntax')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'clauses compile ')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result1')$result1, target = result1), label = 'sub value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result2')$result2, target = result2), label = 'filter dimension value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result3')$result3, target = result3), label = 'filter measure value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result4')$result4, target = result4), label = 'filter attribute value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result5')$result5, target = result5), label = 'calc value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result6')$result6, target = result6), label = 'aggr value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result7')$result7, target = result7), label = 'drop measure value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result8')$result8, target = result8), label = 'drop attr value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result9')$result9, target = result9), label = 'keep measure value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result10')$result10, target = result10), label = 'keep attr value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result11')$result11, target = result11), label = 'rename value correct')
})


###
# SDMX Environment
###
test_that('SDMX dataset clauses work', {
  result = getTimeSeriesTable('ECB', 'EXR.A.USD+GBP.EUR.SP00.A')
  names(result) = tolower(names(result))
  result = within(result, rm(action, validfromdate, connectors_autoname, id))
  result$publ_public = rep(NA, nrow(result))
  result$coverage = rep(NA, nrow(result))
  result$source_pub = rep(NA, nrow(result))
  result$nat_title = rep(NA, nrow(result))
  result$breaks = rep(NA, nrow(result))
  result$compiling_org = rep(NA, nrow(result))
  result$obs_pre_break = rep(NA, nrow(result))
  result$publ_mu = rep(NA, nrow(result))
  result$dom_ser_ids = rep(NA, nrow(result))
  result$compilation = rep(NA, nrow(result))
  result$obs_com = rep(NA, nrow(result))
  result$obs_conf = rep(NA, nrow(result))
  result$unit_index_base = rep(NA, nrow(result))
  result$diss_org = rep(NA, nrow(result))
  result$publ_ecb = rep(NA, nrow(result))
  attr(result, 'identifiers') = c('time_period')
  attr(result, 'measures') = c('obs_value')
  result1 = result[result$currency=='USD', ]
  result2 = result[result$currency=='USD',] 
  result2 = within(result2, rm(currency))
  result3 = result[result$obs_value==0.923612549019608,]
  result4 = result[result$obs_status=='A',] 
  result5 = result
  result5$obs_value2 = result5$obs_value*2
  result6 = aggregate(result$obs_value, by = list(result$currency), FUN = 'sum')
  names(result6) = c('currency', 'obs_agg')
  result7 = within(result, rm(obs_value))
  result8 = within(result, rm(obs_status))
  result9 = result[, c('currency', 'obs_value', 'time_period')]
  result10 = result[, c('currency', 'obs_status', 'time_period')]
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = 'tmp := \'ECB:EXR/A.USD+GBP.EUR.SP00.A\';
                                                      result1 :=tmp[filter currency = "USD"];
                                                      result2 := tmp[sub currency = "USD"];
                                                      result3 := tmp[filter obs_value = 0.923612549019608];
                                                      result4 := tmp[filter obs_status = "A"];
                                                      result5 := tmp[calc obs_value2 := obs_value * 2];
                                                      result6 := tmp[aggr obs_agg := sum(obs_value) group by currency];
                                                      result7 := tmp[drop obs_value];
                                                      result8 := tmp[drop obs_status];
                                                      result9 := tmp[keep obs_value];
                                                      result10 := tmp[keep obs_status];' ,  
                                        restartSession = T), label = 'clauses syntax ')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Compile clauses  ')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result1')$result1, target = result1), label = 'Filter  value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result2')$result2, target = result2), label = 'sub  value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result3')$result3, target = result3), label = 'filter dimension value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result4')$result4, target = result4), label = 'filter attribute value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result5')$result5, target = result5), label = 'calc value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result6')$result6, target = result6), label = 'aggr value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result7')$result7, target = result7), label = 'drop measure value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result8')$result8, target = result8), label = 'drop attr value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result9')$result9, target = result9), label = 'keep measure value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result10')$result10, target = result10), label = 'keep attr value correct')
  
})


###
# CSV Environment
###


test_that('CSV dataset clauses works', {
  result = read.csv(paste0(find.package(package = 'RVTL'), '/vtlStudio2/test_data/ecbexrusd_plain.csv'), stringsAsFactors = F, colClasses='character')
  result$obs_value = as.numeric(result$obs_value)
  attr(result, 'identifiers') = c("currency", "currency_denom", "exr_suffix",
                                  "exr_type", "freq", "time_period" )
  attr(result, 'measures') = c('obs_value')
  result1 = result[result$time_period=='2000',]
  result2 = result[result$time_period=='2000',]
  result2 = within(result2, rm(time_period))
  result3 = result[result$obs_value==0.923612549019608 & !is.na(result$obs_value),]
  result4 = result[result$unit=='USD',]
  result5 = result
  result5$obs_value2 = result5$obs_value*2
  result6 = data.frame('currency'=logical(0), 'obs_agg'=logical(0))
  result6_2 = aggregate(result[result$time_period != '2017','obs_value'], by = list(result[result$time_period != '2017','currency']), FUN = 'sum')
  names(result6_2) = c('currency', 'obs_agg')
  result7 = within(result, rm(obs_value))
  result8 = within(result, rm(obs_status))
  result9 = result[, c('currency', 'currency_denom', 'exr_suffix', 'freq', 'obs_value', 'time_period', 'exr_type')]
  result10 = result[, c('currency', 'currency_denom', 'exr_suffix', 'obs_status', 'freq', 'time_period', 'exr_type')]
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = paste0("tmp := 'csv:", 
                                                            find.package(package = 'RVTL'), 
                                                            "/vtlStudio2/test_data/ecbexrusd_vtl.csv" , "';                                                                        
                                                            result1:= tmp[filter TIME_PERIOD = \"2000\"];
                                                            result2:= tmp[sub TIME_PERIOD = \"2000\"];
                                                            result3:= tmp[filter obs_value = 0.923612549019608];
                                                            result4:= tmp[filter UNIT = \"USD\"];
                                                            result5:= tmp[calc obs_value2 := obs_value * 2];
                                                            result6 := tmp[aggr obs_agg := sum(obs_value) group by currency];
                                                            result6_2 := tmp[filter TIME_PERIOD <> \"2017\"][aggr obs_agg := sum(obs_value) group by currency];
                                                            result7 := tmp[drop obs_value];
                                                            result8 := tmp[drop obs_status];
                                                            result9 := tmp[keep obs_value];
                                                            result10 := tmp[keep obs_status];"), 
                                        restartSession = T), label = 'clauses syntax ')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'clauses  compile ')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result1')$result1, target = result1), label = 'filter dimension value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result2')$result2, target = result2), label = 'sub value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result3')$result3, target = result3), label = 'filter measure value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result4')$result4, target = result4), label = 'filter attribute value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result5')$result5, target = result5), label = 'calc value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result6')$result6, target = result6), label = 'aggr value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result6_2')$result6_2, target = result6_2), label = 'aggr value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result7')$result7, target = result7), label = 'drop measure value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result8')$result8, target = result8), label = 'drop attr value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result9')$result9, target = result9), label = 'keep measure value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result10')$result10, target = result10), label = 'keep attr value correct')
  
})

