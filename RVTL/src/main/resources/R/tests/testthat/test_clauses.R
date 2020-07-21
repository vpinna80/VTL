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
  result = within(result, rm(action, validFromDate, CONNECTORS_AUTONAME, ID))
  attr(result, 'identifiers') = c('TIME_PERIOD')
  attr(result, 'measures') = c('OBS_VALUE')
  result1 = result[result$TIME_PERIOD=='2000', ]
  result2 = result[result$CURRENCY=='USD',] 
  result2 = within(result2, rm(CURRENCY))
  result3 = result[result$OBS_VALUE==0.923612549019608,]
  result4 = result[result$UNIT=='USD',] 
  result5 = result
  result5$OBS_VALUE2 = result5$OBS_VALUE*2
  result6 = aggregate(result$OBS_VALUE, by = list(result$CURRENCY), FUN = 'sum')
  names(result6) = c('CURRENCY', 'OBS_AGG')
  result7 = within(result, rm(OBS_VALUE))
  result8 = within(result, rm(UNIT))
  result9 = result[, c('CURRENCY', 'OBS_VALUE', 'TIME_PERIOD')]
  result10 = result[, c('CURRENCY', 'UNIT', 'TIME_PERIOD')]
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = 'tmp := \'sdmx:ECB.EXR.A.USD+GBP.EUR.SP00.A\';
                                                      result1 :=tmp[filter cast(TIME_PERIOD, string, "YYYY") = "2000"];
                                                      result2 := tmp[sub CURRENCY = "USD"];
                                                      result3 := tmp[filter OBS_VALUE = 0.923612549019608];
                                                      result4 := tmp[filter UNIT = "USD"];
                                                      result5 := tmp[calc OBS_VALUE2 := OBS_VALUE * 2];
                                                      result6 := tmp[aggr OBS_AGG := sum(OBS_VALUE) group by CURRENCY];
                                                      result7 := tmp[drop OBS_VALUE];
                                                      result8 := tmp[drop UNIT];
                                                      result9 := tmp[keep OBS_VALUE];
                                                      result10 := tmp[keep UNIT];' ,  
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
  result$OBS_VALUE = as.numeric(result$OBS_VALUE)
  attr(result, 'identifiers') = c("CURRENCY", "CURRENCY_DENOM", "EXR_SUFFIX",
                                  "EXR_TYPE", "FREQ", "TIME_PERIOD" )
  attr(result, 'measures') = c('OBS_VALUE')
  result1 = result[result$TIME_PERIOD=='2000',]
  result2 = result[result$TIME_PERIOD=='2000',]
  result2 = within(result2, rm(TIME_PERIOD))
  result3 = result[result$OBS_VALUE==0.923612549019608,]
  result4 = result[result$UNIT=='USD',]
  result5 = result
  result5$OBS_VALUE2 = result5$OBS_VALUE*2
  result6 = aggregate(result$OBS_VALUE, by = list(result$CURRENCY), FUN = 'sum')
  names(result6) = c('CURRENCY', 'OBS_AGG')
  result7 = within(result, rm(OBS_VALUE))
  result8 = within(result, rm(UNIT))
  result9 = result[, c('CURRENCY', 'CURRENCY_DENOM', 'EXR_SUFFIX', 'FREQ', 'OBS_VALUE', 'TIME_PERIOD', 'EXR_TYPE')]
  result10 = result[, c('CURRENCY', 'CURRENCY_DENOM', 'EXR_SUFFIX', 'UNIT', 'FREQ', 'TIME_PERIOD', 'EXR_TYPE')]
  
  expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                        statements = paste0("tmp := 'csv:", 
                                                            find.package(package = 'RVTL'), 
                                                            "/vtlStudio2/test_data/ecbexrusd_vtl.csv" , "';                                                                        
                                                            result1:= tmp[filter TIME_PERIOD = \"2000\"];
                                                            result2:= tmp[sub TIME_PERIOD = \"2000\"];
                                                            result3:= tmp[filter OBS_VALUE = 0.923612549019608];
                                                            result4:= tmp[filter UNIT = \"USD\"];
                                                            result5:= tmp[calc OBS_VALUE2 := OBS_VALUE * 2];
                                                            result6 := tmp[aggr OBS_AGG := sum(OBS_VALUE) group by CURRENCY];
                                                            result7 := tmp[drop OBS_VALUE];
                                                            result8 := tmp[drop UNIT];
                                                            result9 := tmp[keep OBS_VALUE];
                                                            result10 := tmp[keep UNIT];"), 
                                        restartSession = T), label = 'clauses syntax ')
  expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'clauses  compile ')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result1')$result1, target = result1), label = 'filter dimension value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result2')$result2, target = result2), label = 'sub value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result3')$result3, target = result3), label = 'filter measure value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result4')$result4, target = result4), label = 'filter attribute value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result5')$result5, target = result5), label = 'calc value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result6')$result6, target = result6), label = 'aggr value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result7')$result7, target = result7), label = 'drop measure value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result8')$result8, target = result8), label = 'drop attr value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result9')$result9, target = result9), label = 'keep measure value correct')
  expect_true(object = dplyr::all_equal(current = vtlEvalNodes(sessionID = 'test_session', 'result10')$result10, target = result10), label = 'keep attr value correct')
  
})

