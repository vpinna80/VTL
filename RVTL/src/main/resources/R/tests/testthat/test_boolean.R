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
context("Boolean")

# SCALARS
test_that('Booleans work',
          {
            expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                                  statements = 't := true;                                                                         
                                                  f := false;
                                                  resultA1 := t and t;
                                                  resultA2 := t and f;
                                                  resultA3 := f and f;
                                                  resultO1 := t or t;
                                                  resultO2 := t or f;
                                                  resultO3 := f or f;
                                                  resultX1 := t xor t;
                                                  resultX2 := t xor f;
                                                  resultX3 := f xor f;
                                                  resultN1 := not t;
                                                  resultN2 := not f;
                                                  ', 
                                                  restartSession = T), label = 'Booleans syntax')
            expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Booleans compile')
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultA1')$resultA1[1,1], 
                             expected = T, label = 'AND value correct')
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultA2')$resultA2[1,1], 
                             expected = F, label = 'AND value correct')
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultA3')$resultA3[1,1], 
                             expected = F, label = 'AND value correct')
            
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultO1')$resultO1[1,1], 
                             expected = T, label = 'OR value correct')
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultO2')$resultO2[1,1], 
                             expected = T, label = 'OR value correct')
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultO3')$resultO3[1,1], 
                             expected = F, label = 'OR value correct')
            
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultX1')$resultX1[1,1], 
                             expected = F, label = 'XOR value correct')
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultX2')$resultX2[1,1], 
                             expected = T, label = 'XOR value correct')
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultX3')$resultX3[1,1], 
                             expected = F, label = 'XOR value correct')
            
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultN1')$resultN1[1,1],
                             expected = F, label = 'NOT value correct')
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultN2')$resultN2[1,1],
                             expected = T, label = 'NOT value correct')

          })


# Dataset
test_that('Dataset Booleans work',
          {
            r_input <<- data.frame(id1 = c('a', 'b', 'c'), m1 = c(T, T, T), stringsAsFactors = F)
            attr(r_input, 'identifiers') <<- c('id1')
            attr(r_input, 'measures') <<- c('m1')
            r_input2 <<- data.frame(id1 = c('a', 'b', 'c'), m1 = c(F, F, F), stringsAsFactors = F)
            attr(r_input2, 'identifiers') <<- c('id1')
            attr(r_input2, 'measures') <<- c('m1')
            
            expect_true(object = vtlAddStatements(sessionID = 'test_session', 
                                                  statements = 't := r_input;                                                                         
                                                  f := r_input2;
                                                  resultA1 := t and t;
                                                  resultA2 := t and f;
                                                  resultA3 := f and f;
                                                  resultO1 := t or t;
                                                  resultO2 := t or f;
                                                  resultO3 := f or f;
                                                  resultX1 := t xor t;
                                                  resultX2 := t xor f;
                                                  resultX3 := f xor f;
                                                  resultN1 := not t;
                                                  resultN2 := not f;
                                                  ', 
                                                  restartSession = T), label = 'Dataset Booleans syntax')
            expect_true(object = vtlCompile(sessionID = 'test_session'), label = 'Dataset Booleans compile')
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultA1')$resultA1$m1, 
                             expected = rep(T,3), label = 'Dataset AND value correct')
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultA2')$resultA2$m1, 
                             expected = rep(F,3), label = 'Dataset AND value correct')
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultA3')$resultA3$m1, 
                             expected = rep(F,3), label = 'Dataset AND value correct')
            
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultO1')$resultO1$m1, 
                             expected = rep(T,3), label = 'Dataset OR value correct')
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultO2')$resultO2$m1, 
                             expected = rep(T,3), label = 'Dataset OR value correct')
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultO3')$resultO3$m1, 
                             expected = rep(F,3), label = 'Dataset OR value correct')
            
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultX1')$resultX1$m1, 
                             expected = rep(F,3), label = 'Dataset XOR value correct')
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultX2')$resultX2$m1, 
                             expected = rep(T,3), label = 'Dataset XOR value correct')
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultX3')$resultX3$m1, 
                             expected = rep(F,3), label = 'Dataset XOR value correct')
            
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultN1')$resultN1$m1,
                             expected = rep(F,3), label = 'Dataset NOT value correct')
            expect_identical(object = vtlEvalNodes(sessionID = 'test_session', nodes = 'resultN2')$resultN2$m1,
                             expected = rep(T,3), label = 'Dataset NOT value correct')
            
          })
