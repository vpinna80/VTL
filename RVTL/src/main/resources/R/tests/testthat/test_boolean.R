library(dplyr)
library(RVTL)
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
