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

testthat::test_that("R env", {
  vtlLogLevel('off')
  
  tryCatch({
  set_vtl_properties()
  ds <- data.frame(l=letters,
				   n=as.integer(1:length(letters)), 
				   m=rnorm(n = length(letters), mean = 0), 
				   b=rnorm(n = length(letters), mean = 0) >0,
				   d=as.Date('2020-01-01') + 1:length(letters))
	attr(ds, 'identifiers') <- c('l', 'n')
	attr(ds, 'measures') <- c('m')
  ds1 <<- ds ## fix problem global env
  
	result_add = vtlAddStatements('test', 'ds2:=ds1;ds3:=ds1+ds2;')
	testthat::expect_true(result_add, label = "vtlAddStatements")
	
	result_cmp = vtlCompile('test')
	testthat::expect_true(result_cmp, label = "compilation")
	ds2=vtlEvalNodes('test', 'ds2')[['ds2']]
	ds3=vtlEvalNodes('test', 'ds3')[['ds3']]

  testthat::expect_equal(
      ds1[ds1$l=='a', 'm'],
      ds2[ds2$l=='a', 'm'],
      label = "first check"
  )
  testthat::expect_equal(
      ds3[ds3$l=='a', 'm'],
      ds1[ds1$l=='a', 'm']+ds2[ds2$l=='a', 'm'],
      label = "second check"
  )
  }, error = function(e) {
            if (!is.null(e$jobj)) {
                e$jobj$printStackTrace()
            }
            testthat::fail()
        }
  )
})

testthat::test_that("R env + json repo", {
  vtlLogLevel('off')
  
  tryCatch({
  set_vtl_r_json_properties()
  ds_local=data.frame(Id_1=c(10L, 10L, 11L, 11L),                                     
                       Id_2 = c('A', 'B',  'A', 'B'),
                       Me_1 = c(5L, 2L, 3L, 4L),        
                       Me_2 = c(5.0, 10.5, 12.2, 20.3)) 
  ds_r_json <<- ds_local # fix problem global env
  vtlAddStatements('test2', 'ds2:=ds_r_json;ds3:=ds_r_json+ds2;')
  vtlCompile('test2')
  ds2=vtlEvalNodes('test2', 'ds2')[['ds2']]
  ds3=vtlEvalNodes('test2', 'ds3')[['ds3']]
  
  testthat::expect_equal(
    ds_r_json[ds_r_json$Id_2=='A' & ds_r_json$Id_1==11, 'Me_1'],
    ds2[ds2$Id_2=='A' & ds2$Id_1==11, 'Me_1'],
    label = "first check"
  )

  testthat::expect_equal(
    ds3[ds3$Id_2=='A' & ds3$Id_1==11, 'Me_1'], 
    ds_r_json[ds_r_json$Id_2=='A' & ds_r_json$Id_1==11, 'Me_1']+ds2[ds2$Id_2=='A' & ds2$Id_1==11, 'Me_1'],
    label = "second check"
  )
  }, error = function(e) {
            if (!is.null(e$jobj)) {
                e$jobj$printStackTrace()
            }
            testthat::fail()

        }
  )
})

testthat::test_that("CSV env + json repo", {
  vtlLogLevel('off')

  tryCatch({
  set_vtl_csv_json_properties()
  vtlAddStatements('test3', 'ds2:=ds_csv;ds3:=ds_csv+ds2;')
  vtlCompile('test3')
  ds_csv=vtlEvalNodes('test3', 'ds_csv')[['ds_csv']]
  ds2=vtlEvalNodes('test3', 'ds2')[['ds2']]
  ds3=vtlEvalNodes('test3', 'ds3')[['ds3']]
  
  ds_csv[ds_csv$Id_2=='A' & ds_csv$Id_1==11, 'Me_1'] == ds2[ds2$Id_2=='A' & ds2$Id_1==11, 'Me_1']
  ds3[ds3$Id_2=='A' & ds3$Id_1==11, 'Me_1'] == 
    ds_csv[ds_csv$Id_2=='A' & ds_csv$Id_1==11, 'Me_1']+ds2[ds2$Id_2=='A' & ds2$Id_1==11, 'Me_1']
  
  
  testthat::expect_equal(
    ds_csv[ds_csv$Id_2=='A' & ds_csv$Id_1==11, 'Me_1'],
    ds2[ds2$Id_2=='A' & ds2$Id_1==11, 'Me_1'],
    label = "first check"
  )
  testthat::expect_equal(
    ds3[ds3$Id_2=='A' & ds3$Id_1==11, 'Me_1'] , 
    ds_csv[ds_csv$Id_2=='A' & ds_csv$Id_1==11, 'Me_1']+ds2[ds2$Id_2=='A' & ds2$Id_1==11, 'Me_1'],
    label = "second check"
  )
  }, error = function(e) {
            if (!is.null(e$jobj)) {
                e$jobj$printStackTrace()
            }
            testthat::fail()
        }
  )
})
