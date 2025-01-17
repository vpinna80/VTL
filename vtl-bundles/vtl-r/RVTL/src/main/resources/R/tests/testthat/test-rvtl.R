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

testthat::test_that("basic transformation with R env", {
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
})