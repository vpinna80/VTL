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