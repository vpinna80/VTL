library(dplyr)
library(RVTL)
library(testthat)
test_results = test_dir('testthat')

sink('test_result.out')
timestamp()
results_df = as.data.frame(test_results)
global_pass = all(results_df$failed == 0) &
              all(!results_df$skipped) &
              all(!results_df$error)

print('')
if(global_pass){
  print ('########## All test are fine! ########## ')
} else{
  print('########## There are failures! ########## ')
}
print('')
print(results_df)

sink()

if(global_pass){
  sink('OK')
  print('OK')
  sink()
}
