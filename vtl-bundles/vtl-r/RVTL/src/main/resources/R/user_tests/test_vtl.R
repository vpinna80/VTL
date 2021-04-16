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

library(RVTL)
library(testthat)

# if we are in interactive mode, rely on the wd(). Otherwise the environment variable has to be set 
# in the calling shell
if(interactive) Sys.setenv(VTL_PATH=paste0(getwd(),'/input_data',';',getwd(),'/output_data'))


sink('test_result.out')
timestamp()
test_results = test_file('test.R')
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
