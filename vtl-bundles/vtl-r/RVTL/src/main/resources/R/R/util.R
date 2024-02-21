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

##########################
#
# Package helpers (not exported)
#
##########################

vtloperators <- function(){
  return(list(VTL=c('sqrt(x)', 'ln(x)', 'abs(x)', 'floor(x)')))
}

vtlTryCatch <- function(expr) {
  return(tryCatch({
    expr
    return(T)
  }, error = function(e) {
    if (is.function(e$jobj$getMessage)) {
      print(paste0("ERROR: ", e$jobj$getMessage()))
      e$jobj$printStackTrace()
    }
    else
      print(e)
    return(F)
  }))
}

convertDF <- function(pager, nc) {
  total <- NULL
  repeat {               
    pager$prepareMore()
    part <- lapply(0:(nc - 1), function(i) {
    	switch (pager$getType(i),
    	  pager$getDoubleColumn(i),  
    	  lapply(pager$getIntColumn(i), as.logical),
    	  lapply(pager$getIntColumn(i), as.Date, as.Date("1970-01-01")),
    	  sapply(pager$getStringColumn(i), .jstrVal)
    	)  
    })
    names(part) <- sapply(0:(nc - 1), function(i) .jstrVal(pager$getName(i)))
    part <- as.data.frame(part)
    if (nrow(part) > 0) {
      total <- if (is.null(total)) part else rbind(total, part)
    } else {
      break
    }
  }
  
  invisible(total)
}