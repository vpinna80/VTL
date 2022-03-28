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

convertToDF <- function(jnode) {
  names <- jnode$keySet()
  node <- lapply(names, function(name) {
    vals <- lapply(.jcast(jnode$get(name), "java.util.List"), function(x) {
      if (x %instanceof% java.lang.Double) {
      	return(x$doubleValue())
      } else if (x %instanceof% java.lang.Long) {
      	return(x$longValue())
      } else if (x %instanceof% java.lang.String) {
      	return(.jstrVal(x))
      } else if (x %instanceof% java.lang.Boolean) {
      	return(as.logical(x$booleanValue()))
      } else if (x %instanceof% java.time.LocalDate) {
      	return(as.Date(.jstrVal(x$toString())))
      } else {
      	return(x)
      }
    })
  })
  if(is.list(node) && length(node) > 0) {
    #there are columns with all nulls
    nulls = which(sapply(node, is.list))
    for(item in nulls) {
      unlisted = unlist(node[[item]])
      if(is.null(unlisted)){
        #all nulls
        unlisted = rep(NA, times = length(node[[item]]))
      }
      node[[item]] = unlisted
    }
  }
  
  names(node) <- names
  return(as.data.frame(node, stringsAsFactors = F))
}

