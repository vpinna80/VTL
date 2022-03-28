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
  nodenames <- sapply(jnode$keySet(), .jstrVal)
  nodesvals <- lapply(nodenames, function (nodename) .jcast(jnode$get(nodename), "java.util.List"))
  dateidxes <- which(sapply(nodesvals, function (nodevals) any(sapply(nodevals, `%instanceof%`, "java.time.LocalDate"))))
  node <- lapply(nodesvals, function(node) {
    sapply(node, function(x) {
      if (is.jnull(x)) {
      	return(NA)
      } else if (!is(x, 'jobjRef')) {
        return(x)
      } else if (x %instanceof% "java.lang.Double") {
      	return(x$doubleValue())
      } else if (x %instanceof% "java.lang.Long") {
      	return(x$longValue())
      } else if (x %instanceof% "java.lang.String") {
      	return(.jstrVal(x))
      } else if (x %instanceof% "java.lang.Boolean") {
      	return(as.logical(x$booleanValue()))
      } else if (x %instanceof% "java.time.LocalDate") {
      	return(as.Date(.jstrVal(x$toString())))
      } else {
      	return(x)
      }
    })
  })
  
  names(node) <- nodenames
  node[dateidxes] <- lapply(dateidxes, function(dateidx) as.Date(node[[dateidx]], as.Date('1970-01-01')))
  return(as.data.frame(node, stringsAsFactors = F))
}