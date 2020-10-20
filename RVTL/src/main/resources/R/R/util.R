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
  node <- jdx::convertToR(jnode, strings.as.factors = F)
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
  
  return(as.data.frame(node, strings.as.factors = F))
}

