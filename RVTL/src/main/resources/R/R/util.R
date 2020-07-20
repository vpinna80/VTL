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
  node <- convertToR(jnode, strings.as.factors = F)
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

