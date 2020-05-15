# Copyright 2019,2019 Bank Of Italy
#
# Licensed under the EUPL, Version 1.1 or - as soon they
# will be approved by the European Commission - subsequent
# versions of the EUPL (the "Licence");
# You may not use this work except in compliance with the
# Licence.
# You may obtain a copy of the Licence at:
#
#
# http://ec.europa.eu/idabc/eupl
#
# Unless required by applicable law or agreed to in
# writing, software distributed under the Licence is
# distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
# See the Licence for the specific language governing
# permissions and limitations under the Licence.
#

# Main class for consuming SDMX web services
#
# Author: Attilio Mattiocco
###############################################################################

vtlStudio <- function() {
  shiny::runApp(system.file('vtlStudio2', package='RVTL'))  
}

vtlKillSessions <- function(sessions) {
  sapply(sessions, J('it.bancaditalia.oss.vtl.impl.session.VTLSessionHandler')$killSession)
  sapply(sessions, function(sessionID) {
    if (exists(x = sessionID, envir = RVTL:::vtlSessions)) 
      rm(list = sessionID, envir = RVTL:::vtlSessions)
  })
  return(invisible())
}

vtlListSessions <- function() {
#  jsessions = J('it.bancaditalia.oss.vtl.impl.session.VTLSessionHandler')$getSessions()
#  sessions = .jcall(jsessions, "[Ljava/lang/Object;","toArray")
#  sessions = sapply(sessions, .jstrVal)
  return(ls(RVTL:::vtlSessions))
}

vtlAddStatements <- function(sessionID, statements, restartSession = F) {
  rc <- vtlTryCatch({ 
    if(restartSession || !exists(sessionID, envir = RVTL:::vtlSessions)) {
      vtlKillSessions(sessionID)
      vtlGetOrCreateSession(sessionID, statements) 
    } else {
      vtlGetSession(sessionID)$addStatements(statements)
    }
    return(T)
  })
  
  if (rc)
    print('Statements added')
  return(rc)
}

vtlEvalNodes <- function(sessionID, nodes) {
  return(tryCatch({
  start = Sys.time()
  session <- vtlGetSession(sessionID)
  jnodes = sapply(X = nodes, session$resolve)
  evalTime = Sys.time() - start
  jstructs = sapply(X = nodes, FUN = function(x) J('it.bancaditalia.oss.vtl.impl.session.VTLSessionHandler')$getNodeStructure(sessionID,x))
  nodesdf = lapply(names(jnodes), FUN = function(x, jnodes, jstructs) {
    jnode = jnodes[[x]]
    if (jnode %instanceof% "it.bancaditalia.oss.vtl.model.data.ScalarValue") {
      jnode <- J("java.util.Collections")$singletonMap("Scalar", 
          J("java.util.Collections")$singletonList(
              .jcall(jnode, returnSig = "Ljava/lang/Object;", method="get")))
    }
    else if (jnode %instanceof% "it.bancaditalia.oss.vtl.model.data.DataSet") {
      pager <- .jnew("it.bancaditalia.oss.vtl.util.Paginator", 
                     .jcast(jnode, "it.bancaditalia.oss.vtl.model.data.DataSet"))
      jnode <- tryCatch({ pager$more(-1L) }, finally = { pager$close() })
    }
    else
      stop(paste0("Unsupported result class: ", jnode$getClass()$getName()))
    jstruct = jstructs[[x]]
    tmp = convertToR(jnode, strings.as.factors = F)
    if(!is.data.frame(tmp)){
      if(is.list(tmp)){
	      if(length(tmp) > 0){
	        #there are columns with all nulls
	        nulls = which(sapply(tmp, is.list))
	        for(item in nulls){
	          unlisted = unlist(tmp[[item]])
	          if(is.null(unlisted)){
	            #all nulls
	            unlisted = rep(NA, times = length(tmp[[item]]))
	          }
	          tmp[[item]] = unlisted
		      }
        }
      }
      tmp = as.data.frame(tmp, strings.as.factors = F)
    }
    measuresJ = jstruct$get('MEASURES')
    identifiersJ = jstruct$get('IDENTIFIERS')
    if(length(measuresJ) > 0){
      measures = convertToR(measuresJ)
      if(length(measures) > 0){
        attr(tmp, 'measures') <- measures
      }
    }
    if(length(identifiersJ) > 0){
      identifiers = convertToR(identifiersJ)
      if(length(identifiers) > 0){
        attr(tmp, 'identifiers') <- identifiers
      }
    }
    attr(tmp, 'evalTime') <- evalTime
    return (tmp)
  }, jnodes, jstructs)
  names(nodesdf) <- names(jnodes)
  return(nodesdf)
  }, error = function(e) {
    print(paste0("ERROR: ", e$jobj$getMessage()))
    e$jobj$printStackTrace()
    return(F)
  }))
}

vtlListStatements <- function(sessionID) {
  jstatements = vtlGetSession(sessionID)$getStatements()
  return(sapply(jstatements$entrySet(), function (x) setNames(list(x$getValue()), x$getKey())))
}

vtlListNodes <- function(sessionID){
  return(vtlGetSession(sessionID)$getNodes())
}

vtlGetCode <- function(sessionID) {
  return(vtlGetSession(sessionID)$text)
}

vtlTopology <- function(session, distance =100, charge = -100) {
  return(vtlGetSession(session)$getTopology(distance, charge))
}

vtlCompile <- function(sessionID) {
  vtlSession <- vtlGetSession(sessionID)
  result <- vtlTryCatch({
    vtlSession$compile()
    print('Compilation successful!')
    return(T)
  })
  return(result)
}

##########################
#
# Package helpers (not exported)
#
##########################

vtloperators <- function(){
  return(list(VTL=c('sqrt(x)', 'ln(x)', 'abs(x)', 'floor(x)')))
}

vtlSessions <- new.env(parent = emptyenv())

vtlGetSession <- function(sessionID) {
  return(get(sessionID, envir = RVTL:::vtlSessions))
}

vtlGetOrCreateSession <- function(sessionID, statements = "") {
  if (is.null(vtlSession <- get0(sessionID, envir = RVTL:::vtlSessions))) {
    vtlSession <- RVTL:::VTLSession$new(name = sessionID, code = statements)
    assign(sessionID, vtlSession, envir = RVTL:::vtlSessions)
  } 
    
  return(vtlSession)
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

# 
# vtlPrint <- function(name) {
#   J('it.bancaditalia.oss.sdmx.vtl.EngineHandler')$print(name)
# }
# 
# vtlBindSDMX <- function(name, query) {
#   J('it.bancaditalia.oss.sdmx.vtl.EngineHandler')$bindSDMXConnector(name, query)
# }
# 
# vtlBind <- function(name, dataset, dimensions, measures) {
#   structureBuilder = J('no.ssb.vtl.model.StaticDataset')$create()
#   for (x in colnames(dataset)){
#     role = J("no.ssb.vtl.model.Component")$Role$ATTRIBUTE
#     clazz = J("java.lang.String")$class
#     if(x %in% measures){
#       role = J("no.ssb.vtl.model.Component")$Role$MEASURE
#       clazz = J("java.lang.Double")$class
#     }
#     else if(x %in% dimensions){
#       role = J("no.ssb.vtl.model.Component")$Role$IDENTIFIER
#     }
#     if(is.numeric(dataset[,x])){
#       clazz = J("java.lang.Double")$class
#     }
#     structureBuilder = structureBuilder$addComponent(x, role, clazz)
#   }
#   ds = structureBuilder$build()
#   valueBuilder = J('no.ssb.vtl.model.StaticDataset')$create(ds$getDataStructure())
# 
#   for(x in 1:nrow(dataset)){
#     points = list()
#     for (y in colnames(dataset)){
#       if(is.numeric(dataset[x, y])){
#         point = .jnew(class = 'java/lang/Double', as.numeric(dataset[x, y]))
#       }
#       else {
#         point = .jnew(class = 'java/lang/String', dataset[x, y])
#       }
#       points = append(points, point)
#     }
#     javapoints = .jarray(points)
#     valueBuilder = valueBuilder$addPoints(javapoints)
#   }
#   ds = valueBuilder$build()
#   J('it.bancaditalia.oss.sdmx.vtl.EngineHandler')$bind(name, ds)
# }
# 
# vtlPop <- function(name){
#   components = J('it.bancaditalia.oss.sdmx.vtl.EngineHandler')$popStructure(name)
#   names = as.list(components$keySet()$toArray())
#   names = sapply(names, function(x) as.character(x$toString()))
# 
#   descriptions = as.list(components$values()$toArray())
#   descriptions = sapply(descriptions, function(x) as.character(x$toString()))
# 
#   javalist = J('it.bancaditalia.oss.sdmx.vtl.EngineHandler')$popData(name)
#   pointList = as.list(javalist)
#   result = lapply(pointList, FUN = function(y) {
#                                         row = as.list(y)
#                                         dfrow = sapply(row, FUN = function(x) ifelse(x %instanceof% J('java.lang.Number'), x$doubleValue(), x$toString()))
#                                         return(dfrow)
#                                       })
#   result = as.data.frame(do.call(rbind, result))
#   names(result) = names
#   attr(result, 'descriptions') = descriptions
#   rownames(result) = NULL
#   return(result)
# }
# 

