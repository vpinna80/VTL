#####################################################################################
#
# Copyright 2020, Bank Of Italy
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
###############################################################################

# R6 Class for managing a VTLSession instance with editor code
#
###############################################################################

VTLSession <- R6Class("VTLSession", 
    private = list(
      instance = NULL,
      finalized = F,
      checkInstance = function() {
        if (private$finalized)
          stop('Session ', self$name, ' was finalized')
        else if (is.null(private$instance)) {
          private$instance <- .jnew("it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl")
        }
        return(invisible(private$instance))
      }
    ), 
    public = list( 
      name = character(0), 
      text = "", 
      initialize = function (name = character(0)) {
                      if (!is.character(name) || length(name) != 1 || nchar(name) == 0)
                        stop("name must be a non-empty character vector with 1 element")
                      self$name <- name
                    }, 
      finalize = function() { 
                    finalized <- T
                    private$instance <- NULL
                    return(invisible()) 
                  },
      isCompiled = function() { !is.null(private$instance) },
      print = function() { print(self$name); return(invisible(self)) },
      setText = function(code) { self$text <- code; return(invisible(self)) },
      addStatements = function(statements) { 
                        self$text = paste0(self$text, statements)
                        private$checkInstance()$addStatements(statements)
                        return(self) 
                      },
      compile = function () { private$checkInstance()$compile() },
      resolve = function(node) { private$checkInstance()$resolve(node) },
      getStatements = function () { private$checkInstance()$getStatements() },
      getNodes = function () { 
                    if (is.null(private$instance))
                      return(list())
                    return(convertToR(private$checkInstance()$getNodes())) 
                  },
      getTopology = function(distance =100, charge = -100) {
          if (is.null(private$instance))
            return(NULL)
        
          jedges = private$checkInstance()$getTopology()
          edges = .jcall(jedges, "[Ljava/lang/Object;","toArray")
          inNodes = sapply(edges[[1]], .jstrVal)
          outNodes = sapply(edges[[2]], .jstrVal)
          allNodes = unique(c(inNodes, outNodes))
          
          statements = sapply(private$checkInstance()$getStatements()$entrySet(), 
                              function (x) setNames(list(x$getValue()), x$getKey()))
          primitiveNodes = allNodes[which(!allNodes %in% names(statements))]
          primitives = rep('PRIMITIVE NODE', times=length(primitiveNodes))
          names(primitives) = primitiveNodes
          statements = append(statements, primitives)
          
          return(tryCatch({
            net = igraph_to_networkD3(make_graph(c(rbind(outNodes, inNodes))))
            net$links$value=rep(3, length(inNodes))
            net$nodes$statement=as.character(statements[as.character(net$nodes$name)])
            return(forceNetwork(Links = net$links, 
                         Nodes = net$nodes, 
                         Source = 'source',
                         Target = 'target',
                         NodeID = 'name',
                         Group = 'statement',
                         Value = 'value',
                         linkDistance = distance,
                         charge = charge,
                         fontSize = 20,
                         opacity = 1,
                         zoom =T,
                         arrows = T,
                         opacityNoHover = 1,
                         clickAction = 'alert(d.group);',
                         bounded = T
            ))
          }, error = function(e) {
            return(NULL)
          }))
        }
    )
  )

as.character.VTLSession <- function(x, ...) { return(x$name) }

VTLSessionManager <- R6Class("VTLSessionManager", 
   private = list(
     sessions = new.env(parent = emptyenv())
   ), public = list(
     initialize = function() {
       assign('test', VTLSession$new('test'), envir = private$sessions)
     },
     finalize = function() {
       self$clear()
     },
     find = function (sessionID) { get(sessionID, envir = private$sessions) },
     list = function() { ls(private$sessions) },
     clear = function() { 
                private$sessions <- new.env(parent = emptyenv())
                assign('test', VTLSession$new('test'), envir = private$sessions)
                gc(verbose = F, full = T)
                return(invisible()) 
              },
     kill = function (sessionID) { 
       if (exists(sessionID, envir = private$sessions))
         rm(list = sessionID, envir = private$sessions)
       return(invisible())
     },
     getOrCreate = function(sessionID) {
       result <- get0(sessionID, envir = private$sessions)
       if (is.null(result))
         result <- assign(sessionID, VTLSession$new(name = sessionID), envir = private$sessions)

       return(result)
     }
   ))$new()
