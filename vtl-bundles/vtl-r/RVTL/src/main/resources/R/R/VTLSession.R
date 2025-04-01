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

#' Manage VTL sessions
#' 
#' @description
#' VTL Session
#' 
#' @details 
#' R6 Class for interacting with a VTL session instance.
#' 
#' @export
VTLSession <- R6Class("VTLSession", 
    public = list( 
      #' @field name The name of this VTL session.
      name = character(0), 

      #' @field text The temporary code buffer of this VTL session used by the editor.
      text = "", 
      
      #' @description
      #' Creates a new VTL session with a given name.
      #' @details 
      #' This method should not be called by the application.
      #' @param name the name of the session
      #' @param category the category of the example, usually not set
      #' The name to identify this session
      initialize = function (name = character(0), category = character(0)) {
        if (!is.character(name) || length(name) != 1 || nchar(name) == 0)
          stop("name must be a non-empty character vector with exactly 1 element")
        
        self$name <- name
        private$env <- new.env(parent = emptyenv())
        
        if (is.character(category) && length(category) == 1 && nchar(category) != 0) {
          tryCatch({
            exampleEnv <- J("it.bancaditalia.oss.vtl.util.VTLExamplesEnvironment")
            private$instance <- exampleEnv$createSession(category, name)
            self$text <- private$instance$getOriginalCode()
            private$updateInstance()$compile()
          }, error = function(e) {
            if (!is.null(e$jobj)) {
              e$jobj$printStackTrace()
            }
            stop(e)
          })
	    	}
      }, 

      #' @description
      #' Check if this session was compiled.
      #' @details 
      #' Returns \code{TRUE} if this VTL session has already been compiled.
      isCompiled = function() { !is.null(private$instance) },
      
      #' @description
      #' Overrides the default print behaviour.
      print = function() { print(self$name); return(invisible(self)) },

      #' @description
      #' Changes the editor text in the session buffer.
      #' @param code
      #' The editor code to associate this session
      setText = function(code) { 
        self$text <- code
        return(invisible(self)) 
      },
      
      #' @description
      #' Compiles the VTL statements submitted for this session.
      compile = function () {
        private$updateInstance()$compile()
      },
      
      #' @description
      #' Refresh the session configuration after a change in the settings.
      refresh = function() {
        private$clearInstance()
      },
      
      #' @description
      #' Obtains a named list of all the VTL statements submitted for this session.
      getStatements = function () { private$checkInstance()$getStatements() },

      #' @description
      #' Obtains the structure of a VTL dataset with the given name.
      #' @param node
      #' The name of the dataset
      getMetadata = function (node) { 
        alias = J('it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl')$of(node)
        private$checkInstance()$getMetadata(alias)
      },
      
      #' @description
      #' Obtains a named list of all rules and values submitted for this session.
      getNodes = function () { 
        if (is.null(private$instance))
          return(list())
        return(lapply(private$checkInstance()$getNodes(), .jstrVal)) 
      },
      
      #' @description
      #' Evaluates the given VTL nodes as data.frames.
      #' @param nodes a list of names of nodes to compute from this session
      #' @param max.rows The maximum number of rows to retrieve from each node
      #' @details
      #' Returns a list of vertors or data frames containing the values of the 
      #' named nodes defined in this session. Each node is retrieved up to 
      #' max.rows number of observations, or all observations are retrieved if
      #' max.rows is not a positive long integer. In the latter case, the dataset
      #' value is also cached.
      getValues = function (nodes, max.rows = -1L) {
          nodesdf <- lapply(nodes, function(node) {
            df <- get0(node, envir = private$env)
            if (!is.null(df)) {
              return(df)
            }
            alias = J('it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl')$of(node)
            jnode <- tryCatch(private$checkInstance()$resolve(alias), error = function(e) {
              if (!is.null(e$jobj)) {
                e$jobj$printStackTrace()
              }
              signalCondition(e)
            })
            if (jnode %instanceof% "it.bancaditalia.oss.vtl.model.data.ScalarValue") {
              df <- as.data.frame(list(Scalar = jnode$get()))
            } else {
              pager <- .jnew("it.bancaditalia.oss.vtl.util.Paginator", 
                             .jcast(jnode, "it.bancaditalia.oss.vtl.model.data.DataSet"), 100L)
              nc <- jnode$getMetadata()$size()
              df <- tryCatch(convertDF(pager, nc, max.rows), error = function(e) {
                if (!is.null(e$jobj)) {
                  e$jobj$printStackTrace()
                }
                signalCondition(e)
              })
              attr(df, 'measures') <- sapply(jnode$getMetadata()$getMeasures(), function(x) { x$getVariable()$getAlias()$getName() })
              attr(df, 'identifiers') <- sapply(jnode$getMetadata()$getIDs(), function(x) { x$getVariable()$getAlias()$getName() })
            }
            
            if (jnode %instanceof% "it.bancaditalia.oss.vtl.model.data.ScalarValue" || !is.integer(max.rows) || max.rows <= 0) {
              assign(node, df, envir = private$env)
            }
            return(df)
          })
          
          names(nodesdf) <- nodes
          return(nodesdf)
        },
      
      #' @description
      #' Returns a lineage for the value of the named node defined in this session.
      #' @param node
      #' a name of a node to compute from this session
      getLineage = function (node) {
          instance <- private$checkInstance()
          alias = J('it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl')$of(node)
          jds <- instance$resolve(alias)
          if (jds %instanceof% "it.bancaditalia.oss.vtl.model.data.DataSet") {
            viewer <- new(J("it.bancaditalia.oss.vtl.util.LineageViewer"), jds)
            matrix <- viewer$generateAdiacenceMatrix(instance)
            df <- data.frame(source = matrix$getFirst(),
                             target = matrix$getSecond(), 
                             value = sapply(matrix$getThird(), function (x) { x$longValue() }),
                             stringsAsFactors = F)
            df <- df[df$source != df$target, ]
            return(df)
          } else {
            warning("Cannot get lineage for ", node, " because it isn't a data.frame.")
            return(data.frame()) 
          }
        },

     #' @description
     #' Creates a fore network representation of all nodes defined in this VTL session.
     #' @param distance
     #' The distance between dots
     #' @param charge
     #' The repelling force between dots
     #' @importFrom igraph make_graph
     getTopology = function(distance = 100, charge = -100) {
        if (is.null(private$instance))
          return(NULL)
       
        jedges <- private$checkInstance()$getTopology()
        edges <- .jcall(jedges, "[Ljava/lang/Object;","toArray")
        inNodes <- sapply(edges[[1]], .jstrVal)
        outNodes <- sapply(edges[[2]], .jstrVal)
        allNodes <- unique(c(inNodes, outNodes))
        
        statements <- tryCatch({
          sapply(private$checkInstance()$getStatements()$entrySet(), function (x) {
            setNames(list(x$getValue()), x$getKey()$getName())
          })
        }, error = function(e) {
          if (!is.null(e$jobj)) {
            e$jobj$printStackTrace()
          }
          signalCondition(e)
        })
        
        primitiveNodes <- allNodes[which(!allNodes %in% names(statements))]
        primitives <- rep('PRIMITIVE NODE', times=length(primitiveNodes))
        names(primitives) <- primitiveNodes
        statements <- append(statements, primitives)
        
        net = networkD3::igraph_to_networkD3(make_graph(c(rbind(outNodes, inNodes))))
        net$links$value=rep(3, length(inNodes))
        net$nodes$statement=as.character(statements[as.character(net$nodes$name)])
        return(networkD3::forceNetwork(Links = net$links, 
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
      },
     
      #' @description
      #' Returns the environments used by this VTLSession
      getEnvs = function() {
        private$checkInstance()$getEnvironments()
      }
    ),
    
    private = list(
      instance = NULL,
      env = NULL,
      finalized = F,
      
      finalize = function() { 
        finalized <- T
        private$clearInstance() 
        return(invisible()) 
      },

      checkInstance = function() {
        if (private$finalized)
          stop('Session ', self$name, ' was finalized')
        else if (is.null(private$instance)) {
          private$instance <- .jnew("it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl", self$text)
        }
        return(invisible(private$instance))
      },

      updateInstance = function() {
        if (private$finalized) {
          stop('Session ', self$name, ' was already finalized')
        } else if (is.null(private$instance)) {
          private$instance <- .jnew("it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl", self$text)
        } else {
          private$instance <- .jnew("it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl", 
              .jcast(private$instance, "it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl"), self$text)
        }

        private$env <- new.env(parent = emptyenv())
        return(invisible(private$instance))
      },
      
      clearInstance = function() {
        private$instance <- NULL
        private$env <- new.env(parent = emptyenv())
        .jgc()
      }
    )
  )

#' @export
as.character.VTLSession <- function(x, ...) { return(x$name) }