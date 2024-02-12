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
      #' The name to identify this session
      initialize = function (name = character(0)) {
                      if (!is.character(name) || length(name) != 1 || nchar(name) == 0)
                        stop("name must be a non-empty character vector with exactly 1 element")
                      self$name <- name
                    }, 

      #' @description
      #' Terminates this VTL session.
      #' @details 
      #' This method should not be called by the application.
      finalize = function() { 
                    finalized <- T
                    private$instance <- NULL
                    return(invisible()) 
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
        instance <- NULL
        return(invisible(self)) 
      },
      
      #' @description
      #' Compiles the VTL statements submitted for this session.
      compile = function () {
        private$instance = NULL; 
        private$checkInstance()$compile()
      },
      
      #' @description
      #' Obtains a named list of all the VTL statements submitted for this session.
      getStatements = function () { private$checkInstance()$getStatements() },

      #' @description
      #' Obtains the structure of a VTL dataset with the given name.
      #' @param node
      #' The name of the dataset
      getMetadata = function (node) { private$checkInstance()$getMetadata(node) },
      
      #' @description
      #' Obtains a named list of all rules and values submitted for this session.
      getNodes = function () { 
                    if (is.null(private$instance))
                      return(list())
                    return(lapply(private$checkInstance()$getNodes(), .jstrVal)) 
                  },
      
      #' @description
      #' Returns a list of data frames containing the values of the named nodes defined in this session.
      #' @param nodes
      #' a list of names of nodes to compute from this session
      getValues = function (nodes) {
                    jnodes <- sapply(X = nodes, private$checkInstance()$resolve)
                    nodesdf <- lapply(names(jnodes), FUN = function(x, jnodes, jstructs) {
                      jnode <- jnodes[[x]]
                      if (jnode %instanceof% "it.bancaditalia.oss.vtl.model.data.ScalarValue") {
                        df <- as.data.frame(list(Scalar = jnode$get()))
                      }
                      else if (jnode %instanceof% "it.bancaditalia.oss.vtl.model.data.DataSet") {
                        pager <- .jnew("it.bancaditalia.oss.vtl.util.Paginator", 
                                       .jcast(jnode, "it.bancaditalia.oss.vtl.model.data.DataSet"))
                        df <- convertToDF(tryCatch({ pager$more(-1L) },
                          error = function(e) {
                            e$jobj$printStackTrace()
                            signalCondition(e)
                          }, finally = { pager$close() })
                        )
                        attr(df, 'measures') <- sapply(jnode$getMetadata()$getMeasures(), function(x) { x$getVariable()$getName() })
                        attr(df, 'identifiers') <- sapply(jnode$getMetadata()$getIDs(), function(x) { x$getVariable()$getName() })
                      }
                      else
                        stop(paste0("Unsupported result class: ", jnode$getClass()$getName()))
                      
                      return (df)
                    }, jnodes, jstructs)
                    names(nodesdf) <- names(jnodes)
                    return(nodesdf)
                  },
      
      #' @description
      #' Returns a lineage for the value of the named node defined in this session.
      #' @param alias
      #' a name of a node to compute from this session
      getLineage = function (alias) {
                    instance <- private$checkInstance()
                    jds <- instance$resolve(alias)
                    viewer <- new(J("it.bancaditalia.oss.vtl.util.LineageViewer"), jds)
                    matrix <- viewer$generateAdiacenceMatrix(instance)
                    df <- data.frame(source = matrix$getFirst(),
                                     target = matrix$getSecond(), 
                                     value = sapply(matrix$getThird(), function (x) { x$longValue() }),
                                     stringsAsFactors = F)
                    df <- df[df$source != df$target, ]
                    return(df)
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
          
          statements <- sapply(private$checkInstance()$getStatements()$entrySet(), 
                              function (x) setNames(list(x$getValue()), x$getKey()))
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
        }
    ),
    private = list(
      instance = NULL,
      finalized = F,
      checkInstance = function() {
        if (private$finalized)
          stop('Session ', self$name, ' was finalized')
        else if (is.null(private$instance)) {
          private$instance <- .jnew("it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl", self$text)
        }
        return(invisible(private$instance))
      }
    )
  )

as.character.VTLSession <- function(x, ...) { return(x$name) }

