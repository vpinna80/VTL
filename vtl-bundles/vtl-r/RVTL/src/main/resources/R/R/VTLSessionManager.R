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
#' R6 class to create, find and destroy VTL sessions
#' 
#' @details 
#' This R6 class is used to create, find and destroy VTL sessions
VTLSessionManagerClass <- R6Class("VTLSessionManager", public = list(

            #' @description
            #' Creates a new manager instance.
            #' @details 
            #' This method should not be called by the application.
            initialize = function() {
              assign('test', VTLSession$new('test'), envir = private$sessions)
            },
            
            #' @description
            #' Finalization
            #' @details
            #' Clears any managed VTL session when this manager is discarded by the garbage collector.
            #' This method should not be called by the application.
            finalize = function() {
              self$clear()
            },
            
            #' @description
            #' List all active named VTL sessions.
            #' If an active SDMX metadata repository is active, also load Transformation schemes from it
            list = function() { 
            	ss <- ls(private$sessions)
            	metaRepo <- J("it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory")$getInstance()$getMetadataRepository()
            	if (metaRepo %instanceof% 'it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXRepository') {
            		sdmxTs <- sapply(metaRepo$getAvailableSchemes(), .jstrVal)
            		ss <- unique(c(ss, sdmxTs))
            	}
            	ss
            },
            
            #' @description
            #' All active VTL sessions are killed and a new VTL session named 'test' is created.
            clear = function() { 
              private$sessions <- new.env(parent = emptyenv())
              assign('test', VTLSession$new('test'), envir = private$sessions)
              gc(verbose = F, full = T)
              return(invisible()) 
            },
            
            #' @description
            #' Silently terminates the named active VTL session if it exists.
            #' @param sessionID
            #' The name of the session to kill
            kill = function (sessionID) { 
              if (exists(sessionID, envir = private$sessions))
              rm(list = sessionID, envir = private$sessions)
              return(invisible())
            },
            
            #' @description
            #' If the named VTL session exists, return it, otherwise create a new VTL session with the given name and possibly code.
            #' @param sessionID The session to retrieve or create
            #' The name of the session to create
            getOrCreate = function(sessionID) {
              result <- get0(sessionID, envir = private$sessions)
              
              if (is.null(result)) {
                result <- assign(sessionID, VTLSession$new(name = sessionID), envir = private$sessions)
                
                metaRepo <- J("it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory")$getInstance()$getMetadataRepository()
              	if (metaRepo %instanceof% 'it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXRepository' && sessionID %in% sapply(metaRepo$getAvailableSchemes(), .jstrVal)) {
              	  code <- metaRepo$getTransformationScheme(sessionID)$getOriginalCode()
            	  result$setText(code)
            	} 
              }
              
              result
            }), 
          private = list(
            sessions = new.env(parent = emptyenv())
          ))

#' Manage VTL sessions
#' 
#' @description
#' R6 singleton object to find and destroy VTL sessions
#' 
#' @details 
#' This object is a preallocated instance of the class \code{\link{VTLSessionManagerClass}}.
#' See the class documentation page to help on the methods of this object.
#' 
#' @export
VTLSessionManager <- VTLSessionManagerClass$new()