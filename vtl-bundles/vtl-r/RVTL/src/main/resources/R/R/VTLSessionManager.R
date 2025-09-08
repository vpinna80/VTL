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
VTLSessionManagerClass <- R6Class("VTLSessionManager", 
  public = list(
    #' @description
    #' Creates a new manager instance.
    #' @details 
    #' This method should not be called by the application.
    initialize = function() {
      
    },
            
    #' @description
    #' List all active named VTL sessions.
    #' @details
    #' If the active metadata repository is using an SDMX Registry, also load transformation schemes from it
    list = function() { 
      tryCatch({
        sessions <- list()
        metaProp <- J("it.bancaditalia.oss.vtl.config.VTLGeneralProperties")$METADATA_REPOSITORY
        metaRepo <- J("it.bancaditalia.oss.vtl.config.ConfigurationManager")$getGlobalPropertyValue(metaProp)
        # TODO: Temporarily disabled, causes issues
#        if (metaRepo == 'it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXRepository' 
#            || metaRepo == 'it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXJsonRepository') {
#          sessions <- sapply(.jnew('it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXRepository', TRUE)$getAvailableSchemes(), .jstrVal)
#        }
        unique(c(ls(private$sessions), sessions))
      }, error = function(e) {
        if (!is.null(e$jobj)) {
          e$jobj$printStackTrace()
        }
      })
    },
    
    #' @description
    #' All active VTL sessions are killed.
    clear = function() {
      private$sessions <- new.env(parent = emptyenv())
      gc(verbose = F, full = T)
      return(invisible(self)) 
    },
    
    #' @description
    #' Silently terminates the named active VTL session if it exists.
    #' @param sessionID The name of the session to kill
    kill = function (sessionID) { 
      if (exists(sessionID, envir = private$sessions))
      rm(list = sessionID, envir = private$sessions)
      return(invisible(self))
    },

    #' @description
    #' Opens an example VTL session for a given operator.
    #' @param category The name of the category of the operator
    #' @param operator The name of the operator in the category
    #' @returns The example session
    #' @details
    #' A new VTL session is created and pre-configured with examples for the given operator.
    #' If the session was already created and alive, it is returned as-is.
    openExample = function(category, operator) {
      example <- get0(operator, envir = private$sessions)
      if (!is.null(example)) example else tryCatch({
        example <- VTLSession$new(operator, category)
        assign(operator, example, envir = private$sessions)
        return(example)
      }, error = function(e) {
        if (!is.null(e$jobj)) {
          e$jobj$printStackTrace()
        }
        stop(e)
      })
    },
    
    #' @description
    #' Get or create a VTL session
    #' @param sessionID The session to retrieve or create
    #' @details
    #' If the named VTL session exists, return it, otherwise create a new VTL session with the given name and possibly code.
    getOrCreate = function(sessionID) {
      result <- NULL
      if (is.character(sessionID) && length(sessionID) == 1) {
        result <- get0(req(sessionID), envir = private$sessions)
        if (is.null(result)) {
          result <- assign(sessionID, VTLSession$new(name = sessionID), envir = private$sessions)
        }
      }
      
      return(result)
    },
    
    #' @description
    #' Reload the configuration of all active VTL sessions from the global configuration.
    reload = function() {
      sapply(ls(private$sessions), \(n) VTLSessionManager$getOrCreate(n)$reset())
      return(invisible(self))
    },

    #' @description
    #' Load a VTL configuration file into the VTL global configuration
    #' @param propfile the property file path
    #' @details
    #' loadGlobalConfig will read the VTL configuration from a specified .properties file. if the file
    #' is not specified, a default file named .vtlStudio.properties from the Java user home directory will be opened.
    loadGlobalConfig = function(propfile = NULL) {
      if (is.null(propfile)) {
        propfile = paste0(J("java.lang.System")$getProperty("user.home"), '/.vtlStudio.properties')
      }
      
      if (file.exists(propfile)) {
        reader = .jnew("java.io.FileReader", propfile)
        tryCatch({
          J("it.bancaditalia.oss.vtl.config.ConfigurationManager")$loadGlobalConfiguration(reader)
          message("VTL settings loaded from ", propfile, ".")
        }, finally = {
          reader$close()
        })
      }
      return(self$reload())
    }
  ), private = list(
    sessions = new.env(parent = emptyenv()),
            
    finalize = function() {
      self$clear()
    }
  )
)

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