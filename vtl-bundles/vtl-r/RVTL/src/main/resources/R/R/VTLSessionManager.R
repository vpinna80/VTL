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
            #' List all active named VTL sessions.
            #' @details
            #' If an active SDMX metadata repository is active, also load Transformation schemes from it
            list = function() { 
              tryCatch({
                sessions <- list()
                metaRepo <- J("it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory")$newManager()$getMetadataRepository()
                if (metaRepo$getClass()$getName() == 'it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXRepository') {
                  sessions <- sapply(metaRepo$getAvailableSchemes(), .jstrVal)
                }
                unique(c(ls(private$sessions), sessions))
              }, error = function(e) {
                if (!is.null(e$jobj)) {
                  e$jobj$printStackTrace()
                }
              })
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
            #' @param sessionID The name of the session to kill
            kill = function (sessionID) { 
              if (exists(sessionID, envir = private$sessions))
              rm(list = sessionID, envir = private$sessions)
              return(invisible())
            },

            #' @description
            #' Initializes all VTL example sessions.
            #' @details
            #' All examples from the VTL documentation are compiled and added to the list of active sessions
            initExampleSessions = function() {
              tryCatch({
                exampleEnv <- J("it.bancaditalia.oss.vtl.util.VTLExamplesEnvironment")
                
                categories <- lapply(exampleEnv$getCategories(), .jstrVal)
                for (category in categories) {
                  operators <- lapply(exampleEnv$getOperators(category), .jstrVal)
                  for (operator in operators) {
                    example <- VTLSession$new(operator, category)
                    assign(operator, example, envir = private$sessions)
                  }
                }
              
                return(ls(envir = private$sessions)[[1]])
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
              if (is.character(sessionID) && length(sessionID) == 1)
              result <- get0(req(sessionID), envir = private$sessions)
              
              if (is.null(result)) {
                result <- assign(sessionID, VTLSession$new(name = sessionID), envir = private$sessions)
                
                tryCatch({
                  metaRepo <- J("it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory")$newManager()$getMetadataRepository()
              	  if (metaRepo$getClass()$getName() == 'it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXRepository' 
              	      && sessionID %in% sapply(metaRepo$getAvailableSchemes(), .jstrVal)) {
                    code <- metaRepo$getTransformationScheme(sessionID)$getOriginalCode()
                    result$setText(code)
                  }
                }, error = function(e) {
                  if (!is.null(e$jobj)) {
                    e$jobj$printStackTrace()
                  }
                })
              }
              
              result
            },
            
            #' @description
            #' Reload the configuration of the current session, reloading the repository and the environments.
            reload = function() {
              sapply(ls(private$sessions), function(n) VTLSessionManager$getOrCreate(n)$refresh())
            },

            #' @description
            #' Load a VTL configuration file
            #' @param propfile the property file path
            #' @details
            #' load_config will read the VTL configuration from a specified .properties file. if the file
            #' is not specified, a default file named vtlStudio.properties from the user home directory will be opened.
            load_config = function(propfile = NULL) {
              if (is.null(propfile)) {
                propfile = paste0(J("java.lang.System")$getProperty("user.home"), '/.vtlStudio.properties')
              }
              
              if (file.exists(propfile)) {
                reader = .jnew("java.io.FileReader", propfile)
                tryCatch({
                  J("it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory")$loadConfiguration(reader)
                  packageStartupMessage("VTL settings loaded from ", propfile, ".")
                }, finally = {
                  reader$close()
                })
              }
              VTLSessionManager$reload()
            }
          ),
          
          private = list(
            sessions = new.env(parent = emptyenv()),
            
            finalize = function() {
              self$clear()
            }
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