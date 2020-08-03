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

#' @title Launch VTL editor
#' @description This command opens a shiny-based rich editor for writing and testing VTL programs.
#' @usage vtlStudio(...)
#' @param ... More options to pass to \code{\link[shiny]{runApp}}.
#' @export
#' @examples \dontrun{
#'     #opens the editor 
#'     vtlStudio()
#' }
vtlStudio <- function(...) {
  shiny::runApp(appDir = system.file('vtlStudio2', package='RVTL'), ...)  
}

#' @title Process VTL statements
#' @description Replaces or adds more statements to an existing VTL session.
#' @usage vtlAddStatements(sessionID, statements, restartSession = F)
#' @param sessionID The symbolic name of an active VTL session
#' @param statements The code to be added to the session
#' @param restartSession \code{TRUE} if session must be restarted (default \code{FALSE})
#' @details If you are replacing one or more already defined rules, 
#'          you need to set \code{restartSession} to \code{TRUE} to avoid errors.
#'
#'          Always returns true.
#' @export
#' @examples \dontrun{
#'   vtlAddStatements(session = 'test', restartSession = T,
#'                    statements = 'a := r_input;
#'                                   b := 3;
#'                                   c := abs(sqrt(14 + a));
#'                                   d := a + b + c;
#'                                   e := c + d / a;
#'                                   f := e - d;
#'                                   g := -f;
#'                                   test := a * b + c / a;')
#'   }
vtlAddStatements <- function(sessionID, statements, restartSession = F) {
  if(restartSession) {
    VTLSessionManager$kill(sessionID)
  }

  session = VTLSessionManager$getOrCreate(sessionID)$addStatements(statements)
  print('Statements added')
  return(T)
}

#' @title List VTL statements
#' @description Lists all statements defined in an existing VTL session.
#' @usage vtlListStatements(sessionID)
#' @param sessionID The symbolic name of an active VTL session
#' @details This function returns a named list containing all the statements defined in the specified VTL session.
#' @export
#' @examples \dontrun{
#'   vtlAddStatements(session = 'test', statements = 'a := r_input;
#'                                b := 3;
#'                                c := abs(sqrt(14 + a));
#'                                d := a + b + c;
#'                                e := c + d / a;
#'                                f := e - d;
#'                                g := -f;
#'                                test := a * b + c / a;')
#'                                
#'   vtlListStatements('test')
#' }
vtlListStatements <- function(sessionID) {
  jstatements = VTLSessionManager$find(sessionID)$getStatements()
  return(sapply(jstatements$entrySet(), function (x) stats::setNames(list(x$getValue()), x$getKey())))
}

#' @title Compile a VTL session
#' @description Compiles all code submitted in  an existing VTL session.
#' @usage vtlCompile(sessionID)
#' @param sessionID The symbolic name of an active VTL session
#' @details This function is used to compile the VTL statements in the specified VTL session. Compilation does not only check the syntax: it also ensures structural consistency.
#' @export
#' @examples \dontrun{
#'   #prepare a VTL compliant dataset in the R environment (compilation would fail otherwise)
#'   r_input <- data.frame(id1 = c('a', 'b', 'c'), 
#'                           m1 = c(1.23, -2.34, 3.45), 
#'                           m2 = c(1.23, -2.34, 3.45), 
#'                           stringsAsFactors = F)
#'   attr(r_input, 'identifiers') <- c('id1')
#'   attr(r_input, 'measures') <- c('m1', 'm2')
#'  
#'   vtlAddStatements(session = 'test', restartSession = T,
#'                   statements = 'a := r_input;
#'                                b := 3;
#'                                c := abs(sqrt(14 + a));
#'                                d := a + b + c;
#'                                e := c + d / a;
#'                                f := e - d;
#'                                g := -f;
#'                                test := a * b + c / a;')
#'  
#'   vtlCompile('test')
#' }
vtlCompile <- function(sessionID) {
  result <- vtlTryCatch({
    VTLSessionManager$find(sessionID)$compile()
    print('Compilation successful!')
    return(T)
  })
  return(result)
}

#' @title Evaluate a list of nodes
#' @description Calculate and return the values of the nodes of a VTL session
#' @usage vtlEvalNodes(sessionID, nodes)
#' @param sessionID The symbolic name of an active VTL session
#' @param nodes The nodes to be evaluated
#' @details This function is used to evaluate specific nodes of a vtl session. The evaluated nodes will be returned in a list
#' @export
#' @examples \dontrun{
#'   #prepare a VTL compliant dataset in R
#'   r_input <- data.frame(id1 = c('a', 'b', 'c'), 
#'                           m1 = c(1.23, -2.34, 3.45), 
#'                           m2 = c(1.23, -2.34, 3.45), 
#'                           stringsAsFactors = F)
#'   attr(r_input, 'identifiers') <- c('id1')
#'   attr(r_input, 'measures') <- c('m1', 'm2')
#'  
#'   vtlAddStatements(session = 'test', restartSession = T,
#'                   statements = 'a := r_input;
#'                                b := 3;
#'                                c := abs(sqrt(14 + a));
#'                                d := a + b + c;
#'                                e := c + d / a;
#'                                f := e - d;
#'                                g := -f;
#'                                test := a * b + c / a;')
#'  
#'   vtlCompile('test')
#'   vtlEvalNodes('test', vtlListNodes('test'))    
#' }
vtlEvalNodes <- function(sessionID, nodes) {
  VTLSessionManager$find(sessionID)$getValues(nodes)
}

#' @title List session nodes
#' @description List all nodes in the specified VTL session
#' @usage vtlListNodes(sessionID)
#' @param sessionID The symbolic name of an active VTL session
#' @details This function returns the list of nodes in the specified VTL session.
#' @export
#' @examples \dontrun{
#'   vtlAddStatements(session = 'test', restartSession = T,
#'                   statements = 'a := r_input;
#'                                b := 3;
#'                                c := abs(sqrt(14 + a));
#'                                d := a + b + c;
#'                                e := c + d / a;
#'                                f := e - d;
#'                                g := -f;
#'                                test := a * b + c / a;')
#'  
#'   vtlListNodes('test')
#' }
vtlListNodes <- function(sessionID){
  return(VTLSessionManager$find(sessionID)$getNodes())
}

#' @title List sessions
#' @description List all sessions in the VTL engine
#' @usage vtlListSessions()
#' @details This function returns the list of sessions created in the engine.
#' @export
#' @examples \dontrun{
#'   vtlAddStatements(session = 'test', restartSession = T,
#'                   statements = 'a := r_input;
#'                                b := 3;
#'                                c := abs(sqrt(14 + a));
#'                                d := a + b + c;
#'                                e := c + d / a;
#'                                f := e - d;
#'                                g := -f;
#'                                test := a * b + c / a;')
#'  
#'   vtlListSessions()
#' }
vtlListSessions <- function(){
  return(VTLSessionManager$list())
}

#' @title Kill sessions
#' @description Close and remove specified sessions from the VTL engine
#' @usage vtlKillSessions(sessions)
#' @param sessions or vector of names of the sessions to be removed 
#' @details This function is used to remove sessions from the engine.
#' @export
#' @examples \dontrun{
#'   vtlAddStatements(session = 'test2', restartSession = T,
#'                   statements = 'a := r_input;
#'                                b := 3;
#'                                c := abs(sqrt(14 + a));
#'                                d := a + b + c;
#'                                e := c + d / a;
#'                                f := e - d;
#'                                g := -f;
#'                                test := a * b + c / a;')
#'  
#'   vtlListSessions()
#'   vtlKillSessions('test2')
#'   vtlListSessions()
#' }
vtlKillSessions <- function(sessions){
  VTLSessionManager$kill(sessions)
}
