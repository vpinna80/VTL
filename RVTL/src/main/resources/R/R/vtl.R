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

vtlStudio <- function(...) {
  Sys.setenv(SDMX_CONF='C:\\Users\\m027907\\eclipse-workspace\\configuration.properties')
  shiny::runApp(appDir = system.file('vtlStudio2', package='RVTL'), ...)  
}

vtlListSessions <- VTLSessionManager$list

vtlAddStatements <- function(sessionID, statements, restartSession = F) {
  if(restartSession) {
    VTLSessionManager$kill(sessionID)
  }

  session = VTLSessionManager$getOrCreate(sessionID)$addStatements(statements)
  print('Statements added')
  return(session)
}

vtlEvalNodes <- function(sessionID, nodes) {
  VTLSessionManager$find(sessionID)$getValues(nodes)
}

vtlListStatements <- function(sessionID) {
  jstatements = VTLSessionManager$find(sessionID)$getStatements()
  return(sapply(jstatements$entrySet(), function (x) setNames(list(x$getValue()), x$getKey())))
}

vtlListNodes <- function(sessionID){
  return(VTLSessionManager$find(sessionID)$getNodes())
}

vtlGetCode <- function(sessionID) {
  return(VTLSessionManager$find(sessionID)$text)
}

vtlTopology <- function(session, distance =100, charge = -100) {
  return(VTLSessionManager$find(session)$getTopology(distance, charge))
}

vtlCompile <- function(sessionID) {
  result <- vtlTryCatch({
    VTLSessionManager$find(sessionID)$compile()
    print('Compilation successful!')
    return(T)
  })
  return(result)
}

