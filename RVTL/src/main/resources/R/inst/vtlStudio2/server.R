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

# Main class for consuming SDMX web services
#
# Author: Attilio Mattiocco
###############################################################################

shinyServer(function(input, output, session) {
  
  ######
  ###### reactive functions
  ######
  
  currentSession <- reactive({
    req(input$sessionID)
    VTLSessionManager$find(input$sessionID)
  })

  evalNode <- reactive({
    return(currentSession()$getValues(req(input$selectDatasets)))
  })

  isCompiled <- reactiveVal(F)
  
  ######
  ###### end reactive functions
  ######
  
  #####
  ##### Dynamic Input controls
  #####

  output$saveas <- downloadHandler(
      filename = function() {
        req(input$sessionID)
        paste0(isolate(input$sessionID), ".vtl")
      }, content = function (file) {
        writeLines(currentSession()$text, file)
      })

  output$dsNames<- renderUI({
    selectInput(inputId = 'selectDatasets', label = 'Select Node', multiple = F, 
                choices = c('', currentSession()$getNodes()), selected ='')
  })

  output$proxyHostUI<- renderUI({
    host = ''
    proxy = J('it.bancaditalia.oss.sdmx.util.Configuration')$getConfiguration()$getProperty('http.proxy.default')
    if(!is.null(proxy) && nchar(proxy) > 0){
      host = unlist(strsplit(proxy, split = ':'))
      if(length(host) == 2 ){
        host = host[1]
      }
    }
    textInput(inputId = 'proxyHost', label = 'Proxy Host', value = host)
  })

  output$proxyPortUI<- renderUI({
    port = ''
    proxy = J('it.bancaditalia.oss.sdmx.util.Configuration')$getConfiguration()$getProperty('http.proxy.default')
    if(!is.null(proxy) && nchar(proxy) > 0){
      port = unlist(strsplit(proxy, split = ':'))
      if(length(port) == 2 ){
        port = port[2]
      }
    }
    textInput(inputId = 'proxyPort', label = 'Proxy Port', value = port)
  })
  
  output$proxyUserUI<- renderUI({
    user = ''
    proxy = J('it.bancaditalia.oss.sdmx.util.Configuration')$getConfiguration()$getProperty('http.auth.user')
    textInput(inputId = 'proxyUser', label = 'Proxy User', value = user)
  })
  
  #compile VTL code (action button)
  output$vtl_output <- renderPrint({
    req(input$compile, cancelOutput = F)
    shinyjs::disable("compile")
    try({
      vtlSession <- currentSession()
      statements <- input$vtlStatements
      print(vtlSession$name)
      print(statements)
      withProgress(message = 'Compiling current session', value = 0, {
        tryCatch({ 
            vtlSession$addStatements(statements, restart = T) 
            setProgress(value = 0.5)
            tryCatch({ 
              vtlSession$compile()
              setProgress(value = 1)
              print("Compilation successful")
              isCompiled(T)
            }, error = function(e) {
              message = conditionMessage(e)
              if (is.list(e) && !is.null(e[['jobj']]))
              {
                e$jobj$printStackTrace()
                message = e$jobj$getLocalizedMessage()
              }
              setProgress(value = 1)
              print(paste0("Compilation error: ", message))
            })
          }, error = function(e) {
            message = conditionMessage(e)
            if (is.list(e) && !is.null(e[['jobj']]))
            {
              e$jobj$printStackTrace()
              message = e$jobj$getLocalizedMessage()
            }
            setProgress(value = 1)
            print(paste0("Syntax error: ", message))
          })
      })
      
      # Update force network
      output$topology <- networkD3::renderForceNetwork({
        vtlSession$getTopology(distance = input$distance)
      })
      #update list of datasets to be explored
      updateSelectInput(session = session, inputId = 'selectDatasets',
                        label = 'Select Node', choices = c('', vtlSession$getNodes()), selected ='')
    })
    shinyjs::enable("compile")
    return(invisible())
  })
  
  #####
  ##### End Dynamic Input controls
  #####
  
  #####
  ##### Output widgets
  #####

  # Disable buttons to create sessions
  observe({
    shinyjs::toggleState("createSession", isTruthy(input$newSession))
    shinyjs::toggleState("dupSession", isTruthy(input$newSession))
  })

  # Disable proxy button if host and port not specified
  observe({
    shinyjs::toggleState("setProxy", isTruthy(input$proxyHost) && isTruthy(input$proxyPort))
  })
  
  # Disable navigator and graph if the session was not compiled
  observe({
    shinyjs::toggleCssClass(selector = ".nav-tabs li:nth-child(2)", class = "tab-disabled", condition = !isCompiled())
    shinyjs::toggleCssClass(selector = ".nav-tabs li:nth-child(3)", class = "tab-disabled", condition = !isCompiled())
    if (isCompiled()) {
      output$topology <- networkD3::renderForceNetwork({
        currentSession()$getTopology(distance = input$distance)
      })
      #update list of datasets to be explored
      updateSelectInput(session = session, inputId = 'selectDatasets', label = 'Select Node', 
                        choices = c('', currentSession()$getNodes()), selected ='')
    } 
  })

  # Change editor theme
  observe({
    session$sendCustomMessage("editor-theme", req(input$editorTheme))
  })
  
  # Change editor font size
  observe({
    session$sendCustomMessage("editor-fontsize", req(input$editorFontSize))
  })
  
  # switch VTL session
  observe({
    vtlSession <- currentSession()
    name <- vtlSession$name
    isCompiled(vtlSession$isCompiled())
    #update list of datasets to be explored
    session$sendCustomMessage("editor-text", vtlSession$text)
    session$sendCustomMessage("editor-focus", message = '')
  })
  
  # load vl script
  observeEvent(input$scriptFile, {
    lines = suppressWarnings(readLines(input$scriptFile$datapath))
    lines = paste0(lines, collapse = '\n')
    vtlSession <- VTLSessionManager$getOrCreate(input$scriptFile$name)$setText(lines)
    isCompiled(vtlSession$isCompiled())
    #update current session
    updateSelectInput(session = session, inputId = 'sessionID', choices = VTLSessionManager$list(), selected = input$scriptFile$name)
  })
  
  # create new session
  observeEvent(input$createSession, {
    name <- req(input$newSession)
    vtlSession <- VTLSessionManager$getOrCreate(name)
    isCompiled(vtlSession$isCompiled())
    updateSelectInput(session = session, inputId = 'sessionID', choices = VTLSessionManager$list(), selected = name)
    updateTextInput(session = session, inputId = 'newSession', value = '')
  })
  
  # duplicate session
  observeEvent(input$dupSession, {
    newName <- req(input$newSession)
    text <- currentSession()$text
    newSession <- VTLSessionManager$getOrCreate(newName)
    newSession$setText(text)
    isCompiled(newSession$isCompiled())
    updateTextInput(session = session, inputId = 'newSession', value = '')
    updateSelectInput(session = session, inputId = 'sessionID', choices = VTLSessionManager$list(), selected = newName)
  })
  
  # configure proxy
  observeEvent(input$setProxy, {
    req(input$proxyHost)
    req(input$proxyPort)
    output$conf_output <- renderPrint({
      isolate(expr = {
        J('it.bancaditalia.oss.sdmx.util.Configuration')$setDefaultProxy(input$proxyHost, input$proxyPort, input$proxyUser, input$proxyPassword)
      })
      print('OK, done.')
    })
  })
  
  observeEvent(input$editorText, {
    currentSession()$setText(req(input$editorText))
  })

  output$datasetsInfo <- renderUI({
    req(input$selectDatasets)
    statements <- currentSession()$getStatements()
    statements <- sapply(statements$entrySet(), function (x) setNames(list(x$getValue()), x$getKey()))
    ddf = evalNode()[[1]]
    formula <- statements[[input$selectDatasets]]
    
    return(tags$div(
      tags$p(tags$span("Node"), tags$span(input$selectDatasets), tags$span(paste("(", nrow(ddf), "by", ncol(ddf), ")"))),
      tags$p(tags$span("Rule:"), ifelse(test = is.null(formula), no = formula, yes = 'Source data'))
    ))
  })

  output$datasets <- DT::renderDataTable({
    req(input$sessionID)
    req(input$selectDatasets)
    req(input$maxlines)
    maxlines = as.integer(input$maxlines)
    result = NULL
    nodes = evalNode()
    if(length(nodes) > 0){
      ddf = nodes[[1]]
      if(ncol(ddf) >= 1 && names(ddf)[1] != 'Scalar'){
        linesLimit = ifelse(nrow(ddf) > maxlines , yes = maxlines, no = nrow(ddf))
        ddf = ddf[1:linesLimit,]
        #not a scalar, order columns and add component role
        neworder = which(names(ddf) %in% attr(ddf, 'measures'))
        neworder = c(neworder, which(names(ddf) %in% attr(ddf, 'identifiers')))
        if(input$showAttrs){
          neworder = c(neworder, which(!(1:ncol(ddf) %in% neworder)))
        }
        
        names(ddf) = sapply(names(ddf), function(x, ddf) {
          if(x %in% attr(ddf, 'identifiers')){
            return(paste0(x, ' (', 'I', ') '))
          }
          else if(x %in% attr(ddf, 'measures')){
            return(paste0(x, ' (', 'M', ') '))
          }
          else{
            return(x)
          }
        }, ddf)
        
        if(ncol(ddf) > 1){
          result = ddf[,neworder]
        }
        
      }
      else{
        result = ddf
      }
    }
    return(result)
  }, options = list(
    lengthMenu = list(c(50, 1000, -1), c('50', '1000', 'All')),
    pageLength = 10
  ))

######
######  End output widgets
######
  
})
