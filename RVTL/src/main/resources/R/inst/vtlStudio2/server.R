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
  ###### reactve functions
  ######
  
  evalNode <- reactive({
    req(input$sessionID)
    req(input$selectDatasets)
    return(vtlEvalNodes(sessionID = input$sessionID, node = input$selectDatasets))
  })
  ######
  ###### end reactive functions
  ######
  
  #####
  ##### Dynamic Input controls
  #####
  output$selectSession <- renderUI({
    selectInput(inputId = 'sessionID', label = 'Session ID', multiple = F, choices = c('', vtlListSessions()), selected ='')
    
  })
  
  output$dsNames<- renderUI({
    req(input$sessionID)
    selectInput(inputId = 'selectDatasets', label = 'Select Node', multiple = F, choices = c('', vtlListNodes(input$sessionID)), selected ='')
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
  
  #VTL Editor
  output$vtl<-renderUI(
    tags$head(HTML('<script src="bundle.js" type="text/javascript"></script>
                    <script src="main.js" type="text/javascript"></script>
                    <link rel="stylesheet" type="text/css" href="codemirror-icons.css"></link>
                    <link rel="stylesheet" type="text/css" href="codemirror-editor.css"> </link>
                    <link rel="stylesheet" type="text/css" href="codemirror-all-themes.css" ></link>
                               
                    <script>Shiny.addCustomMessageHandler(\'editor-text\', function(text) {
                    vtl.editor.editorImplementation.setValue(text)
                  });
                    </script>
                   '))
  )
  
  output$compileBtn<- renderUI({
    actionButton(inputId = 'compile', label = 'Compile and Update Current Session', onClick='Shiny.setInputValue("vtlStatements", vtl.editor.editorImplementation.getValue());')
  })

  #####
  ##### End Dynamic Input controls
  #####
  
  #####
  ##### Output widgets
  #####
  
  # switch VTL session
  observeEvent(input$sessionID, {
    req(input$sessionID)
    if(input$sessionID %in% vtlListSessions()){
      name = isolate(input$sessionID)
      vtlSession = RVTL:::vtlGetSession(name)
      session$sendCustomMessage("editor-text", vtlSession$text)
      #update graph
      output$topology <- renderForceNetwork({
        vtlTopology(session = name, distance = input$distance)
      })
      #update list of datasets to be explored
      updateSelectInput(session = session, inputId = 'selectDatasets',
                        label = 'Select Node', choices = c('', vtlSession$getNodes()), selected ='')
    }
  })
  
  # load vl script
  observeEvent(input$scriptFile, {
    lines = suppressWarnings(readLines(input$scriptFile$datapath))
    lines = paste0(lines, collapse = '\n')
    #update current session
    updateSelectInput(session = session, inputId = 'sessionID', choices = c(input$scriptFile$name, vtlListSessions()), selected = input$scriptFile$name)
    #update editor
    session$sendCustomMessage("editor-text", lines)
  })
  
  # create new session
  observeEvent(input$createSession, {
    req(input$newSession)
    updateSelectInput(session = session, inputId = 'sessionID', choices = c(input$newSession, vtlListSessions()), selected = input$newSession)
    #update editor
    session$sendCustomMessage("editor-text", '')
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
  
  #compile VTL code (action button)
  observeEvent(input$compile, {
    req(isolate(input$sessionID))
    output$vtl_output <- renderPrint({
      name = isolate(input$sessionID)
      statements = isolate(input$vtlStatements)
      print(name)
      print(statements)
      statusAdd = vtlAddStatements(sessionID = name,
                       statements = statements,
                       restartSession = T)
      if(statusAdd){
        RVTL:::vtlTryCatch({
          vtlSession = RVTL:::vtlGetSession(name)
          vtlSession$compile()
          print("Compilation successful")
          #update graph
          output$topology <- renderForceNetwork({
            vtlSession$getTopology(distance = input$distance)
          })
          #update list of datasets to be explored
          updateSelectInput(session = session, inputId = 'selectDatasets',
                            label = 'Select Node', choices = c('', vtlSession$getNodes()), selected ='')
        })
      }
      return(invisible())
    })
  })

  output$datasetsInfo <- renderText({
    req(input$sessionID)
    req(input$selectDatasets)
    text = ''
    nodes = evalNode()
    if(length(nodes) > 0){
      ddf = nodes[[1]]
      nrows = nrow(ddf)
      ncols = ncol(ddf)
      statements = vtlListStatements(input$sessionID)
      formula = statements[[input$selectDatasets]]
      timing = attr(ddf, 'evalTime')
      text = paste("<br><b>Node:</b>", input$selectDatasets, " (", nrows, " x ", ncols, ")","<br><b>Formula:</b> '",ifelse(test = is.null(formula), no = formula, yes = 'PRIMITIVE NODE'), "'<br><b>Evaluation time:</b> ", timing, "<br><br>")
    }
    return(text)
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
