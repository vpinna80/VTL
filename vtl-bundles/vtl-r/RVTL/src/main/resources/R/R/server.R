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

repoImpls <- c(
  `In-Memory repository` = 'it.bancaditalia.oss.vtl.impl.meta.InMemoryMetadataRepository',
  `CSV file repository` = 'it.bancaditalia.oss.vtl.impl.meta.CSVMetadataRepository',
  `Json URL repository` = 'it.bancaditalia.oss.vtl.impl.meta.json.JsonMetadataRepository',
  `SDMX REST Metadata repository` = 'it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXRepository'
)

environments <- list(
  `CSV Path environment` = "it.bancaditalia.oss.vtl.impl.environment.CSVPathEnvironment",
  `SDMX environment` = "it.bancaditalia.oss.vtl.impl.environment.SDMXEnvironment",
  `R Environment` = "it.bancaditalia.oss.vtl.impl.environment.REnvironment",
  `Spark environment` = "it.bancaditalia.oss.vtl.impl.environment.spark.SparkEnvironment",
  `In-Memory environment` = "it.bancaditalia.oss.vtl.impl.environment.WorkspaceImpl"
)

currentEnvironments <- function() {
  sapply(J("it.bancaditalia.oss.vtl.config.VTLGeneralProperties")$ENVIRONMENT_IMPLEMENTATION$getValues(), .jstrVal)
}

activeEnvs <- function(active) {
  items <- names(environments[xor(!active, environments %in% currentEnvironments())])
  if (length(items) > 0) items else NULL
}

vtlServer <- function(input, output, session) {

  configManager <- J("it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory")
  vtlProperties <- J("it.bancaditalia.oss.vtl.config.VTLGeneralProperties")

  currentSession <- reactive(VTLSessionManager$find(input$sessionID)) |> bindEvent(input$sessionID)
  evalNode <- reactive(currentSession()$getValues(input$selectDatasets)) |> bindEvent(input$selectDatasets)
  isCompiled <- reactiveVal(F)
  
  # Upload vtl script button
  output$saveas <- downloadHandler(
    filename = function() {
      req(input$sessionID)
      paste0(isolate(input$sessionID), ".vtl")
    }, content = function (file) {
      writeLines(currentSession()$text, file)
    }
  )

  # Repository Properties box
  output$repoProperties <- renderUI({
    ctrls <- lapply(configManager$getSupportedProperties(J(input$repoClass)@jobj), function (prop) {
      val <- prop$getValue()
      if (val == 'null')
        val <- ''

      if (prop$isPassword()) {
        passwordInput(prop$getName(), prop$getDescription(), val)
      } else {
        textInput(prop$getName(), prop$getDescription(), val, placeholder = prop$getPlaceholder())
      }
    })
    
    do.call(tagList, ctrls)
  }) |> bindEvent(input$repoClass)

  # configure and change repository
  observe({
    output$eng_conf_output <- renderPrint({
      lapply(configManager$getSupportedProperties(J(req(input$repoClass))@jobj), function (prop) {
        val <- input[[prop$getName()]]
        if (val == '')
          val <- NULL
        prop$setValue(val)
        
        if (prop$isPassword()) {
          cat("Set property", prop$getDescription(), "to <masked value>\n")
        } else {
          cat("Set property", prop$getDescription(), "to", val, "\n")
        }
      })
      
      vtlProperties$METADATA_REPOSITORY$setValue(req(input$repoClass))
      cat("Set metadata repository to", input$repoClass, "\n")
    })
  }) |> bindEvent(input$setRepo)

  # Initially populate environment list and load properties
  envlistdone <- observe({
    defaultRepository <- J("it.bancaditalia.oss.vtl.config.VTLGeneralProperties")$METADATA_REPOSITORY$getValue()
    updateSelectInput(inputId = 'selectEnv', choices = unlist(environments))
    updateSelectInput(inputId = 'repoClass', choices = repoImpls, selected = defaultRepository)
    output$sortableEnvs <- renderUI({
      sortable::bucket_list(header = NULL, orientation = 'horizontal',
        sortable::add_rank_list(text = "Available", labels = activeEnvs(F)),
        sortable::add_rank_list(input_id = "envs", text = "Active", labels = activeEnvs(T)),
      )
    })
    # Single execution only when VTL Studio starts
    envlistdone$destroy()
  })
  
  # Environment properties list
  output$envprops <- renderUI({
    ctrls <- lapply(configManager$getSupportedProperties(J(input$selectEnv)@jobj), function (prop) {
      val <- prop$getValue()
      if (val == 'null')
        val <- ''
      
      if (prop$isPassword()) {
        passwordInput(prop$getName(), prop$getDescription(), val)
      } else {
        textInput(prop$getName(), prop$getDescription(), val, placeholder = prop$getPlaceholder())
      }
    })
    
    do.call(tagList, ctrls)
  }) |> bindEvent(input$selectEnv, ignoreInit = T)

  # save environment properties
  observe({
    output$eng_conf_output <- renderPrint({
      lapply(configManager$getSupportedProperties(J(input$selectEnv)@jobj), function (prop) {
        val <- input[[prop$getName()]]
        if (val == '')
          val <- .jnull(class = "java/lang/String")
        prop$setValue(val)
        
        if (prop$isPassword()) {
          cat("Set property", prop$getDescription(), "to <masked value>\n")
        } else {
          cat("Set property", prop$getDescription(), "to", val, "\n")
        }
      })
      cat("Finished setting properties for", input$selectEnv, "\n")
    })
  }) |> bindEvent(input$saveenvprops)
  
  # Save user configuration
  observe({
    propfile = paste0(J("java.lang.System")$getProperty("user.home"), '/.vtlStudio.properties')
    writer = .jnew("java.io.FileWriter", propfile)
    tryCatch({
      configManager$getInstance()$saveConfiguration(writer)
    }, finally = {
      writer$close()
    })
  }) |> bindEvent(input$saveconf)
  
  # Select dataset to browse
  output$dsNames<- renderUI({
    selectInput(inputId = 'selectDatasets', label = 'Select Node', multiple = F, 
                choices = c('', sort(unlist(currentSession()$getNodes()))), selected ='')
  })

  # render the structure of a dataset
  output$dsStr<- DT::renderDataTable({
    req(input$sessionID)
    req(input$structureSelection)
    jstr = currentSession()$getMetadata(req(input$structureSelection))
    if (jstr %instanceof% "it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata") {
      df <- data.table::transpose(data.frame(c(jstr$getDomain()$toString()), check.names = FALSE))
      colnames(df) <- c("Domain")
      df
    } else if (jstr %instanceof% "it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder$DataStructureImpl") {
      df <- data.table::transpose(data.frame(lapply(jstr, function(x) { c(x$getName(), x$getDomain()$toString(), x$getRole()$getSimpleName()) } ), check.names = FALSE))
      colnames(df) <- c("Name", "Domain", "Role")
      df
    } else {
      data.frame(c("ERROR", check.names = FALSE))
    }
  })
  
  # output dataset lineage 
  output$lineage <- networkD3::renderSankeyNetwork({
    req(input$sessionID)
    req(input$selectDatasets)
    edges <- currentSession()$getLineage(input$selectDatasets)
    vertices <- data.frame(name = unique(c(as.character(edges[,'source']), as.character(edges[,'target']))), stringsAsFactors = F)
    edges[, 'source'] <- match(edges[, 'source'], vertices[, 'name']) - 1
    edges[, 'target'] <- match(edges[, 'target'], vertices[, 'name']) - 1
    graph <- networkD3::sankeyNetwork(Links = edges, Nodes = vertices, Source = 'source', 
                                      Target = 'target', Value = 'value', NodeID = 'name', 
                                      nodeWidth = 40, nodePadding = 20, fontSize = 10)
    return(graph)
  })
  
  # output VTL result  
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
    session$sendCustomMessage("editor-theme", input$editorTheme)
  }) |> bindEvent(input$editorTheme)
  
  # Change editor font size
  observe({
    session$sendCustomMessage("editor-fontsize", input$editorFontSize)
  }) |> bindEvent(input$editorFontSize)
  
  # switch VTL session
  observe({
    vtlSession <- currentSession()
    name <- vtlSession$name
    isCompiled(vtlSession$isCompiled())
    #update list of datasets to be explored
    session$sendCustomMessage("editor-text", vtlSession$text)
    session$sendCustomMessage("editor-focus", message = '')
  })

  observeEvent(input$envs, {
    vtlProperties$ENVIRONMENT_IMPLEMENTATION$setValue(paste0(unlist(environments[req(input$envs)]), collapse = ","))
  })
    
  # load vl script
  observe({
    lines = suppressWarnings(readLines(input$scriptFile$datapath))
    lines = paste0(lines, collapse = '\n')
    vtlSession <- VTLSessionManager$getOrCreate(input$scriptFile$name)$setText(lines)
    isCompiled(vtlSession$isCompiled())
    #update current session
    updateSelectInput(session = session, inputId = 'sessionID', choices = VTLSessionManager$list(), selected = input$scriptFile$name)
  }) |> bindEvent(input$scriptFile)
  
  # load CSV file to GlobalEnv
  observeEvent(input$datafile, {
    datasetName = basename(input$datafile$name)
    data = readLines(con = input$datafile$datapath)
    if(length(data > 1)){
      header = as.character(utils::read.csv(text = data[1], header = F))
      ids1 = which(startsWith(header, prefix = '$'))
      ids2 = which(!startsWith(header, prefix = '#') & !grepl(x = header, pattern = '=', fixed = T))
      ids = c(ids1, ids2)
      measures = which(!startsWith(header, prefix = '#') & !startsWith(header, prefix = '$') & grepl(x = header, pattern = '=', fixed = T))
      names = sub(x=
                    sub(x = 
                          sub(x = header, pattern = '\\#', replacement = '')
                    , pattern = '\\$', replacement = '')
              , pattern = '\\=.*', replacement = '')
      body = utils::read.csv(text = data[-1], header = F, stringsAsFactors = F)
      body = stats::setNames(object = body, nm = names)
      
      #type handling very raw for now, to be refined
      # force strings (some cols could be cast to numeric by R)
      stringTypes = which(grepl(x = header, pattern = 'String', fixed = T))
      body[, stringTypes] = as.character(body[, stringTypes])
      
      attr(x = body, which = 'identifiers') = names[ids]
      attr(x = body, which = 'measures') = names[measures]
      assign(x = datasetName, value = body, envir = .GlobalEnv)
      output$vtl_output <- renderPrint({
        cat(paste('File ', input$datafile$name, 'correctly loaded into R Environment. Dataset name to be used:', datasetName, '\n'))
      })
    }
    else{
      output$vtl_output <- renderPrint({
        message('Error: file ', input$datafile$name, ' is malformed.\n')
      })
    }
  })
  
  # create new session
  observe({
    name <- req(input$newSession)
    vtlSession <- VTLSessionManager$getOrCreate(name)
    isCompiled(vtlSession$isCompiled())
    updateSelectInput(session = session, inputId = 'sessionID', choices = VTLSessionManager$list(), selected = name)
    updateTextInput(session = session, inputId = 'newSession', value = '')
  }) |> bindEvent(input$createSession)
  
  # duplicate session
  observe({
    newName <- req(input$newSession)
    text <- currentSession()$text
    newSession <- VTLSessionManager$getOrCreate(newName)
    newSession$setText(text)
    isCompiled(newSession$isCompiled())
    updateTextInput(session = session, inputId = 'newSession', value = '')
    updateSelectInput(session = session, inputId = 'sessionID', choices = VTLSessionManager$list(), selected = newName)
  }) |> bindEvent(input$dupSession)
  
  # compile VTL code
  observe({
    shinyjs::disable("compile")
    vtlSession <- currentSession()
    statements <- input$vtlStatements
    withProgress(message = 'Compiling current session', value = 0, tryCatch({ 
      vtlSession$addStatements(statements, restart = T) 
      setProgress(value = 0.5)
      vtlSession$compile()
      isCompiled(T)
      shinyjs::html("vtl_output", cat("Compilation successful.\n"))
      # Update force network
      output$topology <- networkD3::renderForceNetwork(vtlSession$getTopology(distance = input$distance))
      #update list of datasets to be explored
      updateSelectInput(session = session, inputId = 'selectDatasets',
                        label = 'Select Node', choices = c('', vtlSession$getNodes()), selected ='')
      #update list of dataset structures
      updateSelectInput(inputId = 'structureSelection', label = 'Select Node', choices = c('', sort(unlist(vtlSession$getNodes()))), selected ='')
    }, error = function(e) {
      msg <- conditionMessage(e)
      trace <- NULL
      if (is.list(e) && !is.null(e[['jobj']]))
      {
        writer <- .jnew("java.io.StringWriter")
        e$jobj$printStackTrace(.jnew("java.io.PrintWriter", .jcast(writer, "java/io/Writer")))
        trace <- writer$toString()
        msg <- e$jobj$getLocalizedMessage()
      }
      shinyjs::html("vtl_output", paste0('<span style="color: red">Error during compilation: ', 
        msg, '\n', if (is.null(trace)) '' else trace, '\n</span>')
      )
    }, finally = {
      setProgress(value = 1)
      shinyjs::enable("compile")
    }))
  }) |> bindEvent(input$compile)

  # configure proxy
  observeEvent(input$setProxy, {
    req(input$proxyHost)
    req(input$proxyPort)
    output$conf_output <- renderPrint({
      print(paste('Setting proxy', input$proxyHost, ':', input$proxyPort))
      J('it.bancaditalia.oss.sdmx.util.Configuration')$setDefaultProxy(input$proxyHost, input$proxyPort, input$proxyUser, input$proxyPassword)
      J("java.lang.System")$setProperty("http.proxyHost", input$proxyHost)
      J("java.lang.System")$setProperty("https.proxyHost", input$proxyHost)
      J("java.lang.System")$setProperty("http.proxyPort", input$proxyPort)
      J("java.lang.System")$setProperty("https.proxyPort", input$proxyPort)
      print('OK.')
    })
  })
  
  observeEvent(input$editorText, {
    currentSession()$setText(req(input$editorText))
  })

  output$datasetsInfo <- renderUI({
    req(input$selectDatasets)
    statements <- currentSession()$getStatements()
    statements <- sapply(statements$entrySet(), function (x) stats::setNames(list(x$getValue()), x$getKey()))
    ddf = evalNode()[[1]]
    formula <- statements[[input$selectDatasets]]
    
    return(tags$div(
      tags$p(tags$span("Node"), tags$span(input$selectDatasets), tags$span(paste("(", nrow(ddf), "by", ncol(ddf), ")"))),
      tags$p(tags$span("Rule:"), ifelse(test = is.null(formula), no = formula, yes = 'Source data'))
    ))
  })
}
