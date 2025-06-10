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

library(RVTL)

repoImpls <- c(
  `In-Memory repository` = 'it.bancaditalia.oss.vtl.impl.meta.InMemoryMetadataRepository',
  `Json URL repository` = 'it.bancaditalia.oss.vtl.impl.meta.json.JsonMetadataRepository',
  `SDMX REST Metadata repository` = 'it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXRepository',
  `SDMX REST & Json combined repository` = 'it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXJsonRepository'
)

environments <- c(
  `R Environment` = "it.bancaditalia.oss.vtl.impl.environment.REnvironment"
  , `CSV environment` = "it.bancaditalia.oss.vtl.impl.environment.CSVPathEnvironment"
  , `SDMX environment` = "it.bancaditalia.oss.vtl.impl.environment.SDMXEnvironment"
#  , `Spark environment` = "it.bancaditalia.oss.vtl.impl.environment.spark.SparkEnvironment"
)

configManager <- J("it.bancaditalia.oss.vtl.config.ConfigurationManager")
exampleEnv <- J("it.bancaditalia.oss.vtl.util.VTLExamplesEnvironment")

vtlProps <- list(
  ENVIRONMENT_IMPLEMENTATION = J("it.bancaditalia.oss.vtl.config.VTLGeneralProperties")$ENVIRONMENT_IMPLEMENTATION,
  METADATA_REPOSITORY = J("it.bancaditalia.oss.vtl.config.VTLGeneralProperties")$METADATA_REPOSITORY
)

globalEnvs <- function(newenvs = NULL) {
  if (is.null(newenvs))
    return(sapply(configManager$getGlobalPropertyValues(vtlProps$ENVIRONMENT_IMPLEMENTATION), .jstrVal))
  else
    configManager$setGlobalPropertyValue(vtlProps$ENVIRONMENT_IMPLEMENTATION, newenvs)
}

globalRepo <- function(newrepo = NULL) {
  if (is.null(newrepo))
    return(configManager$getGlobalPropertyValue(vtlProps$METADATA_REPOSITORY))
  else
    configManager$setGlobalPropertyValue(vtlProps$METADATA_REPOSITORY, newrepo)
}

activeEnvs <- function(active) {
  items <- names(environments[xor(!active, environments %in% globalEnvs())])
  if (length(items) > 0) items else NULL
}

makeButton <- \(id) tags$button(
  class = "btn btn-sm btn-link float-end",
  `data-bs-toggle` = "collapse",
  `data-bs-target` = paste0('#', id),
  icon('minus')
)

sanitize <- \(x) gsub("[^A-Za-z0-9_]", "_", x)

volumeRoots <- shinyFiles::getVolumes()()

makeUIElemName <- \(vtlSession) \(name) sanitize(paste(vtlSession, name, sep = '_'))

# Generate controls for VTL properties possibly with id manipulation
controlsGen <- function(javaclass, getValue = configManager$getGlobalPropertyValue, makeID = sanitize) {
  ctrls <- lapply(configManager$getSupportedProperties(J(javaclass)@jobj), function (prop) {
    val <- getValue(prop)
    if (val == 'null')
      val <- ''
    inputID <- makeID(prop$getName())
    
    if (prop$isPassword()) {
      passwordInput(inputID, prop$getDescription(), val)
    } else if (grepl('path', inputID)) {
      inputIDf <- paste0(inputID, '_f')
      tags$div(style = "display: flex; gap: 6px; align-items: flex-end;",
        textInput(inputID, prop$getDescription(), val, placeholder = prop$getPlaceholder()),
        tags$div(class = 'form-group shiny-input-container', style = "align-self: end;",
          shinyFiles::shinyDirButton(inputIDf, '', "Folder chooser", icon = icon('folder-open'), style = "color: yellow")
        )
      )
    } else {
      textInput(inputID, prop$getDescription(), val, placeholder = prop$getPlaceholder())
    }
  })
  
  do.call(tagList, ctrls)
}

createPanel <- function(vtlSession) {
  makeID <- makeUIElemName(isolate(vtlSession))
  newEditorID <- makeID("editor")
  theme <- getCurrentTheme()
  bslib::nav_panel(value = vtlSession, 
    title = tags$span(onclick = "addEditorMenu(this, event)",
      vtlSession, class = "with-editor", id = makeID("pane")
    ), bslib::navset_hidden(id = makeID('sheets'),
      bslib::nav_panel_hidden(value = "editor",
        tags$div(id = newEditorID, class = 'vtlwell'),
        verbatimTextOutput(makeID('output'), placeholder = T),
        tags$script(HTML(sprintf("createEditorPanel('%s')", newEditorID)))
      ), bslib::nav_panel_hidden(value = "structures",
        fluidRow(
          column(width=5, selectInput(makeID('structName'), 'Select structure:', c(''), ''))
        ),
        DT::dataTableOutput(makeID('dsStr'))
      ), bslib::nav_panel_hidden(value = "datasets",
        fluidRow(class = 'g-0',
          column(width=12, selectInput(makeID('datasetName'), 'Select dataset:', c(''), ''))
        ),
        hr(),
        tabsetPanel(id = makeID("dataview"), type = "tabs",
          tabPanel("Data points", value = "dptable",
            fluidRow(
              tags$span('Max Lines:'),
              numericInput(makeID('maxlines'), NULL, 1000, 1, 10000, 1),
              checkboxInput(makeID('showAttrs'), "Show Attributes", T)
            ),
            fluidRow(class = 'g-0',
              textOutput(makeID("datasetsInfo"))
            ),
            DT::dataTableOutput(makeID("dptable"))
          ),
          tabPanel("Lineage", value = "lineage", networkD3::sankeyNetworkOutput(makeID("lineage"), height = "100%"))
        )
      ), bslib::nav_panel_hidden(value = "graph",
        sliderInput(makeID('distance'), label = "Nodes distance", min=50, max=500, step=10, value=100),
        fillPage(networkD3::forceNetworkOutput(makeID('topology'), height = '90vh'))
      ), bslib::nav_panel_hidden(value = "settings",
        bslib::layout_column_wrap(width = 1/2,
          bslib::card(
            bslib::card_header('VTL Environments', makeButton(makeID('card_envs'))),
            bslib::card_body(id = makeID('card_envs'), class = 'collapse show', 
              sortable::bucket_list(header = NULL, orientation = 'horizontal',
                sortable::add_rank_list("Available", activeEnvs(F)),
                sortable::add_rank_list("Active", activeEnvs(T), makeID("envs"))
              )
            )
          ),
          bslib::card(
            bslib::card_header('Environment Properties', makeButton(makeID('card_envprops'))),
            bslib::card_body(id = makeID('card_envprops'), class = 'collapse show',
              selectInput(makeID('selectEnv'), NULL, c("Select an environment..." = "")),
              uiOutput(makeID("envprops"))
            )
          ),
          bslib::card(
            bslib::card_header('Metadata Repository', makeButton(makeID('card_meta'))),
            bslib::card_body(id = makeID('card_meta'), class = 'collapse show',
              selectInput(makeID('repoClass'), NULL, c("Select a repository..." = "")),
              uiOutput(makeID("repoProperties"))
            )
          ),
          bslib::card(
            bslib::card_header('Status', makeButton(makeID('card_status'))),
            bslib::card_body(id = makeID('card_status'), class = 'collapse show',
              verbatimTextOutput(makeID("confOutput"), placeholder = T)
            )
          )
        )
      )
    )
  )
}

# Patch for bslib losing theme version after prependTab
unlockBinding("getCurrentThemeVersion", asNamespace("bslib"))
assign("getCurrentThemeVersion", \() 5, envir = asNamespace("bslib"))
lockBinding("getCurrentThemeVersion", asNamespace("bslib"))

vtlServer <- function(input, output, session) {
  
  vtlSessions <- reactiveVal(NULL)
  isCompiled <- reactiveVal(F)
  currentSession <- reactiveVal(NULL)
  tabs <- reactiveVal(NULL)
  
  # Create startup tabs for each active session
  inittabs <- observe({
    vtlSessions(VTLSessionManager$list())
    inittabs$destroy()
  })
  
  # Monitor switching tabs to determine current session
  observe({
    if (input$navtab != "Global settings") {
      currentSession(VTLSessionManager$getOrCreate(input$navtab))
    }
  }) |> bindEvent(input$navtab)

  # Controls when running on shinyapps
  output$shinyapps <- renderUI({
    if (grepl("shinyapps.io$", session$clientData$url_hostname)) {
      tagList(
        fileInput(inputId = 'datafile', label = 'Load CSV', accept = 'csv'),
        tags$hr()
      )
    }
  })
    
  # Generate observers for a property of a VTL engine class
  # in a given VTL session or for global settings
  observersGen <- function(prop, makeID = sanitize, getValue = configManager$getGlobalPropertyValue, 
                          setValue = configManager$setGlobalPropertyValue) {
    inputID <- makeID(prop$getName())
    if (grepl('path', prop$getName())) {
      inputIDf <- paste0(inputID, '_f')
      # attach the navigator
      observe({
        shinyFiles::shinyDirChoose(input, inputIDf, roots = volumeRoots)
      }) |> bindEvent(T, once = T)
      # insert the chosen folder
      observe({
        dirPath <- shinyFiles::parseDirPath(volumeRoots, req(input[[inputIDf]]))
        updateTextInput(inputId = inputID, value = dirPath)
      }) |> bindEvent(input[[inputIDf]], ignoreInit = T)
    }
    observe({
      value <- req(input[[inputID]])
      setValue(prop, value)
      printValue <- if (prop$isPassword()) "<masked value>" else value
      output[[makeID('confOutput')]] <- renderPrint({
        cat("Set property", prop$getDescription(), "to", printValue, "\n")
      }) |> bindEvent(input[[inputID]])
    }) |> bindEvent(input[[inputID]])
  }

  # Proxy controls observers
  lapply(c('Host', 'Port', 'User', 'Password'), function (prop) {
    inputId <- paste0('proxy', prop)
    output$confOutput <- renderPrint({
      value <- req(input[[inputId]])
      if (value == '') {
        J("java.lang.System")$clearProperty(paste0("http.proxy", prop))
        J("java.lang.System")$clearProperty(paste0("https.proxy", prop))
        cat(paste('Unsetting Proxy', prop))
      } else {
        J("java.lang.System")$setProperty(paste0("http.proxy", prop), value)
        J("java.lang.System")$setProperty(paste0("https.proxy", prop), value)
        if (prop == 'Password') {
          cat(paste('Setting Proxy Password to <masked value>\n'))
        } else {
          cat(paste('Setting Proxy', prop, 'to', value, '\n'))
        }
      }
    }) |> bindEvent(input[[inputId]])
  })

  # Completes new tab creation
  observe({
    vtlSessions <- setdiff(unlist(vtlSessions()), tabs())
    req(length(vtlSessions) > 0)
    vtlSession <- vtlSessions[[1]]
    tabs(c(tabs(), vtlSession))
    makeID <- makeUIElemName(vtlSession)

    # Compilation status box
    output[[makeID('output')]] <- renderPrint(cat(' '))
    
    # Controls and observers for properties of selected environment in active session
    output[[makeID('envprops')]] <- renderUI({
      getValue <- VTLSessionManager$getOrCreate(vtlSession)$getProperty
      setValue <- VTLSessionManager$getOrCreate(vtlSession)$setProperty
      envClasses <- isolate(req(input[[makeID('selectEnv')]]))
      lapply(configManager$getSupportedProperties(J(req(input[[makeID('selectEnv')]]))@jobj), observersGen, 
             getValue = getValue, setValue = setValue, makeID = makeID)
      controlsGen(envClasses, getValue, makeID)
    }) |> bindEvent(input[[makeID('selectEnv')]], ignoreInit = T)
    
    # Controls and observers for properties of selected repository in active session
    output[[makeID('repoProperties')]] <- renderUI({
      getValue <- VTLSessionManager$getOrCreate(vtlSession)$getProperty
      setValue <- VTLSessionManager$getOrCreate(vtlSession)$setProperty
      repoClass <- isolate(req(input[[makeID('repoClass')]]))
      output[[makeID('confOutput')]] <- renderPrint({
        setValue(vtlProps$METADATA_REPOSITORY, repoClass)
        cat("Set metadata repository to", repoClass, "\n")
      })
      lapply(configManager$getSupportedProperties(J(repoClass)@jobj), observersGen,
             getValue = getValue, setValue = setValue, makeID = makeID)
      controlsGen(repoClass, getValue, makeID)
    }) |> bindEvent(input[[makeID('repoClass')]], ignoreInit = T)
    
    # Update active environments in active session
    output[[makeID('confOutput')]] <- renderPrint({
      envs <- req(input$envs)
      VTLSessionManager$getOrCreate(vtlSession)$setProperty(vtlProps$ENVIRONMENT_IMPLEMENTATION, paste0(environments[envs], collapse = ","))
      cat("Set active environments to:\n    - ", paste0(envs, collapse = "\n    - "), "\n")
    }) |> bindEvent(input[[makeID('envs')]])
    
    # dataset table display
    output[[makeID('datasetsInfo')]] <- renderPrint({
      datasetName <- req(input[[makeID('datasetName')]])
      session <- VTLSessionManager$getOrCreate(vtlSession)
      statements <- sapply(session$getStatements()$entrySet(), function (x) {
        stats::setNames(list(x$getValue()), x$getKey()$getName())
      })
      ddf <- session$getValues(datasetName)[[1]]
      formula <- get0(datasetName, as.environment(statements), ifnotfound = 'Source data')
      cat(datasetName, " := ", formula, " (", nrow(ddf), "by", ncol(ddf), ")")
    }) |> bindEvent(input[[makeID('datasetName')]])
    
    # Lineage display
    output[[makeID('lineage')]] <- networkD3::renderSankeyNetwork({
      browser()
      datasetName <- req(input[[makeID('datasetName')]])
      session <- VTLSessionManager$getOrCreate(vtlSession)
      links <- tryCatch({
        session$getLineage(datasetName)
      }, error = function(e) {
          if (is.null(e$jobj))
            e$jobj$printStackTrace()
          signalCondition(e)
        }
      )
      if (nrow(links) > 0) {
        nodes <- data.frame(name = unique(c(as.character(links[,'source']), as.character(links[,'target']))), stringsAsFactors = F)
        links[, 'source'] <- match(links[, 'source'], nodes[, 'name']) - 1
        links[, 'target'] <- match(links[, 'target'], nodes[, 'name']) - 1
        networkD3::sankeyNetwork(
          links, nodes, 'source', 'target', 'value', 'name',
          nodeWidth = 40, nodePadding = 20, fontSize = 10
        )
      }
      else
        shinyjs::alert(paste("Node", datasetName, "is a scalar or a source node."))
    }) |> bindEvent(input[[makeID('datasetName')]])

    output[[makeID('dptable')]] <- DT::renderDataTable({
      maxlines = as.integer(isolate(req(input[[makeID('maxlines')]])))
      selected <- req(input[[makeID('datasetName')]])
      ddf = VTLSessionManager$getOrCreate(vtlSession)$getValues(selected)[[1]]
      if (ncol(ddf) <= 1 || names(ddf)[1] == 'Scalar') {
        return (ddf)
      } else {
        linesLimit = ifelse(nrow(ddf) > maxlines , yes = maxlines, no = nrow(ddf))
        ddf = ddf[1:linesLimit,]
        # not a scalar, order columns and add component role
        neworder = which(names(ddf) %in% attr(ddf, 'measures'))
        neworder = c(neworder, which(names(ddf) %in% attr(ddf, 'identifiers')))
        if (input[[makeID('showAttrs')]]) {
          neworder = c(neworder, which(!(1:ncol(ddf) %in% neworder)))
        }
        
        names(ddf) = sapply(names(ddf), \(x) {
          if (x %in% attr(ddf, 'identifiers')) {
            return(paste0(x, ' (I)'))
          } else if (x %in% attr(ddf, 'measures')) {
            return(paste0(x, ' (M)'))
          } else {
            return(paste0(x, ' (A)'))
          }
        })
        
        if (ncol(ddf) > 1) {
          result = ddf[, neworder]
        }
      }
    }, options = list(
      lengthMenu = list(c(50, 1000, -1), c('50', '1000', 'All')),
      pageLength = 10
    )) |> bindEvent(input[[makeID('datasetName')]])
    
    # View data, structures and transformation graph for this session
    observe({
      switch (
        req(input[[makeID('sheets')]]),
        'structures' = {
          output[[makeID('dsStr')]] <- DT::renderDataTable({
            structure <- req(input[[makeID('structName')]])
            jstr = VTLSessionManager$getOrCreate(vtlSession)$getMetadata(structure)
            if (jstr %instanceof% "it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata") {
              df <- data.table::transpose(data.frame(c(jstr$getDomain()$toString()), check.names = FALSE))
              colnames(df) <- c("Domain")
              df
            } else if (jstr %instanceof% "it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder$DataStructureImpl") {
              df <- data.table::transpose(data.frame(lapply(jstr, function(x) {
                c(x$getVariable()$getAlias()$getName(), x$getVariable()$getDomain()$toString(), x$getRole()$getSimpleName())
              } ), check.names = FALSE))
              colnames(df) <- c("Name", "Domain", "Role")
              df
            } else {
              data.frame(c("ERROR", check.names = FALSE))
            }
          })
        }, 'graph' = {
          output[[makeID('topology')]] <- networkD3::renderForceNetwork({
            VTLSessionManager$getOrCreate(vtlSession)$getTopology(distance = input[[makeID('distance')]])
          })
        } |> bindEvent(input[[makeID('selectDatasets')]])
      )
    }) |> bindEvent(input[[makeID('sheets')]], ignoreInit = T)
    
    # Populate environments and repositories lists in session settings
    envlistdone <- observe({
      updateSelectInput(session, makeID('selectEnv'), NULL, environments)
      updateSelectInput(session, makeID('repoClass'), NULL, repoImpls, globalRepo())
      # Single execution only when the tab is first created
      envlistdone$destroy()
    })

    panel <- createPanel(vtlSession)
    prependTab("navtab", panel, T)
  }) |> bindEvent(vtlSessions(), tabs())
  
  # Update global active environments
  observe({
    output$confOutput <- renderPrint({
      envs <- req(input$envs)
      globalEnvs(paste0(environments[envs], collapse = ","))
      cat("Set active environments to:\n    -", paste0(envs, collapse = "\n    - "), "\n")
    }) |> bindEvent(input$envs)
  }) |> bindEvent(input$envs)

  # contextual menus for session tabs
  observe({
    json <- req(input$sessionMenu)
    vtlSession <- json$session
    if (json$menu == 'close') {
      VTLSessionManager$kill(vtlSession)
      removeTab("navtab", vtlSession)
    } else {
      bslib::nav_select(makeUIElemName(vtlSession)('sheets'), json$menu)
    }
  }) |> bindEvent(input$sessionMenu, ignoreInit = TRUE)
  
  # Download vtl script button
  output$saveas <- downloadHandler(
    filename = function() {
      paste0(isolate(req(currentSession())), ".vtl")
    }, content = function (file) {
      writeLines(currentSession()$text, file)
    }
  )

  # Toggle demo mode
  observe({
    demoindex <- as.numeric(isTruthy(input$demomode))
    fun <- list(shinyjs::addCssClass, shinyjs::removeCssClass)
    fun[[1 + demoindex]]('democtrl', 'd-none')
    fun[[2 - demoindex]]('normalctrl', 'd-none')
    
  	VTLSessionManager$clear()
  	for (tab in tabs())
  	  removeTab('navtab', tab)
	  tabs(NULL)
  	
  	if (isTRUE(input$demomode)) {
  	  vtlSessions(NULL)
	    categories <- c(
	      list(list(label = 'Select category...', value = '', categ = '')),
	      lapply(sapply(exampleEnv$getCategories(), .jstrVal), \(categ) list(label = categ, value = categ, categ = ''))
	    )
	    updateSelectizeInput(session, 'excat', 'Categories', NULL, options = list(
        render = I('{ option: generateLabel }'),
        options = categories
      ))
  	} else {
  	  sList <- VTLSessionManager$list()
  	  vtlSessions(sList)
  	}
  }) |> bindEvent(input$demomode)
  
  # Populate demo mode operators when selecting a category
  observe({
    operators <- c(
      list(list(label = 'Select operator...', value = '', categ = '')),
      lapply(sapply(exampleEnv$getOperators(isolate(req(input$excat))), .jstrVal), 
             \(categ) list(label = categ, value = categ, categ = input$excat))
    )
    updateSelectizeInput(session, 'exoper', 'Operators', NULL, options = list(
      render = I('{ option: generateLabel }'),
      options = operators
    ))
  }) |> bindEvent(input$excat)
  
  # TODO: examples for each operator are joined together in a single session
  # Update demo mode example number when selecting an operator
  observe({
    shinyjs::toggleState('exopen', isTruthy(input$exoper))
  }) |> bindEvent(input$exoper)
  
  observe({
    category <- isolate(req(input$excat))
    operator <- isolate(req(input$exoper))
    vtlSession <- VTLSessionManager$openExample(category, operator)
    vtlSessions(VTLSessionManager$list())
    session$sendCustomMessage("editor-text", list(panel = makeUIElemName(operator)('editor'), text = vtlSession$text))
  }) |> bindEvent(input$exopen)

  # load theme list
  observe({
    updateSelectInput(inputId = 'editorTheme', choices = input$themeNames)
  }) |> bindEvent(input$themeNames)
  
  # Initially populate environment list and load properties
  envlistdone <- observe({
    updateSelectInput(inputId = 'selectEnv', choices = environments)
    updateSelectInput(inputId = 'repoClass', choices = repoImpls, selected = globalRepo())
    # Single execution only when VTL Studio starts
    envlistdone$destroy()
  })

  # Apply configuration to the active session
  observe({
	  currentSession()$refresh()
  }) |> bindEvent(input$applyConfAll, ignoreInit = T)

  # Apply configuration to all active vtlSessions
  observe({
    VTLSessionManager$reload()
  }) |> bindEvent(input$applyConfAll, ignoreInit = T)

  # Environment properties list
  observe({
    env <- isolate(req(input$selectEnv))
    controls <- controlsGen(env)
    output$envprops <- renderUI(controls)
    lapply(configManager$getSupportedProperties(J(req(input$selectEnv))@jobj), observersGen)
  }) |> bindEvent(input$selectEnv, ignoreInit = T)

  # Repository properties list
  observe({
    repoClass <- isolate(req(input$repoClass))
    controls <- controlsGen(repoClass)
    output$repoProperties <- renderUI(controls)
    lapply(configManager$getSupportedProperties(J(repoClass)@jobj), observersGen)

    output$confOutput <- renderPrint({
      globalRepo(req(repoClass))
      cat("Set metadata repository to", repoClass, "\n")
    })
  }) |> bindEvent(input$repoClass, ignoreInit = T)

  # Save Configuration as...
  output$saveconfas <- downloadHandler(
    filename = ".vtlStudio.properties",
    content = function (file) {
      tryCatch({
        writer <- .jnew("java.io.StringWriter")
        configManager$saveGlobalConfiguration(writer)
        string <- .jstrVal(writer$toString())
        writeLines(string, file)
      }, error = function(e) {
        if (!is.null(e$jobj)) {
          e$jobj$printStackTrace()
        }
        stop(e)
      })
    }
  )

  # Disable buttons to create vtlSessions
  observe({
    shinyjs::toggleState("createSession", isTruthy(input$newSession))
    shinyjs::toggleState("dupSession", isTruthy(input$newSession))
  })
  
  # Disable proxy button if host and port not specified
  observe({
    shinyjs::toggleState("setProxy", isTruthy(input$proxyHost) && isTruthy(input$proxyPort))
  })
  
  # Change editor theme
  observe({
    session$sendCustomMessage("editor-theme", input$editorTheme)
  }) |> bindEvent(input$editorTheme, ignoreInit = T)

  # Change editor font size
  observe({
    session$sendCustomMessage("editor-fontsize", input$editorFontSize)
  }) |> bindEvent(input$editorFontSize)
  
  # switch VTL session
  observe({
    vtlSession <- currentSession()
    isCompiled(vtlSession$isCompiled())
    session$sendCustomMessage("editor-focus", makeUIElemName(vtlSession$name)('editor'))
  }) |> bindEvent(currentSession())
  
  # load vtl script into current session
  observe({
    vtlSession <- isolate(req(currentSession()))
    dataPath <- isolate(req(input$scriptFile))$datapath
    text <- suppressWarnings(paste0(readLines(dataPath), collapse = '\n'))
    panel <- makeUIElemName(vtlSession)('editor')
    session$sendCustomMessage("editor-text", list(panel = panel, text = text))
  }) |> bindEvent(input$scriptFile)
  
  # upload CSV file to GlobalEnv
  observe({
    datasetName = basename(input$datafile$name)
    data = readLines(con = input$datafile$datapath)
    if(length(data > 1)){
      header = as.character(utils::read.csv(text = data[1], header = F))
      ids1 = which(startsWith(header, prefix = '$'))
      ids2 = which(!startsWith(header, prefix = '#') & !grepl(x = header, pattern = '=', fixed = T))
      ids = c(ids1, ids2)
      measures = which(!startsWith(header, prefix = '#') & !startsWith(header, prefix = '$') & grepl(x = header, pattern = '=', fixed = T))
      names = sub('[=].*|[$#]', '', header)
      body = utils::read.csv(text = data[-1], header = F, stringsAsFactors = F)
      body = stats::setNames(object = body, nm = names)

      # Type handling very raw for now, to be refined
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
  }) |> bindEvent(input$datafile)
  
  # create new session
  observe({
    name <- req(input$newSession)
    browser()
    vtlSession <- VTLSessionManager$getOrCreate(name)
    isCompiled(vtlSession$isCompiled())
    updateSelectInput(session, 'sessionID', choices = VTLSessionManager$list(), selected = name)
    updateTextInput(session, 'newSession', value = '')
    # Triggers the tab creation for this session
    vtlSessions(VTLSessionManager$list())
  }) |> bindEvent(input$createSession)
  
  # duplicate session
  observe({
    newName <- req(input$newSession)
    text <- isolate(currentSession())$text
    newSession <- VTLSessionManager$getOrCreate(newName)
    isCompiled(newSession$isCompiled())
    updateTextInput(session, 'newSession', value = '')
    vtlSessions(VTLSessionManager$list())
    session$sendCustomMessage("editor-text", list(panel = makeUIElemName(newName)('editor'), text = text))
  }) |> bindEvent(input$dupSession)
  
  # compile VTL code
  observe({
    shinyjs::disable("compile")
    vtlSession <- isolate(currentSession())
    makeID <- makeUIElemName(isolate(vtlSession$name))
    statements <- input$vtlStatements
    withProgress(message = 'Compiling...', value = 0, tryCatch({
      vtlSession$setText(statements)
      setProgress(value = 0.5)
      vtlSession$compile()
      isCompiled(T)
      shinyjs::addClass(makeID('pane'), 'compiled')
      shinyjs::html(makeID('output'), cat("Compilation successful.\n"))
      updateSelectInput(session, makeID('structName'), 'Select Node', c('', sort(unlist(vtlSession$getNodes()))), '')
      updateSelectInput(session, makeID('datasetName'), 'Select Node', c('', sort(unlist(vtlSession$getNodes()))), '')
    }, error = function(e) {
      isCompiled(vtlSession$isCompiled())
      msg <- conditionMessage(e)
      trace <- NULL
      if (is.list(e) && !is.null(e[['jobj']]))
      {
        writer <- .jnew("java.io.StringWriter")
        e$jobj$printStackTrace(.jnew("java.io.PrintWriter", .jcast(writer, "java/io/Writer")))
        trace <- writer$toString()
        msg <- e$jobj$getLocalizedMessage()
      }
      shinyjs::html(makeID('output'), paste0('<span style="color: red">Error during compilation: ',
        msg, '\n', if (is.null(trace)) '' else trace, '\n</span>')
      )
    }, finally = {
      setProgress(value = 1)
      shinyjs::enable("compile")
    }))
  }) |> bindEvent(input$compile)
  
  # Sync code from the editor to the session
  observeEvent(input$editorText, {
    currentSession()$setText(req(input$editorText))
  })

  observe({
    conf = readLines(con = input$uploadconf$datapath)

    reader <- NULL
    tryCatch({
      reader <- .jnew("java.io.StringReader", conf)
      J("it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory")$loadConfiguration(reader)
    }, finally = {
      if (!is.null(reader)) {
        reader$close()
      }
    })
    VTLSessionManager$reload()
  }) |> bindEvent(input$custom_conf)
}