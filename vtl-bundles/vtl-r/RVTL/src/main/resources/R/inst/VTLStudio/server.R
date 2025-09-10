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

repos <- vtlAvailableRepositories()
environments <- vtlAvailableEnvironments()
configManager <- J("it.bancaditalia.oss.vtl.config.ConfigurationManager")
exampleEnv <- J(environments['Documentation Examples environment'])

vtlProps <- list(
  ENVIRONMENT_IMPLEMENTATION = J("it.bancaditalia.oss.vtl.config.VTLGeneralProperties")$ENVIRONMENT_IMPLEMENTATION,
  METADATA_REPOSITORY = J("it.bancaditalia.oss.vtl.config.VTLGeneralProperties")$METADATA_REPOSITORY,
  EXAMPLE_CATEGORY = exampleEnv$EXAMPLES_CATEGORY,
  EXAMPLE_OPERATOR = exampleEnv$EXAMPLES_OPERATOR
)

globalRepo <- function(newrepo = NULL) {
  if (is.null(newrepo))
    return(configManager$getGlobalPropertyValue(vtlProps$METADATA_REPOSITORY))
  else
    configManager$setGlobalPropertyValue(vtlProps$METADATA_REPOSITORY, newrepo)
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
    } else if (grepl('json.metadata.url', inputID)) {
      inputIDj <- paste0(inputID, '_j')
      tags$div(style = "display: flex; gap: 6px; align-items: flex-end;",
        textInput(inputID, prop$getDescription(), val, placeholder = prop$getPlaceholder()),
        tags$div(class = 'form-group shiny-input-container', style = "align-self: end;",
          actionButton(inputIDj, '', tags$i(class = 'bi bi-pencil-square'), class = 'disabled')
        )
      )
    } else if (prop$getName() == 'vtl.examples.category') {
      categories <- sapply(sapply(exampleEnv$getCategories(), .jstrVal), \(categ) categ)
      selectInput(inputID, '', c(`Choose a category...` = '', categories), getValue(prop))
    } else if (prop$getName() == 'vtl.examples.operator') {
      category <- getValue(vtlProps$EXAMPLE_CATEGORY)
      if (is.null(category)) {
        selectInput(inputID, '', c(`Choose an operator...` = ''))
      } else {
        opNames <- sapply(sapply(exampleEnv$getOperators(category), .jstrVal), \(opName) opName)
        selectInput(inputID, '', c(`Choose an operator...` = '', opNames), getValue(prop))
      }
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
  sessionInstance <- VTLSessionManager$getOrCreate(vtlSession)
  isCompiled <- sessionInstance$isCompiled()
  sessionEnvs <- unlist(strsplit(sessionInstance$getProperty(vtlProps$ENVIRONMENT_IMPLEMENTATION), ","))
  activeSessionEnvs <- function(active) {
    items <- names(environments[xor(!active, environments %in% sessionEnvs)])
    if (length(items) > 0) items else NULL
  }

  bslib::nav_panel(value = vtlSession, 
    title = tags$span(onclick = "addEditorMenu(this, event)",
      vtlSession, class = paste("with-editor", if (isCompiled) "compiled" else ""), id = makeID("pane")
    ), bslib::navset_hidden(id = makeID('sheets'),
      bslib::nav_panel_hidden(value = "editor",
        tags$div(id = newEditorID, class = 'vtlwell'),
        verbatimTextOutput(makeID('output'), placeholder = T),
        tags$script(HTML(sprintf("createEditorPanel('%s')", newEditorID)))
      ), bslib::nav_panel_hidden(value = "structures",
        fluidRow(
          column(width=5, uiOutput(makeID('structNameOutput'), inline = T))
        ),
        DT::dataTableOutput(makeID('dsStr'))
      ), bslib::nav_panel_hidden(value = "datasets",
        fluidRow(class = 'g-0',
          column(width=12, uiOutput(makeID('datasetNameOutput'), inline = T))
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
              DT::dataTableOutput(makeID('envs'), fill = F)
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
  currentSession <- reactive({
    if (input$navtab != "Global settings") {
      VTLSessionManager$getOrCreate(input$navtab)
    } else NULL
  }) |> bindEvent(input$navtab)
  tabs <- reactiveVal(NULL)
  
  # Create startup tabs for each active session
  inittabs <- observe({
    vtlSessions(VTLSessionManager$list())
    inittabs$destroy()
  })

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
    
    # React to "choose a path" for path-like properties
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

    # React to change url with enabling/disabling edit button
    if (grepl('json.metadata.url', inputID)) {
      debounced <- debounce(reactive(input[[inputID]]), 2000)
      inputIDj <- paste0(inputID, '_j')
      observe({
        jsonUrl = req(debounced())
        con <- NULL
        allOK <- tryCatch({
            J('it.bancaditalia.oss.vtl.impl.environment.RUtils')$getURLContents(jsonUrl)
            shinyjs::removeClass(inputIDj, 'disabled')
          },
          warning = \(w) shinyjs::addClass(inputIDj, 'disabled'),
          error = \(e) shinyjs::addClass(inputIDj, 'disabled'),
          finally = { if (!is.null(con)) close(con) }
        )
      }) |> bindEvent(debounced(), ignoreInit = T)
      
      observe({
        jsonUrl = req(debounced())
        con <- NULL
        jsonFile <- tryCatch({
            J('it.bancaditalia.oss.vtl.impl.environment.RUtils')$getURLContents(jsonUrl)
          }, error = \(e) {
            invisible(NULL)
          }, finally = {
            if (!is.null(con)) close(con)
          }
        )
        if (!is.null(jsonFile)) {
          session$sendCustomMessage('dict-editor', jsonFile)
        }
      }) |> bindEvent(input[[inputIDj]], ignoreInit = T)
    }

    observe({
      value <- req(input[[inputID]])
      setValue(prop, value)
      printValue <- if (prop$isPassword()) "<masked value>" else value
      output[[makeID('confOutput')]] <- renderPrint({
        cat("Set property", prop$getDescription(), "to", printValue, "\n")
      }) |> bindEvent(input[[inputID]])
      
      # If selecting a operator category also update the operator name select
      if (prop$getName() == 'vtl.examples.category') {
        opNameID <- makeID('vtl.examples.operator')
        opNames <- sapply(sapply(exampleEnv$getOperators(value), .jstrVal), \(opName) opName)
        selected <- input[[opNameID]]
        updateSelectInput(inputId = opNameID, choices = c(`Choose an operator...` = '', opNames), selected = selected)
      }
      
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
    
    # Populate environments and repositories lists in session settings
    sheetID <- makeID('sheets')
    settingsFirstOpened <- observe({
      req(input[[sheetID]] == 'settings')
      vtlSessionInstance <- VTLSessionManager$getOrCreate(vtlSession)
      envs <- strsplit(.jstrVal(vtlSessionInstance$getProperty(vtlProps$ENVIRONMENT_IMPLEMENTATION)), ",", T)[[1]]
      checked <- unname(unlist(which(sapply(vtlAvailableEnvironments(), `%in%`, envs))))
      
      output[[makeID('envs')]] <- DT::renderDT(
        expr = data.frame(name = names(vtlAvailableEnvironments()), check.names = F),
        selection = list(mode = "multiple", selected = checked),
        rownames = FALSE, colnames = NULL,
        options = list(
          dom = 't', paging = FALSE, ordering = FALSE, info = FALSE, stripeClasses = list(),
          columnDefs = list(list(targets = 0, className = "select-checkbox")),
          select = list(style = "multi", selector = "td:first-child"),
          headerCallback = htmlwidgets::JS("function(thead) { thead.classList.add('d-none') }")
        )
      )
      
      # Single execution only when VTL Studio starts
      updateSelectInput(inputId = makeID('selectEnv'), choices = vtlAvailableEnvironments())
      updateSelectInput(inputId = makeID('repoClass'), choices = repos, selected = globalRepo())
      session$sendCustomMessage("editor-text", list(panel = makeID('editor'), text = vtlSessionInstance$text))

      if (vtlSessionInstance$isCompiled()) {
        updateSelectInput(session, makeID('datasetName'), 'Select Node', c('', sort(unlist(vtlSessionInstance$getNodes()))), '')
      }
      
      settingsFirstOpened$destroy()
    }) |> bindEvent(input[[sheetID]])

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
      setValue(vtlProps$METADATA_REPOSITORY, repoClass)
      repoName <- names(vtlAvailableRepositories()[match(repoClass, vtlAvailableRepositories())])
      output[[makeID('confOutput')]] <- renderPrint({
        cat("Set session", vtlSession, "metadata repository to", repoName, "\n")
      })
      lapply(configManager$getSupportedProperties(J(repoClass)@jobj), observersGen,
             getValue = getValue, setValue = setValue, makeID = makeID)
      controlsGen(repoClass, getValue, makeID)
    }) |> bindEvent(input[[makeID('repoClass')]], ignoreInit = T)

    # Update active environments in active session
    observe({
      selected <- input[[makeID('envs_rows_selected')]]
      activeEnvs <- vtlAvailableEnvironments()[selected]
      VTLSessionManager$getOrCreate(vtlSession)$setProperty(vtlProps$ENVIRONMENT_IMPLEMENTATION, paste0(activeEnvs, collapse =","))
      output[[makeID('confOutput')]] <- renderPrint({
        cat("Set ", vtlSession, " environments to:\n", paste0("    - ", activeEnvs, collapse = "\n"), sep = "")
      })
    }) |> bindEvent(input[[makeID('envs_rows_selected')]], ignoreInit = T)
    
    # dataset table display
    observe({
      datasetName <- req(input[[makeID('datasetName')]])
      session <- VTLSessionManager$getOrCreate(vtlSession)
      statements <- sapply(session$getStatements()$entrySet(), function (x) {
        stats::setNames(list(x$getValue()), x$getKey()$getName())
      })
      ddf <- session$getValues(datasetName)[[1]]
      statement <- get0(datasetName, as.environment(statements), ifnotfound = paste0('Source data: ', datasetName))
      
      # Output the rule
      output[[makeID('datasetsInfo')]] <- renderPrint({
        cat(statement, "\nSize is:", nrow(ddf), "by", ncol(ddf))
      })
  }) |> bindEvent(input[[makeID('datasetName')]])
    
    # Lineage display
    output[[makeID('lineage')]] <- networkD3::renderSankeyNetwork({
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

    # Dataset display
    output[[makeID('dptable')]] <- DT::renderDataTable({
      maxlines = as.integer(isolate(req(input[[makeID('maxlines')]])))
      selected <- req(input[[makeID('datasetName')]])
      ddf = tryCatch({
          VTLSessionManager$getOrCreate(vtlSession)$getValues(selected)[[1]]
        }, error = function(e) {
          if (!is.null(e[['jobj']]) && is.function(e$jobj[['getMessage']])) {
            print(paste0("ERROR: ", e$jobj$getMessage()))
            e$jobj$printStackTrace()
          }
          else
            print(e)
          return(F)
        })
      if (!is.data.frame(ddf)) {
        return (data.frame())
      } else if (ncol(ddf) <= 1 || names(ddf)[1] == 'Scalar') {
        return (ddf)
      } else {
        linesLimit = ifelse(nrow(ddf) > maxlines , yes = maxlines, no = nrow(ddf))
        ddf = ddf[1:linesLimit,]

        # not a scalar, order columns and add component role
        if (ncol(ddf) > 1) {
          neworder = which(names(ddf) %in% attr(ddf, 'measures'))
          neworder = c(neworder, which(names(ddf) %in% attr(ddf, 'identifiers')))
          if (input[[makeID('showAttrs')]]) {
            neworder = c(neworder, which(!(1:ncol(ddf) %in% neworder)))
          }
          ddf = ddf[, neworder]
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
        
        return(ddf)
      }
    }, options = list(
      lengthMenu = list(c(50, 1000, -1), c('50', '1000', 'All')),
      pageLength = 10
    )) |> bindEvent(input[[makeID('datasetName')]])
    
    # Display structures and transformation graph
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
            } else if (jstr %instanceof% "it.bancaditalia.oss.vtl.model.data.DataSetStructure") {
              df <- data.table::transpose(data.frame(lapply(jstr, function(x) {
                c(x$getAlias()$getName(), x$getDomain()$toString(), x$getRole()$getSimpleName())
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
    }) |> bindEvent(input[[makeID('sheets')]])

    panel <- createPanel(vtlSession)
    prependTab("navtab", panel, T)
  }) |> bindEvent(vtlSessions(), tabs())

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
    
    if (isTRUE(input$demomode)) {
      categories <- c(
        list(list(label = 'Select category...', value = '', categ = '')),
          lapply(sapply(exampleEnv$getCategories(), .jstrVal), \(categ) list(label = categ, value = categ, categ = ''))
      )
      updateSelectizeInput(session, 'excat', 'Categories', NULL, options = list(
        render = I('{ option: generateLabel }'),
        options = categories
      ))
    }
  }) |> bindEvent(input$demomode, ignoreInit = T)
  
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
    envs <- sapply(configManager$getGlobalPropertyValues(vtlProps$ENVIRONMENT_IMPLEMENTATION), .jstrVal)
    checked <- unname(which(sapply(vtlAvailableEnvironments(), `%in%`, envs)))
    
    output$envs <- DT::renderDT(
      expr = data.frame(name = names(vtlAvailableEnvironments()), check.names = F),
      selection = list(mode = "multiple", selected = checked),
      rownames = FALSE, colnames   = NULL,
      options = list(
        dom = 't', paging = FALSE, ordering = FALSE, info = FALSE, stripeClasses = list(),
        columnDefs = list(list(targets = 0, className = "select-checkbox")),
        select = list(style = "multi", selector = "td:first-child"),
        headerCallback = htmlwidgets::JS("function(thead) { thead.classList.add('d-none') }")
      )
    )

    # Single execution only when VTL Studio starts
    updateSelectInput(inputId = 'selectEnv', choices = vtlAvailableEnvironments())
    updateSelectInput(inputId = 'repoClass', choices = repos, selected = globalRepo())
    envlistdone$destroy()
  })
  
  # Enable/disable environments in global configuration
  observe({
    selected <- input[['envs_rows_selected']]
    activeEnvs <- vtlAvailableEnvironments()[selected]
    configManager$setGlobalPropertyValue(vtlProps$ENVIRONMENT_IMPLEMENTATION, paste0(activeEnvs, collapse =","))
    output$confOutput <- renderPrint({
      cat("Set global environments to:\n", paste0("    - ", names(activeEnvs), collapse = "\n"), sep = "")
    })
  }) |> bindEvent(input[['envs_rows_selected']], ignoreInit = T)

  # Apply configuration to all active vtlSessions
  observe({
    VTLSessionManager$reload()
    for (vtlSession in VTLSessionManager$list()) {
      makeID <- makeUIElemName(vtlSession)
      activeEnvs <- strsplit(VTLSessionManager$getOrCreate(vtlSession)$getProperty(vtlProps$ENVIRONMENT_IMPLEMENTATION), ',')
      activeEnvs <- names(environments)[match(unlist(activeEnvs), environments)]
      session$sendCustomMessage('update-envs', list(rank = makeID('envs'), active = c('', activeEnvs)))
      activeRepo <- VTLSessionManager$getOrCreate(vtlSession)$getProperty(vtlProps$METADATA_REPOSITORY)
      updateSelectInput(session, makeID('repoClass'), NULL, repos, activeRepo)
      updateSelectInput(session, makeID('selectEnv'), NULL, environments, "it.bancaditalia.oss.vtl.impl.environment.REnvironment")
    }
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
    
    globalRepo(req(repoClass))
    repoName <- names(vtlAvailableRepositories()[match(repoClass, vtlAvailableRepositories())])
    output$confOutput <- renderPrint({
      cat("Set metadata repository to", repoName, "\n")
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
    vtlSession <- VTLSessionManager$getOrCreate(name)
    isCompiled(vtlSession$isCompiled())
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
    shinyjs::disable('compile')
    vtlSession <- currentSession()
    makeID <- makeUIElemName(vtlSession$name)
    statements <- input[[makeID('editor_vtlStatements')]]
    vtlSession$setText(statements)
    withProgress(message = 'Compiling...', value = 0, tryCatch({
      setProgress(value = 0.5)
      vtlSession$compile()
      isCompiled(T)
      shinyjs::addClass(makeID('pane'), 'compiled')
      shinyjs::html(makeID('output'), cat("Compilation successful.\n"))
      output[[makeID('structNameOutput')]] <- renderUI({
        selectInput(makeID('structName'), 'Select VTL alias', c('', sort(unlist(vtlSession$getNodes()))), '')
      })
      output[[makeID('datasetNameOutput')]] <- renderUI({
        selectInput(makeID('datasetName'), 'Select VTL alias', c('', sort(unlist(vtlSession$getNodes()))), '')
      })
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
      shinyjs::enable('compile')
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