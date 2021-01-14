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

# Main class for consuming SDMX web services
#
# Author: Attilio Mattiocco
###############################################################################

library(RVTL)

labels <- list(
  sessionID = 'Active VTL session:',
  compile = HTML('<span style="margin-right: 1em">Compile</span><span style="font-family: monospace">(Ctrl+Enter)</span>'), 
  saveas = HTML('<span style="margin-right: 1em">Save as...</span><span style="font-family: monospace">(Ctrl+S)</span>'),
  newSession = HTML('<span style="margin-right: 1em">New session:</span><span style="font-family: monospace">(Ctrl+N)</span>'), 
  createSession = HTML('<span style="margin-right: 1em">Create new</span><span style="font-family: monospace">(Enter)</span>'), 
  dupSession = 'Duplicate session',
  scriptFile = HTML('<span style="margin-right: 1em">Open...</span><span style="font-family: monospace">(Ctrl+O)</span>'),
  editorTheme = 'Select editor theme:'
)

themes <- list('',
               '3024-day',
               '3024-night',
               'abcdef',
               'ambiance',
               'base16-dark',
               'base16-light',
               'bespin',
               'blackboard',
               'cobalt',
               'colorforth',
               'darcula',
               'dracula',
               'duotone-dark',
               'duotone-light',
               'eclipse',
               'elegant',
               'erlang-dark',
               'gruvbox-dark',
               'hopscotch',
               'icecoder',
               'idea',
               'isotope',
               'lesser-dark',
               'liquibyte',
               'lucario',
               'material',
               'material-darker',
               'material-palenight',
               'material-ocean',
               'mbo',
               'mdn-like',
               'midnight',
               'monokai',
               'moxer',
               'neat',
               'neo',
               'night',
               'nord',
               'oceanic-next',
               'panda-syntax',
               'paraiso-dark',
               'paraiso-light',
               'pastel-on-dark',
               'railscasts',
               'rubyblue',
               'seti',
               'shadowfox',
               'solarized dark',
               'solarized light',
               'the-matrix',
               'tomorrow-night-bright',
               'tomorrow-night-eighties',
               'ttcn',
               'twilight',
               'vibrant-ink',
               'xq-dark',
               'xq-light',
               'yeti',
               'yonce',
               'zenburn')

defaultProxy <- (function() {
  config <- J('it.bancaditalia.oss.sdmx.util.Configuration')$getConfiguration()
  proxy = config$getProperty('http.proxy.default')
  if(!is.null(proxy) && nchar(proxy) > 0){
    parts = unlist(strsplit(proxy, split = ':'))
    if(length(parts) == 2 ){
      return(list(host = parts[1], port = parts[2], user = config$getProperty('http.auth.user')))
    }
  }

  return(list(host = '', port = '', user = ''))
})()

defaultRepository <- J("it.bancaditalia.oss.vtl.config.VTLGeneralProperties")$METADATA_REPOSITORY$getValue()

repositoryImplementations <- list(`In-Memory repository` = 'it.bancaditalia.oss.vtl.impl.domains.InMemoryMetadataRepository',
                                  `CSV file repository` = 'it.bancaditalia.oss.vtl.impl.domains.CSVMetadataRepository',
                                  `SDMX Registry repository` = 'it.bancaditalia.oss.vtl.impl.domains.SDMXMetadataRepository')

environments <- list(
  `CSV environment` = "it.bancaditalia.oss.vtl.impl.environment.CSVFileEnvironment",
  `SDMX environment` = "it.bancaditalia.oss.vtl.impl.environment.SDMXEnvironment",
  `R Environment` = "it.bancaditalia.oss.vtl.impl.environment.REnvironment",
  `In-Memory environment` = "it.bancaditalia.oss.vtl.impl.environment.WorkspaceImpl"
)

currentEnvironments <- function() {
  sapply(J("it.bancaditalia.oss.vtl.config.VTLGeneralProperties")$ENVIRONMENT_IMPLEMENTATION$getValues(), .jstrVal)
}

activeEnvs <- function(active) {
  items <- names(environments[xor(!active, environments %in% currentEnvironments())])
  if (length(items) > 0)
    items
  else
    NULL
}

ui <- shinydashboard::dashboardPage(
  
  shinydashboard::dashboardHeader(disable = T),

  shinydashboard::dashboardSidebar(
       div(style = "text-align: center",
          img(src="logo.svg", class="vtlLogo"),
          div(style="display:inline-block",titlePanel("VTL Studio!"))
       ),
       hr(),
       selectInput(inputId = 'sessionID', label = labels$sessionID, multiple = F, choices = VTLSessionManager$list(), selected = VTLSessionManager$list()[1]),
       actionButton(inputId = 'compile', label = labels$compile, 
                    onClick='Shiny.setInputValue("vtlStatements", vtl.editor.editorImplementation.getValue());'),
       downloadButton(outputId = 'saveas', label = labels$saveas),
       hr(),
       textInput(inputId = 'newSession', label = labels$newSession), 
       actionButton(inputId = 'createSession', label = labels$createSession), 
       actionButton(inputId = 'dupSession', label = 'Duplicate session'),
       fileInput(inputId = 'scriptFile', label = NULL, accept = 'vtl', buttonLabel = labels$scriptFile),
       hr(),
       selectInput(inputId = 'editorTheme', label = labels$editorTheme, multiple = F, choices = themes),
       numericInput('editorFontSize', 'Select font size:', 12, min = 8, max = 40, step = 1)
  ),
    
  shinydashboard::dashboardBody(
      shinyjs::useShinyjs(),
      tags$head(
        tags$link(rel = "stylesheet", type = "text/css", href = "codemirror-icons.css"),
        tags$link(rel = "stylesheet", type = "text/css", href = "codemirror-editor.css"),
        tags$link(rel = "stylesheet", type = "text/css", href = "codemirror-all-themes.css"),
        tags$link(rel = "stylesheet", type = "text/css", href = "dialog.css"),
        tags$link(rel = "stylesheet", type = "text/css", href = "simplescrollbars.css"),
        tags$link(rel = "stylesheet", type = "text/css", href = "matchesonscrollbar.css"),
        tags$link(rel = "stylesheet", type = "text/css", href = "vtl-editor.css"),
        tags$script(HTML('Shiny.addCustomMessageHandler("editor-text", text => vtl.editor.editorImplementation.setValue(text))
                          Shiny.addCustomMessageHandler("editor-theme", theme => vtl.editor.setTheme(theme))
                          Shiny.addCustomMessageHandler("editor-fontsize", fontsize => $(".CodeMirror")[0].style.fontSize = fontsize + "pt")
                          Shiny.addCustomMessageHandler("editor-focus", discard => $("textarea")[0].focus())
        '))
      ), shinydashboard::tabBox(width = 12, id = "navtab",
                 tabPanel("VTL Editor", id = "editor-pane",
                          tags$div(id = 'vtlwell'),
                          verbatimTextOutput(outputId = "vtl_output", placeholder = T),
                          tags$script(src="bundle.js", type="text/javascript"),
                          tags$script(src="main.js", type="text/javascript"),
                          tags$script(src="closebrackets.js", type="text/javascript"),
                          tags$script(src="dialog.js", type="text/javascript"),
                          tags$script(src="matchesonscrollbar.js", type="text/javascript"),
                          tags$script(src="matchbrackets.js", type="text/javascript"),
                          tags$script(src="search.js", type="text/javascript"),
                          tags$script(src="show-hint.js", type="text/javascript"),
                          tags$script(src="simplescrollbars.js", type="text/javascript"),
                          tags$script(HTML('vtl.editor.editorImplementation.setOption("matchBrackets", true)
                                            vtl.editor.editorImplementation.setOption("autoCloseBrackets", true)
                                            vtl.editor.editorImplementation.on("blur", function() { 
                                              Shiny.setInputValue("editorText", vtl.editor.editorImplementation.getValue()); 
                                            })
                                            
                                            vtl.editor.editorImplementation.setOption("extraKeys", {
                                              \'Ctrl-Enter\': function() { $("#compile").click() },
                                              \'Ctrl-N\': function() { $("#newSession")[0].focus(); $("#newSession")[0].select() },
                                              \'Ctrl-O\': function() { $("#scriptFile")[0].click() },
                                              \'Ctrl-S\': function() { $("#saveas")[0].click() }
                                            })
                                            
                                            $(document).ready(function () {
                                              $("#newSession").keyup(function(e) {
                                                if (e.keyCode === 13) {
                                                  e.preventDefault()
                                                  $("#createSession").click()
                                                }
                                              })
                                            })'))
                 ),
                 tabPanel("Dataset Explorer",
                          fluidRow(
                            column(width=5,
                              uiOutput(outputId = "dsNames")
                            ),
                            column(width=5,
                              textInput(inputId = 'maxlines', label = 'Max Lines', value = 1000)
                            )
                          ),
                          checkboxInput(inputId = 'showAttrs', label = "Show Attributes", value = T),
                          hr(),
                          uiOutput(outputId = "datasetsInfo"),
                          DT::dataTableOutput(outputId = "datasets")
                 ),
                 tabPanel("Graph Explorer",
                          sliderInput(inputId = 'distance', label = "Nodes distance", min=50, max=500, step=10, value=100),
                          fillPage(networkD3::forceNetworkOutput("topology", height = '90vh'))
                 ),
                 tabPanel("Settings",
                          shinydashboard::box(title = 'VTL Engine', status = 'primary', solidHeader = T, collapsible = T,
                            selectInput(inputId = 'engineClass', label = NULL, 
                                        multiple = F, choices = c("In-Memory engine"), selected = c("In-Memory engine"))
                          ),
                          shinydashboard::box(title = 'Metadata Repository', status = 'primary', solidHeader = T, collapsible = T,
                            selectInput(inputId = 'repoClass', label = NULL, 
                                        multiple = F, choices = repositoryImplementations, selected = defaultRepository),
                            uiOutput(outputId = "repoProperties"),
                            actionButton(inputId = 'setRepo', label = 'Change repository')
                          ),
                          shinydashboard::box(title = 'VTL Environments', status = 'primary', solidHeader = T, collapsible = T,
                            sortable::bucket_list(header = NULL, 
                              sortable::add_rank_list(text = tags$label("Available"), labels = activeEnvs(F)),
                              sortable::add_rank_list(input_id = "envs", text = tags$label("Active"), labels = activeEnvs(T)),
                              orientation = 'horizontal')
                          ),
                          shinydashboard::box(title = 'Network Proxy', status = 'primary', solidHeader = T, collapsible = T,
                                              textInput(inputId = 'proxyHost', label = 'Host:', value = defaultProxy$host),
                                              textInput(inputId = 'proxyPort', label = 'Port:', value = defaultProxy$port),
                                              textInput(inputId = 'proxyUser', label = 'User:', value = defaultProxy$user),
                                              passwordInput(inputId = 'proxyPassword', label = 'Password:'),
                                              actionButton(inputId = 'setProxy', label = 'Save')
                          ),
                          shinydashboard::box(title = 'Status', status = 'primary', solidHeader = T, width = 12,
                            verbatimTextOutput(outputId = "conf_output", placeholder = T)
                          )
                 )
               )                 
  )
)
