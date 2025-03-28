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

defaultProxy <- function() {
  return(list(host = J("java.lang.System")$getProperty("https.proxyHost"), port = J("java.lang.System")$getProperty("https.proxyPort"), user = ''))
}

vtlUI <- shinydashboard::dashboardPage(title="VTL Studio!",
    
    shinydashboard::dashboardHeader(disable = T),
  
    shinydashboard::dashboardSidebar(
      div(style = "text-align: center",
        img(src="static/logo.svg", class="vtlLogo"),
        div(style="display:inline-block; vertical-align: bottom",
          h2(style="margin-bottom: 0", "VTL Studio!"),
          div(style = "text-align: right", "1.2.0-20250116152911")       
        )
      ),
      hr(),
      fileInput(inputId = 'datafile', label = 'Load CSV', accept = 'csv'),
      bslib::input_switch(inputId = 'demomode', label = 'Demo mode'),
      selectInput(inputId = 'sessionID', label = labels$sessionID, multiple = F, choices = c()),
      actionButton(inputId = 'compile', label = labels$compile, 
        onClick='Shiny.setInputValue("vtlStatements", VTLEditor.view.state.doc.toString());'),
      downloadButton(outputId = 'saveas', label = labels$saveas),
      hr(),
      textInput(inputId = 'newSession', label = labels$newSession), 
      actionButton(inputId = 'createSession', label = labels$createSession), 
      actionButton(inputId = 'dupSession', label = 'Duplicate session'),
      fileInput(inputId = 'scriptFile', label = NULL, accept = 'vtl', buttonLabel = labels$scriptFile),
      hr(),
      selectInput(inputId = 'editorTheme', label = labels$editorTheme, multiple = F, choices = ''),
      numericInput('editorFontSize', 'Select font size:', 12, min = 8, max = 40, step = 1)
    ),
      
    shinydashboard::dashboardBody(
      shinyjs::useShinyjs(),
      tags$head(
        tags$link(rel = "stylesheet", type = "text/css", href = "static/vtl-editor.css")
      ), shinydashboard::tabBox(width = 12, id = "navtab",
        tabPanel("VTL Editor", id = "editor-pane",
          tags$div(id = 'vtlwell'),
          verbatimTextOutput(outputId = "vtl_output", placeholder = T),
          tags$script(src="static/bundle.js", type="text/javascript"),
          tags$script(HTML('
            document.getElementById("vtlwell").appendChild(VTLEditor.view.dom)
            
            $(document).on("shiny:connected", () => {
              Shiny.setInputValue("themeNames", VTLEditor.themes)
              Shiny.addCustomMessageHandler("editor-text", text => VTLEditor.view.dispatch({changes: {from: 0, to: VTLEditor.view.state.doc.length, insert: text}}))
              Shiny.addCustomMessageHandler("editor-theme", theme => VTLEditor.setTheme(theme))
              Shiny.addCustomMessageHandler("editor-fontsize", fontsize => { document.getElementsByClassName("cm-scroller")[0].style.fontSize = fontsize + "pt"; VTLEditor.view.requestMeasure() })
              Shiny.addCustomMessageHandler("editor-focus", discard => VTLEditor.view.contentDOM.focus())
            })
  
            $(document).ready(function () {
              $("#newSession").keyup(function(e) {
                if (e.keyCode === 13) {
                  e.preventDefault()
                  $("#createSession").click()
                }
              })
  
              VTLEditor.view.dom.onblur = () => Shiny.setInputValue("editorText", VTLEditor.view.state.doc.toString())
              VTLEditor.addHotKey("Ctrl-Enter", () => { $("#compile").click(); return true })
              VTLEditor.addHotKey("Ctrl-n", () => { $("#newSession")[0].focus(); $("#newSession")[0].select(); return true })
              VTLEditor.addHotKey("Ctrl-o", () => { $("#scriptFile")[0].click(); return true })
              VTLEditor.addHotKey("Ctrl-s", () => { $("#saveas")[0].click(); return true })
            })'))
        ),
        tabPanel("Structure Explorer",
          fluidRow(
            column(width=5,
              selectInput('structureSelection', 'Structure selection:', c(''), '')
            )
          ),
          DT::dataTableOutput(outputId = 'dsStr')
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
          tabsetPanel(id = "dataview", type = "tabs",
            tabPanel("Data points", 
              uiOutput(outputId = "datasetsInfo"),
              DT::dataTableOutput(outputId = "datasets")
            ),
            tabPanel("Lineage", networkD3::sankeyNetworkOutput("lineage", height = "100%"))
          ),
        ),
        tabPanel("Graph Explorer",
          sliderInput(inputId = 'distance', label = "Nodes distance", min=50, max=500, step=10, value=100),
          fillPage(networkD3::forceNetworkOutput("topology", height = '90vh'))
        ),
        tabPanel("Engine settings",
          shinydashboard::box(title = 'Network Proxy', status = 'primary', solidHeader = T, collapsible = T,
            uiOutput(outputId = "proxyControls")
          ),
          shinydashboard::box(title = 'Metadata Repository', status = 'primary', solidHeader = T, collapsible = T,
            selectInput(inputId = 'repoClass', label = NULL, choices = c("Select a repository..." = "")),
            uiOutput(outputId = "repoProperties")
          ),
          tags$div(class = "col-sm-12"),
          shinydashboard::box(title = 'VTL Environments', status = 'primary', solidHeader = T, collapsible = T,
            uiOutput(outputId = "sortableEnvs")
          ),
          shinydashboard::box(title = 'Environment Properties', status = 'primary', solidHeader = T, collapsible = T,
            selectInput(inputId = 'selectEnv', label = NULL, choices = c("Select an environment..." = "")),
            uiOutput(outputId = "envprops")
          ),
          shinydashboard::box(title = 'Status', status = 'primary', solidHeader = T, width = 12,
            fluidRow(
              column(6,
                actionButton(inputId = 'saveconf', label = 'Save & apply configuration')
              ),
              column(6,
                fileInput(inputId = "custom_conf", label="import"))
              )
            ,
            tags$div(style = "height: 1em"),
            verbatimTextOutput(outputId = "eng_conf_output", placeholder = T)
          )
        )
      )                 
    )
  )
