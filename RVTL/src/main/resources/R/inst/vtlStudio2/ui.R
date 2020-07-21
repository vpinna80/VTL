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

ui <- shinydashboard::dashboardPage(
  
  shinydashboard::dashboardHeader(disable = T),

  shinydashboard::dashboardSidebar(
       div(style = "text-align: center",
          img(src="logo.svg", class="vtlLogo"),
          div(style="display:inline-block",titlePanel("VTL Studio!"))
       ),
       hr(),
       selectInput(inputId = 'sessionID', label = 'Active VTL session:', multiple = F, choices = VTLSessionManager$list(), selected = VTLSessionManager$list()[1]),
       actionButton(inputId = 'compile', 
                    label = HTML('<span style="margin-right: 1em">Compile</span><span style="font-family: monospace">(Ctrl+Enter)</span>'), 
                    onClick='Shiny.setInputValue("vtlStatements", vtl.editor.editorImplementation.getValue());'),
       downloadButton(outputId = 'saveas', label = HTML('<span style="margin-right: 1em">Save as...</span><span style="font-family: monospace">(Ctrl+S)</span>')),
       hr(),
       textInput(inputId = 'newSession',
                 label = HTML('<span style="margin-right: 1em">New session:</span><span style="font-family: monospace">(Ctrl+N)</span>')), 
       actionButton(inputId = 'createSession', 
                    label = HTML('<span style="margin-right: 1em">Create new</span><span style="font-family: monospace">(Enter)</span>')), 
       actionButton(inputId = 'dupSession', label = 'Duplicate session'),
       fileInput(inputId = 'scriptFile', label = NULL, accept = 'vtl',
                 buttonLabel = HTML('<span style="margin-right: 1em">Open...</span><span style="font-family: monospace">(Ctrl+O)</span>')),
       hr(),
       selectInput(inputId = 'editorTheme', label = 'Select editor theme:', multiple = F, choices = themes),
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
                 tabPanel("Network",
                          h4('Proxy Settings'),
                          uiOutput(outputId = "proxyHostUI"),
                          uiOutput(outputId = "proxyPortUI"),
                          uiOutput(outputId = "proxyUserUI"),
                          passwordInput(inputId = 'proxyPassword', label = 'Proxy Password'),
                          actionButton(inputId = 'setProxy', label = 'Save proxy settings'),
                          h4("Messages"),
                          wellPanel(id = 'confout', verbatimTextOutput(outputId = "conf_output", placeholder =T), height = "40vh")
                          
                 )
               )                 
  )
)

