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

ui <- dashboardPage(
  
  dashboardHeader(disable = T),

  dashboardSidebar(
       div(style = "text-align: center",
          img(src="logo.svg", class="vtlLogo"),
          div(style="display:inline-block",titlePanel("VTL Studio!"))
       ),
       hr(),
       selectInput(inputId = 'sessionID', label = 'Active VTL session:', multiple = F, choices = VTLSessionManager$list(), selected = VTLSessionManager$list()[1]),
       actionButton(inputId = 'compile', label = 'Compile session', onClick='Shiny.setInputValue("vtlStatements", vtl.editor.editorImplementation.getValue());'),
       downloadButton(outputId = 'saveas', label = 'Save session as...'),
       hr(),
       textInput(inputId = 'newSession', label = 'New Session'),
       actionButton(inputId = 'createSession', label = 'Create new session'),
       actionButton(inputId = 'dupSession', label = 'Duplicate session'),
       fileInput(inputId = 'scriptFile', label = NULL, buttonLabel = 'Load script from file...'),
       hr(),
       selectInput(inputId = 'editorTheme', label = 'Select editor theme:', multiple = F, choices = themes),
       numericInput('editorFontSize', 'Select font size:', 12, min = 8, max = 40, step = 1)
  ),
    
    dashboardBody(
      useShinyjs(),
      tags$head(
        tags$link(rel = "stylesheet", type = "text/css", href = "codemirror-icons.css"),
        tags$link(rel = "stylesheet", type = "text/css", href = "codemirror-editor.css"),
        tags$link(rel = "stylesheet", type = "text/css", href = "codemirror-all-themes.css"),
        tags$link(rel = "stylesheet", type = "text/css", href = "dialog.css"),
        tags$link(rel = "stylesheet", type = "text/css", href = "simplescrollbars.css"),
        tags$link(rel = "stylesheet", type = "text/css", href = "matchesonscrollbar.css"),
        tags$link(rel = "stylesheet", type = "text/css", href = "vtl-editor.css"),
        tags$script(HTML('Shiny.addCustomMessageHandler("editor-text", text => vtl.editor.editorImplementation.setValue(text))')),
        tags$script(HTML('Shiny.addCustomMessageHandler("editor-theme", theme => vtl.editor.setTheme(theme));')),
        tags$script(HTML('Shiny.addCustomMessageHandler("editor-fontsize", fontsize => $(".CodeMirror")[0].style.fontSize = fontsize + "pt");'))
      ),
      tabBox(width = 12, id = "navtab",
                 tabPanel("VTL Editor", id = "editor-pane",
                          tags$div(id = 'vtlwell'),
                          verbatimTextOutput(outputId = "vtl_output", placeholder = T),
                          tags$script(src="bundle.js", type="text/javascript"),
                          tags$script(src="main.js", type="text/javascript"),
                          tags$script(src="closebrackets.js", type="text/javascript"),
                          tags$script(src="dialog.js", type="text/javascript"),
                          tags$script(src="matchesonscrollbar.js", type="text/javascript"),
                          tags$script(src="search.js", type="text/javascript"),
                          tags$script(src="show-hint.js", type="text/javascript"),
                          tags$script(src="simplescrollbars.js", type="text/javascript"),
                          tags$script('vtl.editor.editorImplementation.on("blur", function() { Shiny.setInputValue("editorText", vtl.editor.editorImplementation.getValue()); })'),
                          tags$script('vtl.editor.editorImplementation.setOption("extraKeys", {\'Ctrl-Enter\': function() { $("#compile").click() }})')
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
                          fillPage(forceNetworkOutput("topology", height = '90vh'))
                 ),
                 tabPanel("Network",
                          h4('Proxy Settings'),
                          # textInput(inputId = 'proxyHost', label = 'Proxy Host'),
                          # textInput(inputId = 'proxyPort', label = 'Proxy Port'),
                          # textInput(inputId = 'proxyUser', label = 'Proxy User'),
                          uiOutput(outputId = "proxyHostUI"),
                          uiOutput(outputId = "proxyPortUI"),
                          uiOutput(outputId = "proxyUserUI"),
                          passwordInput(inputId = 'proxyPassword', label = 'Proxy Password'),
                          actionButton(inputId = 'setProxy', label = 'OK'),
                          h4("Messages"),
                          wellPanel(id = 'confout', verbatimTextOutput(outputId = "conf_output", placeholder =T), height = "40vh")
                          
                 )
               )                 
  )
)

