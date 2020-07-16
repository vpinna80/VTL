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

ui <- dashboardPage(
  
  dashboardHeader(disable = T),


  dashboardSidebar(
       img(src="logo.svg", class="vtlLogo"),
       div(style="display:inline-block",titlePanel("VTL Studio!")),
       hr(),
       uiOutput(outputId='selectSession'),
       actionButton(inputId = 'compile', label = 'Compile session', onClick='Shiny.setInputValue("vtlStatements", vtl.editor.editorImplementation.getValue());'),
       downloadButton(outputId = 'saveas', label = 'Save session as...'),
       hr(),
       textInput(inputId = 'newSession', label = 'New Session'),
       actionButton(inputId = 'createSession', label = 'Create'),
       hr(),
       fileInput(inputId = 'scriptFile', label = 'Load VTL Script Session')
    ),
    
    dashboardBody(#Panel( width = 10, style="height: 93vh; overflow-y: auto;",
      tags$head(
        tags$link(rel = "stylesheet", type = "text/css", href = "codemirror-icons.css"),
        tags$link(rel = "stylesheet", type = "text/css", href = "codemirror-editor.css"),
        tags$link(rel = "stylesheet", type = "text/css", href = "codemirror-all-themes.css"),
        tags$link(rel = "stylesheet", type = "text/css", href = "dialog.css"),
        tags$link(rel = "stylesheet", type = "text/css", href = "simplescrollbars.css"),
        tags$link(rel = "stylesheet", type = "text/css", href = "matchesonscrollbar.css"),
        tags$link(rel = "stylesheet", type = "text/css", href = "vtl-editor.css"),
        tags$script('Shiny.addCustomMessageHandler(\'editor-text\', function(text) {
                    vtl.editor.editorImplementation.setValue(text)
                  });')
      ),
      tabBox(width = 12,
                 tabPanel("VTL Editor", id = "editor-pane",
                          includeHTML('index.html')  ,
                          verbatimTextOutput(outputId = "vtl_output", placeholder =T)
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
                          htmlOutput(outputId = "datasetsInfo", inline = F),
                          DT::dataTableOutput(outputId = "datasets")
                 ),
                 tabPanel("Graph Explorer",
                          sliderInput(inputId = 'distance', label = "Nodes distance", min=50, max=500, step=10, value=100),
                          fillPage(forceNetworkOutput("topology", height = '90vh'))
                 ),
                 tabPanel("Configuration",
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

