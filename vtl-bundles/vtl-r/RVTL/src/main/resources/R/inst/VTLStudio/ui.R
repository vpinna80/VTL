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
  compile = HTML('<span style="margin-right: 1em">Compile</span><span style="font-family: monospace">(Ctrl+Enter)</span>'), 
  saveas = HTML('<span style="margin-right: 1em">Export code as...</span><span style="font-family: monospace">(Ctrl+S)</span>'),
  saveconfas = HTML('<span style="margin-right: 1em">Save current configuration as...</span>'),
  newSession = HTML('<span style="margin-right: 1em">New session:</span><span style="font-family: monospace">(Ctrl+N)</span>'), 
  createSession = HTML('<span style="margin-right: 1em">Create new</span><span style="font-family: monospace">(Enter)</span>'), 
  dupSession = 'Duplicate session',
  scriptFile = HTML('<span style="margin-right: 1em">Replace editor content...</span><span style="font-family: monospace">(Ctrl+O)</span>'),
  editorTheme = 'Select editor theme:'
)

defaultProxy <- \() list(
  host = J("java.lang.System")$getProperty("https.proxyHost"), 
  port = J("java.lang.System")$getProperty("https.proxyPort"), 
  user = ''
)

makeButton <- \(id) tags$button(
  class = "btn btn-sm btn-link float-end",
  `data-bs-toggle` = "collapse",
  `data-bs-target` = paste0('#', id),
  icon('minus')
)

vtlUI <- bslib::page_sidebar(
    window_title = 'VTL Studio!',
    theme = bslib::bs_theme(version = 5, preset = 'cosmo', `bs5icons` = TRUE)
      |> bslib::bs_add_variables(bslib_spacer = "0.5rem"),
    sidebar = bslib::sidebar(
      width = 350,
      img(src = "static/logo.svg", class = "vtlLogo"),
      tags$div(style = "text-align: right; width: 98%", "${r.package.version}"),
      uiOutput("shinyapps"),
      hr(),
      bslib::input_switch(id = 'demomode', label = 'Demo mode'),
      tags$div(id = "normalctrl", class="bslib-gap-spacing d-flex flex-column",
        textInput(inputId = 'newSession', label = labels$newSession), 
        actionButton(inputId = 'createSession', label = labels$createSession), 
        actionButton(inputId = 'dupSession', label = 'Duplicate session'),
        fileInput(inputId = 'scriptFile', label = NULL, accept = 'vtl', buttonLabel = labels$scriptFile)
      ),
      tags$div(id = "democtrl", class="bslib-gap-spacing d-flex flex-column",
        selectizeInput('excat', 'Categories', multiple = F, choices = ''),
        selectizeInput('exoper', 'Operators', multiple = F, choices = ''),
        # numericInput('exnum', 'Select exanple number:', 1, min = 1, max = 1, step = 1),
        actionButton('exopen', 'Open operator examples', disabled = TRUE)
      ),
      hr(),
      actionButton(inputId = 'compile', label = labels$compile, onClick='updateSessionText()'),
      downloadButton(outputId = 'saveas', label = labels$saveas),
      hr(),
      selectInput(inputId = 'editorTheme', label = labels$editorTheme, multiple = F, choices = ''),
      numericInput('editorFontSize', 'Select font size:', 12, min = 8, max = 40, step = 1)
    ),

    shinyjs::useShinyjs(),
    tags$head(
      tags$link(rel = "stylesheet", type = "text/css", href = "static/vtl-editor.css"),
      tags$link(rel = "stylesheet", type = "text/css", href = "static/bootstrap-icons.css"),
      tags$script(src = "static/vtl-editor.js"),
      tags$script(src = "static/bundle.js")
    ), bslib::navset_tab(id = "navtab",
      bslib::nav_panel("Global settings",
        bslib::layout_column_wrap(width = 1/2,
          bslib::card(
            bslib::card_header('Network Proxy', makeButton('card_proxy')),
            bslib::card_body(id = 'card_proxy', class = 'collapse show', uiOutput(outputId = "proxyControls"))
          ),
          bslib::card(
            bslib::card_header('Metadata Repository', makeButton('card_meta')),
            bslib::card_body(id = 'card_meta', class = 'collapse show',
              selectInput(inputId = 'repoClass', label = NULL, choices = c("Select a repository..." = "")),
              uiOutput(outputId = "repoProperties")
            )
          ),
          bslib::card(
            bslib::card_header('VTL Environments', makeButton('card_envs')),
            bslib::card_body(id = 'card_envs', class = 'collapse show', uiOutput(outputId = "sortableEnvs"))
          ),
          bslib::card(
            bslib::card_header('Environment Properties', makeButton('card_envprops')),
            bslib::card_body(id = 'card_envprops', class = 'collapse show',
              selectInput(inputId = 'selectEnv', label = NULL, choices = c("Select an environment..." = "")),
              uiOutput(outputId = "envprops")
            )
          ),
          bslib::card(
            bslib::card_header('Configuration Management', makeButton('card_confman')),
            bslib::card_body(id = 'card_confman', class = 'collapse show',
              fluidRow(
                column(width = 6, actionButton('applyConfOne', 'Apply to this session')),
                column(width = 6, actionButton('applyConfAll', 'Apply to all sessions'))
              ),
              fluidRow(
                column(width = 6, downloadButton('saveconfas', labels$saveconfas)),
                column(width = 6, fileInput("uploadconf", NULL, buttonLabel = "Upload configuration", width = "100%"))
              )
            )
          ),
          bslib::card(
            bslib::card_header('Status', makeButton('card_status')),
            bslib::card_body(id = 'card_status', class = 'collapse show',
              verbatimTextOutput(outputId = "eng_conf_output", placeholder = T)
            )
          )
        )
      )
    )
  )