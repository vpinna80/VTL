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

labels <- list(
  compile = HTML('<span style="margin-right: 1em">Compile</span><span style="font-family: monospace">(Ctrl+Enter)</span>'), 
  saveas = HTML('<span style="margin-right: 1em">Export code as...</span><span style="font-family: monospace">(Ctrl+S)</span>'),
  saveconfas = HTML('<span style="margin-right: 1em">Save current configuration as...</span>'),
  loadconf = HTML('<i class="fas fa-upload" style="margin-right: 0.2em"></i><span>Upload configuration...</span>'),
  newSession = HTML('<span style="margin-right: 1em">New session:</span><span style="font-family: monospace">(Ctrl+N)</span>'), 
  createSession = HTML('<span style="margin-right: 1em">Create new</span><span style="font-family: monospace">(Enter)</span>'), 
  dupSession = 'Duplicate session',
  scriptFile = HTML('<span style="margin-right: 1em">Replace editor content...</span><span style="font-family: monospace">(Ctrl+O)</span>'),
  editorTheme = 'Select editor theme:'
)

makeMinButton <- \(id) tags$button(
  class = "btn btn-sm btn-link float-end",
  type = "button",
  `data-bs-toggle` = "collapse",
  `data-bs-target` = paste0('#', id),
  icon('minus')
)

makeAddButton <- \(func) tags$button(
  class = "btn btn-sm btn-link float-end",
  type = "button",
  onclick = sprintf("%s()", func), 
  icon('plus')
)

makeGrip <- \() {
  tags$div(class = "handle d-flex justify-content-center gap-0",
    tags$i(class = "bi bi-grip-horizontal"),
    tags$i(class = "bi bi-grip-horizontal"),
    tags$i(class = "bi bi-grip-horizontal")
  )
}

makeAddModal <- function(modalID, kind, ...) {
  tags$div(id = modalID, class = "modal fade add-modal", tabindex = "-1", `data-bs-backdrop` = "static",
    tags$div(class = "modal-dialog", tags$div(class = "modal-content",
      tags$div(class = "modal-header py-1",
        tags$div(class = "bi bi-justify me-2", style = "cursor: move"),
        tags$h5(class = "modal-title"),
        tags$button(type = "button", class = "btn-close", `data-bs-dismiss` = "modal")
      ),
      tags$div(class = "modal-body d-flex flex-wrap align-items-center gap-2", 
        tags$div(class = "d-none", DT::datatable(data.frame())),
        tags$label("Name:", `for` = paste0(kind, 'Name'), class = "form-label flex-grow-0 mb-0"),
        tags$div(class = "position-relative d-flex align-items-center flex-grow-1 w-auto",
          tags$input(id = paste0(kind, 'Name'), type = "text", class = "form-control form-control-sm flex-grow-1 w-auto", onblur="validateName(event.target)"), 
          tags$span(class = "position-absolute end-0 me-2 text-success", tags$i())
        ),
        tags$div(class = "flex-breaker"),
        tags$label("Description:", `for` = paste0(kind, 'Desc'), class = "form-label flex-grow-0 mb-0"),
        tags$input(id = paste0(kind, 'Desc'), type = "text", class = "form-control form-control-sm flex-grow-1 w-auto"), 
        tags$div(class = "flex-breaker"),
        ...),
      tags$div(class = "modal-footer justify-content-end",
        tags$button(type = "button", class = "btn btn-secondary", `data-bs-dismiss` = "modal", "Cancel"),
        tags$button(type = "button", class = "btn btn-primary", "Add")
      )
    ))
  )
}

makeDictSearchableList <- \(item) tagList(
  tags$div(class = "position-relative mb-2",
    tags$input(class = "form-control pe-5 dict-filter", `data-target` = paste0(item, 'sList'), 
          name = item, `aria-label` = paste('Search a', item), type = "text"),
    tags$i(class = "bi bi-search position-absolute top-50 end-0 translate-middle-y me-3 text-muted")
  ),
  tags$div(id = paste0(item, 'sList'), class = "list-group scroll-list")
)

vtlUI <- bslib::page_sidebar(
  window_title = 'VTL Studio!',
  theme = bslib::bs_theme(version = 5, preset = 'cosmo', `bs5icons` = TRUE)
    |> bslib::bs_add_variables(bslib_spacer = "0.5rem"),
  sidebar = bslib::sidebar(
    width = 350,
    img(src = "static/logo.svg", class = "vtlLogo"),
    tags$div(style = "text-align: right; width: 98%", "1.2.2-20250903090920"),
    uiOutput("shinyapps"),
    hr(),
    bslib::input_switch(id = 'demomode', label = 'Demo mode'),
    tags$div(id = "normalctrl", class="bslib-gap-spacing d-flex flex-column",
      textInput(inputId = 'newSession', label = labels$newSession), 
      actionButton(inputId = 'createSession', label = labels$createSession), 
      actionButton(inputId = 'dupSession', label = 'Duplicate session'),
      fileInput(inputId = 'scriptFile', label = NULL, accept = 'vtl', buttonLabel = labels$scriptFile)
    ),
    tags$div(id = "democtrl", class="bslib-gap-spacing d-flex flex-column d-none",
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
    tags$link(rel = "stylesheet", type = "text/css", href = "static/jqueryui.css"),
    tags$link(rel = "stylesheet", type = "text/css", href = "static/rowreorder.css"),
    tags$link(rel = "icon", type = "image/png", href = "static/favicon-96x96.png", sizes = "96x96"),
    tags$link(rel = "icon", type = "image/svg+xml", href = "static/favicon.svg"),
    tags$link(rel = "shortcut icon", href = "static/favicon.ico"),
    tags$link(rel = "apple-touch-icon", sizes = "180x180", href = "static/apple-touch-icon.png"),
    tags$link(rel = "manifest", href = "static/site.webmanifest"),
    tags$script(src = "static/dict-editor.js"),
    tags$script(src = "static/vtl-editor.js"),
    tags$script(src = "static/bundle.js"),
    tags$script(src = "static/jqueryui.js"),
    tags$script(src = "static/rowreorder.js"),
  ), bslib::navset_tab(id = "navtab",
    bslib::nav_panel("Global settings",
      bslib::layout_column_wrap(width = 1/2,
        bslib::card(
          bslib::card_header('Network Proxy', makeMinButton('card_proxy')),
          bslib::card_body(id = 'card_proxy', class = 'collapse show',
            bslib::layout_column_wrap(width = 1/2, gap = "1em",
              textInput('proxyHost', 'Host:', J("java.lang.System")$getProperty("https.proxyHost")),
              textInput('proxyPort', 'Port:', J("java.lang.System")$getProperty("https.proxyPort")),
              textInput('proxyUser', 'User:', J("java.lang.System")$getProperty("https.proxyUser")),
              passwordInput('proxyPassword', 'Password:', J("java.lang.System")$getProperty("https.proxyPassword"))
            )
          )
        ),
        bslib::card(
          bslib::card_header('Metadata Repository', makeMinButton('card_meta')),
          bslib::card_body(id = 'card_meta', class = 'collapse show',
            selectInput(inputId = 'repoClass', label = NULL, choices = c("Select a repository..." = "")),
            uiOutput(outputId = "repoProperties")
          )
        ),
        bslib::card(
          bslib::card_header('VTL Environments', makeMinButton('card_envs')),
          bslib::card_body(id = 'card_envs', class = 'collapse show', 
            DT::dataTableOutput("envs", fill = F)
          )
        ),
        bslib::card(
          bslib::card_header('Environment Properties', makeMinButton('card_envprops')),
          bslib::card_body(id = 'card_envprops', class = 'collapse show',
            selectInput(inputId = 'selectEnv', label = NULL, choices = c("Select an environment..." = "")),
            uiOutput(outputId = "envprops")
          )
        ),
        bslib::card(
          bslib::card_header('Configuration Management', makeMinButton('card_confman')),
          bslib::card_body(id = 'card_confman', class = 'collapse show',
            fluidRow(
              column(width = 6, actionButton('applyConfAll', 'Apply to all sessions')),
              column(width = 6, downloadButton('saveconfas', labels$saveconfas))
            ),
            fluidRow(
              column(width = 12, fileInput("uploadconf", NULL, buttonLabel = labels$loadconf, width = "100%"))
            )
          )
        ),
        bslib::card(
          bslib::card_header('Status', makeMinButton('card_status')),
          bslib::card_body(id = 'card_status', class = 'collapse show',
            verbatimTextOutput(outputId = "confOutput", placeholder = T)
          )
        )
      )
    )
  ), tags$div(id = "dictEditor", class = "modal fade", tabindex = "-1", `data-bs-backdrop` = "static", `data-bs-keyboard` = "false",
    tags$div(class = "modal-dialog modal-xl", tags$div(class = "modal-content",
      tags$div(class = "modal-header",
        tags$h5(class = "modal-title", "Json Dictionary Editor"),
        tags$button(type = "button", class = "btn-close", `data-bs-dismiss` = "modal")
      ),
      tags$div(class = "modal-body container-fluid",
        do.call(bslib::layout_columns, c(
          list(gap = "1rem", row_heights = bslib::breakpoints(sm = 4, md = 2, lg = 1)),
          lapply(c('dataset', 'structure', 'variable', 'domain'), function(item) {
            bslib::card(
              bslib::card_header(tools::toTitleCase(item), makeAddButton(paste0(item, 'Modal'))),
              bslib::card_body(makeDictSearchableList(item))
            )  
          })
        ))
      ),
      tags$div(class = "modal-footer justify-content-end",
        tags$button(type = "button", class = "btn btn-secondary", `data-bs-dismiss` = "modal", "Close"),
        tags$button(type = "button", class = "btn btn-primary", onclick = "downloadJson()",
          "Export JSON...", tags$i(class = "bi bi-download ms-2"))
      )
    ))
  ), makeAddModal('datasetModal', kind = 'addds',
    tags$label("Structure:", `for` = "adddsStructure", class = "form-label flex-grow-0 mb-0"),
    tags$select(id = "adddsStructure", class = "form-select flex-grow-1 w-auto",
      tags$option(value = "", "Select a Structure...", disabled = NA, selected = NA)
    ), 
    tags$div(class = "flex-breaker"),
    tags$div(class = "resizable-table flex-shrink-0 w-100",
      tags$table(id = "adddsComps", class = "table table-sm table-hover mb-0",
        tags$thead(class = "table-light sticky-top", tags$tr(
          tags$th(scope = "col", "Component"),
          tags$th(scope = "col", "Subset"),
          tags$th(scope = "col", tags$i(class = "bi bi-diagram-3")),
          tags$th(scope = "col", tags$i(class = "bi bi-sliders2")),
          tags$th(scope = "col", "∅")
        )), tags$tbody(), tags$tfoot(tags$tr())
      ), makeGrip()
    )
  ), makeAddModal('structureModal', kind = 'addstr',
    tags$datalist(id = "varnamelist"),
    tags$label("Import components from:", `for` = "dsstrImport", class = "form-label flex-grow-0 mb-0"),
    tags$select(id = "dsstrImport", type = "text", class = "form-control form-control-sm flex-grow-1 w-auto", placeholder = "Select another DataStructure..."),
    tags$button(class = "btn btn-sm btn-secondary", tags$i(class = "bi bi-plus-lg")),
    tags$div(class = "flex-breaker"),
    tags$div(class = "resizable-table flex-shrink-0 w-100",
      tags$table(id = "addstrComps", class = "table table-sm table-hover mb-0",
        tags$thead(class = "table-light sticky-top", tags$tr(
          tags$th(scope = "col", "Component"),
          tags$th(scope = "col", "Description"),
          tags$th(scope = "col", "Role"),
          tags$th(scope = "col", "Domain"),
          tags$th(scope = "col", colspan="2", "∅")
        )), 
        tags$tbody(), tags$tfoot(tags$tr())
      ), makeGrip()
    ), tags$div(class = "add-row text-end w-100",
      tags$button(class = "btn btn-sm btn-outline-success", tags$i(class = "bi bi-plus-lg"))
    )
  ),
  makeAddModal('variableModal', kind = 'addvar',
    tags$label("Value Domain:", `for` = "addvarDomain", class = "form-label flex-grow-0 mb-0"),
    tags$select(id = "addvarDomain", class = "form-select flex-grow-1 w-auto")
  ),
  makeAddModal('domainModal', kind = 'adddom',
    tags$label("Parent Value Domain:", `for` = "adddomParent", class = "form-label flex-grow-0 mb-0"),
    tags$select(id = "adddomParent", class = "form-select flex-grow-1 w-auto"),
    tags$div(class = "flex-breaker"),
    tags$input(id = "adddomIsEnum", type = "radio", name = "domtype", 
        value = "enumerated", class = "form-check-input", onchange = "showDomainModalPane()"),
    tags$label(class = "form-check-label flex-grow-1", `for` = "adddomIsEnum", "Enumerated"),
    tags$input(id = "adddomIsDesc", type = "radio", name = "domtype", 
        value = "described", class = "form-check-input", onchange = "showDomainModalPane()"),
    tags$label(class = "form-check-label flex-grow-1", `for` = "adddomIsDesc", "Described"),
    tags$div(class = "domain-type enumerated d-flex d-none flex-wrap align-items-center gap-2 flex-shrink-0 w-100",
      tags$div(class = "resizable-table w-100",
        tags$table(id = "adddomCodes", class = "table table-sm table-hover mb-0",
          tags$thead(class = "table-light sticky-top", tags$tr(
            tags$th(scope = "col", "Code"),
            tags$th(scope = "col", "Description"),
            tags$th(scope = "col", "")
          )), 
          tags$tbody()
        ), makeGrip()
      ), tags$div(class = "add-row text-end w-100",
        tags$button(class = "btn btn-sm btn-outline-success", tags$i(class = "bi bi-plus-lg"))
      )
    ), tags$div(class = "domain-type described w-100 d-none",
      tags$label(class = "form-check-label", `for` = "adddomFormula", "Value Domain Expression:"),
      tags$textarea(id = "adddomFormula", rows = "4", class = "form-control form-control-sm font-monospace overflow-auto text-nowrap"),
    )
  )
)