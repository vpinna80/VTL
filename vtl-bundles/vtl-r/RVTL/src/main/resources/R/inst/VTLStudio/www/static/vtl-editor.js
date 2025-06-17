/*
 * Copyright © 2020 Banca D'Italia
 *
 * Licensed under the EUPL, Version 1.2 (the "License");
 * You may not use this work except in compliance with the
 * License.
 * You may obtain a copy of the License at:
 *
 * https://joinup.ec.europa.eu/sites/default/files/custom-page/attachment/2020-03/EUPL-1.2%20EN.txt
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the License is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 *
 * See the License for the specific language governing
 * permissions and limitations under the License.
 */
// Must match the value= in nav_panel_hidden in createPanel
const vtlEditorItems = [
  { label: 'VTL Code',             action: "editor",     isdisabled: false },
  { label: 'Structures',           action: "structures", isdisabled: true },
  { label: 'Data & Lineage',       action: "datasets",   isdisabled: true },
  { label: 'Transformation Graph', action: "graph",      isdisabled: true },
  { label: 'Settings',             action: "settings",   isdisabled: false },
  { label: 'hr',                   action: "",           isdisabled: false },
  { label: 'Close',                action: "close",      isdisabled: false }
]

function createEditorPanel(panelName) {
  const vtlwell = document.getElementById(panelName)
  if (!vtlwell?.querySelector('.cm-editor')) {
    let view = VTLEditor.views[panelName]
    if (!view) {
      const themeName = document.querySelector('#editorTheme-selectized')?.parentElement?.querySelector('.item')?.textContent
      view = VTLEditor.createEditor(panelName, themeName)
      view.dom.onblur = () => Shiny.setInputValue('editorText', view.state.doc.toString())
      VTLEditor.addHotKey(view, 'Ctrl-Enter', () => { $('#compile').click(); return true })
      VTLEditor.addHotKey(view, 'Ctrl-n', () => { $('#newSession')[0].focus(); $('#newSession')[0].select(); return true })
      VTLEditor.addHotKey(view, 'Ctrl-o', () => { $('#scriptFile')[0].click(); return true })
      VTLEditor.addHotKey(view, 'Ctrl-s', () => { $('#saveas')[0].click(); return true })
      Shiny.addCustomMessageHandler('editor-focus', panel => panel == panelName && view.contentDOM.focus())
    }
    vtlwell.appendChild(view.dom)
    vtlwell.nextElementSibling.classList.toggle('vtloutput')
  }
}

function addEditorMenu(span, event) {
  event.stopPropagation()
  const tab = span.parentElement.getAttribute('data-value')
  const wasOpen = span.classList.contains('open')
  document.querySelectorAll('.injected-vtl-editor-tab').forEach(el => el.remove())
  document.querySelectorAll('.with-editor.open').forEach(el => el.classList.remove('open'))
  if (wasOpen) return

  const menu = document.createElement('div')
  menu.className = 'dropdown-menu vtl-editor-tab injected-vtl-editor-tab show'
  menu.setAttribute('data-tab', tab)

  vtlEditorItems.forEach(({ label, action, isdisabled }) => {
    if (action) {
      const disabledClass = (!span.classList.contains('compiled') && isdisabled) && " disabled" || ""
      const html = `<a href="#" class="dropdown-item menu-item ${disabledClass}" data-action="${action}">${label}</a>`
      const menuitem = document.createRange().createContextualFragment(html).firstElementChild

      menuitem.addEventListener('click', function (e) {
        e.preventDefault()
        document.querySelectorAll('.injected-vtl-editor-tab').forEach(el => el.remove())
        document.querySelectorAll('.with-editor.open').forEach(el => el.classList.remove('open'))
        Shiny.setInputValue('sessionMenu', { session: tab, menu: action }, { priority: 'event' })
      })
      menu.appendChild(menuitem)
    } else {
      menu.appendChild(document.createRange().createContextualFragment('<div class="dropdown-divider"></div>').firstElementChild)
    }
  })

  // Position and inject
  const rect = span.getBoundingClientRect()
  menu.style.position = 'absolute'
  menu.style.left = `${rect.right - 20 + window.scrollX}px`
  menu.style.top = `${rect.bottom + window.scrollY}px`

  document.body.appendChild(menu)
  span.classList.add('open')
}

// Closing session menu handler
document.addEventListener('click', e => {
  if (!e.target.closest('.vtl-editor-tab') && !e.target.closest('.with-editor')) {
    document.querySelectorAll('.injected-vtl-editor-tab').forEach(el => el.remove())
    document.querySelectorAll('.with-editor.open').forEach(el => el.classList.remove('open'))
  }
})

$(document).on("shiny:connected", () => {
  Shiny.setInputValue("themeNames", VTLEditor.themes)
  Shiny.addCustomMessageHandler('editor-theme', VTLEditor.setTheme)
  Shiny.addCustomMessageHandler('editor-fontsize', VTLEditor.setFontSize)
  Shiny.addCustomMessageHandler('editor-text', updateEditorText)
  Shiny.addCustomMessageHandler('update-envs', updateSessionEnvs)
})

// Replace the text of a given editor view
function updateEditorText({ panel, text }) {
  const view = VTLEditor.views[panel]
  if (view) {
    view.dispatch({changes: {from: 0, to: view.state.doc.length, insert: text}})
  } else {
    setTimeout(() => updateEditorText({ panel, text }), 20)
  }
}

function updateSessionText() {
  const activeTab = document.querySelector('.active .vtlwell').id;
  Shiny.setInputValue('vtlStatements', VTLEditor.views[activeTab].state.doc.toString(), { priority: 'event' });
}

$(document).ready(function () {
  $("#newSession").keyup(function(e) {
    if (e.keyCode === 13) {
      e.preventDefault()
      $("#createSession").click()
    }
  })
})

function generateLabel(input, escape) {
  return `<div class="d-flex flex-row flex-nowrap align-items-center"><div class="btn">` + (input.value 
    ? `<i class='bi bi-box-arrow-up-right text-primary' ` +
      `onmousedown='openLink(event, "${ escape(input.label) }", "${ escape(input.categ) }")'></div>` +
      `</i><span>${ escape(input.label) }</span></div>`
    : `<i class='bi bi-box-arrow-up-right invisible'></i><span>${ escape(input.label) }</span></div>`
  )
}

function openLink(event, label, categ) {
  event.stopImmediatePropagation()
  url = 'https://sdmx-twg.github.io/vtl/2.2/html/reference_manual/operators'
  window.open(categ ? `${ url }/${ categ }/${ label }` : `${ url }/${ label }`, '_blank')
}

function updateSessionEnvs({ rank, active }) {
  const allNodes = Array.from(document.querySelectorAll(`#${rank} *, #${rank}_inactive *`))
  const activeNodes = active.flatMap(a => allNodes.find(e => e.textContent === a) || [])
  activeNodes.forEach(e => document.querySelector(`#${rank}`).appendChild(e))
  allNodes.filter(node => !activeNodes.includes(node)).forEach(e => document.querySelector(`#${rank}_inactive`).appendChild(e))
}