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
jQuery.fn.dataTable.ext.errMode = 'throw';

function validateName(input, list) {
  const pattern = /^[A-Za-z_][A-Za-z0-9_.]*$|^'.*'$|^[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_.]*(\([0-9]+(\.[0-9]+)*(\.[_+*~])?\))?(:([A-Za-z0-9_.])+)?$/
  const valid = !list?.includes(input.value) && pattern.test(input.value)
  const badge = input.nextElementSibling
  input.setAttribute('aria-invalid', valid ? 'false' : 'true')
  badge.classList.toggle('text-success', valid)
  badge.classList.toggle('text-danger', !valid)
  badge.querySelector('i').className = valid ? 'bi bi-check-circle-fill' : 'bi bi-x-circle-fill'
  return valid
}

function getDatasets() {
  return Array.from(document.getElementById("datasetsList").children)?.map(ds => ds.dataset.name)
}

function getDSComps(dsname, strname) {
  const ds = document.getElementById("datasetsList").querySelector(`[data-name="${ CSS.escape(dsname) }"]`)
  const dsComps = Array.from(ds?.querySelectorAll("template") || [])
    .map(c => ({
      name: c.dataset.name,
      ...(c.dataset.description && { description: c.dataset.description }),
      subset: c.dataset.subset,
      ...(c.dataset.nullable && { nullable: JSON.parse(c.dataset.nullable) })
    })).reduce((acc, c) => (acc[c.name] = c, acc), {})

  const queryName = strname || (document.getElementById("structuresList")
      .querySelector(`[data-name="${ CSS.escape(ds.dataset.structure) }"]`).dataset.name)
  const comps = getStrComps(queryName)
  return comps.map(c => ({
        ...c, 
        subset: dsComps[c.name]?.subset || c.domain,
        ...(c.nullable !== undefined && { nullable: c.nullable })
    }))
}

function getStructures() {
  return Array.from(document.getElementById("structuresList").children)?.map(ds => ds.dataset.name)
}

function getStrComps(name) {
  const comps = Array.from(document.getElementById("structuresList")
        .querySelectorAll(`[data-name="${ CSS.escape(name) }"] template`) || [])
  return comps.map(c => ({
      name: c.dataset.name,
      role: c.dataset.role,
      domain: getVarDomain(c.dataset.name),
      ...(c.dataset.description && { description: c.dataset.description }),
      ...(c.dataset.nullable && { nullable: JSON.parse(c.dataset.nullable) })
    }))
}

function getVariables() {
  return Array.from(document.getElementById("variablesList").children)
    .map(v => ({
      name: v.dataset.name,
      domain: v.dataset.domain
    }))
}

function getVarDomain(name) {
  return document.getElementById("variablesList")?.querySelector(`[data-name="${ CSS.escape(name) }"]`)?.dataset?.domain
}

function getDomainTree(name) {
  const stdDoms = [ 'string', 'number', 'integer', 'boolean', 'date', 'time', 'time_period', 'duration' ]
  if (!name) {
    return Array.from(document.getElementById('domainsList').children)
      .map(d => ({ name: d.dataset.name }))
      .concat(stdDoms.map(d => ({ name: d })))
  } else {
    const more = [ name ]
    const result = []
    while (more.length) {
      const elem = more.pop()
      result.push(elem?.dataset || { name: elem })
      more.push(...Array.from(document.getElementById('domainsList').querySelectorAll(`[data-parent="${ CSS.escape(elem) }"]`)))
    }
    return result
  }
}

function createModelElement(item) {
  const elem = document.createElement("button")
  elem.className = "list-group-item list-group-item-action d-flex justify-content-between align-items-center"
  elem.textContent = item.name
  elem.tabIndex = 0
  elem.type = "button"
  elem.dataset.name = item.name
  item.description && (elem.dataset.description = item.description)
  const badge = document.createElement("span")
  badge.className = 'badge text-secondary'
  elem.appendChild(badge)
  return elem
}

function datasetModal(source) {
  const bodyHeight = 164
  const modal = document.getElementById('datasetModal')
  const classes = "list-group-item list-group-item-action d-flex justify-content-between align-items-center"
  const dsname = document.getElementById("adddsName")
  const dsdesc = document.getElementById("adddsDesc")
  const strSelect = document.getElementById("adddsStructure")
  const datasets = document.getElementById('datasetsList')
  const structures = document.getElementById("structuresList")
  const domains = document.getElementById('domainsList')
  const handle = modal.querySelector('.handle')
  const submit = modal.querySelector(".btn-primary").cloneNode()
  const tableElem = jQuery('#adddsComps')
  const table = jQuery.fn.dataTable.isDataTable(tableElem) ? tableElem.DataTable() : (() => {
    resizeHandleListener(modal, handle, bodyHeight)
    return jQuery('#adddsComps').DataTable({ paging: false, searching: false, info: false, ordering: false, 
      scrollY: bodyHeight, autoWidth: false, columns: [
        {
          width: '40%'
        }, {
          createdCell: td => {
            td.addEventListener('focusin', e => {
              if (e.target.closest('.selectize-control')) return
              td.innerHTML = '<select class="form-control form-control-sm"></select>'
              const select = td.children[0]
              select.add(new Option('', ''))
              const name = td.nextElementSibling?.querySelector("i")?.dataset?.parent
              getDomainTree(name).forEach(({ name }) => select.add(new Option(name, name)))
              const selectCompDomain = select.selectize || jQuery(select).selectize ({
                dropdownParent: 'body', selectOnTab: true,
                onBlur: () => {
                  const value = selectCompDomain.getValue()
                  selectCompDomain.destroy()
                  value ? table.cell(td).data(value) : table.cell(td).draw()
                  submit.disabled = Array.from(table.column(1).nodes().map(e => e.textContent)).some(v => !v)
                }
              })[0].selectize
              setTimeout(() => {
                selectCompDomain.focus()
                selectCompDomain.positionDropdown()
              }, 0)
            })
          }, render: value => {
            if (value) {
              const span = document.createElement("span")
              span.tabIndex = 0
              span.textContent = value
              return span.outerHTML
            } else {
              return '<i tabindex="0" class="bi bi-x-circle-fill text-danger"></i>'
            }
          }
        }, {
          width: '1%', render: value => value && `<i class="bi bi-diagram-3 text-info" data-parent="${ value }"></i>` || ''
        }, {
          width: '1%', render: value => {
            const icon = value === 'Identifier' ? 'key' : value === 'Measure' ? 'bar-chart' : 'tag'
            return `<i class="bi bi-${ icon }"></i>`
          }
        }, {
          width: '1%', createdCell: (td, value, row, r) => {
            td.innerHTML = `<input type="checkbox" id="ds-nullable-${ r }" class="form-check-input" ${ value ? 'checked' : 'disabled' }>`
          }
        }
      ]
    })
  })()

  // clear all states and reinitialize the modal
  modal.querySelector("h5").textContent = `${ source ? 'Edit' : 'Add' } a Dataset`
  table.clear().draw()
  dsname.value = source?.dataset.name || ''
  dsname.disabled = !!source
  dsname.nextElementSibling.querySelector('i').className = ""
  dsdesc.value = source?.dataset?.description || ''
  while (strSelect.options.length > 1) strSelect.remove(1)
  strSelect.value = ""

  if (!strSelect.selectize) {
    Array.from(structures.children).forEach(s => strSelect.add(new Option(s.dataset.name, s.dataset.name)))
    jQuery(strSelect).selectize({
      render: {
        option: function(data, escape) {
          const count = getStrComps(data.value).length
          return `<div class="${ classes } ps-3">${ data.value }<span class="badge text-secondary">${ count } components</span></div>`
        }
      }, onChange: value => {
        table.clear()
        if (!value)
          return

        const comps = getDSComps(dsname.value, value)
        const rows = comps.map(({ name, role, domain, subset, nullable }) => [
          name, subset || '', domain || '', role, role !== 'Identifier' && nullable !== false
        ])
        table.rows.add(rows).draw()
        submit.disabled = comps.some(({ name }) => !getVarDomain(name))
      }
    })
  }

  strSelect.selectize.clear()
  source && strSelect.selectize.setValue(source.dataset.structure)

  modal.querySelector(".btn-primary").replaceWith(submit)
  submit.textContent = source ? 'Save' : 'Add'
  submit.disabled = !source
  submit.addEventListener("click", e => {
    if (validateName(dsname, source ? [] : getDatasets())) {
      const ds = createModelElement({ name: dsname.value, description: dsdesc.value })
      const badge = ds.querySelector('span')
      const icon = document.createElement('i')
      icon.className = 'bi bi-table'
      badge.appendChild(icon)
      ds.dataset.structure = strSelect.value
      nullables = table.column(4).nodes().toArray().map(n => n.children[0])
      table.rows().data().toArray()
        .filter((r, i) => r[2] !== r[3] || !nullables[r.i].disabled && !nullables[r.i].checked)
        .forEach(r => {
          const component = document.createElement('template')
          component.dataset.name = r[0]
          component.dataset.subset = r[2]
          r[4] && (component.dataset.nullable = JSON.stringify(r[4]))
          ds.appendChild(component)
        })
      ds.addEventListener('click', e => datasetModal(ds))
      source && datasets.replaceChild(ds, source) || datasets.appendChild(ds)
      bootstrap.Modal.getOrCreateInstance(modal).hide()
    }
  })

  bootstrap.Modal.getInstance(document.getElementById('dictEditor'))?.hide()
  modal.addEventListener('hide.bs.modal', () => document.activeElement.blur(), { once: true })
  modal.addEventListener('hidden.bs.modal', () => bootstrap.Modal.getInstance(document.getElementById('dictEditor'))?.show(), { once: true })
  setTimeout(() => new bootstrap.Modal(modal).show(), 0)
}

function structureModal(source) {
  const bodyHeight = 192
  const modal = document.getElementById('structureModal')
  const classes = "list-group-item list-group-item-action d-flex justify-content-between align-items-center"
  const strname = document.getElementById("addstrName")
  const strdesc = document.getElementById("addstrDesc")
  const structures = document.getElementById("structuresList")
  const strImport = document.getElementById("dsstrImport")
  const strImportBtn = document.getElementById("dsstrImport").nextElementSibling
  const domains = document.getElementById('domainsList')
  const submit = modal.querySelector(".btn-primary").cloneNode()
  const handle = modal.querySelector('.handle')
  const tableElem = jQuery('#addstrComps')
  const varnamelist = document.getElementById('varnamelist')
  const table = jQuery.fn.dataTable.isDataTable(tableElem) ? tableElem.DataTable() : (() => {
    resizeHandleListener(modal, handle, bodyHeight)
    modal.querySelector('.add-row .btn').onclick = e => {
      const row = table.row.add([ '', '', 'Identifier', '', false, '' ])
      row.draw()
      row.node().children[0].click()
    }
    return tableElem.DataTable({ paging: false, searching: false, info: false, ordering: false, 
      rowReorder: { selector: 'i.handle' }, scrollY: bodyHeight, autoWidth: false, columns: [
        {
          width: '30%', createdCell: (td, data) => {
            td.classList.add("position-relative", "d-flex", "align-items-center", "w-auto")
            td.innerHTML = `<i class="handle bi bi-grip-vertical"></i><input name="varname" value="${ CSS.escape(data) }"
                type="text" list="varnamelist" class="form-control form-control-sm flex-grow-1"></input>
                <span class="position-absolute end-0 me-3"><i class="bi"></i></span>`
            const varname = td.querySelector("input[type=text]")
            varname.addEventListener('blur', () => {
              const r = table.row(td).index()
              const prev = Array.from(table.column(0).nodes()).map(n => n.querySelector("input").value)
              prev.length = r
              table.cell({ row: r, column: 3 }).data(validateName(varname, prev) ? getVarDomain(varname.value) || '' : '')
            })
          }
        }, {
          width: '50%', createdCell: (td, data) => {
            td.innerHTML = `<input name="codedesc" type="text" class="form-control form-control-sm
                w-100" value="${ data }"><span class="position-absolute end-0 me-3"><i></i></span>`
          }
        }, {
          width: '24px', render: value => {
            const icon = value === 'Identifier' ? 'key' : value === 'Measure' ? 'bar-chart' : 'tag'
            return `<button type="button" class="btn btn-sm btn-light"><i class="bi bi-${ icon }" aria-label="${ value }"></i></button>`
          }, createdCell: td => td.addEventListener('click', () => {
            cell = table.cell(td)
            switch (cell.data()) {
              case 'Identifier': cell.data('Measure'); break;
              case 'Measure': cell.data('Attribute'); break;
              case 'Attribute': cell.data('Identifier'); break;
            }
            const checkbox = table.cell({ row: cell.index().row, column: 4 }).node().querySelector('input[type=checkbox]')
            checkbox.checked = cell.data() !== 'Identifier'
            checkbox.disabled = cell.data() === 'Identifier'
            setTimeout(() => td.querySelector('button')?.focus(), 0)
          })
        }, {
          width: '20%', render: value => value || '<i class="bi bi-exclamation-triangle-fill text-warning"></i>'
        }, {
          width: '24px', createdCell: (td, value, row) => {
            const disabled = row[2] === 'Identifier' ? 'disabled' : ''
            const checked = value && !disabled ? 'checked' : ''
            td.innnerHTML = `<input type="checkbox" name="str-nullable" class="form-check-input"
                            aria-label="Nullable" ${ disabled } ${ checked }></input>`
          }
        }, {
          width: '24px', createdCell: td => {
            td.classList.add('text-nowrap')
            td.innerHTML = '<button type="button" class="btn btn-sm btn-light" aria-label="Delete"><i class="bi bi-trash3"></i></button>'
            td.children[0].onclick = () => table.row(td).remove().draw()
          }
        }
      ]
    })
  })()

  if (!strImport.selectize) {
    strImportBtn.addEventListener('click', () => {
      const comps = getStrComps(strImport.value)
      const rows = comps.map(({ name, description, role, nullable }) => [ name, description || '', role, 
              getVarDomain(name) || '', role !== 'Identifier' && nullable !== false, '' ])
      table.rows.add(rows)
    })
    jQuery(strImport).selectize({
      render: {
        option: function(data, escape) {
          const count = getStrComps(data.value).length
          return `<div class="${ classes } ps-3">${ data.value }<span class="badge text-secondary">${ count } components</span></div>`
        }
      }, onChange: value => {
        strImportBtn.disabled = !value || value === strname.value
      }
    })
  }
  strImport.selectize.clearOptions()
  Array.from(structures.children).map(d => d.dataset.name).forEach(d => strImport.selectize.addOption({ value: d, text: d }))
  strImport.selectize.clear()

  modal.classList.add('modal-lg')
  modal.querySelector("h5").textContent = `${ source ? 'Edit' : 'Add' } a DataStructure`
  submit.textContent = source ? 'Save' : 'Add'
  !source && setTimeout(() => strname.focus(), 0)
  strname.disabled = !!source
  strname.value = source?.dataset.name || ''
  strdesc.value = source?.dataset?.description || ''
  varnamelist.innerHTML = ''
  getVariables().map(v => new Option('', v.name)).forEach(o => varnamelist.appendChild(o))
  table.rows().clear().draw()
  if (source) {
    const comps = getStrComps(source.dataset.name)
    const rows = comps.map(({ name, description, role, nullable }) => [ name, description || '', role, 
            getVarDomain(name) || '', role !== 'Identifier' && nullable !== false, '' ])
    table.rows.add(rows).draw()
    submit.disabled = false
  }

  modal.querySelector(".btn-primary").replaceWith(submit)
  submit.addEventListener("click", e => {
    const comps = Array.from(table.column(0).nodes().map(c => c.querySelector("input")))
    if (validateName(strname, source ? [] : getStructures()) && 
        comps.every((c, r) => validateName(c, comps.filter((_, i) => i < r).map(c => c.value)))) {
      const struct = createModelElement({ name: strname.value, description: strdesc.value })
      const badge = struct.querySelector('span')
      badge.textContent = table.rows().count() + " components"
      table.rows().nodes().toArray().map(n => Array.from(n.children)).forEach(r => {
        const component = document.createElement('template')
        component.dataset.name = r[0].querySelector("input").value
        r[1].querySelector("input").value && (component.dataset.description = r[1].querySelector("input").value)
        const role = table.cell(r[2]).data()
        component.dataset.role = role
        const notnull = role !== 'Identifier' && !r[4].children[0].checked
        notnull && (component.dataset.nullable = JSON.stringify(false))
        struct.appendChild(component)
      })
      struct.addEventListener('click', e => structureModal(struct))
      bootstrap.Modal.getOrCreateInstance(modal).hide()
      source ? source.replaceWith(struct) : structures.appendChild(struct)
    }
  })
  
  bootstrap.Modal.getInstance(document.getElementById('dictEditor'))?.hide()
  modal.addEventListener('hide.bs.modal', () => document.activeElement.blur(), { once: true })
  modal.addEventListener('hidden.bs.modal', () => bootstrap.Modal.getInstance(document.getElementById('dictEditor'))?.show(), { once: true })
  modal.addEventListener('shown.bs.modal', () => table.columns.adjust(), { once: true })
  setTimeout(() => new bootstrap.Modal(modal).show(), 0)
}

function variableModal(source) {
  const classes = "list-group-item list-group-item-action d-flex justify-content-between align-items-center"
  const modal = document.getElementById('variableModal')
  const varname = document.getElementById("addvarName")
  const vardesc = document.getElementById("addvarDesc")
  const domainname = document.getElementById("addvarDomain")
  const submit = modal.querySelector(".btn-primary").cloneNode()
  const variables = document.getElementById("variablesList")
  const domains = document.getElementById('domainsList')
  
  varname.disabled = !!source
  varname.value = source?.dataset?.name || ''
  vardesc.value = source?.dataset?.description || ''
  
  const domainselect = domainname.selectize || (jQuery(domainname).selectize({
      render: {
        option: data => {
          return domainsList.querySelector(`[data-name="${ data.value }"]`)?.cloneNode(true) ||
              `<div class="${ classes } ps-3">${ data.value }</div>`
        }
      }, onChange: (value) => modal.querySelector(".btn-primary").disabled = !value
    }), domainname.selectize)
  domainselect.clearOptions()
  domainselect.clear()
  getDomainTree().forEach(({ name }) => domainselect.addOption({ value: name, text: name }))
  source && domainselect.setValue(source.dataset.domain)

  modal.querySelector("h5").textContent = `${ source ? 'Edit' : 'Add' } a Variable`
  modal.querySelector(".btn-primary").replaceWith(submit)
  submit.textContent = source ? 'Save' : 'Add'
  submit.disabled = !source
  submit.addEventListener("click", () => {
    if (domainname.value && validateName(varname, source ? [] : getVariables().map(v => v.name))) {
      const variable = createModelElement({ name: varname.value, description: vardesc.value })
      variable.dataset.domain = domainname.value
      variable.addEventListener('click', e => variableModal(variable))

      bootstrap.Modal.getOrCreateInstance(modal).hide()
      source ? source.replaceWith(variable) : variables.appendChild(variable)
    }
  })

  bootstrap.Modal.getInstance(document.getElementById('dictEditor'))?.hide()
  modal.addEventListener('hide.bs.modal', () => document.activeElement.blur(), { once: true })
  modal.addEventListener('hidden.bs.modal', () => bootstrap.Modal.getInstance(document.getElementById('dictEditor'))?.show(), { once: true })
  setTimeout(() => new bootstrap.Modal(modal).show(), 0)
}

function domainModal(source) {
  const bodyHeight = 192
  const classes = "list-group-item list-group-item-action d-flex justify-content-between align-items-center"
  const modal = document.getElementById('domainModal')
  const submit = modal.querySelector(".btn-primary").cloneNode()
  const domname = document.getElementById("adddomName")
  const domdesc = document.getElementById("adddomDesc")
  const domparent = document.getElementById("adddomParent")
  const formula = document.getElementById("adddomFormula")
  const radios = modal.querySelectorAll("input[type=radio]")
  const domains = document.getElementById('domainsList')
  const tableElem = jQuery(document.getElementById('adddomCodes'))
  const table = tableElem.DataTable({ paging: false, searching: false, info: false, 
      ordering: false, scrollY: bodyHeight, autoWidth: false, columns: [
        {
          width: '20%', createdCell: (td, data) => {
            td.className = "d-flex align-items-center position-relative"
            td.innerHTML = `<input name="codeitem" type="text" class="form-control form-control-sm
                w-100" value="${ CSS.escape(data) }"><span class="position-absolute end-0 me-3"><i></i></span>`
            const codeInput = td.children[0]
            codeInput.addEventListener('blur', e => {
              const list = table.column(td).nodes().toArray().map(cell => cell.children[0].value)
              list.length = table.cell(td).index().row
              validateName(codeInput, list)
            })
          }
        }, {
          width: '80%', createdCell: (td, data) => {
            td.innerHTML = `<input name="codedesc" type="text" class="form-control form-control-sm
                w-100" value="${ CSS.escape(data) }"><span class="position-absolute end-0 me-3"><i></i></span>`
          }
        }, {
          width: '1px', createdCell: td => {
            td.innerHTML = `<button type="button" class="btn btn-sm btn-light" aria-label="Delete"><i class="bi bi-trash3"></i></button>`
            td.querySelector('.btn').addEventListener('click', () => table.row(td).remove().draw(), { once: true })
          }
        }
      ]
    })

  resizeHandleListener(modal, modal.querySelector('.handle'), bodyHeight)
  addrowListener(modal.querySelector('.add-row .btn'), table, [ '', '', '' ]).draw()
  domname.disabled = !!source
  domname.value = source?.dataset?.name || ''
  domdesc.value = source?.dataset?.description || ''

  const domainselect = domparent.selectize || (jQuery(domparent).selectize({
      create: true,
      render: {
        option: data => domainsList.querySelector(`[data-name="${ data.value }"]`)?.cloneNode(true) ||
              `<div class="${ classes } ps-3">${ data.value }</div>`
      }, onChange: (value) => modal.querySelector(".btn-primary").disabled = !value
    }), domparent.selectize)
  domainselect.clearOptions()
  domainselect.clear()
  getDomainTree().forEach(({ name }) => domainselect.addOption({ value: name, text: name }))
  const pname = source?.dataset?.parent
  pname && domainselect.addOption({ value: pname, text: pname })
  pname && domainselect.setValue(pname)
  Array.from(radios).forEach(r => r.checked = false)
  
  if (source?.querySelector("template")) {
    radios[0].checked = true
    table.rows().clear()
    table.rows.add(Array.from(source.querySelectorAll("template") || [])
      .map(c => [ c.dataset.name, c.dataset.description || '', '' ])).draw()
  } else if (source?.dataset?.described) {
    radios[1].checked = true
    formula.value = source.dataset.described
  }

  if (source) {
    modal.querySelectorAll(".domain-type").forEach(el => el.classList.add("d-none"))
    modal.getElementsByClassName(modal.querySelector('input:checked')?.value)[0].classList.remove("d-none")
  }

  modal.querySelector("h5").textContent = `${ source ? 'Edit' : 'Add' } a Value Domain`
  modal.querySelector(".btn-primary").replaceWith(submit)
  submit.textContent = source ? 'Save' : 'Add'
  submit.disabled = !source
  submit.addEventListener("click", () => {
    if (domname.value && validateName(domname, source ? [] : getDomainTree().map(v => v.name))) {
      const domain = createModelElement({ name: domname.value, description: domdesc.value })
      const badge = domain.querySelector('span')
      const icon = radios[0].checked ? "bi bi-list-ul" : "bi bi-calculator"
      badge.innerHTML = `<i class="${ icon }"></i>`
      domain.dataset.parent = domainselect.getValue()
      if (radios[0].checked) {
        const codeCol = table.column(0).nodes().toArray().map(td => td.children[0])
        const descs = table.column(1).nodes().toArray().map(td => td.children[0].value)
        const codes = codeCol.map(input => input.value)
        if (codeCol.every((input, i) => validateName(input, codes.slice(0, i)))) {
          domain.dataset.enumerated = true
          codes.forEach((c, i) => {
            const code = document.createElement('template')
            code.dataset.name = c
            descs[i] && (code.dataset.description = descs[i])
            domain.appendChild(code)
          })
          domain.addEventListener('click', e => domainModal(domain))
          source ? domains.replaceChild(domain, source) : domains.appendChild(domain)
          bootstrap.Modal.getOrCreateInstance(modal).hide()
        }
      } else {
        domain.dataset.described = formula.value
        domain.addEventListener('click', e => domainModal(domain))
        source && domains.replaceChild(domain, source) || domains.appendChild(domain)
        bootstrap.Modal.getOrCreateInstance(modal).hide()
      }
    }
  })
  
  bootstrap.Modal.getInstance(document.getElementById('dictEditor'))?.hide()
  modal.addEventListener('hide.bs.modal', () => {
    document.activeElement.blur()
    table.destroy()
  }, { once: true })
  modal.addEventListener('hidden.bs.modal', () => bootstrap.Modal.getInstance(document.getElementById('dictEditor'))?.show(), { once: true })
  modal.addEventListener('shown.bs.modal', () => table.columns.adjust(), { once: true })
  setTimeout(() => new bootstrap.Modal(modal).show(), 0)
}

function createEditDictWindow(msg) {
  const modal = document.getElementById('dictEditor')
  const classes = "list-group-item list-group-item-action d-flex justify-content-between align-items-center ps-3"

  msg?.data?.forEach(item => {
    const ds = createModelElement(item)
    const badge = ds.querySelector("span")
    ds.dataset[item.structure ? "structure" : "subset"] = item.structure || item.subset
    item.structure && item.components.forEach(c => {
      const component = document.createElement('template')
      component.dataset.name = c.name
      c.description && (component.dataset.description = c.description)
      component.dataset.subset = c.subset
      c.nullable && (component.dataset.nullable = JSON.stringify(c.nullable))
      ds.appendChild(component)
    })
    const icon = document.createElement('i')
    icon.className = item.structure ? 'bi bi-table' : 'bi bi-123'
    badge.appendChild(icon)
    ds.appendChild(badge)
    item.structure && ds.addEventListener('click', e => datasetModal(ds))
    document.getElementById('datasetsList').appendChild(ds)
  })
  msg?.structures?.forEach(item => {
    const struct = createModelElement(item)
    const badge = struct.querySelector("span")
    item.components.forEach(c => {
      const component = document.createElement('template')
      component.dataset.name = c.name
      component.dataset.role = c.role
      c.description && (component.dataset.description = c.description)
      c.nullable && (component.dataset.nullable = JSON.stringify(c.nullable))
      struct.appendChild(component)
    })
    document.getElementById("structuresList").appendChild(struct)
    badge.textContent = getStrComps(item.name).length + " components"
    struct.appendChild(badge)
    struct.addEventListener('click', e => structureModal(struct))
  })
  msg?.variables?.forEach(item => {
    const variable = createModelElement(item)
    variable.dataset.domain = item.domain
    variable.addEventListener('click', e => variableModal(variable))
    document.getElementById("variablesList").appendChild(variable)
  }) // ({ name, parent, enumerated, described })
  msg?.domains?.forEach(item => {
    const domain = createModelElement(item)
    const badge = domain.querySelector("span")
    const icon = item.enumerated ? "bi bi-list-ul" : item.described ? "bi bi-calculator" : ""
    badge.innerHTML = `<i class="${ icon }"></i>`
    domain.dataset.parent = item.parent
    if (item.enumerated) {
      domain.dataset.enumerated = true
      item.enumerated.forEach((c, i) => {
        const code = document.createElement('template')
        if (typeof(c) === 'object') {
          code.dataset.name = c.name
          c.description && (code.dataset.description = c.description)
        } else {
          code.dataset.name = c
        }
        domain.appendChild(code)
      })
    }
    item.described && (domain.dataset.described = item.described)
    domain.addEventListener('click', e => domainModal(domain))
    document.getElementById("domainsList").appendChild(domain)
  })

  modal.addEventListener('hide.bs.modal', () => document.activeElement.blur(), { once: true })
  setTimeout(() => {
    new bootstrap.Modal(modal).show()
  }, 0)
}

function resizeHandleListener(modal, handle, minH) {
  const newhandle = handle.cloneNode(true)
  handle.replaceWith(newhandle);
  newhandle.onmousedown = e => {
    e.preventDefault()
    const resizable = modal.querySelector('.dataTables_scrollBody')
    const startY = e.clientY
    const startHeight = parseInt(window.getComputedStyle(resizable).maxHeight || resizable.offsetHeight, 10)
    const mmv = e => {
      const nh = Math.min(Math.max(minH, startHeight + e.clientY - startY), window.innerHeight - 400)
      resizable.style.maxHeight = `${ nh }px`
      resizable.style.height = `${ nh }px`
    }
    const mup = e => {
      document.removeEventListener('mousemove', mmv)
      document.removeEventListener('mousemove', mup)
    }
    document.addEventListener('mousemove', mmv)
    document.addEventListener('mouseup', mup)
  }
}

function addrowListener(button, table, row) {
  const newbutton = button.cloneNode(true)
  button.replaceWith(newbutton)
  return table.row.add(row)
}

function downloadJson() {
  const json = {
    data: Array.from(document.getElementById("datasetsList").children)
            .map(ds => ({ 
              name: ds.dataset.name,
              ...(ds.dataset.description && { description: ds.dataset.description }),
              ...(ds.dataset.structure && { structure: ds.dataset.structure }),
              ...(ds.dataset.subset && { subset: ds.dataset.subset }),
              ...(ds.querySelector("template") && {
                    components: Array.from(ds.querySelectorAll("template"))
                      .map(c => ({
                        name: c.dataset.name,
                        ...(c.dataset.description && { description: c.dataset.description }),
                        subset: c.dataset.subset,
                        ...(c.dataset.nullable && { nullable: c.dataset.nullable })
                      }))
                  })
            })),
    structures: Array.from(document.getElementById("structuresList").children)
            .map(str => ({ 
              name: str.dataset.name,
              ...(str.dataset.description && { description: str.dataset.description }),
              ...(str.querySelector("template") && {
                    components: Array.from(str.querySelectorAll("template"))
                      .map(c => ({
                        name: c.dataset.name,
                        role: c.dataset.role,
                        ...(c.dataset.description && { description: c.dataset.description }),
                        ...(c.dataset.domain && { domain: c.dataset.domain }),
                        ...(c.dataset.nullable && { nullable: c.dataset.nullable })
                      }))
                  }),
            })),
    variables: Array.from(document.getElementById("variablesList").children)
            .map(vari => ({
              name: vari.dataset.name,
              ...(vari.dataset.description && { description: vari.dataset.description }),
              domain: vari.dataset.domain,
            })),
    domains: Array.from(document.getElementById("domainsList").children)
            .map(dom => ({
              name: dom.dataset.name,
              parent: dom.dataset.parent,
              ...(dom.dataset.description && { description: dom.dataset.description }),
              ...(dom.dataset.enumerated && { 
                    enumerated: Array.from(dom.querySelectorAll("template"))
                      .map(c => ({
                        name: c.dataset.name,
                        ...(c.dataset.description && { description: c.dataset.description })
                      }))
                  }),
              ...(dom.dataset.described && { described: dom.dataset.described }),
            }))
  }
  const url = URL.createObjectURL(new Blob([ JSON.stringify(json, null, 2) ], { type: "application/json" }))
  Object.assign(document.createElement("a"), { href: url, download: "model.json" }).click()
  URL.revokeObjectURL(url);
}