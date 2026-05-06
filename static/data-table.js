// ── Field panel ───────────────────────────────────────────────────────────────
function toggleFieldPanel() {
  fieldPanelCollapsed_ = !fieldPanelCollapsed_;
  const panel = document.getElementById('field-panel');
  const btn   = panel.querySelector('.collapse-btn');
  panel.classList.toggle('collapsed', fieldPanelCollapsed_);
  btn.textContent = fieldPanelCollapsed_ ? '›' : '‹';
  btn.title       = fieldPanelCollapsed_ ? 'Expand' : 'Collapse';
}

function _lsFieldsKey()    { return `kibana_cols_${selJobId || 'default'}`; }
function _saveFieldsToLS() { try { localStorage.setItem(_lsFieldsKey(), JSON.stringify([...dataSelectedFields_])); } catch(e) {} }
function _loadFieldsFromLS(available) {
  try {
    const saved = JSON.parse(localStorage.getItem(_lsFieldsKey()) || 'null');
    if (Array.isArray(saved) && saved.length) {
      const fset  = new Set(available);
      const valid = saved.filter(f => f.includes(LINKED_SEP) || fset.has(f));
      if (valid.length) return new Set(valid);
    }
  } catch(e) {}
  return null;
}

async function loadFields(jobId) {
  try { dataFields_ = await api.get(`/api/jobs/${jobId}/fields`); }
  catch(e) { dataFields_ = DEFAULT_FIELDS.slice(); }
  try {
    const gf = await api.get(`/api/jobs/${jobId}/group-fields`);
    linkedJobFields_ = gf.linked || [];
  } catch(e) { linkedJobFields_ = []; }
  if (!dataSelectedFields_) {
    const fromLS = _loadFieldsFromLS(dataFields_);
    if (fromLS) {
      dataSelectedFields_ = fromLS;
    } else {
      const fset  = new Set(dataFields_);
      const smart = PRIORITY_FIELDS.filter(f => fset.has(f));
      dataSelectedFields_ = new Set(smart.length ? smart.slice(0,7) : dataFields_.slice(0,7));
    }
  }
  renderFieldPanel();
}

function renderFieldPanel() {
  const el = document.getElementById('field-list');
  if (!dataFields_.length) { el.innerHTML = '<div style="padding:10px;color:var(--tx2);font-size:11px;">No fields</div>'; return; }
  const q = fieldSearch_.toLowerCase();

  const visibleOwn = q ? dataFields_.filter(f => f.toLowerCase().includes(q)) : dataFields_;
  const ownHtml = visibleOwn.map((f, i) => {
    const checked = dataSelectedFields_.has(f);
    return `<div class="field-item ${checked?'selected':''}">
      <input type="checkbox" id="fld-${i}" data-field="${esc(f)}" data-linked="0" ${checked?'checked':''} onchange="toggleField(this)">
      <label for="fld-${i}" title="${esc(f)}">${esc(f)}</label>
    </div>`;
  }).join('');

  let linkedHtml = '';
  for (const lj of linkedJobFields_) {
    const prefix = lj.label + LINKED_SEP;
    const visibleLinked = q ? lj.fields.filter(f => f.toLowerCase().includes(q) || lj.label.toLowerCase().includes(q)) : lj.fields;
    if (!visibleLinked.length) continue;
    const items = visibleLinked.map((f, i) => {
      const key     = prefix + f;
      const checked = dataSelectedFields_.has(key);
      const uid     = `lfld-${lj.job_id}-${i}`;
      return `<div class="field-item linked ${checked?'selected':''}">
        <input type="checkbox" id="${uid}" data-field="${esc(key)}" data-linked="1" ${checked?'checked':''} onchange="toggleField(this)">
        <label for="${uid}" title="${esc(lj.label + ': ' + f)}">${esc(f)}</label>
      </div>`;
    }).join('');
    linkedHtml += `<div class="field-source-hdr"><span class="src-dot"></span>🔗 ${esc(lj.label)}</div>${items}`;
  }

  el.innerHTML = ownHtml + linkedHtml;
  if (!ownHtml && !linkedHtml) el.innerHTML = '<div style="padding:10px;color:var(--tx2);font-size:11px;">No match</div>';
}

function toggleField(cb) {
  const f = cb.getAttribute('data-field');
  if (cb.checked) dataSelectedFields_.add(f); else dataSelectedFields_.delete(f);
  cb.closest('.field-item').classList.toggle('selected', cb.checked);
  _saveFieldsToLS();
  if (lastDataRows_) renderDataTable(lastDataRows_);
}

function filterFields(val) { fieldSearch_ = val.toLowerCase(); renderFieldPanel(); }

// ── Shared field value helpers (promoted from loadCorrInline closure) ─────────
function _strVal(v) {
  if (Array.isArray(v)) v = v[0] ?? '';
  if (v !== null && typeof v === 'object') return JSON.stringify(v);
  return String(v ?? '');
}

function _fieldRow(k, v, highlight) {
  const disp  = renderKfValue(_strVal(v));
  const safeV = esc(_strVal(v));
  const safeF = esc(k);
  const style = highlight ? ' style="background:rgba(255,166,87,.08);"' : '';
  return `<tr class="kibana-field-row"${style}>
    <td class="kf-actions">
      <button class="kf-btn kf-plus"  title="Filter: ${safeF} = ${safeV.slice(0,40)}" data-f="${safeF}" data-v="${safeV}" onclick="filterInField(this.dataset.f, this.dataset.v)">+</button>
      <button class="kf-btn kf-minus" title="Exclude: ${safeF} = ${safeV.slice(0,40)}" data-f="${safeF}" data-v="${safeV}" onclick="filterOutField(this.dataset.f, this.dataset.v)">−</button>
    </td>
    <td class="kf-name">${esc(k)}</td>
    <td class="kf-value">${disp}</td>
  </tr>`;
}

// ── Table rendering ───────────────────────────────────────────────────────────
function getFieldValue(row, field, rj) {
  if (field.includes(LINKED_SEP)) {
    const sepIdx   = field.indexOf(LINKED_SEP);
    const srcLabel = field.slice(0, sepIdx);
    const srcField = field.slice(sepIdx + 1);
    // In multi-source mode this row IS from that source — read directly from its own JSON
    if (row._source && row._source === srcLabel) {
      const val = rj[srcField];
      let v = Array.isArray(val) ? (val[0]??'') : (val??'');
      if (v !== null && typeof v === 'object') v = JSON.stringify(v);
      return v;
    }
    const linked   = corrDataCache_[String(row.id)] || corrDataCache_[row.id] || [];
    const match    = linked.find(c => c._source_label === srcLabel);
    if (!match) return '';
    const val = (match.raw_json || {})[srcField];
    let v = Array.isArray(val) ? (val[0]??'') : (val??'');
    if (v !== null && typeof v === 'object') v = JSON.stringify(v);
    return v;
  }
  if (field in DATA_DB_MAP) return DATA_DB_MAP[field](row) ?? '';
  const val = rj[field];
  let v = Array.isArray(val) ? (val[0]??'') : (val??'');
  if (v !== null && typeof v === 'object') v = JSON.stringify(v);
  return v;
}

function errorLevelClass(val) {
  if (!val) return '';
  const v = String(val).toUpperCase();
  if (v.includes('ERROR') || v.includes('CRITICAL')) return ' class="el-error"';
  if (v.includes('WARN'))  return ' class="el-warn"';
  if (v.includes('INFO'))  return ' class="el-info"';
  if (v.includes('DEBUG')) return ' class="el-debug"';
  return '';
}

function httpColor(s) {
  if (!s) return '';
  const n = parseInt(s);
  if (n < 300) return 'color:var(--green)';
  if (n < 400) return 'color:var(--blue)';
  if (n < 500) return 'color:var(--orange)';
  return 'color:var(--red)';
}

const SEV_LABEL = {critical:'CRITICAL',high:'HIGH',medium:'MEDIUM',low:'LOW',info:'INFO'};

function sevBadge(sev, rowId, hasSummary) {
  const cls   = sev ? `sev-${sev}` : 'sev-unchecked';
  const label = sev ? (SEV_LABEL[sev] || sev.toUpperCase()) : '·';
  const click = hasSummary ? `onclick="event.stopPropagation();openCodeRefs(${rowId})"` : `onclick="event.stopPropagation();"`;
  return `<span class="sev ${cls}" ${click} title="${hasSummary?'Click for AI summary':''}">${label}</span>`;
}

function renderDataTable(rows) {
  lastDataRows_ = rows;
  const fields     = dataSelectedFields_ ? [...dataSelectedFields_] : DEFAULT_FIELDS;
  const allChecked = rows.length > 0 && rows.every(r => selectedRows_.has(r.id));

  const _sortArrow = f => {
    if (f !== sortField_) return ' <span style="opacity:.25;font-size:9px;">⇅</span>';
    return sortDir_==='asc' ? ' <span style="color:var(--blue);font-size:10px;">▲</span>'
                            : ' <span style="color:var(--blue);font-size:10px;">▼</span>';
  };
  const _thStyle = f => f===sortField_ ? 'cursor:pointer;white-space:nowrap;background:rgba(88,166,255,.08);' : 'cursor:pointer;white-space:nowrap;';

  document.getElementById('data-thead').innerHTML =
    `<tr><th class="col-select"><input type="checkbox" class="row-checkbox" id="select-all-cb" ${allChecked?'checked':''} onchange="selectAll(this.checked)" title="Select all"></th>` +
    '<th style="width:76px;min-width:76px;">Severity</th>' +
    fields.map(f => {
      const fEsc = esc(f);
      if (f.includes(LINKED_SEP)) {
        const parts = f.split(LINKED_SEP);
        return `<th data-v="${fEsc}" style="${_thStyle(f)}" onclick="sortBy(this.dataset.v)" title="${esc(parts[0]+': '+parts[1])}"><span class="linked-col-hdr">[${esc(parts[0])}]</span> ${esc(parts[1])}${_sortArrow(f)}</th>`;
      }
      return `<th data-v="${fEsc}" style="${_thStyle(f)}" onclick="sortBy(this.dataset.v)">${esc(f)}${_sortArrow(f)}</th>`;
    }).join('') +
    '<th style="width:110px;min-width:110px;text-align:right;">Actions</th></tr>';

  const tbody = document.getElementById('data-tbody');
  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="${fields.length+3}" style="color:var(--tx2);padding:20px 14px;text-align:center;">No data — run the job first or adjust filters.</td></tr>`;
    return;
  }

  tbody.innerHTML = rows.map(r => {
    let rj = {};
    try { rj = r.raw_json ? (typeof r.raw_json==='string' ? JSON.parse(r.raw_json) : r.raw_json) : {}; } catch(e) {}
    const rowCls    = r.relevance==='relevant' ? 'relevant-row' : r.relevance==='irrelevant' ? 'irrelevant-row' : '';
    const srcBadge  = r._source
      ? `<span style="font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.05em;padding:1px 5px;border-radius:3px;background:var(--bg3);color:var(--tx2);margin-right:4px;vertical-align:middle;">${esc(r._source)}</span>`
      : '';
    const sev       = sevBadge(r.severity, r.id, !!r.ai_summary);
    const dataCells = fields.map(f => {
      const raw  = getFieldValue(r, f, rj);
      const disp = raw == null ? '' : String(raw);
      let attr   = '';
      if (f==='error_level') attr = errorLevelClass(disp);
      else if (f==='http_status_code'||f==='status') { const c=httpColor(disp); if(c) attr=` style="${c};font-weight:600;"`; }
      return `<td${attr} title="${esc(disp)}">${esc(disp.length>90 ? disp.slice(0,90)+'…' : disp)}</td>`;
    }).join('');
    return `<tr class="${rowCls}" onclick="expandRow(this,${r.id})">
      <td class="col-select" onclick="event.stopPropagation()">
        <input type="checkbox" class="row-checkbox" ${selectedRows_.has(r.id)?'checked':''} onchange="toggleSelectRow(${r.id},this)">
      </td>
      <td style="width:76px;">${sev}${srcBadge}</td>${dataCells}
      <td class="row-actions" onclick="event.stopPropagation()" style="width:110px;text-align:right;padding-right:8px;">
        <button class="btn btn-sm mark-relevant-btn"   style="background:rgba(63,185,80,.15);color:var(--green);border:1px solid rgba(63,185,80,.3);padding:2px 6px;font-size:10px;" onclick="markRow(${r.id},'relevant',this)">✓ Relevant</button>
        <button class="btn btn-sm mark-irrelevant-btn" style="background:rgba(248,81,73,.1);color:var(--red);border:1px solid rgba(248,81,73,.3);padding:2px 6px;font-size:10px;margin-left:3px;" onclick="markRow(${r.id},'irrelevant',this)">✗ Irrel.</button>
      </td>
    </tr>
    <tr id="expand-${r.id}" style="display:none;">
      <td colspan="${fields.length+3}" style="background:var(--bg0);padding:0;border-bottom:1px solid var(--border);">
        ${buildKibanaDocView(r, rj)}
      </td>
    </tr>`;
  }).join('');
}

function expandRow(tr, rowId) {
  const detail = document.getElementById('expand-'+rowId);
  if (!detail) return;
  const open = detail.style.display === 'table-row';
  detail.style.display = open ? 'none' : 'table-row';
  tr.classList.toggle('expanded-row', !open);
  if (!open) {
    const inlineEl = document.getElementById(`corr-inline-${rowId}`);
    if (inlineEl && inlineEl.dataset.loaded !== '1') {
      inlineEl.dataset.loaded = '1';
      loadCorrInline(rowId, inlineEl);
    }
    if (typeof classifiers_ !== 'undefined' && classifiers_.length) {
      const clfSection = document.getElementById(`clf-results-${rowId}`);
      const clfBody    = document.getElementById(`clf-results-body-${rowId}`);
      if (clfSection && clfBody && clfSection.dataset.loaded !== '1') {
        clfSection.dataset.loaded = '1';
        clfSection.style.display = 'block';
        loadLogClassifications(rowId, clfBody);
      }
    }
  }
}

async function loadCorrInline(rowId, el) {
  try {
    const results = await api.get(`/api/logs/${rowId}/correlated`);
    if (!results || !results.length) { el.style.display = 'none'; return; }
    el.style.cssText = '';

    const bySource = {};
    for (const r of results) {
      const lbl = r._source_label || 'Linked';
      if (!bySource[lbl]) bySource[lbl] = [];
      let rj = {};
      try { rj = typeof r.raw_json==='string' ? JSON.parse(r.raw_json) : (r.raw_json||{}); } catch(e) {}
      bySource[lbl].push({...r, _rj: rj});
    }

    el.innerHTML = Object.entries(bySource).map(([label, recs]) => {
      const keys = (recs[0]._matched_keys||[]).map(k=>`<span class="corr-key-badge">${esc(k)}</span>`).join(' ');

      if (recs.length === 1) {
        const r    = recs[0];
        const sev  = r.severity ? `<span class="sev sev-${r.severity}">${r.severity}</span>` : '';
        const rows = Object.entries(r._rj).map(([k,v]) => _fieldRow(k, v, false)).join('');
        return `<div style="border-top:2px solid rgba(88,166,255,.25);margin-top:2px;">
          <div style="background:rgba(88,166,255,.07);padding:6px 56px;display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
            <span class="corr-source-badge">🔗 ${esc(label)}</span>${keys}${sev}
            <span style="color:var(--tx2);font-size:10px;margin-left:auto;">${r.timestamp||''}</span>
          </div>
          <table class="kibana-doc-table"><colgroup><col style="width:56px"><col style="width:180px"><col></colgroup>
            <tbody>${rows}</tbody></table></div>`;
      }

      const allKeys    = [...new Set(recs.flatMap(r => Object.keys(r._rj)))];
      const commonKeys = allKeys.filter(k => recs.every(r => _strVal(r._rj[k]) === _strVal(recs[0]._rj[k])));
      const diffKeys   = allKeys.filter(k => !commonKeys.includes(k));
      const commonRows = commonKeys.map(k => _fieldRow(k, recs[0]._rj[k], false)).join('');
      const uid        = `corr-common-${rowId}-${label.replace(/\W/g,'_')}`;

      const diffSections = recs.map((r, i) => {
        const sev      = r.severity ? `<span class="sev sev-${r.severity}">${r.severity}</span>` : '';
        const diffRows = diffKeys.length
          ? diffKeys.map(k => _fieldRow(k, r._rj[k], true)).join('')
          : `<tr><td colspan="3" style="padding:6px 12px;color:var(--tx2);font-size:11px;">All fields identical to record above</td></tr>`;
        return `<div style="border-top:1px solid rgba(88,166,255,.15);margin-left:56px;">
          <div style="padding:4px 0;display:flex;align-items:center;gap:8px;color:var(--tx2);font-size:10px;">
            <span style="font-weight:600;color:var(--blue);">#${i+1}</span> ${sev}
            <span>${r.timestamp||''}</span>
            ${diffKeys.length ? `<span style="color:var(--orange);">${diffKeys.length} field${diffKeys.length>1?'s':''} differ</span>` : ''}
          </div>
          <table class="kibana-doc-table"><colgroup><col style="width:56px"><col style="width:180px"><col></colgroup>
            <tbody>${diffRows}</tbody></table></div>`;
      }).join('');

      return `<div style="border-top:2px solid rgba(88,166,255,.25);margin-top:2px;">
        <div style="background:rgba(88,166,255,.07);padding:6px 56px;display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
          <span class="corr-source-badge">🔗 ${esc(label)}</span>${keys}
          <span style="color:var(--tx2);font-size:10px;">${recs.length} records</span>
          <span style="color:var(--tx2);font-size:10px;margin-left:auto;">${commonKeys.length} shared · ${diffKeys.length} differ</span>
        </div>
        ${commonRows ? `<div style="padding:3px 0;">
          <div style="padding:3px 56px;font-size:10px;color:var(--tx2);display:flex;align-items:center;gap:6px;cursor:pointer;"
               onclick="this.nextElementSibling.style.display=this.nextElementSibling.style.display==='none'?'':'none';this.querySelector('.toggle-arrow').textContent=this.nextElementSibling.style.display===''?'▾':'▸';">
            <span class="toggle-arrow">▾</span> ${commonKeys.length} fields shared across all ${recs.length} records
          </div>
          <div id="${uid}">
            <table class="kibana-doc-table"><colgroup><col style="width:56px"><col style="width:180px"><col></colgroup>
              <tbody>${commonRows}</tbody></table>
          </div>
        </div>` : ''}
        ${diffSections}
      </div>`;
    }).join('');
  } catch(e) { el.style.display = 'none'; }
}

// ── Data loading ──────────────────────────────────────────────────────────────

/**
 * When "All Sources" is active and the current view is a collection source,
 * returns a comma-separated string of all job IDs in that collection.
 * This enables true multi-source search (vs. correlation-based cross-source).
 */
function _collectionJobIds() {
  if (!crossSourceMode_ || !selCollId) return null;
  const coll = (collections_ || []).find(c => c.id === selCollId);
  if (!coll?.sources?.length) return null;
  const ids = coll.sources.map(s => s.job_id).filter(Boolean);
  return ids.length > 1 ? ids.join(',') : null;
}

async function loadData(jobId, page) {
  const search  = document.getElementById('data-search')?.value.trim() || '';
  const fromRaw = document.getElementById('data-from')?.value || '';
  const toRaw   = document.getElementById('data-to')?.value   || '';
  const params  = new URLSearchParams({page, per_page: perPage_});
  if (search)   params.set('search', search);
  if (regexMode_) params.set('regex', 'true');
  const collIds = _collectionJobIds();
  if (collIds) {
    params.set('job_ids', collIds);          // multi-source: search all collection jobs
  } else if (crossSourceMode_ && (search || fieldFilters_.length)) {
    params.set('cross_source', 'true');      // correlation-based cross-source (standalone jobs)
  }
  if (fromRaw)  params.set('from_date', fromRaw.replace('T',' '));
  if (toRaw)    params.set('to_date',   toRaw.replace('T',' '));
  if (relFilter_) params.set('relevance', relFilter_);
  params.set('sort_by',  sortField_);
  params.set('sort_dir', sortDir_);
  if (fieldFilters_.length) params.set('field_filters', JSON.stringify(fieldFilters_));
  if (typeof classifierFilter_ !== 'undefined' && classifierFilter_ !== null)
    params.set('classifier_id', classifierFilter_);
  if (page === 1) loadHistogram(jobId);
  const d = await api.get(`/api/jobs/${jobId}/data?${params}`);
  const total = d.total || 0;
  const pages = Math.ceil(total / d.per_page) || 1;
  const _csLabel = crossSourceMode_ && (search || fieldFilters_.length)
    ? (collIds ? ' <span style="font-size:10px;color:var(--blue);font-weight:500;">⊕ all sources</span>'
               : ' <span style="font-size:10px;color:var(--blue);font-weight:500;">🔗 incl. correlated</span>')
    : '';
  document.getElementById('data-total').innerHTML = `<b>${total.toLocaleString()}</b> records${_csLabel}`;
  document.getElementById('data-page-info').innerHTML = `Page <b>${d.page}</b> / <b>${pages}</b>`;
  _updateRowsChip(total, (jobs.find(j=>j.id===jobId)||{}).name || null);
  renderDataTable(d.rows);
  const hasLinked = dataSelectedFields_ && [...dataSelectedFields_].some(f => f.includes(LINKED_SEP));
  if (hasLinked && d.rows && d.rows.length) {
    corrDataCache_ = {};
    try {
      const batch = await api.post('/api/logs/correlated-batch', {ids: d.rows.map(r=>r.id)});
      corrDataCache_ = batch || {};
      renderDataTable(d.rows);
    } catch(e) {}
  }
}

function applyDataFilter() { dataPage_=1; loadData(selJobId, 1); }
function clearDataFilter() {
  document.getElementById('data-search').value = '';
  document.getElementById('data-from').value   = '';
  document.getElementById('data-to').value     = '';
  dataPage_=1; loadData(selJobId, 1);
}
function dataPage(delta) { dataPage_ = Math.max(1, dataPage_+delta); clearSelection(); loadData(selJobId, dataPage_); }
function refreshData()   { if (selJobId) loadData(selJobId, dataPage_); }

// ── Histogram ─────────────────────────────────────────────────────────────────
let _histChart = null;

async function loadHistogram(jobId) {
  const wrap   = document.getElementById('histogram-wrap');
  const canvas = document.getElementById('histogram-canvas');
  if (!wrap || !canvas) return;
  const search  = document.getElementById('data-search')?.value.trim() || '';
  const fromRaw = document.getElementById('data-from')?.value || '';
  const toRaw   = document.getElementById('data-to')?.value   || '';
  const p = new URLSearchParams();
  if (search)                     p.set('search', search);
  if (regexMode_)                 p.set('regex', 'true');
  const _hCollIds = _collectionJobIds();
  if (_hCollIds)                  p.set('job_ids', _hCollIds);
  else if (crossSourceMode_ && (search || fieldFilters_.length)) p.set('cross_source', 'true');
  if (fromRaw)                    p.set('from_date', fromRaw.replace('T',' '));
  if (toRaw)                      p.set('to_date',   toRaw.replace('T',' '));
  if (relFilter_)                 p.set('relevance', relFilter_);
  if (fieldFilters_.length)       p.set('field_filters', JSON.stringify(fieldFilters_));
  let hd;
  try { hd = await api.get(`/api/jobs/${jobId}/histogram?${p}`); }
  catch(e) { wrap.classList.add('hidden'); return; }
  if (!hd || !hd.buckets || !hd.buckets.length) { wrap.classList.add('hidden'); return; }
  wrap.classList.remove('hidden');
  const fmt = ts => ts ? ts.replace('T',' ').slice(0,16) : '';
  document.getElementById('histogram-hits').textContent   = `${(hd.total||0).toLocaleString()} hits`;
  document.getElementById('histogram-range').textContent  = `${fmt(hd.from)} – ${fmt(hd.to)}`;
  document.getElementById('histogram-bucket').textContent = `Bucket: ${hd.bucket_label}`;
  const labels = hd.buckets.map(b => b.t);
  const data   = hd.buckets.map(b => b.c);
  if (_histChart) { _histChart.destroy(); _histChart = null; }
  _histChart = new Chart(canvas, {
    type: 'bar',
    data: { labels, datasets: [{ data, backgroundColor:'rgba(63,185,80,0.55)', hoverBackgroundColor:'rgba(63,185,80,0.85)', borderWidth:0, barPercentage:0.9, categoryPercentage:1.0 }] },
    options: {
      responsive:true, maintainAspectRatio:false, animation:false,
      plugins:{ legend:{display:false}, tooltip:{ callbacks:{
        title: items => items[0].label.replace('T',' '),
        label: i    => `${i.raw.toLocaleString()} logs`
      }}},
      scales:{
        x:{ ticks:{color:'#6e7681',maxTicksLimit:8,maxRotation:0,callback:(v,i)=>labels[i]?.replace('T',' ').slice(0,13)}, grid:{display:false}, border:{display:false} },
        y:{ ticks:{color:'#6e7681',maxTicksLimit:4}, grid:{color:'rgba(48,54,61,0.6)'}, border:{display:false} }
      },
      onClick(evt, elems) {
        if (!elems.length) return;
        const idx  = elems[0].index;
        const tFrom = labels[idx];
        const tTo   = idx+1 < labels.length ? labels[idx+1] : hd.to;
        const fEl = document.getElementById('data-from');
        const tEl = document.getElementById('data-to');
        if (fEl) fEl.value = tFrom.slice(0,16).replace(' ','T');
        if (tEl) tEl.value = tTo.slice(0,16).replace(' ','T');
        applyDataFilter();
      }
    }
  });
}

// ── Relevance filter ──────────────────────────────────────────────────────────
function setRelFilter(val) {
  relFilter_ = val;
  const map = {all:{val:'',cls:'active'}, relevant:{val:'relevant',cls:'active-green'}, irrelevant:{val:'irrelevant',cls:'active-red'}, unchecked:{val:'unchecked',cls:'active-orange'}};
  Object.entries(map).forEach(([k,{val:v,cls}]) => {
    const el = document.getElementById('rpill-'+k);
    if (!el) return;
    el.className = 'rel-pill';
    if (val===v) el.className += ' '+cls;
  });
  dataPage_=1; loadData(selJobId, 1);
}

// ── Sort / Regex / Cross-source / Per-page ─────────────────────────────────────
function sortBy(field) {
  if (field.includes(LINKED_SEP)) return;
  if (sortField_ === field) sortDir_ = sortDir_==='asc'?'desc':'asc';
  else { sortField_ = field; sortDir_ = 'desc'; }
  dataPage_=1; loadData(selJobId, 1);
}

function toggleRegex() {
  regexMode_ = !regexMode_;
  const btn = document.getElementById('regex-toggle');
  if (btn) btn.classList.toggle('active', regexMode_);
  const inp = document.getElementById('data-search');
  if (inp) inp.placeholder = regexMode_ ? 'Regex pattern (e.g. Error.*timeout)…' : 'Search message / URI / store_id…';
}

function toggleCrossSource() {
  crossSourceMode_ = !crossSourceMode_;
  const btn = document.getElementById('cross-source-toggle');
  if (btn) btn.classList.toggle('active', crossSourceMode_);
  if (crossSourceMode_) showToast('🔗','Cross-source ON','Search matches correlated records from all sources',1);
  else                  showToast('🔍','Single-source mode','Search restricted to this source only',1);
  dataPage_=1; loadData(selJobId, 1);
}

function _updateCrossSourceBtn() {
  const btn = document.getElementById('cross-source-toggle');
  if (!btn) return;
  let hasCorrKeys = false;
  if (selCollId) {
    const coll = collections_.find(c => c.id === selCollId);
    hasCorrKeys = !!(coll && coll.corr_keys && coll.corr_keys.length && coll.sources && coll.sources.length > 1);
  }
  btn.classList.toggle('hidden', !hasCorrKeys);
  if (hasCorrKeys) { crossSourceMode_=true;  btn.classList.add('active'); }
  else             { crossSourceMode_=false; btn.classList.remove('active'); }
}

function setPerPage(n) { perPage_=n; dataPage_=1; loadData(selJobId, 1); }

// ── Bulk row selection ────────────────────────────────────────────────────────
function toggleSelectRow(id, cb) {
  if (cb.checked) selectedRows_.add(id); else selectedRows_.delete(id);
  updateBulkBar();
  const allCb = document.getElementById('select-all-cb');
  if (allCb && lastDataRows_) {
    allCb.checked       = lastDataRows_.length > 0 && lastDataRows_.every(r => selectedRows_.has(r.id));
    allCb.indeterminate = !allCb.checked && selectedRows_.size > 0;
  }
}
function selectAll(checked) {
  if (!lastDataRows_) return;
  lastDataRows_.forEach(r => { if (checked) selectedRows_.add(r.id); else selectedRows_.delete(r.id); });
  document.querySelectorAll('input.row-checkbox:not(#select-all-cb)').forEach(cb => { cb.checked = checked; });
  const allCb = document.getElementById('select-all-cb');
  if (allCb) { allCb.checked=checked; allCb.indeterminate=false; }
  updateBulkBar();
}
function updateBulkBar() {
  const bar = document.getElementById('bulk-bar');
  const num = document.getElementById('bulk-count-num');
  if (!bar) return;
  if (selectedRows_.size > 0) { bar.classList.add('visible'); if (num) num.textContent = selectedRows_.size; }
  else bar.classList.remove('visible');
}
function clearSelection() {
  selectedRows_.clear();
  document.querySelectorAll('input.row-checkbox').forEach(cb => { cb.checked=false; cb.indeterminate=false; });
  updateBulkBar();
}
async function bulkMarkSelected(relevance) {
  if (!selectedRows_.size) return;
  const ids = [...selectedRows_];
  await api.post('/api/logs/mark-bulk', { ids, relevance, severity:'info' });
  showToast('✓', `Marked ${ids.length} records as ${relevance}`, '', 100); hideToast(3000);
  clearSelection(); loadData(selJobId, dataPage_);
}

// ── Mark row ──────────────────────────────────────────────────────────────────
async function markRow(rowId, relevance, btn) {
  btn.disabled = true;
  await api.post(`/api/logs/${rowId}/mark`, { relevance, severity:'info' });
  const row = (lastDataRows_||[]).find(r => r.id === rowId);
  if (row) row.relevance = relevance;
  const row2 = (lastDataRows_||[]).find(r => r.id === rowId);
  if (row2) {
    let rj = {};
    try { rj = typeof row2.raw_json==='string' ? JSON.parse(row2.raw_json) : (row2.raw_json||{}); } catch(e) {}
    const m = rj.message;
    const message = Array.isArray(m) ? (m[0]||'') : (m||'');
    if (message) {
      const similar = await api.get(`/api/logs/similar?job_id=${selJobId}&message=${encodeURIComponent(message)}&exclude_id=${rowId}`);
      const targets = (similar||[]).filter(r => r.relevance !== relevance);
      if (targets.length > 0) showSimilarPrompt(targets, message, relevance);
    }
  }
  btn.disabled = false;
  refreshData();
}

// ── Similar prompt ────────────────────────────────────────────────────────────
let _similarIds_       = [];
let _similarRelevance_ = 'irrelevant';

function showSimilarPrompt(rows, message, relevance) {
  _similarIds_       = rows.map(r => r.id);
  _similarRelevance_ = relevance || 'irrelevant';
  document.getElementById('similar-count').textContent = rows.length;
  document.getElementById('similar-msg').textContent   = message.length > 80 ? message.slice(0,80)+'…' : message;
  const btn = document.querySelector('#similar-modal .btn-red');
  if (btn) {
    btn.textContent         = relevance==='relevant' ? '✓ Mark All Relevant' : '✗ Mark All Irrelevant';
    btn.style.background    = relevance==='relevant' ? 'rgba(63,185,80,.2)' : 'rgba(248,81,73,.15)';
    btn.style.color         = relevance==='relevant' ? 'var(--green)'       : 'var(--red)';
    btn.style.borderColor   = relevance==='relevant' ? 'rgba(63,185,80,.4)' : 'rgba(248,81,73,.4)';
  }
  document.getElementById('similar-modal').classList.add('open');
}
async function bulkMarkSimilar() {
  document.getElementById('similar-modal').classList.remove('open');
  if (!_similarIds_.length) return;
  await api.post('/api/logs/mark-bulk', { ids:_similarIds_, relevance:_similarRelevance_, severity:'info' });
  showToast('✓', `Marked ${_similarIds_.length} similar records as ${_similarRelevance_==='relevant'?'relevant ✓':'irrelevant ✗'}`, '', 100); hideToast(3000);
  refreshData(); _similarIds_=[];
}

// ── Kibana doc view ───────────────────────────────────────────────────────────
function syntaxHighlightJson(json) {
  return json
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, m => {
      if (/^"/.test(m)) { if (/:$/.test(m)) return `<span class="json-key">${m}</span>`; return `<span class="json-str">${m}</span>`; }
      if (/true|false/.test(m)) return `<span class="json-bool">${m}</span>`;
      if (/null/.test(m))       return `<span class="json-null">${m}</span>`;
      return `<span class="json-num">${m}</span>`;
    });
}
function renderKfValue(raw) {
  const trimmed = raw.trim();
  if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
    try { const pretty = JSON.stringify(JSON.parse(trimmed), null, 2); return `<div class="kf-json">${syntaxHighlightJson(pretty)}</div>`; } catch(e) {}
  }
  return `<span style="white-space:pre-wrap;word-break:break-all;">${esc(raw)}</span>`;
}

function buildKibanaDocView(row, rj) {
  const allFields = {};
  allFields['@timestamp'] = row.timestamp || '';
  allFields['_relevance'] = row.relevance  || '';
  allFields['_severity']  = row.severity   || '';
  if (row.ai_summary) allFields['_ai_summary'] = row.ai_summary;
  Object.entries(rj).forEach(([k, v]) => {
    if (k !== '@timestamp') {
      let val = Array.isArray(v) ? (v[0]??'') : v;
      if (val !== null && typeof val === 'object') val = JSON.stringify(val);
      allFields[k] = String(val??'');
    }
  });
  const rows = Object.entries(allFields).map(([field, value]) => {
    const isMeta   = field.startsWith('_');
    const safeV    = esc(String(value));
    const safeF    = esc(field);
    const valueCell = renderKfValue(String(value));
    const filterBtns = isMeta ? '' : `
      <button class="kf-btn kf-plus"  title="Filter: ${safeF} = ${safeV.slice(0,40)}" data-f="${safeF}" data-v="${safeV}" onclick="filterInField(this.dataset.f, this.dataset.v)">+</button>
      <button class="kf-btn kf-minus" title="Exclude: ${safeF} = ${safeV.slice(0,40)}" data-f="${safeF}" data-v="${safeV}" onclick="filterOutField(this.dataset.f, this.dataset.v)">−</button>`;
    return `<tr class="kibana-field-row">
      <td class="kf-actions">${filterBtns}</td>
      <td class="kf-name ${isMeta?'kf-meta':''}">${esc(field)}</td>
      <td class="kf-value">${valueCell}</td>
    </tr>`;
  }).join('');
  return `<div class="kibana-doc-wrap">
    <div class="kibana-doc-header">
      <span style="font-size:11px;font-weight:600;color:var(--tx1);">Document #${row.id}</span>
      <div style="display:flex;gap:6px;">
        <button class="btn btn-sm" style="background:rgba(63,185,80,.15);color:var(--green);border:1px solid rgba(63,185,80,.3);font-size:10px;" onclick="markRow(${row.id},'relevant',this)">✓ Mark Relevant</button>
        <button class="btn btn-sm" style="background:rgba(248,81,73,.1);color:var(--red);border:1px solid rgba(248,81,73,.3);font-size:10px;" onclick="markRow(${row.id},'irrelevant',this)">✗ Mark Irrelevant</button>
      </div>
    </div>
    <table class="kibana-doc-table">
      <colgroup><col style="width:56px"><col style="width:180px"><col></colgroup>
      <tbody>${rows}</tbody>
    </table>
    <div id="corr-inline-${row.id}"></div>
    <div id="clf-results-${row.id}" class="clf-results-section" style="display:none;">
      <div class="clf-results-hdr">🏷 Classifier Scores</div>
      <div class="clf-results-body" id="clf-results-body-${row.id}">Loading…</div>
    </div>
  </div>`;
}

// ── Field filter chips ────────────────────────────────────────────────────────
function filterInField(field, value) {
  fieldFilters_ = fieldFilters_.filter(f => !(f.field===field && f.op==='eq'));
  fieldFilters_.push({field, value, op:'eq'});
  renderExcludeChips(); dataPage_=1; loadData(selJobId, 1);
}
function filterOutField(field, value) {
  fieldFilters_ = fieldFilters_.filter(f => !(f.field===field && f.value===value && f.op==='neq'));
  fieldFilters_.push({field, value, op:'neq'});
  renderExcludeChips(); dataPage_=1; loadData(selJobId, 1);
}
function renderExcludeChips() {
  const el = document.getElementById('exclude-chips');
  if (!el) return;
  if (!fieldFilters_.length) { el.style.display='none'; return; }
  el.style.display='flex';
  el.innerHTML = fieldFilters_.map((f, i) => {
    const prefix = f.op==='eq' ? '+' : '−';
    const label  = `${esc(f.field)}:${esc(String(f.value).length>30 ? String(f.value).slice(0,30)+'…' : String(f.value))}`;
    return `<span class="exclude-chip op-${f.op}">${prefix}${label}
      <button onclick="removeFieldFilter(${i})" title="Remove filter">×</button>
    </span>`;
  }).join('');
}
function removeFieldFilter(idx) {
  fieldFilters_.splice(idx, 1);
  renderExcludeChips(); dataPage_=1; loadData(selJobId, 1);
}

// ── Export ────────────────────────────────────────────────────────────────────
function exportData() {
  if (!selJobId) return;
  document.getElementById('export-modal').classList.add('open');
}
async function doExport() {
  const fmt    = document.getElementById('export-format').value;
  const incAI  = document.getElementById('export-inc-ai').checked;
  const incRaw = document.getElementById('export-inc-raw').checked;
  const search  = document.getElementById('data-search')?.value.trim() || '';
  const fromRaw = document.getElementById('data-from')?.value || '';
  const toRaw   = document.getElementById('data-to')?.value   || '';
  const params  = new URLSearchParams({ format: fmt });
  if (search)   params.set('search', search);
  if (regexMode_) params.set('regex', 'true');
  if (fromRaw)  params.set('from_date', fromRaw.replace('T',' '));
  if (toRaw)    params.set('to_date',   toRaw.replace('T',' '));
  if (relFilter_) params.set('relevance', relFilter_);
  if (!incAI)   params.set('no_ai', 'true');
  if (incRaw)   params.set('raw_fields', 'true');
  if (fieldFilters_.length) params.set('field_filters', JSON.stringify(fieldFilters_));
  document.getElementById('export-modal').classList.remove('open');
  showToast('⬇', 'Exporting…', 'Preparing download…', 30);
  const a = document.createElement('a');
  a.href = `/api/jobs/${selJobId}/export?${params}`;
  a.download = '';
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
  hideToast(2000);
}

// ── Clusters ──────────────────────────────────────────────────────────────────
async function openClusters() {
  if (!selJobId) return;
  document.getElementById('clusters-modal').classList.add('open');
  const el = document.getElementById('clusters-list');
  el.innerHTML = '<div style="padding:20px;color:var(--tx2);font-size:12px;text-align:center;">Loading clusters…</div>';
  const search  = document.getElementById('data-search')?.value.trim() || '';
  const fromRaw = document.getElementById('data-from')?.value || '';
  const toRaw   = document.getElementById('data-to')?.value   || '';
  const params  = new URLSearchParams({ limit: 500 });
  if (search)   params.set('search', search);
  if (fromRaw)  params.set('from_date', fromRaw.replace('T',' '));
  if (toRaw)    params.set('to_date',   toRaw.replace('T',' '));
  if (relFilter_) params.set('relevance', relFilter_);
  if (fieldFilters_.length) params.set('field_filters', JSON.stringify(fieldFilters_));
  try {
    const data = await api.get(`/api/jobs/${selJobId}/clusters?${params}`);
    if (!data.clusters || !data.clusters.length) {
      el.innerHTML = '<div style="padding:20px;color:var(--tx2);font-size:12px;text-align:center;">No error clusters found in current view.<br>Try filtering by relevant logs or run AI analysis first.</div>';
      return;
    }
    el.innerHTML = data.clusters.map(c =>
      `<div class="cluster-item" onclick="applyClusterFilter(${JSON.stringify(c.pattern).replace(/"/g,'&quot;')})">
        <div style="display:flex;align-items:center;gap:10px;">
          <span class="cluster-count">${c.count}</span>
          <span style="font-size:12px;font-weight:500;color:var(--tx0);">
            ${c.severity ? `<span class="sev sev-${c.severity}" style="margin-right:6px;">${c.severity.toUpperCase()}</span>` : ''}
            ${esc((c.representative||'').slice(0,80))}
          </span>
        </div>
        <div class="cluster-pattern">${esc(c.pattern)}</div>
      </div>`
    ).join('');
    const totalUniq = data.clusters.length;
    const totalLogs = data.clusters.reduce((s,c)=>s+c.count, 0);
    el.insertAdjacentHTML('afterbegin',
      `<div style="padding:8px 12px;font-size:11px;color:var(--tx2);border-bottom:1px solid var(--border);margin-bottom:8px;">
        ${totalUniq} unique patterns · ${totalLogs} total log entries
        <span style="float:right;font-size:10px;color:var(--tx2);">Click a cluster to filter</span>
      </div>`);
  } catch(e) {
    el.innerHTML = `<div style="padding:20px;color:var(--red);font-size:12px;">Error loading clusters: ${esc(String(e))}</div>`;
  }
}
function applyClusterFilter(pattern) {
  document.getElementById('clusters-modal').classList.remove('open');
  document.getElementById('data-search').value = pattern;
  regexMode_ = true;
  document.getElementById('regex-toggle').classList.add('active');
  dataPage_=1; loadData(selJobId, 1);
}

// ── Saved searches ────────────────────────────────────────────────────────────
function openSaveSearch() {
  document.getElementById('save-search-name').value = '';
  loadSavedSearches();
  document.getElementById('saved-search-modal').classList.add('open');
}
function openLoadSearch() { openSaveSearch(); }

async function loadSavedSearches() {
  const el = document.getElementById('saved-searches-list');
  el.innerHTML = '<div style="padding:12px;color:var(--tx2);font-size:12px;">Loading…</div>';
  try {
    const params = new URLSearchParams();
    if (selJobId) params.set('job_id', selJobId);
    const list = await api.get(`/api/saved-searches?${params}`);
    if (!list.length) { el.innerHTML='<div style="padding:12px;color:var(--tx2);font-size:12px;">No saved searches yet.</div>'; return; }
    el.innerHTML = list.map(s => {
      const parts = [];
      if (s.search) parts.push(`"${esc(s.search)}"`);
      if (s.relevance) parts.push(s.relevance);
      if (s.from_date || s.to_date) parts.push(`${s.from_date||''}…${s.to_date||''}`);
      return `<div class="saved-search-row">
        <span class="saved-search-name" onclick="applySavedSearch(${s.id})">${esc(s.name)}</span>
        <span class="saved-search-meta">${parts.join(' · ')}</span>
        <button class="btn btn-sm" style="padding:2px 6px;font-size:10px;color:var(--red);background:none;border:none;cursor:pointer;" onclick="deleteSavedSearch(${s.id})">✕</button>
      </div>`;
    }).join('');
  } catch(e) {
    el.innerHTML = `<div style="padding:12px;color:var(--red);font-size:12px;">Error loading searches</div>`;
  }
}
async function saveSearch() {
  const name = document.getElementById('save-search-name').value.trim();
  if (!name) { alert('Please enter a name'); return; }
  try {
    await api.post('/api/saved-searches', {
      job_id:selJobId||null, name,
      search:    document.getElementById('data-search')?.value.trim()||'',
      from_date: document.getElementById('data-from')?.value||'',
      to_date:   document.getElementById('data-to')?.value||'',
      relevance: relFilter_, field_filters: fieldFilters_, sort_by: sortField_, sort_dir: sortDir_,
    });
    showToast('💾','Saved',`Search "${name}" saved`,100); hideToast(2500);
    await loadSavedSearches();
    document.getElementById('save-search-name').value = '';
  } catch(e) { showToast('!','Save failed',String(e),0); hideToast(4000); }
}
async function applySavedSearch(id) {
  try {
    const list = await api.get('/api/saved-searches');
    const s = list.find(x => x.id===id);
    if (!s) return;
    document.getElementById('data-search').value = s.search||'';
    document.getElementById('data-from').value   = s.from_date||'';
    document.getElementById('data-to').value     = s.to_date||'';
    fieldFilters_ = (typeof s.field_filters==='string') ? JSON.parse(s.field_filters||'[]') : (s.field_filters||[]);
    if (s.sort_by) { sortField_=s.sort_by; sortDir_=s.sort_dir||'desc'; }
    setRelFilter(s.relevance||'');
    renderExcludeChips(); dataPage_=1;
    document.getElementById('saved-search-modal').classList.remove('open');
    loadData(selJobId, 1);
    showToast('📂','Search loaded',`Applied "${esc(s.name)}"`,100); hideToast(2500);
  } catch(e) {}
}
async function deleteSavedSearch(id) {
  if (!confirm('Delete this saved search?')) return;
  try { await api.del(`/api/saved-searches/${id}`); await loadSavedSearches(); } catch(e) {}
}
