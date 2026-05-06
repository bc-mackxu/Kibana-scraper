// ── Classifier state ──────────────────────────────────────────────────────────
let classifiers_ = [];
let classifierFilter_ = null;   // classifier_id currently filtering the log table
let classifyRunES_ = null;      // active SSE for classification job

// ── Load + render classifiers ─────────────────────────────────────────────────
async function loadClassifiers() {
  try { classifiers_ = await api.get('/api/classifiers'); }
  catch(e) { classifiers_ = []; }
  renderClassifierList();
  renderClassifierFilterPills();
}

function renderClassifierList() {
  const el = document.getElementById('classifier-list');
  if (!el) return;
  if (!classifiers_.length) {
    el.innerHTML = '<div class="clf-empty">No classifiers yet. Create one below.</div>';
    return;
  }
  el.innerHTML = classifiers_.map(c => `
    <div class="clf-item ${c.enabled ? '' : 'clf-disabled'}">
      <div class="clf-item-info">
        <div class="clf-item-name">${esc(c.name)}</div>
        <div class="clf-item-desc">${esc(c.description)}</div>
        ${c.keywords ? `<div class="clf-item-meta">🔑 ${esc(c.keywords)}</div>` : ''}
      </div>
      <div class="clf-item-actions">
        <button class="btn btn-ghost btn-sm" onclick="openEditClassifier(${c.id})" title="Edit">✏</button>
        <button class="btn btn-ghost btn-sm clf-del-btn" onclick="deleteClassifier(${c.id}, '${esc(c.name)}')" title="Delete">🗑</button>
      </div>
    </div>`).join('');
}

function renderClassifierFilterPills() {
  const el  = document.getElementById('classifier-filter-pills');
  const row = document.getElementById('classifier-filter-row');
  if (!el) return;
  const enabled = classifiers_.filter(c => c.enabled);
  if (!enabled.length) {
    el.innerHTML = '';
    if (row) row.style.display = 'none';
    return;
  }
  if (row) row.style.display = 'flex';
  const pills = enabled.map(c => {
    const active = classifierFilter_ === c.id;
    return `<button class="clf-pill ${active ? 'clf-pill-active' : ''}"
      onclick="setClassifierFilter(${c.id})" title="${esc(c.description)}">${esc(c.name)}</button>`;
  }).join('');
  el.innerHTML = `
    <button class="clf-pill ${classifierFilter_ === null ? 'clf-pill-active' : ''}"
      onclick="setClassifierFilter(null)">All</button>
    ${pills}`;
}

function setClassifierFilter(id) {
  classifierFilter_ = (classifierFilter_ === id) ? null : id;
  renderClassifierFilterPills();
  applyDataFilter();
}

// ── Classifier Modal ──────────────────────────────────────────────────────────
let editClassifierId_ = null;

function openAddClassifier() {
  editClassifierId_ = null;
  document.getElementById('clf-modal-title').textContent = 'Add Classifier';
  document.getElementById('clf-f-name').value = '';
  document.getElementById('clf-f-desc').value = '';
  document.getElementById('clf-f-pos').value = '';
  document.getElementById('clf-f-neg').value = '';
  document.getElementById('clf-f-kw').value = '';
  document.getElementById('clf-f-enabled').value = 'true';
  document.getElementById('clf-modal').classList.add('open');
  document.getElementById('clf-f-name').focus();
}

function openEditClassifier(id) {
  const c = classifiers_.find(x => x.id === id);
  if (!c) return;
  editClassifierId_ = id;
  document.getElementById('clf-modal-title').textContent = 'Edit Classifier';
  document.getElementById('clf-f-name').value = c.name;
  document.getElementById('clf-f-desc').value = c.description;
  document.getElementById('clf-f-pos').value = c.positive_examples || '';
  document.getElementById('clf-f-neg').value = c.negative_examples || '';
  document.getElementById('clf-f-kw').value = c.keywords || '';
  document.getElementById('clf-f-enabled').value = c.enabled ? 'true' : 'false';
  document.getElementById('clf-modal').classList.add('open');
  document.getElementById('clf-modal-status').textContent = '';
}

function closeClassifierModal() {
  document.getElementById('clf-modal').classList.remove('open');
  editClassifierId_ = null;
}

async function saveClassifier() {
  const name = document.getElementById('clf-f-name').value.trim();
  const desc = document.getElementById('clf-f-desc').value.trim();
  if (!name || !desc) { alert('Name and description are required.'); return; }

  const body = {
    name,
    description: desc,
    positive_examples: document.getElementById('clf-f-pos').value.trim(),
    negative_examples: document.getElementById('clf-f-neg').value.trim(),
    keywords: document.getElementById('clf-f-kw').value.trim(),
    enabled: document.getElementById('clf-f-enabled').value === 'true',
  };

  const statusEl = document.getElementById('clf-modal-status');
  statusEl.textContent = '⏳ Computing embedding…';
  const saveBtn = document.getElementById('clf-modal-save');
  saveBtn.disabled = true;

  try {
    if (editClassifierId_ !== null) {
      await api.put(`/api/classifiers/${editClassifierId_}`, body);
    } else {
      await api.post('/api/classifiers', body);
    }
    closeClassifierModal();
    await loadClassifiers();
    toast('✓', 'Classifier saved', editClassifierId_ ? 'Updated.' : 'Created and embedded.', 'success');
  } catch(e) {
    statusEl.textContent = `Error: ${e.message || e}`;
  } finally {
    saveBtn.disabled = false;
  }
}

async function deleteClassifier(id, name) {
  if (!confirm(`Delete classifier "${name}"?\nThis also removes all classification results for this classifier.`)) return;
  await api.delete(`/api/classifiers/${id}`);
  if (classifierFilter_ === id) { classifierFilter_ = null; applyDataFilter(); }
  await loadClassifiers();
  toast('🗑', 'Classifier deleted', name, 'info');
}

// ── Classifier Management Panel ───────────────────────────────────────────────
function openClassifierPanel() {
  loadClassifiers();
  document.getElementById('clf-panel-modal').classList.add('open');
}

function closeClassifierPanel() {
  document.getElementById('clf-panel-modal').classList.remove('open');
}

// ── Run Classification Job ────────────────────────────────────────────────────
function openRunClassify() {
  if (!classifiers_.filter(c => c.enabled).length) {
    alert('No enabled classifiers. Create one first.');
    return;
  }
  document.getElementById('clf-run-mode').value = 'missing';
  document.getElementById('clf-run-range').style.display = 'none';
  document.getElementById('clf-run-threshold').value = '0.5';
  document.getElementById('clf-run-reasons').checked = false;
  document.getElementById('clf-run-status').innerHTML = '';
  document.getElementById('clf-run-modal').classList.add('open');
}

function closeRunClassify() {
  if (classifyRunES_) { classifyRunES_.close(); classifyRunES_ = null; }
  document.getElementById('clf-run-modal').classList.remove('open');
}

function onClfRunModeChange(val) {
  document.getElementById('clf-run-range').style.display = val === 'range' ? 'flex' : 'none';
}

async function startClassifyRun() {
  if (classifyRunES_) { classifyRunES_.close(); classifyRunES_ = null; }

  const mode = document.getElementById('clf-run-mode').value;
  const threshold = parseFloat(document.getElementById('clf-run-threshold').value) || 0.5;
  const withReasons = document.getElementById('clf-run-reasons').checked;
  const fromDate = document.getElementById('clf-run-from').value || '';
  const toDate = document.getElementById('clf-run-to').value || '';

  const params = new URLSearchParams({
    mode, threshold, with_reasons: withReasons,
    ...(selJobId ? { job_id: selJobId } : {}),
    ...(fromDate ? { from_date: fromDate } : {}),
    ...(toDate ? { to_date: toDate } : {}),
  });

  const statusEl = document.getElementById('clf-run-status');
  const barFill = document.getElementById('clf-run-bar-fill');
  const runBtn = document.getElementById('clf-run-btn');

  statusEl.innerHTML = '<span style="color:var(--blue);">⏳ Starting…</span>';
  barFill.style.width = '0%';
  runBtn.disabled = true;

  classifyRunES_ = new EventSource(`/api/classifiers/run/stream?${params}`);

  classifyRunES_.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.type === 'start') {
      statusEl.innerHTML = '<span style="color:var(--blue);">Running…</span>';
    } else if (msg.type === 'info') {
      statusEl.innerHTML = `<span style="color:var(--tx2);">${esc(msg.message)}</span>`;
    } else if (msg.type === 'progress') {
      const pct = msg.total ? Math.round(msg.done / msg.total * 100) : 0;
      barFill.style.width = pct + '%';
      statusEl.innerHTML = `<span>${msg.done} / ${msg.total} logs (${msg.matched || 0} matched)</span>`;
    } else if (msg.type === 'warning') {
      statusEl.innerHTML = `<span style="color:var(--orange);">⚠ ${esc(msg.message)}</span>`;
    } else if (msg.type === 'error') {
      statusEl.innerHTML = `<span style="color:var(--red);">✗ ${esc(msg.message)}</span>`;
      classifyRunES_.close(); classifyRunES_ = null; runBtn.disabled = false;
    } else if (msg.type === 'done') {
      barFill.style.width = '100%';
      statusEl.innerHTML = `<span style="color:var(--green);">✓ Done — ${msg.processed} logs, ${msg.matched} matches</span>`;
      classifyRunES_.close(); classifyRunES_ = null; runBtn.disabled = false;
      if (lastDataRows_) refreshData();
    }
  };

  classifyRunES_.onerror = () => {
    statusEl.innerHTML = '<span style="color:var(--red);">Connection error</span>';
    classifyRunES_.close(); classifyRunES_ = null; runBtn.disabled = false;
  };
}

// ── Per-log classifier results ────────────────────────────────────────────────
async function loadLogClassifications(logId, containerEl) {
  const rows = await api.get(`/api/logs/${logId}/classifications`);
  if (!rows || !rows.length) {
    containerEl.innerHTML = '<span style="color:var(--tx2);font-size:11px;">No classifier results</span>';
    return;
  }
  containerEl.innerHTML = rows.map(r => {
    const pct = Math.round((r.confidence || 0) * 100);
    const color = r.matched ? 'var(--green)' : 'var(--tx2)';
    const icon = r.matched ? '✓' : '·';
    const bar = `<div style="display:inline-block;width:${pct}px;max-width:80px;height:6px;
      background:${r.matched ? 'var(--green)' : 'var(--border)'};border-radius:3px;vertical-align:middle;margin:0 4px;"></div>`;
    return `<div class="clf-result-row">
      <span style="color:${color};font-weight:600;min-width:12px;">${icon}</span>
      <span class="clf-result-name" title="${esc(r.name)}">${esc(r.name)}</span>
      ${bar}
      <span style="color:var(--tx2);font-size:10px;min-width:32px;">${pct}%</span>
      ${r.reason ? `<span style="color:var(--tx2);font-size:10px;margin-left:4px;font-style:italic;">${esc(r.reason)}</span>` : ''}
    </div>`;
  }).join('');
}

// ── Patch loadData to include classifier_id param ─────────────────────────────
const _origBuildDataParams = typeof _buildDataParams !== 'undefined' ? _buildDataParams : null;

function _getClassifierFilterParam() {
  return classifierFilter_ ? `&classifier_id=${classifierFilter_}` : '';
}
