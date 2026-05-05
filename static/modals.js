// ── Add / Edit collection modal ───────────────────────────────────────────────
let editCollId = null;   // null = add mode

function _modalSources() { return document.getElementById('f-sources'); }
function _modalKeys()    { return document.getElementById('f-keys'); }

function addSourceRow(label='', url='') {
  const div = document.createElement('div');
  div.className = 'source-row';
  div.innerHTML = `
    <input class="form-input src-label" placeholder="Label (e.g. Syslog)" value="${esc(label)}" />
    <input class="form-input src-url"   placeholder="https://kibana.bigcommerce.net/goto/…" value="${esc(url)}" />
    <button class="src-del" onclick="this.closest('.source-row').remove()" title="Remove">✕</button>`;
  _modalSources().appendChild(div);
}

function addKeyRow(value='') {
  if (value) { _addKeyChip(value); return; }
  if (document.getElementById('key-input-tmp')) return;
  const wrap = document.createElement('div');
  wrap.className = 'key-input-row';
  wrap.id = 'key-input-tmp';
  wrap.innerHTML = `<input class="form-input" id="key-input-val" placeholder="field name, e.g. request_id" style="width:200px;" />
    <button class="btn btn-ghost btn-sm" onclick="_commitKey()">Add</button>
    <button class="btn btn-ghost btn-sm" onclick="document.getElementById('key-input-tmp').remove()">✕</button>`;
  _modalKeys().appendChild(wrap);
  setTimeout(() => document.getElementById('key-input-val')?.focus(), 50);
  document.getElementById('key-input-val').onkeydown = e => { if (e.key === 'Enter') { e.preventDefault(); _commitKey(); } };
}

function _commitKey() {
  const v = (document.getElementById('key-input-val')?.value || '').trim();
  document.getElementById('key-input-tmp')?.remove();
  if (v) _addKeyChip(v);
}

function _addKeyChip(value) {
  const existing = [..._modalKeys().querySelectorAll('.key-chip')].map(c => c.dataset.v);
  if (existing.includes(value)) return;
  const chip = document.createElement('span');
  chip.className = 'key-chip'; chip.dataset.v = value;
  chip.innerHTML = `${esc(value)}<button onclick="this.parentElement.remove()" title="Remove">✕</button>`;
  _modalKeys().appendChild(chip);
}

function _getModalSources() {
  return [..._modalSources().querySelectorAll('.source-row')].map(row => ({
    label:      row.querySelector('.src-label').value.trim(),
    kibana_url: row.querySelector('.src-url').value.trim(),
  })).filter(s => s.label && s.kibana_url);
}

function _getModalKeys() {
  return [..._modalKeys().querySelectorAll('.key-chip')].map(c => c.dataset.v);
}

function _resetModal() {
  document.getElementById('f-name').value = '';
  document.getElementById('f-time').value = 'Last 7 days';
  document.getElementById('f-hour').value = '2';
  document.getElementById('f-chunk').value = '0';
  document.getElementById('f-enabled').value = 'true';
  _modalSources().innerHTML = '';
  _modalKeys().innerHTML = '';
}

function openAddJob() {
  editCollId = null;
  document.getElementById('modal-title').textContent = 'Add Job';
  document.getElementById('modal-save').textContent = 'Save Job';
  _resetModal();
  addSourceRow();
  document.getElementById('modal').classList.add('open');
}

function openEditJob() {
  const coll = collections_.find(c => c.id === selCollId);
  if (!coll) return;
  editCollId = coll.id;
  document.getElementById('modal-title').textContent = 'Edit Job';
  document.getElementById('modal-save').textContent = 'Save Changes';
  _resetModal();
  document.getElementById('f-name').value = coll.name;
  document.getElementById('f-time').value = coll.time_range || 'Last 7 days';
  document.getElementById('f-hour').value = coll.schedule_hour ?? 2;
  document.getElementById('f-chunk').value = String(coll.chunk_hours || 0);
  document.getElementById('f-enabled').value = coll.enabled ? 'true' : 'false';
  (coll.sources || []).forEach(s => addSourceRow(s.label, s.kibana_url));
  (coll.corr_keys || []).forEach(k => _addKeyChip(k));
  if (!coll.sources?.length) addSourceRow();
  document.getElementById('modal').classList.add('open');
}

function closeModal() { document.getElementById('modal').classList.remove('open'); }

async function saveCollection() {
  const name = document.getElementById('f-name').value.trim();
  if (!name) { alert('Collection name is required.'); return; }
  const sources = _getModalSources();
  if (!sources.length) { alert('Add at least one log source.'); return; }
  const payload = {
    name,
    time_range:    document.getElementById('f-time').value,
    chunk_hours:   parseInt(document.getElementById('f-chunk').value) || 0,
    schedule_hour: Math.min(Math.max(parseInt(document.getElementById('f-hour').value) || 2, 0), 23),
    enabled:       document.getElementById('f-enabled').value === 'true',
    sources,
    corr_keys: _getModalKeys(),
  };
  try {
    let newId = editCollId;
    if (editCollId) {
      await api.put(`/api/collections/${editCollId}`, payload);
    } else {
      const r = await api.post('/api/collections', payload);
      if (r.detail) { alert('Error: ' + JSON.stringify(r.detail)); return; }
      newId = r.id;
    }
    closeModal();
    await loadCollections();
    if (newId) selectCollection(newId);
  } catch(e) { alert('Save failed: ' + e); }
}

// ── Config form (single job) ──────────────────────────────────────────────────
function renderConfigForm(job) {
  document.getElementById('config-form').innerHTML = `
    <div class="config-section">
      <div class="form-grid full"><div class="form-field">
        <label class="form-label">Name</label>
        <input class="form-input" id="cf-name" value="${esc(job.name)}"/>
      </div></div>
      <div class="form-grid full"><div class="form-field">
        <label class="form-label">Kibana URL</label>
        <input class="form-input" id="cf-url" value="${esc(job.kibana_url)}"/>
      </div></div>
      <div class="form-grid">
        <div class="form-field">
          <label class="form-label">Time Range</label>
          <select class="form-input" id="cf-time">${timeOptions(job.time_range)}</select>
        </div>
        <div class="form-field">
          <label class="form-label">Daily Hour (UTC)</label>
          <input class="form-input" id="cf-hour" type="number" value="${job.schedule_hour}" min="0" max="23"/>
        </div>
      </div>
      <div class="form-grid">
        <div class="form-field">
          <label class="form-label">Chunk Window <span style="font-size:10px;color:var(--tx2);">splits large time ranges</span></label>
          <select class="form-input" id="cf-chunk" onchange="updateChunkPreview(${job.id})">
            ${chunkOptions(job.chunk_hours || 0)}
          </select>
        </div>
        <div class="form-field">
          <label class="form-label">Enabled</label>
          <select class="form-input" id="cf-enabled">
            <option value="true" ${job.enabled ? 'selected' : ''}>Yes — run daily</option>
            <option value="false" ${!job.enabled ? 'selected' : ''}>No — manual only</option>
          </select>
        </div>
      </div>
      <div id="chunk-preview" style="font-size:11px;color:var(--tx2);padding:2px 0 6px 0;"></div>
      <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:4px;">
        <button class="btn btn-blue" onclick="saveConfig(${job.id})">Save Changes</button>
      </div>
      <div class="config-danger">
        <div class="config-danger-text">
          <b>Clear Data</b>
          Delete all log records. Job config is kept.
        </div>
        <button class="btn btn-ghost btn-sm" style="border-color:var(--orange);color:var(--orange);" onclick="clearJobData(${job.id})">🗑 Clear Data</button>
      </div>
      <div class="config-danger" style="margin-top:8px;">
        <div class="config-danger-text">
          <b>Delete Job</b>
          Permanently delete this job and all its data.
        </div>
        <button class="btn btn-red btn-sm" onclick="deleteJob(${job.id})">✕ Delete Job</button>
      </div>
    </div>`;
}

// ── Collection config panel ───────────────────────────────────────────────────
function renderConfigFormCollection(coll) {
  if (!coll) return;
  const keysHtml = (coll.corr_keys || []).map(k =>
    `<span class="key-chip" style="font-size:11px;">${esc(k)}</span>`
  ).join('');
  const srcHtml = (coll.sources || []).map((s, i) =>
    `<div class="source-row" style="margin-bottom:4px;">
       <span class="form-input src-label" style="background:transparent;border-color:transparent;pointer-events:none;">${esc(s.label)}</span>
       <span class="form-input src-url"   style="background:transparent;border-color:transparent;pointer-events:none;font-size:10px;overflow:hidden;text-overflow:ellipsis;">${esc(s.kibana_url)}</span>
     </div>`
  ).join('');
  document.getElementById('config-form').innerHTML = `
    <div class="config-section">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
        <span style="font-size:13px;font-weight:600;">${esc(coll.name)}</span>
        <button class="btn btn-ghost btn-sm" onclick="openEditJob()">✏ Edit</button>
      </div>
      <div style="font-size:11px;color:var(--tx2);margin-bottom:8px;">${esc(coll.time_range)} · ${coll.chunk_hours > 0 ? coll.chunk_hours + 'h chunks' : 'single pass'} · Daily @${coll.schedule_hour}:00 UTC</div>
      <div style="font-size:11px;font-weight:600;color:var(--tx2);margin-bottom:4px;">SOURCES</div>
      ${srcHtml}
      ${keysHtml ? `<div style="font-size:11px;font-weight:600;color:var(--tx2);margin:8px 0 4px;">CORRELATION KEYS</div><div style="display:flex;flex-wrap:wrap;gap:4px;">${keysHtml}</div>` : ''}
      <div class="config-danger" style="margin-top:14px;">
        <div class="config-danger-text"><b>Clear Data</b> Delete all log records. Collection config is kept.</div>
        <button class="btn btn-ghost btn-sm" style="border-color:var(--orange);color:var(--orange);" onclick="clearCollectionData(${coll.id})">🗑 Clear Data</button>
      </div>
      <div class="config-danger" style="margin-top:8px;">
        <div class="config-danger-text"><b>Delete Collection</b> Permanently delete this collection and all its data.</div>
        <button class="btn btn-red btn-sm" onclick="deleteCollection(${coll.id})">✕ Delete</button>
      </div>
    </div>`;
}

async function saveConfig(id) {
  await api.put(`/api/jobs/${id}`, {
    name:          document.getElementById('cf-name').value,
    kibana_url:    document.getElementById('cf-url').value,
    time_range:    document.getElementById('cf-time').value,
    max_pages:     9999,
    schedule_hour: parseInt(document.getElementById('cf-hour').value),
    chunk_hours:   parseInt(document.getElementById('cf-chunk').value) || 0,
    enabled:       document.getElementById('cf-enabled').value === 'true',
  });
  await loadJobs();
  renderConfigForm(jobs.find(j => j.id === id));
}

async function deleteJob(id) {
  const job = jobs.find(j => j.id === id);
  const name = job ? job.name : `Job ${id}`;
  let rowCount = '?';
  try {
    const d = await api.get(`/api/jobs/${id}/data?page=1&per_page=1`);
    rowCount = (d.total || 0).toLocaleString();
  } catch(e) {}
  if (!confirm(`Delete "${name}"?\n\nThis will permanently remove:\n• ${rowCount} log records\n• All run history\n• Group memberships & correlation keys\n• Pre-computed correlations\n\nThis cannot be undone.`)) return;
  showToast('🗑', 'Deleting…', `Removing all data for "${name}"`, 0);
  const _bjId = bgJobStart('🗑', `Delete: ${name}`);
  try {
    await api.del(`/api/jobs/${id}`);
    bgJobDone(_bjId, `"${name}" deleted`);
    showToast('✓', 'Deleted', `"${name}" and all its data removed`, 100);
    hideToast(3000);
  } catch(e) {
    bgJobError(_bjId, 'Delete failed');
    showToast('✗', 'Delete failed', String(e), 0);
    hideToast(5000);
    return;
  }
  selJobId = null;
  document.getElementById('job-detail').style.display = 'none';
  document.getElementById('welcome').style.display = 'flex';
  await loadJobs();
  await loadStats();
}

async function clearJobData(id) {
  const job = jobs.find(j => j.id === id);
  const name = job ? job.name : `Job ${id}`;
  let rowCount = '?';
  try {
    const d = await api.get(`/api/jobs/${id}/data?page=1&per_page=1`);
    rowCount = (d.total || 0).toLocaleString();
  } catch(e) {}
  if (!confirm(`Clear data for "${name}"?\n\nThis will permanently remove:\n• ${rowCount} log records\n• All run history\n• Pre-computed correlations\n\nJob config will be kept — you can re-scrape afterwards.\n\nContinue?`)) return;
  showToast('🗑', 'Clearing…', `Deleting logs for "${name}"`, 0);
  const _bjId = bgJobStart('🗑', `Clear: ${name}`);
  try {
    await api.del(`/api/jobs/${id}/data`);
    bgJobDone(_bjId, `"${name}" data cleared`);
    showToast('✓', 'Cleared', `All log data for "${name}" removed. Job config kept.`, 100);
    hideToast(3000);
  } catch(e) {
    bgJobError(_bjId, 'Clear failed');
    showToast('✗', 'Clear failed', String(e), 0);
    hideToast(5000);
    return;
  }
  await loadJobs();
  await loadStats();
  const updJob = jobs.find(j => j.id === id);
  if (updJob) renderConfigForm(updJob);
  if (selJobId === id) {
    dataPage_ = 1;
    dataSelectedFields_ = new Set();
    lastDataRows_ = [];
    selectedRows_.clear();
    await loadData(id, 1);
  }
}

async function clearCollectionData(id) {
  const coll = collections_.find(c => c.id === id);
  const name = coll ? coll.name : `Collection ${id}`;
  const srcCount = coll ? (coll.sources || []).length : '?';
  if (!confirm(`Clear data for collection "${name}"?\n\nThis will permanently remove all log records and run history for ${srcCount} source(s).\n\nCollection config will be kept — you can re-scrape afterwards.\n\nContinue?`)) return;
  showToast('🗑', 'Clearing…', `Deleting all logs for "${name}"`, 0);
  const _bjId = bgJobStart('🗑', `Clear: ${name}`);
  try {
    await api.del(`/api/collections/${id}/data`);
    bgJobDone(_bjId, `"${name}" data cleared`);
    showToast('✓', 'Cleared', `All log data for "${name}" removed. Collection config kept.`, 100);
    hideToast(3000);
  } catch(e) {
    bgJobError(_bjId, 'Clear failed');
    showToast('✗', 'Clear failed', String(e), 0);
    hideToast(5000);
    return;
  }
  await loadJobs();
  await loadCollections();
  await loadStats();
  const updColl = collections_.find(c => c.id === id);
  if (updColl) renderConfigFormCollection(updColl);
  if (selJobId) {
    const coll2 = collections_.find(c => c.id === id);
    const srcJobIds = (coll2?.sources || []).map(s => s.job_id).filter(Boolean);
    if (srcJobIds.includes(selJobId)) {
      dataPage_ = 1;
      dataSelectedFields_ = new Set();
      lastDataRows_ = [];
      selectedRows_.clear();
      await loadData(selJobId, 1);
    }
  }
}

// ── Chunk preview helper ──────────────────────────────────────────────────────
async function updateChunkPreview(jobId) {
  const ch = parseInt(document.getElementById('cf-chunk')?.value) || 0;
  const el = document.getElementById('chunk-preview');
  if (!el) return;
  if (ch === 0) { el.textContent = ''; return; }
  try {
    const tr = document.getElementById('cf-time')?.value || 'Last 7 days';
    const trHours = { 'Last 15 minutes': 0.25, 'Last 1 hour': 1, 'Last 24 hours': 24, 'Last 7 days': 168, 'Last 30 days': 720, 'Last 90 days': 2160, 'Last 1 year': 8760 };
    const total = trHours[tr] || 168;
    const n = Math.ceil(total / ch);
    el.innerHTML = `⏱ ${n} chunks × ${ch}h — full scan will run <b>${n} browser sessions</b> sequentially`;
  } catch(e) { el.textContent = ''; }
}

// ── Name suggestion helpers ───────────────────────────────────────────────────
const NAME_HINTS = [['syslog','Syslog'],['nginx','Nginx'],['checkout','Checkout'],['payment','Payment'],['cart','Cart'],['order','Orders'],['error','Errors'],['auth','Auth'],['api','API'],['coupon','Coupon']];
function suggestName(url) {
  if (!url) return '';
  const low = url.toLowerCase();
  for (const [kw, name] of NAME_HINTS) if (low.includes(kw)) return name;
  try { return new URL(url).hostname.split('.')[0]; } catch(e) {}
  return '';
}
function autoFillName(url) {
  const el = document.getElementById('f-name');
  if (!el.value.trim()) el.value = suggestName(url);
}

// ── Log Correlation Groups ────────────────────────────────────────────────────
let groups_ = [];

async function openGroupModal() {
  document.getElementById('group-modal').classList.add('open');
  await refreshGroups();
}

async function refreshGroups() {
  groups_ = await api.get('/api/groups');
  renderGroupsList();
}

function renderGroupsList() {
  const el = document.getElementById('groups-list-container');
  if (!groups_.length) {
    el.innerHTML = '<div style="color:var(--tx2);font-size:12px;padding:8px 0;">No groups yet. Create one above.</div>';
    return;
  }
  el.innerHTML = groups_.map(g => `
    <div style="border:1px solid var(--border);border-radius:var(--radius);padding:14px;margin-bottom:12px;background:var(--bg0);">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;">
        <div>
          <span style="font-weight:600;font-size:13px;">${esc(g.name)}</span>
          ${g.description ? `<span style="color:var(--tx2);font-size:11px;margin-left:8px;">${esc(g.description)}</span>` : ''}
        </div>
        <div style="display:flex;gap:6px;">
          <button class="btn btn-sm" style="background:rgba(88,166,255,.15);color:var(--blue);border:1px solid rgba(88,166,255,.3);font-size:10px;"
                  onclick="detectKeys(${g.id})">⚡ Detect Keys</button>
          <button class="btn btn-sm" style="background:rgba(63,185,80,.1);color:var(--green);border:1px solid rgba(63,185,80,.3);font-size:10px;"
                  onclick="buildCorrelations(${g.id}, this.dataset.name)" data-name="${esc(g.name)}">🔗 Build Correlations</button>
          <button class="btn btn-sm btn-ghost" style="font-size:10px;" onclick="deleteGroup(${g.id})">✕</button>
        </div>
      </div>

      <div style="margin-bottom:10px;">
        <div style="font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px;">Jobs in group</div>
        <div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:8px;" id="members-${g.id}">
          ${(g.members || []).map(m => `
            <span class="group-member-chip">
              <span style="color:var(--blue);font-size:10px;">${esc(m.label || m.job_name)}</span>
              <span style="color:var(--tx2);">(Job ${m.job_id})</span>
              <button onclick="removeMember(${g.id},${m.job_id})" title="Remove">×</button>
            </span>
          `).join('')}
        </div>
        <div style="display:flex;gap:6px;align-items:center;">
          <select class="filter-input" id="add-job-${g.id}" style="width:180px;padding:4px 6px;">
            <option value="">— Add job —</option>
            ${jobs.map(j => `<option value="${j.id}">${esc(j.name)} (${j.id})</option>`).join('')}
          </select>
          <input class="form-input" id="add-label-${g.id}" placeholder="Label (e.g. syslog)" style="width:120px;" />
          <button class="btn btn-green btn-sm" onclick="addMember(${g.id})">Add</button>
        </div>
      </div>

      <div>
        <div style="font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px;">Correlation Keys</div>
        <div id="keys-${g.id}" style="display:flex;flex-direction:column;gap:4px;margin-bottom:8px;">
          ${(g.keys || []).length === 0
            ? '<div style="color:var(--tx2);font-size:11px;">No keys yet — click ⚡ Detect Keys</div>'
            : (g.keys || []).map(k => `
              <div class="key-row">
                <span class="key-src key-src-${k.source || 'auto'}">${k.source || 'auto'}</span>
                <span style="color:var(--orange);">${esc(k.job_name_a || '?')}</span>
                <span style="color:var(--blue);font-weight:600;">${esc(k.field_a)}</span>
                <span style="color:var(--tx2);">↔</span>
                <span style="color:var(--orange);">${esc(k.job_name_b || '?')}</span>
                <span style="color:var(--blue);font-weight:600;">${esc(k.field_b)}</span>
                <span class="key-conf">${Math.round((k.confidence || 0) * 100)}%</span>
                <div style="flex:1;"></div>
                <button class="btn btn-sm btn-ghost" style="padding:1px 6px;font-size:10px;" onclick="deleteKey(${g.id},${k.id})">✕</button>
              </div>`).join('')
          }
        </div>
        <details style="font-size:11px;" open>
          <summary style="color:var(--tx2);cursor:pointer;margin-bottom:6px;">+ Add key manually</summary>
          <div style="display:flex;gap:4px;flex-wrap:wrap;align-items:center;">
            <select class="filter-input" id="mk-ja-${g.id}" style="width:150px;padding:3px;">
              <option value="">— Job A —</option>
              ${(g.members || []).map(m => `<option value="${m.job_id}">${esc(m.label || m.job_name)} (${m.job_id})</option>`).join('')}
            </select>
            <input class="form-input" id="mk-fa-${g.id}" placeholder="field name" style="width:120px;" />
            <span style="color:var(--tx2);font-size:14px;">↔</span>
            <select class="filter-input" id="mk-jb-${g.id}" style="width:150px;padding:3px;">
              <option value="">— Job B —</option>
              ${(g.members || []).map(m => `<option value="${m.job_id}">${esc(m.label || m.job_name)} (${m.job_id})</option>`).join('')}
            </select>
            <input class="form-input" id="mk-fb-${g.id}" placeholder="field name" style="width:120px;" />
            <button class="btn btn-green btn-sm" onclick="addKey(${g.id})">Add Key</button>
          </div>
          <div id="mk-err-${g.id}" style="color:var(--red);font-size:11px;margin-top:4px;display:none;"></div>
        </details>
      </div>

      <div id="detect-progress-${g.id}" style="display:none;margin-top:10px;">
        <div class="analyze-progress-bar active" style="margin:0;"><div class="analyze-bar-fill" id="detect-fill-${g.id}" style="width:5%;background:var(--purple);"></div></div>
        <div style="font-size:11px;color:var(--tx2);margin-top:4px;" id="detect-msg-${g.id}">Starting…</div>
      </div>
    </div>
  `).join('');
}

async function createGroup() {
  const name = document.getElementById('new-group-name').value.trim();
  if (!name) return;
  await api.post('/api/groups', { name });
  document.getElementById('new-group-name').value = '';
  await refreshGroups();
}

async function deleteGroup(gid) {
  if (!confirm('Delete this group and all its correlation keys?')) return;
  await api.del(`/api/groups/${gid}`);
  await refreshGroups();
}

async function addMember(gid) {
  const jid = +document.getElementById(`add-job-${gid}`).value;
  const label = document.getElementById(`add-label-${gid}`).value.trim();
  if (!jid) return;
  await api.post(`/api/groups/${gid}/members`, { job_id: jid, label });
  await refreshGroups();
}

async function removeMember(gid, jid) {
  await api.del(`/api/groups/${gid}/members/${jid}`);
  await refreshGroups();
}

async function addKey(gid) {
  const jid_a   = +document.getElementById(`mk-ja-${gid}`).value;
  const field_a = document.getElementById(`mk-fa-${gid}`).value.trim();
  const jid_b   = +document.getElementById(`mk-jb-${gid}`).value;
  const field_b = document.getElementById(`mk-fb-${gid}`).value.trim();
  const errEl   = document.getElementById(`mk-err-${gid}`);
  const missing = [];
  if (!jid_a)   missing.push('Job A');
  if (!field_a) missing.push('field name for Job A');
  if (!jid_b)   missing.push('Job B');
  if (!field_b) missing.push('field name for Job B');
  if (missing.length) {
    errEl.textContent = `Missing: ${missing.join(', ')}`;
    errEl.style.display = 'block';
    return;
  }
  if (jid_a === jid_b && field_a === field_b) {
    errEl.textContent = 'Job A and Job B cannot be the same job with the same field';
    errEl.style.display = 'block';
    return;
  }
  errEl.style.display = 'none';
  await api.post(`/api/groups/${gid}/keys`, { job_id_a: jid_a, field_a, job_id_b: jid_b, field_b });
  await refreshGroups();
}

async function deleteKey(gid, kid) {
  await api.del(`/api/groups/${gid}/keys/${kid}`);
  await refreshGroups();
}

function detectKeys(gid) {
  const prog = document.getElementById(`detect-progress-${gid}`);
  const fill = document.getElementById(`detect-fill-${gid}`);
  const msg  = document.getElementById(`detect-msg-${gid}`);
  prog.style.display = 'block';
  fill.style.width = '5%';
  const _bjId = bgJobStart('🔍', 'Detect Correlation Keys');
  const es = new EventSource(`/api/groups/${gid}/detect-keys/stream`);
  es.onmessage = e => {
    const d = JSON.parse(e.data);
    if (d.type === 'progress') {
      msg.textContent = d.message || '';
      fill.style.width = '40%';
      bgJobProgress(_bjId, 40, d.message || '');
    } else if (d.type === 'key_found') {
      fill.style.width = '70%';
      const m = `Found: ${d.field_a} ↔ ${d.field_b} (${d.source})`;
      msg.textContent = m;
      bgJobProgress(_bjId, 70, m);
    } else if (d.type === 'done') {
      es.close();
      fill.style.width = '100%';
      const m = `Done — ${d.keys_added} key(s) added`;
      msg.textContent = m;
      bgJobDone(_bjId, m);
      setTimeout(() => { prog.style.display = 'none'; refreshGroups(); }, 2000);
    }
  };
  es.onerror = () => {
    es.close();
    msg.textContent = 'Error — check server logs';
    fill.style.width = '0%';
    bgJobError(_bjId, 'Server error');
  };
}

function buildCorrelations(gid, groupName) {
  const _bjId = bgJobStart('🔗', `Build Correlations: ${groupName}`);
  const es = new EventSource(`/api/groups/${gid}/build-correlations/stream`);
  es.onmessage = e => {
    const d = JSON.parse(e.data);
    if (d.type === 'progress') {
      bgJobProgress(_bjId, 30, d.message || '');
    } else if (d.type === 'pair_done') {
      bgJobProgress(_bjId, 80, `${d.label_a} ↔ ${d.label_b}: ${d.linked} pairs linked`);
    } else if (d.type === 'done') {
      es.close();
      bgJobDone(_bjId, `${d.linked} correlation pairs stored`);
    }
  };
  es.onerror = () => {
    es.close();
    bgJobError(_bjId, 'Server error');
  };
}
