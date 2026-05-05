
// ── State ─────────────────────────────────────────────────────────────────────
let jobs = [], selJobId = null, editJobId = null;
let collections_ = [], selCollId = null;
let dataPage_ = 1;
let activeRunIds_ = {};   // {jobId: runId}  — one active run per job
let activeESes_   = {};   // {jobId: EventSource}
let sortField_ = 'timestamp', sortDir_ = 'desc';
let dataFields_ = [], dataSelectedFields_ = null, lastDataRows_ = null;
let relFilter_ = '';
let anthropicReady_ = false;   // set after first /api/settings fetch
let fieldPanelCollapsed_ = false;
let perPage_ = 50;
let selectedRows_ = new Set();
let regexMode_ = false;
let crossSourceMode_ = false;  // when true, search also matches correlated records
let fieldSearch_ = '';
const LINKED_SEP = '\x1E';   // separates "SourceLabel\x1Efield_name" for linked fields
let linkedJobFields_ = [];   // [{label, job_id, fields:[...]}]
let corrDataCache_ = {};     // {log_id: [{_source_label, raw_json, ...}]}

const DATA_DB_MAP = { '@timestamp': r => r.timestamp };
const DEFAULT_FIELDS = ['@timestamp'];
const PRIORITY_FIELDS = [
  '@timestamp','error_level','message','store_id',
  'request_uri','server_name','domain','http_status_code',
  'remote_addr','request_method'
];

// ── Init ──────────────────────────────────────────────────────────────────────
async function init() {
  // Fetch settings early so anthropicReady_ is known before any button click
  api.get('/api/settings').then(s => { anthropicReady_ = !!s.anthropic_key_set; });
  await Promise.all([loadStats(), loadJobs()]);
  loadDashboard();
  checkHealth();
  setInterval(checkHealth, 30_000);
  // B4 fix: single adaptive polling loop instead of two overlapping intervals
  _startSmartPoll();
}

// P4: Smart polling — fast (2s) when any run is active, slow (10s) when idle
let _pollTimer = null;
function _anyRunning() {
  return Object.keys(activeESes_).length > 0 ||
         jobs.some(j => j.is_running) ||
         (collections_ || []).some(c => (c.sources||[]).some(s => s.is_running));
}
function _startSmartPoll() {
  if (_pollTimer) clearTimeout(_pollTimer);
  const delay = _anyRunning() ? 2000 : 10_000;
  _pollTimer = setTimeout(async () => {
    await Promise.all([loadStats(), loadJobs()]);
    if (selJobId) loadRuns(selJobId);
    _startSmartPoll();  // reschedule
  }, delay);
}

// ── API ───────────────────────────────────────────────────────────────────────
const api = {
  get:    url      => fetch(url).then(r => r.json()),
  post:   (url, b) => fetch(url, {method:'POST',  headers:{'Content-Type':'application/json'}, body:JSON.stringify(b)}).then(r=>r.json()),
  put:    (url, b) => fetch(url, {method:'PUT',   headers:{'Content-Type':'application/json'}, body:JSON.stringify(b)}).then(r=>r.json()),
  delete: url      => fetch(url, {method:'DELETE'}).then(r=>r.json()),
  del:    async (url) => { const r = await fetch(url, {method:'DELETE', headers:{'Content-Type':'application/json'}}); if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); },
};

// ── Stats ─────────────────────────────────────────────────────────────────────
async function loadStats() {
  const s = await api.get('/api/stats');
  document.getElementById('s-jobs').textContent = s.total_jobs;
  // Only overwrite the rows chip when no job is selected (otherwise loadData keeps it current)
  if (!selJobId) {
    document.getElementById('s-rows').textContent = (s.total_rows||0).toLocaleString();
    document.getElementById('rows-chip').title = 'Total rows across all jobs';
  }
  // Update dashboard stat cards if visible
  const jEl = document.getElementById('dash-jobs');
  const rEl = document.getElementById('dash-rows');
  const aEl = document.getElementById('dash-analyzed');
  if (jEl) jEl.textContent = (s.total_jobs||0).toLocaleString();
  if (rEl) rEl.textContent = (s.total_rows||0).toLocaleString();
  if (aEl && s.total_rows) {
    const pct = Math.round((s.analyzed_rows||0) / s.total_rows * 100);
    aEl.textContent = pct + '%';
    const aSubEl = document.getElementById('dash-analyzed-sub');
    if (aSubEl) aSubEl.textContent = `${(s.analyzed_rows||0).toLocaleString()} of ${s.total_rows.toLocaleString()} rows`;
  }
}

function _updateRowsChip(count, jobName) {
  document.getElementById('s-rows').textContent = (count||0).toLocaleString();
  document.getElementById('rows-chip').title = jobName ? `Rows in "${jobName}"` : 'Total rows across all jobs';
}

// ── Collections + Jobs ───────────────────────────────────────────────────────
async function loadCollections() {
  try { collections_ = await api.get('/api/collections') || []; } catch(e) { collections_ = []; }
}

async function loadJobs() {
  // Load collections first so sidebar dots use fresh source statuses
  await loadCollections();
  jobs = await api.get('/api/jobs');
  // Collect job IDs that belong to collections (don't show them as standalone)
  const collJobIds = new Set(
    collections_.flatMap(c => (c.sources||[]).map(s=>s.job_id).filter(Boolean))
  );
  renderSidebar(collJobIds);
  if (selCollId) {
    const c = collections_.find(c=>c.id===selCollId);
    if (c) {
      if (selJobId && (c.sources||[]).some(s=>s.job_id===selJobId)) {
        updateDetailHeaderForCollectionSource(c, selJobId);
      } else {
        updateDetailHeaderForCollection(c);
      }
    }
  } else if (selJobId) {
    const j = jobs.find(j=>j.id===selJobId);
    if (j) updateDetailHeader(j);
  }
  // Refresh dashboard when home screen visible
  if (!selJobId && !selCollId) loadDashboard();
  // Keep cross-source button visibility in sync (corr_keys may have changed)
  _updateCrossSourceBtn();
}

function renderSidebar(collJobIds) {
  const el = document.getElementById('job-list');
  const parts = [];

  // ── Collections ──────────────────────────────────────────────────────────────
  for (const c of collections_) {
    const anyRunning = (c.sources||[]).some(s => s.is_running);
    const status = c.last_run_status;
    const dotCls = anyRunning ? 'dot-running' : status==='success' ? 'dot-success' : status==='failed' ? 'dot-failed' : 'dot-none';
    const when   = c.last_run_at ? timeAgo(c.last_run_at) : 'never';
    const keysStr = (c.corr_keys||[]).length ? `<span style="font-size:10px;color:var(--blue);margin-left:4px;">🔗 ${c.corr_keys.join(', ')}</span>` : '';

    const srcHtml = (c.sources||[]).map(s => {
      const srcCls = s.is_running ? 'running' : s.last_run_status==='success' ? 'success' : '';
      const srcActive = s.job_id === selJobId && selCollId === c.id ? ' style="color:var(--tx0);font-weight:600;"' : '';
      return `<div class="coll-source-item ${srcCls}" onclick="event.stopPropagation();selectCollectionSource(${c.id},${s.job_id})" title="${esc(s.label)}"${srcActive}>${esc(s.label)}</div>`;
    }).join('');

    parts.push(`<div class="coll-item ${c.id===selCollId?'active':''}" onclick="selectCollection(${c.id})">
      <div class="coll-name">${esc(c.name)}</div>
      <div class="job-meta" style="margin-top:2px;">
        <span class="dot ${dotCls}"></span>
        <span class="job-meta-text" style="font-size:10px;">${anyRunning?'Running…':when}</span>
        ${keysStr}
      </div>
      ${srcHtml ? `<div class="coll-sources-list">${srcHtml}</div>` : ''}
    </div>`);
  }

  // ── Standalone jobs (not in any collection) ──────────────────────────────────
  const standaloneJobs = jobs.filter(j => !collJobIds.has(j.id));
  if (standaloneJobs.length && collections_.length) {
    parts.push(`<div style="font-size:10px;color:var(--tx2);padding:8px 14px 2px;text-transform:uppercase;letter-spacing:.05em;">Standalone</div>`);
  }
  for (const j of standaloneJobs) {
    const dotCls = j.is_running ? 'dot-running' : j.last_run_status==='success' ? 'dot-success' : j.last_run_status==='failed' ? 'dot-failed' : 'dot-none';
    const when   = j.last_run_at ? timeAgo(j.last_run_at) : 'never';
    parts.push(`<div class="job-item ${j.id===selJobId&&!selCollId?'active':''}" onclick="selectJob(${j.id})">
      <div class="job-name">${esc(j.name)}</div>
      <div class="job-meta">
        <span class="dot ${dotCls}"></span>
        <span class="job-meta-text">${j.is_running?'Running…':when}</span>
      </div>
    </div>`);
  }

  el.innerHTML = parts.join('') || '<div style="padding:14px;color:var(--tx2);font-size:12px;">No jobs yet</div>';
}

async function selectJob(id) {
  if (selJobId !== id) {
    dataFields_=[]; dataSelectedFields_=null; lastDataRows_=null; dataPage_=1;
    linkedJobFields_ = []; corrDataCache_ = {};
    selectedRows_.clear(); updateBulkBar();
    document.getElementById('data-search').value='';
    document.getElementById('data-from').value='';
    document.getElementById('data-to').value='';
    fieldFilters_ = []; renderExcludeChips();
  }
  selJobId = id;
  selCollId = null;   // selecting a standalone job clears collection selection
  const collJobIds = new Set(collections_.flatMap(c=>(c.sources||[]).map(s=>s.job_id).filter(Boolean)));
  renderSidebar(collJobIds);
  document.getElementById('welcome').style.display='none';
  const d=document.getElementById('job-detail'); d.style.display='flex';
  const job = jobs.find(j=>j.id===id);
  updateDetailHeader(job);
  _updateCrossSourceBtn();

  // If this job has an active stream, go straight to Live Log and reconnect
  if (activeESes_[id] || (job && job.is_running && activeRunIds_[id])) {
    switchTab('live');
    reconnectLiveLog(id);
  } else {
    switchTab('data');
  }
  await loadRuns(id);
}

function updateDetailHeader(job) {
  if (!job) return;
  document.getElementById('d-name').textContent = job.name;
  document.getElementById('d-url').textContent  = job.kibana_url;
  const chunkLabel = job.chunk_hours > 0 ? ` · ${job.chunk_hours}h chunks` : '';
  document.getElementById('d-schedule').textContent =
    `Daily @ ${String(job.schedule_hour).padStart(2,'0')}:00 UTC · ${job.time_range}${chunkLabel}`;
  const btn = document.getElementById('btn-run');
  btn.disabled    = !!job.is_running;
  btn.textContent = job.is_running ? '⟳ Running…' : '▶ Run Now';
  btn.onclick     = () => runNow();
}

// ── Collection selection + run ────────────────────────────────────────────────
async function selectCollection(cid) {
  selCollId = cid;
  // Use first source's job as the "active job" for data/runs tabs
  const coll = collections_.find(c=>c.id===cid);
  const firstJobId = coll?.sources?.[0]?.job_id;
  if (firstJobId && selJobId !== firstJobId) {
    dataFields_=[]; dataSelectedFields_=null; lastDataRows_=null; dataPage_=1;
    linkedJobFields_=[]; corrDataCache_={};
    selectedRows_.clear(); updateBulkBar();
    document.getElementById('data-search').value='';
    document.getElementById('data-from').value='';
    document.getElementById('data-to').value='';
    fieldFilters_ = []; renderExcludeChips();
    selJobId = firstJobId;
  }
  const collJobIds = new Set(collections_.flatMap(c=>(c.sources||[]).map(s=>s.job_id).filter(Boolean)));
  renderSidebar(collJobIds);
  document.getElementById('welcome').style.display='none';
  document.getElementById('job-detail').style.display='flex';
  if (coll) updateDetailHeaderForCollection(coll);
  _updateCrossSourceBtn();
  if (firstJobId) await loadRuns(firstJobId);
}

// Click a specific source within a collection — keeps collection context
async function selectCollectionSource(collId, jobId) {
  selCollId = collId;
  if (selJobId !== jobId) {
    dataFields_=[]; dataSelectedFields_=null; lastDataRows_=null; dataPage_=1;
    linkedJobFields_=[]; corrDataCache_={};
    selectedRows_.clear(); updateBulkBar();
    document.getElementById('data-search').value='';
    document.getElementById('data-from').value='';
    document.getElementById('data-to').value='';
    fieldFilters_ = []; renderExcludeChips();
    selJobId = jobId;
  }
  const coll = collections_.find(c=>c.id===collId);
  const collJobIds = new Set(collections_.flatMap(c=>(c.sources||[]).map(s=>s.job_id).filter(Boolean)));
  renderSidebar(collJobIds);
  document.getElementById('welcome').style.display='none';
  document.getElementById('job-detail').style.display='flex';
  // Header: show collection name + source name, subtitle shows both source URLs
  if (coll) updateDetailHeaderForCollectionSource(coll, jobId);
  _updateCrossSourceBtn();
  if (activeESes_[jobId] || (jobs.find(j=>j.id===jobId)||{}).is_running) {
    switchTab('live');
    reconnectLiveLog(jobId);
  } else {
    switchTab('data');
  }
  await loadRuns(jobId);
}

function updateDetailHeaderForCollectionSource(coll, jobId) {
  const src = (coll.sources||[]).find(s=>s.job_id===jobId);
  const srcName = src ? src.label : `Job ${jobId}`;
  const anyRunning = (coll.sources||[]).some(s=>s.is_running);
  document.getElementById('d-name').textContent = `${coll.name}  ›  ${srcName}`;
  // Subtitle: all source URLs joined
  const srcUrls = (coll.sources||[]).map(s=>s.kibana_url).join('  |  ');
  document.getElementById('d-url').textContent = srcUrls || '–';
  const chunkLabel = coll.chunk_hours > 0 ? ` · ${coll.chunk_hours}h chunks` : '';
  const keysLabel  = (coll.corr_keys||[]).length ? ` · 🔗 ${coll.corr_keys.join(', ')}` : '';
  document.getElementById('d-schedule').textContent =
    `Daily @ ${String(coll.schedule_hour||2).padStart(2,'0')}:00 UTC · ${coll.time_range}${chunkLabel}${keysLabel}`;
  const btn = document.getElementById('btn-run');
  btn.disabled    = anyRunning;
  btn.textContent = anyRunning ? '⟳ Running…' : '▶ Run All';
  btn.onclick     = () => runCollection(coll.id);
}

function updateDetailHeaderForCollection(coll) {
  const anyRunning = (coll.sources||[]).some(s=>s.is_running);
  document.getElementById('d-name').textContent = coll.name;
  const srcNames = (coll.sources||[]).map(s=>s.label).join(' · ');
  document.getElementById('d-url').textContent = srcNames || '–';
  const chunkLabel = coll.chunk_hours > 0 ? ` · ${coll.chunk_hours}h chunks` : '';
  const keysLabel  = (coll.corr_keys||[]).length ? ` · 🔗 ${coll.corr_keys.join(', ')}` : '';
  document.getElementById('d-schedule').textContent =
    `Daily @ ${String(coll.schedule_hour||2).padStart(2,'0')}:00 UTC · ${coll.time_range}${chunkLabel}${keysLabel}`;
  const btn = document.getElementById('btn-run');
  btn.disabled    = anyRunning;
  btn.textContent = anyRunning ? '⟳ Running…' : '▶ Run All';
  btn.onclick     = () => runCollection(coll.id);
}

async function runCollection(cid) {
  const coll = collections_.find(c=>c.id===cid);
  const btn = document.getElementById('btn-run');
  btn.disabled = true; btn.textContent = '⟳ Running…';
  const r = await api.post(`/api/collections/${cid}/run`, {});
  if (r.run_id) {
    // Register the run_id under ALL source job IDs so cancel works from any source view
    for (const src of (coll?.sources || [])) {
      if (src.job_id) activeRunIds_[src.job_id] = r.run_id;
    }
    const firstJobId = coll?.sources?.[0]?.job_id;
    switchTab('live');
    streamRun(r.run_id, firstJobId || selJobId);
  }
  setTimeout(()=>loadJobs(), 1000);
}

async function deleteCollection(cid) {
  const coll = collections_.find(c=>c.id===cid);
  const name = coll?.name || `Collection ${cid}`;
  if (!confirm(`Delete "${name}"?\n\nThis will permanently remove all sources, log data, run history, and correlations.\n\nThis cannot be undone.`)) return;
  showToast('🗑', 'Deleting…', `Removing "${name}"`, 0);
  await api.del(`/api/collections/${cid}`);
  showToast('✓', 'Deleted', `"${name}" removed`, 100);
  hideToast(3000);
  selCollId = null; selJobId = null;
  document.getElementById('job-detail').style.display='none';
  document.getElementById('welcome').style.display='flex';
  await loadJobs();
  await loadStats();
}

// ── Tabs ──────────────────────────────────────────────────────────────────────
function switchTab(name) {
  ['runs','live','data','config'].forEach(t => {
    document.getElementById('tab-'+t).classList.toggle('active', t===name);
    document.getElementById('pane-'+t).classList.toggle('active', t===name);
  });
  if (name==='data' && selJobId) {
    if (!dataFields_.length) loadFields(selJobId).then(()=>loadData(selJobId, dataPage_));
    else loadData(selJobId, dataPage_);
  }
  if (name==='config') {
    if (selCollId) renderConfigFormCollection(collections_.find(c=>c.id===selCollId));
    else if (selJobId) renderConfigForm(jobs.find(j=>j.id===selJobId));
  }
  // When manually clicking Live Log tab, reconnect to this job's active stream if any
  if (name==='live' && selJobId && activeESes_[selJobId]) reconnectLiveLog(selJobId);
}

// ── Runs ──────────────────────────────────────────────────────────────────────
async function loadRuns(jobId) {
  const runs = await api.get(`/api/jobs/${jobId}/runs`);
  document.getElementById('run-count').textContent = `${runs.length} run${runs.length!==1?'s':''}`;
  const el = document.getElementById('runs-list');
  if (!runs.length) {
    el.innerHTML = '<div style="padding:16px 14px;color:var(--tx2);font-size:12px;">No runs yet — click Run Now to start.</div>';
    return;
  }
  el.innerHTML = runs.map(r => {
    const dur = r.finished_at ? elapsed(r.started_at, r.finished_at) : 'running…';
    return `<div class="run-row" onclick="viewRun(${r.id})">
      <span class="badge badge-${r.status}">${r.status}</span>
      <span style="color:var(--tx0);">${fmtDate(r.started_at)}</span>
      <span style="color:var(--tx1);font-size:11px;">${dur}</span>
      <span style="color:var(--green);font-size:11px;">${r.rows_saved>0?'+'+r.rows_saved+' rows':''}</span>
      <span style="color:var(--tx2);font-size:11px;">View →</span>
    </div>`;
  }).join('');
}

function viewRun(runId) { switchTab('live'); streamRun(runId, selJobId); }

// ── Run Now ───────────────────────────────────────────────────────────────────
async function runNow() {
  const btn=document.getElementById('btn-run');
  btn.disabled=true; btn.textContent='⟳ Running…';
  const jobId = selJobId;
  const r=await api.post(`/api/jobs/${jobId}/run`);
  switchTab('live');
  streamRun(r.run_id, jobId);
  setTimeout(()=>loadJobs(), 1000);
}

// ── SSE run stream ────────────────────────────────────────────────────────────
// jobId is which job owns this stream (defaults to selJobId for backwards compat)
function streamRun(runId, jobId) {
  if (jobId == null) jobId = selJobId;

  // Close any existing stream for this job
  if (activeESes_[jobId]) { try{activeESes_[jobId].close();}catch(e){} }
  activeRunIds_[jobId] = runId;

  const isVisible = (selJobId === jobId);   // is this job currently on screen?

  // Only touch the DOM if this job is the one currently selected
  function _dom(fn) { if (selJobId === jobId) fn(); }

  _dom(()=>{
    document.getElementById('terminal').innerHTML='';
    document.getElementById('log-done').style.display='none';
    document.getElementById('live-title').textContent=`Run #${runId} — streaming…`;
  });

  const es = new EventSource(`/api/runs/${runId}/stream`);
  activeESes_[jobId] = es;

  // Show cancel button while streaming
  _dom(() => {
    const cancelBtn = document.getElementById('btn-cancel-run');
    if (cancelBtn) cancelBtn.style.display = 'inline-flex';
  });

  es.onmessage=e=>{
    const d=JSON.parse(e.data);
    if (d.type==='log') {
      _dom(()=>{
        const term=document.getElementById('terminal');
        const line=document.createElement('div');
        line.className='log-line '+lineClass(d.line);
        line.textContent=d.line;
        term.appendChild(line); term.scrollTop=term.scrollHeight;
      });
    } else if (d.type==='done') {
      es.close();
      delete activeESes_[jobId];
      _dom(()=>{
        const cancelBtn = document.getElementById('btn-cancel-run');
        if (cancelBtn) cancelBtn.style.display = 'none';
        const runId2=activeRunIds_[jobId];
        document.getElementById('live-title').textContent=`Run #${runId2} — ${d.status}`;
        const done=document.getElementById('log-done');
        done.className=`log-done ${d.status}`;
        done.textContent=d.status==='success'?'✓ Completed successfully':'✗ Run failed';
        done.style.display='block';
      });
      loadJobs(); loadRuns(jobId === selJobId ? selJobId : null);
      _startSmartPoll(); // speed ramp-down now that run finished
    }
  };
  es.onerror=()=>{
    _dom(()=>{
      document.getElementById('live-title').textContent=`Run #${runId} — connection closed`;
      const cancelBtn = document.getElementById('btn-cancel-run');
      if (cancelBtn) cancelBtn.style.display = 'none';
    });
  };
}

// F7: Cancel the active run for selJobId (or any collection source sharing the same run)
async function cancelRun() {
  // Try current job first, then fall back to any run in the current collection
  let runId = activeRunIds_[selJobId];
  if (!runId && selCollId) {
    const coll = collections_.find(c => c.id === selCollId);
    for (const src of (coll?.sources || [])) {
      if (activeRunIds_[src.job_id]) { runId = activeRunIds_[src.job_id]; break; }
    }
  }
  if (!runId) { showToast('!', 'Nothing to cancel', 'No active run found', 0); hideToast(3000); return; }
  if (!confirm(`Cancel run #${runId}?`)) return;
  try {
    await api.post(`/api/runs/${runId}/cancel`, {});
    showToast('✕', 'Cancelling…', `Sent cancel signal to run #${runId}`, 50);
    hideToast(3000);
  } catch(e) {
    showToast('!', 'Cancel failed', String(e), 0);
    hideToast(4000);
  }
}

// Reconnect Live Log to whichever run is active for a job (called on tab/job switch)
function reconnectLiveLog(jobId) {
  const runId = activeRunIds_[jobId];
  if (!runId) return;          // no active stream for this job
  const es = activeESes_[jobId];

  const term  = document.getElementById('terminal');
  const done  = document.getElementById('log-done');
  const title = document.getElementById('live-title');

  if (es && es.readyState !== EventSource.CLOSED) {
    // Stream still live — just update the title; new messages will appear via onmessage
    title.textContent = `Run #${runId} — streaming…`;
    done.style.display='none';
    // The onmessage handler uses _dom() which checks selJobId === jobId,
    // so after selectJob sets selJobId we automatically get DOM updates
  } else {
    // Stream ended or not started — show completed state
    title.textContent = `Run #${runId} — finished`;
    done.style.display='none';
    // Replay log from server
    streamRun(runId, jobId);
  }
}

function lineClass(line) {
  if (!line) return '';
  if (line.startsWith('━━━ CHUNK')) return 'chunk-hdr';
  if (line.includes('[✓]')||line.includes('Done')) return 'ok';
  if (line.includes('[✗]')||line.includes('ERROR')||line.includes('failed')) return 'err';
  if (line.includes('[!]')||line.includes('warning')) return 'warn';
  if (line.includes('[→]')||line.includes('[net]')||line.includes('[bsearch]')) return 'info';
  return '';
}
function clearLog() {
  document.getElementById('terminal').innerHTML='';
  document.getElementById('log-done').style.display='none';
}

// ── Field panel ───────────────────────────────────────────────────────────────
function toggleFieldPanel() {
  fieldPanelCollapsed_ = !fieldPanelCollapsed_;
  const panel=document.getElementById('field-panel');
  const btn=panel.querySelector('.collapse-btn');
  panel.classList.toggle('collapsed', fieldPanelCollapsed_);
  btn.textContent = fieldPanelCollapsed_ ? '›' : '‹';
  btn.title = fieldPanelCollapsed_ ? 'Expand' : 'Collapse';
}

// B1: per-job column persistence — key includes job ID
function _lsFieldsKey() { return `kibana_cols_${selJobId || 'default'}`; }

function _saveFieldsToLS() {
  try { localStorage.setItem(_lsFieldsKey(), JSON.stringify([...dataSelectedFields_])); } catch(e) {}
}

function _loadFieldsFromLS(available) {
  try {
    const saved = JSON.parse(localStorage.getItem(_lsFieldsKey()) || 'null');
    if (Array.isArray(saved) && saved.length) {
      const fset = new Set(available);
      // Keep own fields that still exist + all linked fields (they have LINKED_SEP)
      const valid = saved.filter(f => f.includes(LINKED_SEP) || fset.has(f));
      if (valid.length) return new Set(valid);
    }
  } catch(e) {}
  return null;
}

async function loadFields(jobId) {
  try { dataFields_ = await api.get(`/api/jobs/${jobId}/fields`); }
  catch(e) { dataFields_ = DEFAULT_FIELDS.slice(); }

  // Load linked job fields
  try {
    const gf = await api.get(`/api/jobs/${jobId}/group-fields`);
    linkedJobFields_ = gf.linked || [];
  } catch(e) { linkedJobFields_ = []; }

  if (!dataSelectedFields_) {
    const fromLS = _loadFieldsFromLS(dataFields_);
    if (fromLS) {
      dataSelectedFields_ = fromLS;
    } else {
      const fset = new Set(dataFields_);
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

  // Own fields
  const visibleOwn = q ? dataFields_.filter(f => f.toLowerCase().includes(q)) : dataFields_;
  const ownHtml = visibleOwn.map((f, i) => {
    const checked = dataSelectedFields_.has(f);
    return `<div class="field-item ${checked ? 'selected' : ''}">
      <input type="checkbox" id="fld-${i}" data-field="${esc(f)}" data-linked="0" ${checked ? 'checked' : ''} onchange="toggleField(this)">
      <label for="fld-${i}" title="${esc(f)}">${esc(f)}</label>
    </div>`;
  }).join('');

  // Linked job fields — one section per linked job
  let linkedHtml = '';
  for (const lj of linkedJobFields_) {
    const prefix = lj.label + LINKED_SEP;
    const visibleLinked = q
      ? lj.fields.filter(f => f.toLowerCase().includes(q) || lj.label.toLowerCase().includes(q))
      : lj.fields;
    if (!visibleLinked.length) continue;
    const items = visibleLinked.map((f, i) => {
      const key = prefix + f;
      const checked = dataSelectedFields_.has(key);
      const uid = `lfld-${lj.job_id}-${i}`;
      return `<div class="field-item linked ${checked ? 'selected' : ''}">
        <input type="checkbox" id="${uid}" data-field="${esc(key)}" data-linked="1" ${checked ? 'checked' : ''} onchange="toggleField(this)">
        <label for="${uid}" title="${esc(lj.label + ': ' + f)}">${esc(f)}</label>
      </div>`;
    }).join('');
    linkedHtml += `<div class="field-source-hdr"><span class="src-dot"></span>🔗 ${esc(lj.label)}</div>${items}`;
  }

  el.innerHTML = ownHtml + linkedHtml;
  if (!ownHtml && !linkedHtml) el.innerHTML = '<div style="padding:10px;color:var(--tx2);font-size:11px;">No match</div>';
}

function toggleField(cb) {
  const f=cb.getAttribute('data-field');
  if (cb.checked) dataSelectedFields_.add(f); else dataSelectedFields_.delete(f);
  cb.closest('.field-item').classList.toggle('selected', cb.checked);
  _saveFieldsToLS();
  if (lastDataRows_) renderDataTable(lastDataRows_);
}

// ── Table rendering ───────────────────────────────────────────────────────────
function getFieldValue(row, field, rj) {
  // Linked field: "SourceLabel\x1Efield_name"
  if (field.includes(LINKED_SEP)) {
    const sepIdx = field.indexOf(LINKED_SEP);
    const srcLabel = field.slice(0, sepIdx);
    const srcField = field.slice(sepIdx + 1);
    const linked = (corrDataCache_[String(row.id)] || corrDataCache_[row.id] || []);
    const match = linked.find(c => c._source_label === srcLabel);
    if (!match) return '';
    const lrj = match.raw_json || {};
    const val = lrj[srcField];
    let v = Array.isArray(val) ? (val[0] ?? '') : (val ?? '');
    if (v !== null && typeof v === 'object') v = JSON.stringify(v);
    return v;
  }
  // Own field
  if (field in DATA_DB_MAP) return DATA_DB_MAP[field](row) ?? '';
  const val = rj[field];
  let v = Array.isArray(val) ? (val[0] ?? '') : (val ?? '');
  if (v !== null && typeof v === 'object') v = JSON.stringify(v);
  return v;
}

function errorLevelClass(val) {
  if (!val) return '';
  const v=String(val).toUpperCase();
  if (v.includes('ERROR')||v.includes('CRITICAL')) return ' class="el-error"';
  if (v.includes('WARN')) return ' class="el-warn"';
  if (v.includes('INFO')) return ' class="el-info"';
  if (v.includes('DEBUG')) return ' class="el-debug"';
  return '';
}

function httpColor(s) {
  if (!s) return '';
  const n=parseInt(s);
  if (n<300) return 'color:var(--green)';
  if (n<400) return 'color:var(--blue)';
  if (n<500) return 'color:var(--orange)';
  return 'color:var(--red)';
}

function renderDataTable(rows) {
  lastDataRows_=rows;
  const fields=dataSelectedFields_ ? [...dataSelectedFields_] : DEFAULT_FIELDS;
  const allChecked = rows.length > 0 && rows.every(r => selectedRows_.has(r.id));

  const _sortArrow = f => {
    if (f !== sortField_) return ' <span style="opacity:.25;font-size:9px;">⇅</span>';
    return sortDir_ === 'asc'
      ? ' <span style="color:var(--blue);font-size:10px;">▲</span>'
      : ' <span style="color:var(--blue);font-size:10px;">▼</span>';
  };
  const _thStyle = f => f === sortField_
    ? 'cursor:pointer;white-space:nowrap;background:rgba(88,166,255,.08);'
    : 'cursor:pointer;white-space:nowrap;';

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

  const tbody=document.getElementById('data-tbody');
  if (!rows.length) {
    tbody.innerHTML=`<tr><td colspan="${fields.length+3}" style="color:var(--tx2);padding:20px 14px;text-align:center;">No data — run the job first or adjust filters.</td></tr>`;
    return;
  }

  tbody.innerHTML=rows.map(r=>{
    let rj={};
    try { rj=r.raw_json ? (typeof r.raw_json==='string' ? JSON.parse(r.raw_json) : r.raw_json) : {}; } catch(e){}
    const rowCls = r.relevance==='relevant' ? 'relevant-row' : r.relevance==='irrelevant' ? 'irrelevant-row' : '';
    const sev=sevBadge(r.severity, r.id, !!r.ai_summary);
    const dataCells=fields.map(f=>{
      const raw=getFieldValue(r, f, rj);
      const disp=raw==null ? '' : String(raw);
      let attr='';
      if (f==='error_level') attr=errorLevelClass(disp);
      else if (f==='http_status_code'||f==='status') { const c=httpColor(disp); if (c) attr=` style="${c};font-weight:600;"`; }
      return `<td${attr} title="${esc(disp)}">${esc(disp.length>90 ? disp.slice(0,90)+'…' : disp)}</td>`;
    }).join('');

    return `<tr class="${rowCls}" onclick="expandRow(this,${r.id})">
      <td class="col-select" onclick="event.stopPropagation()">
        <input type="checkbox" class="row-checkbox" ${selectedRows_.has(r.id)?'checked':''} onchange="toggleSelectRow(${r.id},this)">
      </td>
      <td style="width:76px;">${sev}</td>${dataCells}
      <td class="row-actions" onclick="event.stopPropagation()" style="width:110px;text-align:right;padding-right:8px;">
        <button class="btn btn-sm mark-relevant-btn" style="background:rgba(63,185,80,.15);color:var(--green);border:1px solid rgba(63,185,80,.3);padding:2px 6px;font-size:10px;" onclick="markRow(${r.id},'relevant',this)">✓ Relevant</button>
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
  const detail=document.getElementById('expand-'+rowId);
  if (!detail) return;
  const open=detail.style.display==='table-row';
  detail.style.display=open ? 'none' : 'table-row';
  tr.classList.toggle('expanded-row', !open);
  // Load correlated records inline when first opened
  if (!open) {
    const inlineEl = document.getElementById(`corr-inline-${rowId}`);
    if (inlineEl && inlineEl.dataset.loaded !== '1') {
      inlineEl.dataset.loaded = '1';
      loadCorrInline(rowId, inlineEl);
    }
  }
}

async function loadCorrInline(rowId, el) {
  try {
    const results = await api.get(`/api/logs/${rowId}/correlated`);
    if (!results || !results.length) { el.style.display = 'none'; return; }

    el.style.cssText = '';

    // ── Group by source label ─────────────────────────────────────────────────
    const bySource = {};
    for (const r of results) {
      const lbl = r._source_label || 'Linked';
      if (!bySource[lbl]) bySource[lbl] = [];
      let rj = {};
      try { rj = typeof r.raw_json === 'string' ? JSON.parse(r.raw_json) : (r.raw_json || {}); } catch(e) {}
      bySource[lbl].push({...r, _rj: rj});
    }

    function _strVal(v) {
      if (Array.isArray(v)) v = v[0] ?? '';
      if (v !== null && typeof v === 'object') return JSON.stringify(v);
      return String(v ?? '');
    }

    function _fieldRow(k, v, highlight) {
      const disp = renderKfValue(_strVal(v));
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

    el.innerHTML = Object.entries(bySource).map(([label, recs]) => {
      const keys = (recs[0]._matched_keys || []).map(k => `<span class="corr-key-badge">${esc(k)}</span>`).join(' ');

      if (recs.length === 1) {
        // Single record — show all fields
        const r = recs[0];
        const sev = r.severity ? `<span class="sev sev-${r.severity}">${r.severity}</span>` : '';
        const rows = Object.entries(r._rj).map(([k,v]) => _fieldRow(k, v, false)).join('');
        return `<div style="border-top:2px solid rgba(88,166,255,.25);margin-top:2px;">
          <div style="background:rgba(88,166,255,.07);padding:6px 56px;display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
            <span class="corr-source-badge">🔗 ${esc(label)}</span>${keys}${sev}
            <span style="color:var(--tx2);font-size:10px;margin-left:auto;">${r.timestamp||''}</span>
          </div>
          <table class="kibana-doc-table"><colgroup><col style="width:56px"><col style="width:180px"><col></colgroup>
            <tbody>${rows}</tbody></table></div>`;
      }

      // Multiple records from same source — find common vs differing fields
      const allKeys = [...new Set(recs.flatMap(r => Object.keys(r._rj)))];
      const commonKeys = allKeys.filter(k =>
        recs.every(r => _strVal(r._rj[k]) === _strVal(recs[0]._rj[k]))
      );
      const diffKeys = allKeys.filter(k => !commonKeys.includes(k));
      const commonSet = new Set(commonKeys);

      // Common fields section (shown once)
      const commonRows = commonKeys.map(k => _fieldRow(k, recs[0]._rj[k], false)).join('');
      const uid = `corr-common-${rowId}-${label.replace(/\W/g,'_')}`;

      // Per-record diff sections
      const diffSections = recs.map((r, i) => {
        const sev = r.severity ? `<span class="sev sev-${r.severity}">${r.severity}</span>` : '';
        const diffRows = diffKeys.length
          ? diffKeys.map(k => _fieldRow(k, r._rj[k], true)).join('')
          : `<tr><td colspan="3" style="padding:6px 12px;color:var(--tx2);font-size:11px;">All fields identical to record above</td></tr>`;
        return `<div style="border-top:1px solid rgba(88,166,255,.15);margin-left:56px;">
          <div style="padding:4px 0 4px 0;display:flex;align-items:center;gap:8px;color:var(--tx2);font-size:10px;">
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
        ${commonRows ? `
        <div style="padding:3px 0;">
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

  } catch(e) {
    el.style.display = 'none';
  }
}

// ── Data loading ──────────────────────────────────────────────────────────────
async function loadData(jobId, page) {
  const search   = document.getElementById('data-search')?.value.trim()||'';
  const fromRaw  = document.getElementById('data-from')?.value||'';
  const toRaw    = document.getElementById('data-to')?.value||'';
  const params   = new URLSearchParams({page, per_page: perPage_});
  if (search)   params.set('search', search);
  if (regexMode_) params.set('regex', 'true');
  if (crossSourceMode_ && search) params.set('cross_source', 'true');
  if (fromRaw)  params.set('from_date', fromRaw.replace('T',' '));
  if (toRaw)    params.set('to_date', toRaw.replace('T',' '));
  if (relFilter_) params.set('relevance', relFilter_);
  params.set('sort_by', sortField_);
  params.set('sort_dir', sortDir_);
  if (fieldFilters_.length) params.set('field_filters', JSON.stringify(fieldFilters_));
  if (page === 1) loadHistogram(jobId);
  const d=await api.get(`/api/jobs/${jobId}/data?${params}`);
  const total=d.total||0;
  const pages=Math.ceil(total/d.per_page)||1;
  document.getElementById('data-total').innerHTML=
    `<b>${total.toLocaleString()}</b> records${crossSourceMode_ && search ? ' <span style="font-size:10px;color:var(--blue);font-weight:500;">🔗 incl. correlated</span>' : ''}`;
  document.getElementById('data-page-info').innerHTML=
    `Page <b>${d.page}</b> / <b>${pages}</b>`;
  // Keep header rows chip in sync with the currently viewed job
  const _jName = (jobs.find(j=>j.id===jobId)||{}).name || null;
  _updateRowsChip(total, _jName);
  renderDataTable(d.rows);
  // If any linked fields are selected, batch-fetch correlated data for visible rows
  const hasLinked = dataSelectedFields_ && [...dataSelectedFields_].some(f => f.includes(LINKED_SEP));
  if (hasLinked && d.rows && d.rows.length) {
    const ids = d.rows.map(r => r.id);
    corrDataCache_ = {};  // clear cache for new page
    try {
      const batch = await api.post('/api/logs/correlated-batch', {ids});
      corrDataCache_ = batch || {};
      renderDataTable(d.rows);  // re-render with correlated data populated
    } catch(e) { /* ignore, table already rendered without linked values */ }
  }
}

function applyDataFilter() { dataPage_=1; loadData(selJobId, 1); }
function clearDataFilter() {
  document.getElementById('data-search').value='';
  document.getElementById('data-from').value='';
  document.getElementById('data-to').value='';
  dataPage_=1; loadData(selJobId, 1);
}
function dataPage(delta) { dataPage_=Math.max(1, dataPage_+delta); clearSelection(); loadData(selJobId, dataPage_); }

// ── Histogram ─────────────────────────────────────────────────────────────────
let _histChart = null;

async function loadHistogram(jobId) {
  const wrap   = document.getElementById('histogram-wrap');
  const canvas = document.getElementById('histogram-canvas');
  if (!wrap || !canvas) return;

  const search  = document.getElementById('data-search')?.value.trim()||'';
  const fromRaw = document.getElementById('data-from')?.value||'';
  const toRaw   = document.getElementById('data-to')?.value||'';
  const p = new URLSearchParams();
  if (search)         p.set('search', search);
  if (regexMode_)     p.set('regex', 'true');
  if (crossSourceMode_ && search) p.set('cross_source', 'true');
  if (fromRaw)        p.set('from_date', fromRaw.replace('T',' '));
  if (toRaw)          p.set('to_date', toRaw.replace('T',' '));
  if (relFilter_)     p.set('relevance', relFilter_);
  if (fieldFilters_.length) p.set('field_filters', JSON.stringify(fieldFilters_));

  let hd;
  try { hd = await api.get(`/api/jobs/${jobId}/histogram?${p}`); }
  catch(e) { wrap.classList.add('hidden'); return; }

  if (!hd || !hd.buckets || !hd.buckets.length) { wrap.classList.add('hidden'); return; }
  wrap.classList.remove('hidden');

  // Meta labels
  const fmt = ts => ts ? ts.replace('T',' ').slice(0,16) : '';
  document.getElementById('histogram-hits').textContent   = `${(hd.total||0).toLocaleString()} hits`;
  document.getElementById('histogram-range').textContent  = `${fmt(hd.from)} – ${fmt(hd.to)}`;
  document.getElementById('histogram-bucket').textContent = `Bucket: ${hd.bucket_label}`;

  const labels = hd.buckets.map(b => b.t);
  const data   = hd.buckets.map(b => b.c);

  if (_histChart) { _histChart.destroy(); _histChart = null; }
  _histChart = new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        data,
        backgroundColor: 'rgba(63,185,80,0.55)',
        hoverBackgroundColor: 'rgba(63,185,80,0.85)',
        borderWidth: 0,
        barPercentage: 0.9,
        categoryPercentage: 1.0,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      animation: false,
      plugins: { legend: {display:false}, tooltip: {
        callbacks: { title: items => items[0].label.replace('T',' '), label: i => `${i.raw.toLocaleString()} logs` }
      }},
      scales: {
        x: { ticks: { color:'#6e7681', maxTicksLimit:8, maxRotation:0,
                       callback: (v, i) => labels[i]?.replace('T',' ').slice(0,13) },
             grid: {display:false}, border:{display:false} },
        y: { ticks: { color:'#6e7681', maxTicksLimit:4 },
             grid: { color:'rgba(48,54,61,0.6)' }, border:{display:false} }
      },
      onClick(evt, elems) {
        if (!elems.length) return;
        const idx   = elems[0].index;
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
  relFilter_=val;
  const map={all:{val:'',cls:'active'}, relevant:{val:'relevant',cls:'active-green'}, irrelevant:{val:'irrelevant',cls:'active-red'}, unchecked:{val:'unchecked',cls:'active-orange'}};
  Object.entries(map).forEach(([k,{val:v,cls}])=>{
    const el=document.getElementById('rpill-'+k);
    if (!el) return;
    el.className='rel-pill';
    if (val===v) el.className+=' '+cls;
  });
  dataPage_=1; loadData(selJobId, 1);
}

// ── Severity badges ───────────────────────────────────────────────────────────
const SEV_LABEL={critical:'CRITICAL',high:'HIGH',medium:'MEDIUM',low:'LOW',info:'INFO'};

function sevBadge(sev, rowId, hasSummary) {
  const cls=sev?`sev-${sev}`:'sev-unchecked';
  const label=sev?(SEV_LABEL[sev]||sev.toUpperCase()):'·';
  const click=hasSummary?`onclick="event.stopPropagation();openCodeRefs(${rowId})"`:
                         `onclick="event.stopPropagation();"`;
  return `<span class="sev ${cls}" ${click} title="${hasSummary?'Click for AI summary':''}">${label}</span>`;
}

// ── Code refs modal ───────────────────────────────────────────────────────────
function openCodeRefs(rowId) {
  const row=(lastDataRows_||[]).find(r=>r.id===rowId);
  if (!row) return;
  const sev=row.severity||'info';
  document.getElementById('cr-title').textContent='AI Analysis — Log #'+rowId;
  document.getElementById('cr-rel-label').textContent =
    row.relevance==='relevant' ? '✓ Checkout / payment related' :
    row.relevance==='irrelevant' ? '✗ Not related to checkout' : '';
  document.getElementById('cr-severity-badge').innerHTML=sevBadge(sev, rowId, false);
  document.getElementById('cr-summary').textContent=row.ai_summary||'No summary available.';

  let refs=row.code_refs;
  if (typeof refs==='string'){try{refs=JSON.parse(refs);}catch(e){refs=[];}}
  const refsEl=document.getElementById('cr-refs');
  if (!refs||!refs.length) {
    refsEl.innerHTML=row.relevance==='relevant'
      ?`<div style="color:var(--tx2);font-size:12px;display:flex;align-items:center;gap:8px;">
          No GitHub code references found.
          <button class="btn btn-ghost btn-sm" onclick="fetchCodeRefs(${rowId})">🔍 Search GitHub</button>
        </div>`
      :'<div style="color:var(--tx2);font-size:12px;">Not checkout-related — no code search performed.</div>';
  } else {
    refsEl.innerHTML='<div class="coderef-section-label" style="margin-bottom:8px;">Code References</div>'+
      refs.map(r=>`<div class="coderef-item">
        <a href="${esc(r.url)}" target="_blank">📄 ${esc(r.path)}</a>
        <div class="coderef-repo">${esc(r.repo)}</div>
      </div>`).join('');
  }
  document.getElementById('coderef-modal').classList.add('open');
}

async function fetchCodeRefs(rowId) {
  const settings=await api.get('/api/settings');
  if (!settings.github_token_set) {
    document.getElementById('coderef-modal').classList.remove('open');
    openSettings();
  }
}

// ── MCP prompt modal (shown when no Anthropic key is configured) ──────────────
function showMcpPromptModal(mode) {
  if (!selJobId) return;
  const jobName = (jobs.find(j => j.id === selJobId) || {}).name || `Job ${selJobId}`;

  let prompt;
  if (mode === 'analyze') {
    prompt =
`I need you to analyze logs from the kibana-scraper MCP server for job "${jobName}" (id: ${selJobId}).

Steps:
1. Call fetch_logs_for_analysis(job_id=${selJobId}, limit=50)
2. For each log entry, determine:
   - relevant: true if related to checkout / cart / payment / order processing
   - severity: "critical" | "high" | "medium" | "low" | "info"
   - summary: one concise sentence describing what happened
   - github_terms: 1-2 class/method names or error codes to search in source code
3. Call save_analysis_results() with ALL your classifications in one call
4. Repeat steps 1-3 until fetch_logs_for_analysis returns status "no_more_logs"`;
  } else {
    prompt =
`Please run auto-classification on the kibana-scraper MCP server for job "${jobName}" (id: ${selJobId}).

Steps:
1. Call get_stats(job_id=${selJobId}) to see the current breakdown
2. Call fetch_logs_for_analysis(job_id=${selJobId}, limit=100) to get unanalyzed logs
3. Classify each log (relevant, severity, summary, github_terms) based on already-analyzed patterns for this job
4. Call save_analysis_results() with all results
5. Repeat until no more unanalyzed logs remain
6. Call get_analysis_summary(job_id=${selJobId}) and show me the results`;
  }

  document.getElementById('mcp-modal-title').textContent =
    mode === 'analyze' ? '🤖 Analyze via Claude Desktop MCP' : '⚡ Auto-Classify via Claude Desktop MCP';
  document.getElementById('mcp-prompt-text').value = prompt;
  document.getElementById('mcp-modal').classList.add('open');
}

function copyMcpPrompt() {
  const ta = document.getElementById('mcp-prompt-text');
  ta.select();
  navigator.clipboard.writeText(ta.value).then(() => {
    const btn = document.getElementById('mcp-copy-btn');
    btn.textContent = '✓ Copied!';
    setTimeout(() => { btn.textContent = '📋 Copy prompt'; }, 2000);
  });
}

// ── AI Analysis with toast ────────────────────────────────────────────────────
let toastTimer_=null;
function showToast(icon, title, body, pct) {
  const t=document.getElementById('toast');
  document.getElementById('toast-icon').textContent=icon;
  document.getElementById('toast-title').textContent=title;
  document.getElementById('toast-body').textContent=body;
  document.getElementById('toast-fill').style.width=(pct||0)+'%';
  t.classList.add('visible');
  if (toastTimer_) clearTimeout(toastTimer_);
}
function hideToast(delay=3000) {
  toastTimer_=setTimeout(()=>document.getElementById('toast').classList.remove('visible'), delay);
}

function startAnalysis() {
  if (!selJobId) return;
  if (!anthropicReady_) { showMcpPromptModal('analyze'); return; }
  const btn=document.getElementById('btn-analyze');
  btn.disabled=true; btn.textContent='⟳ Analyzing…';
  const pbar=document.getElementById('analyze-pbar');
  const fill=document.getElementById('analyze-bar-fill');
  const statusTxt=document.getElementById('analyze-status');
  pbar.classList.add('active'); fill.style.width='0%';
  statusTxt.style.display='block'; statusTxt.textContent='Starting…';
  showToast('🤖','Analyzing logs…','Sending to Claude Haiku', 0);

  const _bjId = bgJobStart('🤖', 'Analyze with AI');
  const es=new EventSource(`/api/jobs/${selJobId}/analyze/stream?limit=30`);
  es.onmessage=e=>{
    const d=JSON.parse(e.data);
    if (d.type==='progress') {
      const pct=d.total?Math.round(d.done/d.total*100):0;
      fill.style.width=pct+'%';
      statusTxt.textContent=`${d.done}/${d.total}`;
      showToast('🤖','Analyzing…',`${d.done} of ${d.total} logs processed`, pct);
      bgJobProgress(_bjId, pct, `${d.done} / ${d.total} logs`);
    } else if (d.type==='done') {
      es.close();
      btn.disabled=false; btn.textContent='🤖 Analyze with AI';
      fill.style.width='100%';
      pbar.classList.remove('active');
      const msg=d.analyzed===0
        ?'All logs already analyzed'
        :`${d.analyzed} analyzed · ${d.relevant} checkout-related`;
      statusTxt.textContent=msg;
      showToast('✓','Analysis complete', msg, 100);
      hideToast(4000);
      bgJobDone(_bjId, msg);
      loadData(selJobId, dataPage_);
    }
  };
  es.onerror=()=>{
    es.close();
    btn.disabled=false; btn.textContent='🤖 Analyze with AI';
    statusTxt.textContent='Error — check console';
    pbar.classList.remove('active');
    showToast('✗','Analysis error','Check the server logs', 0);
    hideToast(5000);
    bgJobError(_bjId, 'Server error — check logs');
  };
}

// ── Background Jobs tracker ───────────────────────────────────────────────────
let bgJobs_ = [];        // [{id, icon, name, pct, msg, status, ts}]
let bgJobSeq_ = 0;
let bgPanelOpen_ = false;

function bgJobStart(icon, name) {
  const id = ++bgJobSeq_;
  bgJobs_.push({id, icon, name, pct: 0, msg: 'Starting…', status: 'running', ts: Date.now()});
  _renderBgPanel();
  return id;
}

function bgJobProgress(id, pct, msg) {
  const j = bgJobs_.find(x => x.id === id);
  if (j) { j.pct = pct; j.msg = msg; _renderBgPanel(); }
}

function bgJobDone(id, msg) {
  const j = bgJobs_.find(x => x.id === id);
  if (j) { j.pct = 100; j.msg = msg; j.status = 'done'; _renderBgPanel(); }
  // Auto-clear after 30s
  setTimeout(() => { bgJobs_ = bgJobs_.filter(x => x.id !== id); _renderBgPanel(); }, 30000);
}

function bgJobError(id, msg) {
  const j = bgJobs_.find(x => x.id === id);
  if (j) { j.msg = msg; j.status = 'error'; _renderBgPanel(); }
}

function clearFinishedJobs() {
  bgJobs_ = bgJobs_.filter(j => j.status === 'running');
  _renderBgPanel();
}

function toggleBgPanel() {
  bgPanelOpen_ = !bgPanelOpen_;
  document.getElementById('bgjobs-panel').style.display = bgPanelOpen_ ? 'block' : 'none';
}

// Close panel when clicking outside
document.addEventListener('click', e => {
  if (bgPanelOpen_ && !e.target.closest('#bgjobs-btn') && !e.target.closest('#bgjobs-panel')) {
    bgPanelOpen_ = false;
    document.getElementById('bgjobs-panel').style.display = 'none';
  }
});

function _renderBgPanel() {
  const running = bgJobs_.filter(j => j.status === 'running');
  const btn     = document.getElementById('bgjobs-btn');
  const icon    = document.getElementById('bgjobs-icon');
  const label   = document.getElementById('bgjobs-label');
  const count   = document.getElementById('bgjobs-count');
  const list    = document.getElementById('bgjobs-list');

  // Update button state
  btn.classList.toggle('has-running', running.length > 0);
  btn.classList.toggle('has-error',   !running.length && bgJobs_.some(j => j.status === 'error'));

  if (running.length > 0) {
    icon.textContent = '⚙';
    icon.className = 'bj-spinner';
    label.textContent = running.length === 1 ? running[0].name.slice(0,18) : `${running.length} running`;
    count.style.display = 'inline';
    count.textContent = running.length;
  } else {
    icon.textContent = '⚙';
    icon.className = '';
    label.textContent = 'Jobs';
    count.style.display = bgJobs_.length ? 'inline' : 'none';
    count.textContent = bgJobs_.length;
    if (count.style.display !== 'none') count.style.background = bgJobs_.some(j=>j.status==='error') ? 'var(--red)' : 'var(--bg3)';
  }

  // Render list
  if (!bgJobs_.length) {
    list.innerHTML = '<div class="bgjobs-empty">No background jobs yet</div>';
    return;
  }
  list.innerHTML = [...bgJobs_].reverse().map(j => `
    <div class="bgjob-item">
      <div class="bgjob-row1">
        <span class="bgjob-icon">${j.icon}</span>
        <span class="bgjob-name">${esc(j.name)}</span>
        <span class="bgjob-status ${j.status}">${j.status}</span>
      </div>
      <div class="bgjob-msg">${esc(j.msg)}</div>
      <div class="bgjob-bar"><div class="bgjob-bar-fill ${j.status}" style="width:${j.pct}%;"></div></div>
    </div>
  `).join('');
}

// ── Auto-Classify ─────────────────────────────────────────────────────────────
function startAutoClassify() {
  if (!selJobId) return;
  if (!anthropicReady_) { showMcpPromptModal('classify'); return; }
  const btn  = document.getElementById('btn-autoclassify');
  const pbar = document.getElementById('autoclassify-pbar');
  const fill = document.getElementById('autoclassify-bar-fill');
  btn.disabled = true; btn.textContent = '⟳ Classifying…';
  pbar.classList.add('active'); fill.style.width = '0%';
  showToast('⚡', 'Auto-Classify', 'Building pattern lookup…', 0);

  const _bjId2 = bgJobStart('⚡', 'Auto-Classify');
  const es = new EventSource(`/api/jobs/${selJobId}/auto-classify/stream`);
  es.onmessage = e => {
    const d = JSON.parse(e.data);
    if (d.type === 'lookup_built') {
      showToast('⚡', 'Auto-Classify', `${d.patterns} patterns learned`, 5);
      bgJobProgress(_bjId2, 5, `${d.patterns} patterns learned`);
    } else if (d.type === 'progress') {
      const pct = d.total ? Math.round(d.done / d.total * 100) : 0;
      fill.style.width = pct + '%';
      showToast('⚡', 'Auto-Classify', `${d.done} / ${d.total} checked`, pct);
      bgJobProgress(_bjId2, pct, `${d.done} / ${d.total} records`);
    } else if (d.type === 'done') {
      es.close();
      btn.disabled = false; btn.textContent = '⚡ Auto-Classify';
      fill.style.width = '100%';
      pbar.classList.remove('active');
      const msg = d.classified === 0
        ? (d.message || 'Nothing to classify')
        : `Classified ${d.classified} · Skipped ${d.skipped} (no match)`;
      showToast('✓', 'Auto-Classify done', msg, 100);
      hideToast(4000);
      bgJobDone(_bjId2, msg);
      loadData(selJobId, dataPage_);
    }
  };
  es.onerror = () => {
    es.close();
    btn.disabled = false; btn.textContent = '⚡ Auto-Classify';
    pbar.classList.remove('active');
    showToast('✗', 'Auto-Classify error', 'Check server logs', 0);
    hideToast(5000);
    bgJobError(_bjId2, 'Server error');
  };
}

// ── Settings ──────────────────────────────────────────────────────────────────
async function openSettings() {
  const s = await api.get('/api/settings');   // single call — reuse for all fields
  document.getElementById('ak-status').textContent = s.anthropic_key_set ? '✓ set' : '';
  document.getElementById('gh-status').textContent = s.github_token_set ? '✓ set' : '';
  document.getElementById('btn-settings').style.borderColor = !s.anthropic_key_set ? 'var(--orange)' : '';
  document.getElementById('system-prompt-input').value = s.system_prompt || '';
  document.getElementById('gh-modal').classList.add('open');
}

async function saveApiKeys() {
  const ak = document.getElementById('anthropic-key-input').value.trim();
  const gh = document.getElementById('gh-token-input').value.trim();
  const sp = document.getElementById('system-prompt-input').value.trim();
  const payload = { anthropic_key: ak, github_token: gh };
  if (sp) payload.system_prompt = sp;
  await api.post('/api/settings/keys', payload);
  document.getElementById('gh-modal').classList.remove('open');
  document.getElementById('anthropic-key-input').value = '';
  document.getElementById('gh-token-input').value = '';
  document.getElementById('btn-settings').style.borderColor = '';
  // Refresh anthropicReady_ so buttons respond immediately after key entry
  api.get('/api/settings').then(s => { anthropicReady_ = !!s.anthropic_key_set; });
  showToast('✓', 'Saved', 'Settings saved', 100);
  hideToast(2500);
}

// ── Config form ───────────────────────────────────────────────────────────────
function renderConfigForm(job) {
  document.getElementById('config-form').innerHTML=`
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
            ${chunkOptions(job.chunk_hours||0)}
          </select>
        </div>
        <div class="form-field">
          <label class="form-label">Enabled</label>
          <select class="form-input" id="cf-enabled">
            <option value="true" ${job.enabled?'selected':''}>Yes — run daily</option>
            <option value="false" ${!job.enabled?'selected':''}>No — manual only</option>
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
  const keysHtml = (coll.corr_keys||[]).map(k=>
    `<span class="key-chip" style="font-size:11px;">${esc(k)}</span>`
  ).join('');
  const srcHtml = (coll.sources||[]).map((s,i)=>
    `<div class="source-row" style="margin-bottom:4px;">
       <span class="form-input src-label" style="background:transparent;border-color:transparent;pointer-events:none;">${esc(s.label)}</span>
       <span class="form-input src-url"   style="background:transparent;border-color:transparent;pointer-events:none;font-size:10px;overflow:hidden;text-overflow:ellipsis;">${esc(s.kibana_url)}</span>
     </div>`
  ).join('');
  document.getElementById('config-form').innerHTML=`
    <div class="config-section">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
        <span style="font-size:13px;font-weight:600;">${esc(coll.name)}</span>
        <button class="btn btn-ghost btn-sm" onclick="openEditJob()">✏ Edit</button>
      </div>
      <div style="font-size:11px;color:var(--tx2);margin-bottom:8px;">${esc(coll.time_range)} · ${coll.chunk_hours>0?coll.chunk_hours+'h chunks':'single pass'} · Daily @${coll.schedule_hour}:00 UTC</div>
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
    chunk_hours:   parseInt(document.getElementById('cf-chunk').value)||0,
    enabled:       document.getElementById('cf-enabled').value==='true',
  });
  await loadJobs();
  renderConfigForm(jobs.find(j=>j.id===id));
}

async function deleteJob(id) {
  const job = jobs.find(j => j.id === id);
  const name = job ? job.name : `Job ${id}`;
  // Count records first
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
  // Force-refresh the data pane so it shows 0 records immediately
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
  const srcCount = coll ? (coll.sources||[]).length : '?';

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
  // Force-refresh data pane if we're currently viewing one of the cleared sources
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

// ── Add / Edit modal ──────────────────────────────────────────────────────────
// ── Collection modal state ────────────────────────────────────────────────────
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
  // If value provided add chip directly; otherwise show inline input
  if (value) { _addKeyChip(value); return; }
  // Check if input already open
  if (document.getElementById('key-input-tmp')) return;
  const wrap = document.createElement('div');
  wrap.className = 'key-input-row';
  wrap.id = 'key-input-tmp';
  wrap.innerHTML = `<input class="form-input" id="key-input-val" placeholder="field name, e.g. request_id" style="width:200px;" />
    <button class="btn btn-ghost btn-sm" onclick="_commitKey()">Add</button>
    <button class="btn btn-ghost btn-sm" onclick="document.getElementById('key-input-tmp').remove()">✕</button>`;
  _modalKeys().appendChild(wrap);
  setTimeout(()=>document.getElementById('key-input-val')?.focus(), 50);
  document.getElementById('key-input-val').onkeydown = e => { if(e.key==='Enter'){e.preventDefault();_commitKey();} };
}

function _commitKey() {
  const v = (document.getElementById('key-input-val')?.value||'').trim();
  document.getElementById('key-input-tmp')?.remove();
  if (v) _addKeyChip(v);
}

function _addKeyChip(value) {
  // Avoid duplicates
  const existing = [..._modalKeys().querySelectorAll('.key-chip')].map(c=>c.dataset.v);
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
  return [..._modalKeys().querySelectorAll('.key-chip')].map(c=>c.dataset.v);
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
  addSourceRow();   // start with one empty source row
  document.getElementById('modal').classList.add('open');
}

function openEditJob() {
  // Edit the currently selected collection
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
    chunk_hours:   parseInt(document.getElementById('f-chunk').value)||0,
    schedule_hour: Math.min(Math.max(parseInt(document.getElementById('f-hour').value)||2,0),23),
    enabled:       document.getElementById('f-enabled').value==='true',
    sources,
    corr_keys: _getModalKeys(),
  };
  try {
    let newId = editCollId;
    if (editCollId) {
      await api.put(`/api/collections/${editCollId}`, payload);
    } else {
      const r = await api.post('/api/collections', payload);
      if (r.detail) { alert('Error: '+JSON.stringify(r.detail)); return; }
      newId = r.id;
    }
    closeModal();
    await loadCollections();
    if (newId) selectCollection(newId);
  } catch(e) { alert('Save failed: '+e); }
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function esc(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}
function timeAgo(dt){const d=Math.floor((Date.now()-new Date(dt+'Z'))/1000);if(d<60)return`${d}s ago`;if(d<3600)return`${Math.floor(d/60)}m ago`;if(d<86400)return`${Math.floor(d/3600)}h ago`;return`${Math.floor(d/86400)}d ago`;}
function fmtDate(dt){return new Date(dt+'Z').toLocaleString();}
function elapsed(a,b){const s=Math.round((new Date(b+'Z')-new Date(a+'Z'))/1000);if(s<60)return`${s}s`;return`${Math.floor(s/60)}m ${s%60}s`;}
function timeOptions(sel){return['Last 15 minutes','Last 1 hour','Last 24 hours','Last 7 days','Last 30 days','Last 90 days','Last 1 year'].map(o=>`<option ${o===sel?'selected':''}>${o}</option>`).join('');}

function chunkOptions(sel){
  const opts=[['0','Disabled (single pass)'],['1','1h per chunk'],['2','2h per chunk'],['4','4h per chunk'],['6','6h per chunk'],['12','12h per chunk'],['24','24h per chunk'],['48','48h per chunk']];
  return opts.map(([v,l])=>`<option value="${v}" ${String(sel)===v?'selected':''}>${l}</option>`).join('');
}

async function updateChunkPreview(jobId) {
  const ch = parseInt(document.getElementById('cf-chunk')?.value)||0;
  const el = document.getElementById('chunk-preview');
  if (!el) return;
  if (ch === 0) { el.textContent=''; return; }
  try {
    // Use current UI time_range for preview
    const tr = document.getElementById('cf-time')?.value || 'Last 7 days';
    const d = await api.get(`/api/jobs/${jobId}/chunks/preview`);
    // Re-calculate locally for instant feedback
    const trHours = {'Last 15 minutes':0.25,'Last 1 hour':1,'Last 24 hours':24,'Last 7 days':168,'Last 30 days':720,'Last 90 days':2160,'Last 1 year':8760};
    const total = trHours[tr] || 168;
    const n = Math.ceil(total / ch);
    el.innerHTML = `⏱ ${n} chunks × ${ch}h — full scan will run <b>${n} browser sessions</b> sequentially`;
  } catch(e) { el.textContent=''; }
}

const NAME_HINTS=[['syslog','Syslog'],['nginx','Nginx'],['checkout','Checkout'],['payment','Payment'],['cart','Cart'],['order','Orders'],['error','Errors'],['auth','Auth'],['api','API'],['coupon','Coupon']];
function suggestName(url){if(!url)return'';const low=url.toLowerCase();for(const[kw,name]of NAME_HINTS)if(low.includes(kw))return name;try{return new URL(url).hostname.split('.')[0];}catch(e){}return'';}
function autoFillName(url){const el=document.getElementById('f-name');if(!el.value.trim())el.value=suggestName(url);}

// ── Refresh ───────────────────────────────────────────────────────────────────
function refreshData() {
  if (selJobId) loadData(selJobId, dataPage_);
}

// ── Field search ──────────────────────────────────────────────────────────────
function filterFields(val) {
  fieldSearch_ = val.toLowerCase();
  renderFieldPanel();
}

// ── Sort ──────────────────────────────────────────────────────────────────────
function sortBy(field) {
  // Linked fields (cross-job) can't be sorted server-side — ignore
  if (field.includes(LINKED_SEP)) return;
  if (sortField_ === field) {
    sortDir_ = sortDir_ === 'asc' ? 'desc' : 'asc';
  } else {
    sortField_ = field;
    sortDir_ = 'desc';
  }
  dataPage_ = 1;
  loadData(selJobId, 1);
}

// ── Regex toggle ──────────────────────────────────────────────────────────────
function toggleRegex() {
  regexMode_ = !regexMode_;
  const btn = document.getElementById('regex-toggle');
  if (btn) btn.classList.toggle('active', regexMode_);
  const inp = document.getElementById('data-search');
  if (inp) inp.placeholder = regexMode_ ? 'Regex pattern (e.g. Error.*timeout)…' : 'Search message / URI / store_id…';
}

// ── Cross-source search toggle ─────────────────────────────────────────────────
function toggleCrossSource() {
  crossSourceMode_ = !crossSourceMode_;
  const btn = document.getElementById('cross-source-toggle');
  if (btn) btn.classList.toggle('active', crossSourceMode_);
  if (crossSourceMode_) showToast('🔗', 'Cross-source ON', 'Search matches correlated records from all sources', 1);
  else showToast('🔍', 'Single-source mode', 'Search restricted to this source only', 1);
  dataPage_ = 1;
  loadData(selJobId, 1);
}

/**
 * Show/hide the cross-source toggle based on whether the current job
 * belongs to a collection that has correlation keys defined.
 * When the job is part of a correlated collection, cross-source is AUTO-ENABLED
 * so search keywords from any source are found immediately without manual toggling.
 */
function _updateCrossSourceBtn() {
  const btn = document.getElementById('cross-source-toggle');
  if (!btn) return;
  let hasCorrKeys = false;
  if (selCollId) {
    const coll = collections_.find(c => c.id === selCollId);
    hasCorrKeys = !!(coll && coll.corr_keys && coll.corr_keys.length && coll.sources && coll.sources.length > 1);
  }
  btn.classList.toggle('hidden', !hasCorrKeys);
  if (hasCorrKeys) {
    // Auto-enable cross-source when entering a correlated collection view
    crossSourceMode_ = true;
    btn.classList.add('active');
  } else {
    // Disable when leaving collection context
    crossSourceMode_ = false;
    btn.classList.remove('active');
  }
}

// ── Per-page selector ─────────────────────────────────────────────────────────
function setPerPage(n) {
  perPage_ = n; dataPage_ = 1;
  loadData(selJobId, 1);
}

// ── Bulk row selection ────────────────────────────────────────────────────────
function toggleSelectRow(id, cb) {
  if (cb.checked) selectedRows_.add(id); else selectedRows_.delete(id);
  updateBulkBar();
  // Sync select-all checkbox state
  const allCb = document.getElementById('select-all-cb');
  if (allCb && lastDataRows_) {
    allCb.checked = lastDataRows_.length > 0 && lastDataRows_.every(r => selectedRows_.has(r.id));
    allCb.indeterminate = !allCb.checked && selectedRows_.size > 0;
  }
}

function selectAll(checked) {
  if (!lastDataRows_) return;
  lastDataRows_.forEach(r => { if (checked) selectedRows_.add(r.id); else selectedRows_.delete(r.id); });
  // Refresh checkboxes in DOM
  document.querySelectorAll('input.row-checkbox:not(#select-all-cb)').forEach(cb => { cb.checked = checked; });
  const allCb = document.getElementById('select-all-cb');
  if (allCb) { allCb.checked = checked; allCb.indeterminate = false; }
  updateBulkBar();
}

function updateBulkBar() {
  const bar = document.getElementById('bulk-bar');
  const num = document.getElementById('bulk-count-num');
  if (!bar) return;
  if (selectedRows_.size > 0) {
    bar.classList.add('visible');
    if (num) num.textContent = selectedRows_.size;
  } else {
    bar.classList.remove('visible');
  }
}

function clearSelection() {
  selectedRows_.clear();
  document.querySelectorAll('input.row-checkbox').forEach(cb => { cb.checked = false; cb.indeterminate = false; });
  updateBulkBar();
}

async function bulkMarkSelected(relevance) {
  if (!selectedRows_.size) return;
  const ids = [...selectedRows_];
  await api.post('/api/logs/mark-bulk', { ids, relevance, severity: 'info' });
  showToast('✓', `Marked ${ids.length} records as ${relevance}`, '', 100);
  hideToast(3000);
  clearSelection();
  loadData(selJobId, dataPage_);
}

// ── Mark row ──────────────────────────────────────────────────────────────────
async function markRow(rowId, relevance, btn) {
  btn.disabled = true;
  await api.post(`/api/logs/${rowId}/mark`, { relevance, severity: 'info' });
  // Update local cache
  const row = (lastDataRows_ || []).find(r => r.id === rowId);
  if (row) row.relevance = relevance;

  // Find similar records and offer bulk action (both directions)
  const row2 = (lastDataRows_ || []).find(r => r.id === rowId);
  let message = '';
  if (row2) {
    let rj = {};
    try { rj = typeof row2.raw_json === 'string' ? JSON.parse(row2.raw_json) : (row2.raw_json || {}); } catch(e) {}
    const m = rj.message;
    message = Array.isArray(m) ? (m[0] || '') : (m || '');
  }
  if (message) {
    const similar = await api.get(`/api/logs/similar?job_id=${selJobId}&message=${encodeURIComponent(message)}&exclude_id=${rowId}`);
    const opposite = relevance === 'irrelevant' ? 'irrelevant' : 'relevant';
    // For irrelevant: find records not yet irrelevant; for relevant: find records not yet relevant
    const targets = (similar || []).filter(r => r.relevance !== relevance);
    if (targets.length > 0) {
      showSimilarPrompt(targets, message, relevance);
    }
  }

  btn.disabled = false;
  refreshData();
}

// ── Similar prompt ────────────────────────────────────────────────────────────
let _similarIds_ = [];
let _similarRelevance_ = 'irrelevant';

function showSimilarPrompt(rows, message, relevance) {
  _similarIds_ = rows.map(r => r.id);
  _similarRelevance_ = relevance || 'irrelevant';
  document.getElementById('similar-count').textContent = rows.length;
  document.getElementById('similar-msg').textContent = message.length > 80 ? message.slice(0,80)+'…' : message;
  // Update button label to match direction
  const btn = document.querySelector('#similar-modal .btn-red');
  if (btn) {
    btn.textContent = relevance === 'relevant' ? '✓ Mark All Relevant' : '✗ Mark All Irrelevant';
    btn.style.background = relevance === 'relevant' ? 'rgba(63,185,80,.2)' : 'rgba(248,81,73,.15)';
    btn.style.color = relevance === 'relevant' ? 'var(--green)' : 'var(--red)';
    btn.style.borderColor = relevance === 'relevant' ? 'rgba(63,185,80,.4)' : 'rgba(248,81,73,.4)';
  }
  document.getElementById('similar-modal').classList.add('open');
}

async function bulkMarkSimilar() {
  document.getElementById('similar-modal').classList.remove('open');
  if (!_similarIds_.length) return;
  const rel = _similarRelevance_;
  await api.post('/api/logs/mark-bulk', { ids: _similarIds_, relevance: rel, severity: 'info' });
  const label = rel === 'relevant' ? 'relevant ✓' : 'irrelevant ✗';
  showToast('✓', `Marked ${_similarIds_.length} similar records as ${label}`, '', 100);
  hideToast(3000);
  refreshData();
  _similarIds_ = [];
}

// ── Summary ───────────────────────────────────────────────────────────────────
async function showSummary() {
  if (!selJobId || !lastDataRows_ || !lastDataRows_.length) return;
  const btn = document.getElementById('btn-summary');
  btn.disabled = true; btn.textContent = '⟳ Summarizing…';
  const ids = lastDataRows_.map(r => r.id);
  try {
    const res = await api.post(`/api/jobs/${selJobId}/summarize`, { ids });
    document.getElementById('summary-text').textContent = res.summary || 'No summary available.';
    document.getElementById('summary-count').textContent = res.count || ids.length;
    document.getElementById('summary-modal').classList.add('open');
  } catch(e) {
    showToast('✗', 'Summary failed', String(e), 0);
    hideToast(4000);
  }
  btn.disabled = false; btn.textContent = '📋 Summary';
}

// ── Kibana-style document view ────────────────────────────────────────────────
function syntaxHighlightJson(json) {
  return json
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, m => {
      if (/^"/.test(m)) {
        if (/:$/.test(m)) return `<span class="json-key">${m}</span>`;
        return `<span class="json-str">${m}</span>`;
      }
      if (/true|false/.test(m)) return `<span class="json-bool">${m}</span>`;
      if (/null/.test(m))       return `<span class="json-null">${m}</span>`;
      return `<span class="json-num">${m}</span>`;
    });
}

function renderKfValue(raw) {
  const trimmed = raw.trim();
  // Try to parse as JSON (object or array)
  if ((trimmed.startsWith('{') || trimmed.startsWith('[')) ) {
    try {
      const parsed = JSON.parse(trimmed);
      const pretty = JSON.stringify(parsed, null, 2);
      return `<div class="kf-json">${syntaxHighlightJson(pretty)}</div>`;
    } catch(e) { /* fall through */ }
  }
  return `<span style="white-space:pre-wrap;word-break:break-all;">${esc(raw)}</span>`;
}

function buildKibanaDocView(row, rj) {
  const allFields = {};
  // DB meta fields first
  allFields['@timestamp'] = row.timestamp || '';
  allFields['_relevance'] = row.relevance || '';
  allFields['_severity'] = row.severity || '';
  if (row.ai_summary) allFields['_ai_summary'] = row.ai_summary;
  // Raw JSON fields
  Object.entries(rj).forEach(([k, v]) => {
    if (k !== '@timestamp') {
      let val = Array.isArray(v) ? (v[0] ?? '') : v;
      if (val !== null && typeof val === 'object') val = JSON.stringify(val);
      allFields[k] = String(val ?? '');
    }
  });

  const rows = Object.entries(allFields).map(([field, value]) => {
    const isMeta = field.startsWith('_');
    const safeV = esc(String(value));
    const safeF = esc(field);
    const valueCell = renderKfValue(String(value));
    const filterBtns = isMeta ? '' : `
        <button class="kf-btn kf-plus"  title="Filter: ${safeF} = ${safeV.slice(0,40)}" data-f="${safeF}" data-v="${safeV}" onclick="filterInField(this.dataset.f, this.dataset.v)">+</button>
        <button class="kf-btn kf-minus" title="Exclude: ${safeF} = ${safeV.slice(0,40)}" data-f="${safeF}" data-v="${safeV}" onclick="filterOutField(this.dataset.f, this.dataset.v)">−</button>`;
    return `<tr class="kibana-field-row">
      <td class="kf-actions">${filterBtns}</td>
      <td class="kf-name ${isMeta ? 'kf-meta' : ''}">${esc(field)}</td>
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
  </div>`;
}

// ── Field-specific filter chips ──────────────────────────────────────────────
// Each entry: {field, value, op}  where op = 'eq' (include) | 'neq' (exclude)
let fieldFilters_ = [];

function filterInField(field, value) {
  // Remove any existing same-field filter, then add eq
  fieldFilters_ = fieldFilters_.filter(f => !(f.field === field && f.op === 'eq'));
  fieldFilters_.push({field, value, op: 'eq'});
  renderExcludeChips();
  dataPage_ = 1;
  loadData(selJobId, 1);
}

function filterOutField(field, value) {
  // Remove any existing same-field+value neq, then add
  fieldFilters_ = fieldFilters_.filter(f => !(f.field === field && f.value === value && f.op === 'neq'));
  fieldFilters_.push({field, value, op: 'neq'});
  renderExcludeChips();
  dataPage_ = 1;
  loadData(selJobId, 1);
}

// Keep old names as aliases so nothing else breaks
// filterInValue / filterOutValue removed — use filterInField / filterOutField directly
let excludeFilters_ = new Set(); // kept for compat, no longer used

function rowMatchesExclude() { return false; } // server-side now

function renderExcludeChips() {
  const el = document.getElementById('exclude-chips');
  if (!el) return;
  if (!fieldFilters_.length) { el.style.display = 'none'; return; }
  el.style.display = 'flex';
  el.innerHTML = fieldFilters_.map((f, i) => {
    const prefix = f.op === 'eq' ? '+' : '−';
    const label  = `${esc(f.field)}:${esc(String(f.value).length > 30 ? String(f.value).slice(0,30)+'…' : String(f.value))}`;
    return `<span class="exclude-chip op-${f.op}">${prefix}${label}
      <button onclick="removeFieldFilter(${i})" title="Remove filter">×</button>
    </span>`;
  }).join('');
}

function removeFieldFilter(idx) {
  fieldFilters_.splice(idx, 1);
  renderExcludeChips();
  dataPage_ = 1;
  loadData(selJobId, 1);
}

// ── Modal close on backdrop ───────────────────────────────────────────────────
['modal','gh-modal','coderef-modal','similar-modal','summary-modal','group-modal',
 'saved-search-modal','export-modal','clusters-modal','shortcuts-modal'].forEach(id=>{
  const el = document.getElementById(id);
  if (!el) return;
  el.addEventListener('click', e=>{
    if(e.target.id===id) {
      if (id==='modal') closeModal();
      else {
        document.getElementById(id).classList.remove('open');
        if (id === 'similar-modal') _similarIds_ = []; // always clear on any close
      }
    }
  });
});

init();

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

      <!-- Members -->
      <div style="margin-bottom:10px;">
        <div style="font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px;">Jobs in group</div>
        <div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:8px;" id="members-${g.id}">
          ${(g.members||[]).map(m => `
            <span class="group-member-chip">
              <span style="color:var(--blue);font-size:10px;">${esc(m.label||m.job_name)}</span>
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

      <!-- Correlation keys -->
      <div>
        <div style="font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px;">Correlation Keys</div>
        <div id="keys-${g.id}" style="display:flex;flex-direction:column;gap:4px;margin-bottom:8px;">
          ${(g.keys||[]).length === 0
            ? '<div style="color:var(--tx2);font-size:11px;">No keys yet — click ⚡ Detect Keys</div>'
            : (g.keys||[]).map(k => `
              <div class="key-row">
                <span class="key-src key-src-${k.source||'auto'}">${k.source||'auto'}</span>
                <span style="color:var(--orange);">${esc(k.job_name_a||'?')}</span>
                <span style="color:var(--blue);font-weight:600;">${esc(k.field_a)}</span>
                <span style="color:var(--tx2);">↔</span>
                <span style="color:var(--orange);">${esc(k.job_name_b||'?')}</span>
                <span style="color:var(--blue);font-weight:600;">${esc(k.field_b)}</span>
                <span class="key-conf">${Math.round((k.confidence||0)*100)}%</span>
                <div style="flex:1;"></div>
                <button class="btn btn-sm btn-ghost" style="padding:1px 6px;font-size:10px;" onclick="deleteKey(${g.id},${k.id})">✕</button>
              </div>`).join('')
          }
        </div>
        <!-- Manual add -->
        <details style="font-size:11px;" open>
          <summary style="color:var(--tx2);cursor:pointer;margin-bottom:6px;">+ Add key manually</summary>
          <div style="display:flex;gap:4px;flex-wrap:wrap;align-items:center;">
            <select class="filter-input" id="mk-ja-${g.id}" style="width:150px;padding:3px;">
              <option value="">— Job A —</option>
              ${(g.members||[]).map(m=>`<option value="${m.job_id}">${esc(m.label||m.job_name)} (${m.job_id})</option>`).join('')}
            </select>
            <input class="form-input" id="mk-fa-${g.id}" placeholder="field name" style="width:120px;" />
            <span style="color:var(--tx2);font-size:14px;">↔</span>
            <select class="filter-input" id="mk-jb-${g.id}" style="width:150px;padding:3px;">
              <option value="">— Job B —</option>
              ${(g.members||[]).map(m=>`<option value="${m.job_id}">${esc(m.label||m.job_name)} (${m.job_id})</option>`).join('')}
            </select>
            <input class="form-input" id="mk-fb-${g.id}" placeholder="field name" style="width:120px;" />
            <button class="btn btn-green btn-sm" onclick="addKey(${g.id})">Add Key</button>
          </div>
          <div id="mk-err-${g.id}" style="color:var(--red);font-size:11px;margin-top:4px;display:none;"></div>
        </details>
      </div>

      <!-- Detect progress -->
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
  await api.post('/api/groups', {name});
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
  await api.post(`/api/groups/${gid}/members`, {job_id: jid, label});
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
  await api.post(`/api/groups/${gid}/keys`, {job_id_a:jid_a, field_a, job_id_b:jid_b, field_b});
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

// ── Correlated records in expanded view ───────────────────────────────────────
async function loadCorrelated(logId, containerId) {
  const el = document.getElementById(containerId);
  if (!el) return;
  try {
    const results = await api.get(`/api/logs/${logId}/correlated`);
    if (!results.length) {
      el.innerHTML = '<div style="color:var(--tx2);font-size:11px;padding:8px 12px;">No correlated records found</div>';
      return;
    }
    el.innerHTML = results.map((r, i) => {
      let rj = {};
      try { rj = typeof r.raw_json === 'string' ? JSON.parse(r.raw_json) : (r.raw_json||{}); } catch(e){}
      const msg = rj.message || '';
      const m = Array.isArray(msg) ? (msg[0]||'') : String(msg||'');
      const uid = `corr-${logId}-${i}`;

      // Build full field rows from raw_json (flatten, skip empties)
      const fieldRows = Object.entries(rj).map(([k, v]) => {
        let disp = Array.isArray(v) ? (v[0]??'') : v;
        if (disp !== null && typeof disp === 'object') disp = renderKfValue(JSON.stringify(disp));
        else disp = renderKfValue(String(disp ?? ''));
        return `<tr><td>${esc(k)}</td><td>${disp}</td></tr>`;
      }).join('');

      // Also add DB meta fields
      const metaRows = [
        r.timestamp ? `<tr><td>@timestamp</td><td>${esc(String(r.timestamp))}</td></tr>` : '',
        r.relevance  ? `<tr><td>_relevance</td><td>${esc(r.relevance)}</td></tr>` : '',
        r.severity   ? `<tr><td>_severity</td><td>${esc(r.severity)}</td></tr>` : '',
        r.ai_summary ? `<tr><td>_ai_summary</td><td>${esc(r.ai_summary)}</td></tr>` : '',
      ].join('');

      return `<div class="corr-record">
        <div class="corr-record-hdr" onclick="toggleCorrRecord('${uid}')">
          <span class="corr-expand-arrow" id="arrow-${uid}">▶</span>
          <span class="corr-source-badge">${esc(r._source_label)}</span>
          ${(r._matched_keys||[]).map(k=>`<span class="corr-key-badge">${esc(k)}</span>`).join(' ')}
          ${r.severity ? `<span class="sev sev-${r.severity}">${r.severity}</span>` : ''}
          <span class="corr-message">${esc(m.slice(0, 150))}</span>
          <span class="corr-meta">${r.timestamp||''}</span>
        </div>
        <div class="corr-fields" id="fields-${uid}">
          <table><tbody>${metaRows}${fieldRows}</tbody></table>
        </div>
      </div>`;
    }).join('');
  } catch(e) {
    el.innerHTML = `<div style="color:var(--red);font-size:11px;padding:8px 12px;">Error: ${esc(String(e))}</div>`;
  }
}

function toggleCorrRecord(uid) {
  const fields = document.getElementById(`fields-${uid}`);
  const arrow  = document.getElementById(`arrow-${uid}`);
  const open   = fields.classList.toggle('open');
  arrow.classList.toggle('open', open);
  arrow.textContent = open ? '▼' : '▶';
}

function toggleCorrPanel(rowId) {
  const body = document.getElementById(`corr-body-${rowId}`);
  const toggle = document.getElementById(`corr-toggle-${rowId}`);
  const isOpen = body.classList.toggle('open');
  toggle.textContent = isOpen ? '▼ Loaded' : '▶ Load';
  if (isOpen) {
    const contentEl = document.getElementById(`corr-content-${rowId}`);
    contentEl.innerHTML = '<div style="color:var(--tx2);font-size:11px;padding:8px 12px;">Loading…</div>';
    loadCorrelated(rowId, `corr-content-${rowId}`);
  }
}

// ── F9: Session health check ──────────────────────────────────────────────────
async function checkHealth() {
  const el = document.getElementById('health-badge');
  if (!el) return;
  try {
    const h = await api.get('/api/session/health');
    el.style.display = 'inline-flex';
    if (h.status === 'ok') {
      el.className = 'health-ok';
      el.textContent = `● Session OK (${h.age_hours}h)`;
    } else if (h.status === 'stale') {
      el.className = 'health-warn';
      el.innerHTML = `⚠ ${esc(h.message || 'Session may be stale')}`;
    } else {
      el.className = 'health-err';
      el.innerHTML = `✗ ${esc(h.message || 'No session — run login')}`;
    }
  } catch(e) {
    el.style.display = 'inline-flex';
    el.className = 'health-err';
    el.textContent = '✗ Server unreachable';
  }
}

// ── U2: Dashboard ─────────────────────────────────────────────────────────────
async function loadDashboard() {
  try {
    const s = await api.get('/api/stats');
    const jCount = s.total_jobs || 0;
    const rCount = s.total_rows || 0;
    const analyzed = s.analyzed_rows || 0;
    const pct = rCount ? Math.round(analyzed / rCount * 100) : 0;

    const jEl = document.getElementById('dash-jobs');
    const rEl = document.getElementById('dash-rows');
    const aEl = document.getElementById('dash-analyzed');
    if (jEl) jEl.textContent = jCount.toLocaleString();
    if (rEl) rEl.textContent = rCount.toLocaleString();
    if (aEl) { aEl.textContent = pct + '%'; }
    const aSubEl = document.getElementById('dash-analyzed-sub');
    if (aSubEl) aSubEl.textContent = `${analyzed.toLocaleString()} of ${rCount.toLocaleString()} rows`;
    const jSubEl = document.getElementById('dash-jobs-sub');
    if (jSubEl) {
      const cols = (collections_ || []).length;
      jSubEl.textContent = cols ? `${cols} collection${cols!==1?'s':''}` : 'No collections yet';
    }

    // Severity bars
    const sevBarsEl = document.getElementById('dash-sev-bars');
    const sevStacked = document.getElementById('dash-sev-stacked');
    if (sevBarsEl && s.severity_counts) {
      const sc = s.severity_counts;
      const total = Object.values(sc).reduce((a, b) => a + b, 0) || 1;
      const sevs = [
        {k:'critical', color:'var(--red)',    label:'Critical'},
        {k:'high',     color:'var(--orange)', label:'High'},
        {k:'medium',   color:'var(--yellow)', label:'Medium'},
        {k:'low',      color:'var(--blue)',   label:'Low'},
        {k:'info',     color:'var(--tx2)',    label:'Info'},
      ];
      sevBarsEl.innerHTML = sevs.filter(s => sc[s.k]).map(({k, color, label}) => {
        const cnt = sc[k] || 0;
        const pct = Math.round(cnt / total * 100);
        return `<div style="display:flex;align-items:center;gap:8px;font-size:11px;">
          <span style="width:52px;color:${color};font-weight:600;">${label}</span>
          <div style="flex:1;height:4px;background:var(--bg3);border-radius:2px;overflow:hidden;">
            <div style="width:${pct}%;height:100%;background:${color};border-radius:2px;transition:width .4s;"></div>
          </div>
          <span style="color:var(--tx2);width:48px;text-align:right;">${cnt.toLocaleString()}</span>
        </div>`;
      }).join('');
      if (sevStacked) sevStacked.innerHTML = sevs.filter(s => sc[s.k]).map(({k, color}) => {
        const pct = Math.round((sc[k]||0) / total * 100);
        return `<div style="flex:${pct};background:${color};min-width:2px;"></div>`;
      }).join('');
    }

    // Quick access — top 5 jobs
    const qaEl = document.getElementById('dash-quick-jobs');
    if (qaEl && jobs.length) {
      qaEl.innerHTML = jobs.slice(0, 5).map(j =>
        `<button class="dash-job-btn" onclick="selectJob(${j.id})">${esc(j.name)}</button>`
      ).join('');
    } else if (qaEl) {
      qaEl.innerHTML = '<span style="color:var(--tx2);font-size:12px;">No jobs yet</span>';
    }

    // Recent runs across all jobs
    await loadDashboardRecentRuns();
  } catch(e) { /* non-critical */ }
}

async function loadDashboardRecentRuns() {
  const el = document.getElementById('dash-recent-runs');
  if (!el || !jobs.length) return;
  try {
    // Fetch runs for first few jobs in parallel
    const jobsToCheck = jobs.slice(0, 6);
    const allRuns = (await Promise.all(
      jobsToCheck.map(j => api.get(`/api/jobs/${j.id}/runs`).then(runs =>
        runs.map(r => ({...r, _jobName: j.name, _jobId: j.id}))
      ).catch(() => []))
    )).flat();
    allRuns.sort((a, b) => new Date(b.started_at) - new Date(a.started_at));
    const recent = allRuns.slice(0, 8);
    if (!recent.length) {
      el.innerHTML = '<div style="color:var(--tx2);font-size:12px;">No runs yet. Select a job and click ▶ Run Now to start.</div>';
      return;
    }
    el.innerHTML = recent.map(r => {
      const dur = r.finished_at ? elapsed(r.started_at, r.finished_at) : 'running…';
      const statusCls = r.status === 'success' ? 'badge-success' : r.status === 'running' ? 'badge-running' : 'badge-failed';
      return `<div class="dash-run-item">
        <span class="badge ${statusCls}">${r.status}</span>
        <button class="dash-job-btn" onclick="selectJob(${r._jobId})">${esc(r._jobName)}</button>
        <span style="color:var(--tx2);font-size:11px;flex:1;">${fmtDate(r.started_at)}</span>
        <span style="color:var(--tx2);font-size:11px;">${dur}</span>
        ${r.rows_saved > 0 ? `<span style="color:var(--green);font-size:11px;">+${r.rows_saved}</span>` : ''}
      </div>`;
    }).join('');
  } catch(e) { /* ignore */ }
}

// ── U4: Quick filter presets ──────────────────────────────────────────────────
function applyPreset(name) {
  // Clear existing filters first
  document.getElementById('data-search').value = '';
  document.getElementById('data-from').value = '';
  document.getElementById('data-to').value = '';
  fieldFilters_ = [];
  relFilter_ = '';
  // Reset rel pill to "All"
  ['all','relevant','irrelevant','unchecked'].forEach(k => {
    const el = document.getElementById('rpill-' + k);
    if (el) el.className = 'rel-pill' + (k === 'all' ? ' active' : '');
  });

  const now = new Date();
  const isoLocal = d => d.toISOString().slice(0, 16);

  if (name === 'errors500') {
    fieldFilters_ = [{ field: 'http_status_code', op: 'eq', value: '500' }];
  } else if (name === 'critical') {
    fieldFilters_ = [{ field: 'severity', op: 'eq', value: 'critical' }];
    relFilter_ = 'relevant';
    // Update rel pills manually since we already cleared them above
    const relPillMap = {all:'',relevant:'active-green',irrelevant:'active-red',unchecked:'active-orange'};
    Object.entries(relPillMap).forEach(([k,cls])=>{
      const e = document.getElementById('rpill-'+k);
      if (e) e.className = 'rel-pill' + (k==='relevant' ? ' '+cls : '');
    });
  } else if (name === 'unanalyzed') {
    setRelFilter('unchecked');
    return;
  } else if (name === 'last1h') {
    const from = new Date(now - 3600_000);
    document.getElementById('data-from').value = isoLocal(from);
    document.getElementById('data-to').value = isoLocal(now);
  } else if (name === 'last24h') {
    const from = new Date(now - 86400_000);
    document.getElementById('data-from').value = isoLocal(from);
    document.getElementById('data-to').value = isoLocal(now);
  }

  renderExcludeChips();
  dataPage_ = 1;
  loadData(selJobId, 1);
}

// ── U3: Saved searches ────────────────────────────────────────────────────────
function openSaveSearch() {
  document.getElementById('save-search-name').value = '';
  loadSavedSearches();
  document.getElementById('saved-search-modal').classList.add('open');
}

function openLoadSearch() {
  document.getElementById('save-search-name').value = '';
  loadSavedSearches();
  document.getElementById('saved-search-modal').classList.add('open');
}

async function loadSavedSearches() {
  const el = document.getElementById('saved-searches-list');
  el.innerHTML = '<div style="padding:12px;color:var(--tx2);font-size:12px;">Loading…</div>';
  try {
    const params = new URLSearchParams();
    if (selJobId) params.set('job_id', selJobId);
    const list = await api.get(`/api/saved-searches?${params}`);
    if (!list.length) {
      el.innerHTML = '<div style="padding:12px;color:var(--tx2);font-size:12px;">No saved searches yet.</div>';
      return;
    }
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
  const search   = document.getElementById('data-search')?.value.trim() || '';
  const fromDate = document.getElementById('data-from')?.value || '';
  const toDate   = document.getElementById('data-to')?.value || '';
  try {
    await api.post('/api/saved-searches', {
      job_id: selJobId || null,
      name,
      search,
      from_date: fromDate,
      to_date: toDate,
      relevance: relFilter_,
      field_filters: fieldFilters_,
      sort_by: sortField_,
      sort_dir: sortDir_,
    });
    showToast('💾', 'Saved', `Search "${name}" saved`, 100);
    hideToast(2500);
    await loadSavedSearches();
    document.getElementById('save-search-name').value = '';
  } catch(e) {
    showToast('!', 'Save failed', String(e), 0);
    hideToast(4000);
  }
}

async function applySavedSearch(id) {
  try {
    const list = await api.get('/api/saved-searches');
    const s = list.find(x => x.id === id);
    if (!s) return;
    document.getElementById('data-search').value = s.search || '';
    document.getElementById('data-from').value   = s.from_date || '';
    document.getElementById('data-to').value     = s.to_date || '';
    fieldFilters_ = (typeof s.field_filters === 'string') ? JSON.parse(s.field_filters || '[]') : (s.field_filters || []);
    if (s.sort_by) { sortField_ = s.sort_by; sortDir_ = s.sort_dir || 'desc'; }
    setRelFilter(s.relevance || '');
    renderExcludeChips();
    dataPage_ = 1;
    document.getElementById('saved-search-modal').classList.remove('open');
    loadData(selJobId, 1);
    showToast('📂', 'Search loaded', `Applied "${esc(s.name)}"`, 100);
    hideToast(2500);
  } catch(e) { /* ignore */ }
}

async function deleteSavedSearch(id) {
  if (!confirm('Delete this saved search?')) return;
  try {
    await api.del(`/api/saved-searches/${id}`);
    await loadSavedSearches();
  } catch(e) { /* ignore */ }
}

// ── U5: Export ────────────────────────────────────────────────────────────────
function exportData() {
  if (!selJobId) return;
  document.getElementById('export-modal').classList.add('open');
}

async function doExport() {
  const fmt = document.getElementById('export-format').value;
  const incAI  = document.getElementById('export-inc-ai').checked;
  const incRaw = document.getElementById('export-inc-raw').checked;
  const search   = document.getElementById('data-search')?.value.trim() || '';
  const fromRaw  = document.getElementById('data-from')?.value || '';
  const toRaw    = document.getElementById('data-to')?.value || '';

  const params = new URLSearchParams({ format: fmt });
  if (search)       params.set('search', search);
  if (regexMode_)   params.set('regex', 'true');
  if (fromRaw)      params.set('from_date', fromRaw.replace('T', ' '));
  if (toRaw)        params.set('to_date', toRaw.replace('T', ' '));
  if (relFilter_)   params.set('relevance', relFilter_);
  if (!incAI)       params.set('no_ai', 'true');
  if (incRaw)       params.set('raw_fields', 'true');
  if (fieldFilters_.length) params.set('field_filters', JSON.stringify(fieldFilters_));

  document.getElementById('export-modal').classList.remove('open');
  showToast('⬇', 'Exporting…', 'Preparing download…', 30);

  // Trigger file download via hidden link
  const a = document.createElement('a');
  a.href = `/api/jobs/${selJobId}/export?${params}`;
  a.download = '';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  hideToast(2000);
}

// ── F1: Clusters ──────────────────────────────────────────────────────────────
async function openClusters() {
  if (!selJobId) return;
  document.getElementById('clusters-modal').classList.add('open');
  const el = document.getElementById('clusters-list');
  el.innerHTML = '<div style="padding:20px;color:var(--tx2);font-size:12px;text-align:center;">Loading clusters…</div>';

  const search  = document.getElementById('data-search')?.value.trim() || '';
  const fromRaw = document.getElementById('data-from')?.value || '';
  const toRaw   = document.getElementById('data-to')?.value || '';
  const params  = new URLSearchParams({ limit: 500 });
  if (search)       params.set('search', search);
  if (fromRaw)      params.set('from_date', fromRaw.replace('T', ' '));
  if (toRaw)        params.set('to_date', toRaw.replace('T', ' '));
  if (relFilter_)   params.set('relevance', relFilter_);
  if (fieldFilters_.length) params.set('field_filters', JSON.stringify(fieldFilters_));

  try {
    const data = await api.get(`/api/jobs/${selJobId}/clusters?${params}`);
    if (!data.clusters || !data.clusters.length) {
      el.innerHTML = '<div style="padding:20px;color:var(--tx2);font-size:12px;text-align:center;">No error clusters found in current view.<br>Try filtering by relevant logs or run AI analysis first.</div>';
      return;
    }
    el.innerHTML = data.clusters.map((c, i) =>
      `<div class="cluster-item" onclick="applyClusterFilter(${JSON.stringify(c.pattern).replace(/"/g,'&quot;')})">
        <div style="display:flex;align-items:center;gap:10px;">
          <span class="cluster-count">${c.count}</span>
          <span style="font-size:12px;font-weight:500;color:var(--tx0);">
            ${c.severity ? `<span class="sev sev-${c.severity}" style="margin-right:6px;">${c.severity.toUpperCase()}</span>` : ''}
            ${esc((c.representative || '').slice(0, 80))}
          </span>
        </div>
        <div class="cluster-pattern">${esc(c.pattern)}</div>
      </div>`
    ).join('');
    // Total label
    const totalUniq = data.clusters.length;
    const totalLogs = data.clusters.reduce((s, c) => s + c.count, 0);
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
  // Use the normalized pattern as a regex search
  document.getElementById('data-search').value = pattern;
  regexMode_ = true;
  document.getElementById('regex-toggle').classList.add('active');
  dataPage_ = 1;
  loadData(selJobId, 1);
}

// ── U6: Keyboard shortcuts ────────────────────────────────────────────────────
document.addEventListener('keydown', e => {
  // Don't trigger shortcuts when typing in inputs
  const tag = document.activeElement?.tagName?.toLowerCase();
  const isInput = tag === 'input' || tag === 'textarea' || tag === 'select';
  const anyModalOpen = [...document.querySelectorAll('.modal-overlay.open')].length > 0;

  if (e.key === 'Escape') {
    // Close any open modal
    document.querySelectorAll('.modal-overlay.open').forEach(m => m.classList.remove('open'));
    // Also blur search
    document.activeElement?.blur();
    return;
  }
  if (e.key === '?' && !isInput) {
    document.getElementById('shortcuts-modal').classList.add('open');
    return;
  }
  if (anyModalOpen || isInput) return;

  switch (e.key) {
    case '/':
      e.preventDefault();
      document.getElementById('data-search')?.focus();
      // Make sure we're on the data tab
      if (selJobId) switchTab('data');
      break;
    case 'r':
      if (selJobId) {
        const btn = document.getElementById('btn-run');
        if (btn && !btn.disabled) btn.click();
      }
      break;
    case 'a':
      if (selJobId) {
        const btn = document.getElementById('btn-analyze');
        if (btn && !btn.disabled) btn.click();
      }
      break;
    case 'n':
      if (selJobId && document.getElementById('pane-data')?.classList.contains('active'))
        dataPage(1);
      break;
    case 'p':
      if (selJobId && document.getElementById('pane-data')?.classList.contains('active'))
        dataPage(-1);
      break;
    case 'd':
      if (selJobId) switchTab('data');
      break;
    case 'l':
      if (selJobId) switchTab('live');
      break;
  }
});

// Dashboard is loaded on init and whenever welcome is shown
