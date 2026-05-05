// ── Init ──────────────────────────────────────────────────────────────────────
async function init() {
  api.get('/api/settings').then(s => { anthropicReady_ = !!s.anthropic_key_set; });
  await Promise.all([loadStats(), loadJobs()]);
  loadDashboard();
  checkHealth();
  setInterval(checkHealth, 30_000);
  _startSmartPoll();
}

// ── Smart polling ─────────────────────────────────────────────────────────────
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
    _startSmartPoll();
  }, delay);
}

// ── Stats ─────────────────────────────────────────────────────────────────────
async function loadStats() {
  const s = await api.get('/api/stats');
  document.getElementById('s-jobs').textContent = s.total_jobs;
  if (!selJobId) {
    document.getElementById('s-rows').textContent = (s.total_rows||0).toLocaleString();
    document.getElementById('rows-chip').title = 'Total rows across all jobs';
  }
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

// ── Collections + Jobs ────────────────────────────────────────────────────────
async function loadCollections() {
  try { collections_ = await api.get('/api/collections') || []; } catch(e) { collections_ = []; }
}

async function loadJobs() {
  await loadCollections();
  jobs = await api.get('/api/jobs');
  const collJobIds = new Set(
    collections_.flatMap(c => (c.sources||[]).map(s=>s.job_id).filter(Boolean))
  );
  renderSidebar(collJobIds);
  if (selCollId) {
    const c = collections_.find(c=>c.id===selCollId);
    if (c) {
      if (selJobId && (c.sources||[]).some(s=>s.job_id===selJobId))
        updateDetailHeaderForCollectionSource(c, selJobId);
      else
        updateDetailHeaderForCollection(c);
    }
  } else if (selJobId) {
    const j = jobs.find(j=>j.id===selJobId);
    if (j) updateDetailHeader(j);
  }
  if (!selJobId && !selCollId) loadDashboard();
  _updateCrossSourceBtn();
}

function renderSidebar(collJobIds) {
  const el = document.getElementById('job-list');
  const parts = [];

  for (const c of collections_) {
    const anyRunning = (c.sources||[]).some(s => s.is_running);
    const status = c.last_run_status;
    const dotCls = anyRunning ? 'dot-running' : status==='success' ? 'dot-success' : status==='failed' ? 'dot-failed' : 'dot-none';
    const when   = c.last_run_at ? timeAgo(c.last_run_at) : 'never';
    const keysStr = (c.corr_keys||[]).length ? `<span style="font-size:10px;color:var(--blue);margin-left:4px;">🔗 ${c.corr_keys.join(', ')}</span>` : '';
    const srcHtml = (c.sources||[]).map(s => {
      const srcCls    = s.is_running ? 'running' : s.last_run_status==='success' ? 'success' : '';
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

  const standaloneJobs = jobs.filter(j => !collJobIds.has(j.id));
  if (standaloneJobs.length && collections_.length)
    parts.push(`<div style="font-size:10px;color:var(--tx2);padding:8px 14px 2px;text-transform:uppercase;letter-spacing:.05em;">Standalone</div>`);

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

// ── Job / collection selection ────────────────────────────────────────────────
async function selectJob(id) {
  if (selJobId !== id) _resetJobView();
  selJobId = id;
  selCollId = null;
  const collJobIds = new Set(collections_.flatMap(c=>(c.sources||[]).map(s=>s.job_id).filter(Boolean)));
  renderSidebar(collJobIds);
  document.getElementById('welcome').style.display='none';
  const d = document.getElementById('job-detail'); d.style.display='flex';
  const job = jobs.find(j=>j.id===id);
  updateDetailHeader(job);
  _updateCrossSourceBtn();
  if (activeESes_[id] || (job && job.is_running && activeRunIds_[id])) {
    switchTab('live'); reconnectLiveLog(id);
  } else {
    switchTab('data');
  }
  await loadRuns(id);
}

async function selectCollection(cid) {
  selCollId = cid;
  const coll = collections_.find(c=>c.id===cid);
  const firstJobId = coll?.sources?.[0]?.job_id;
  if (firstJobId && selJobId !== firstJobId) {
    _resetJobView();
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

async function selectCollectionSource(collId, jobId) {
  selCollId = collId;
  if (selJobId !== jobId) { _resetJobView(); selJobId = jobId; }
  const coll = collections_.find(c=>c.id===collId);
  const collJobIds = new Set(collections_.flatMap(c=>(c.sources||[]).map(s=>s.job_id).filter(Boolean)));
  renderSidebar(collJobIds);
  document.getElementById('welcome').style.display='none';
  document.getElementById('job-detail').style.display='flex';
  if (coll) updateDetailHeaderForCollectionSource(coll, jobId);
  _updateCrossSourceBtn();
  if (activeESes_[jobId] || (jobs.find(j=>j.id===jobId)||{}).is_running) {
    switchTab('live'); reconnectLiveLog(jobId);
  } else {
    switchTab('data');
  }
  await loadRuns(jobId);
}

// ── Detail header helpers ─────────────────────────────────────────────────────
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

function updateDetailHeaderForCollection(coll) {
  const anyRunning = (coll.sources||[]).some(s=>s.is_running);
  document.getElementById('d-name').textContent = coll.name;
  document.getElementById('d-url').textContent  = (coll.sources||[]).map(s=>s.label).join(' · ') || '–';
  const chunkLabel = coll.chunk_hours > 0 ? ` · ${coll.chunk_hours}h chunks` : '';
  const keysLabel  = (coll.corr_keys||[]).length ? ` · 🔗 ${coll.corr_keys.join(', ')}` : '';
  document.getElementById('d-schedule').textContent =
    `Daily @ ${String(coll.schedule_hour||2).padStart(2,'0')}:00 UTC · ${coll.time_range}${chunkLabel}${keysLabel}`;
  const btn = document.getElementById('btn-run');
  btn.disabled    = anyRunning;
  btn.textContent = anyRunning ? '⟳ Running…' : '▶ Run All';
  btn.onclick     = () => runCollection(coll.id);
}

function updateDetailHeaderForCollectionSource(coll, jobId) {
  const src        = (coll.sources||[]).find(s=>s.job_id===jobId);
  const anyRunning = (coll.sources||[]).some(s=>s.is_running);
  document.getElementById('d-name').textContent = `${coll.name}  ›  ${src ? src.label : `Job ${jobId}`}`;
  document.getElementById('d-url').textContent  = (coll.sources||[]).map(s=>s.kibana_url).join('  |  ') || '–';
  const chunkLabel = coll.chunk_hours > 0 ? ` · ${coll.chunk_hours}h chunks` : '';
  const keysLabel  = (coll.corr_keys||[]).length ? ` · 🔗 ${coll.corr_keys.join(', ')}` : '';
  document.getElementById('d-schedule').textContent =
    `Daily @ ${String(coll.schedule_hour||2).padStart(2,'0')}:00 UTC · ${coll.time_range}${chunkLabel}${keysLabel}`;
  const btn = document.getElementById('btn-run');
  btn.disabled    = anyRunning;
  btn.textContent = anyRunning ? '⟳ Running…' : '▶ Run All';
  btn.onclick     = () => runCollection(coll.id);
}

// ── Session health ────────────────────────────────────────────────────────────
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
