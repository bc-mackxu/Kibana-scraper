// ── Tabs ──────────────────────────────────────────────────────────────────────
function switchTab(name) {
  ['runs','live','data','config'].forEach(t => {
    document.getElementById('tab-'+t).classList.toggle('active', t===name);
    document.getElementById('pane-'+t).classList.toggle('active', t===name);
  });
  if (name==='data' && selJobId) {
    if (!dataFields_.length) loadFields(selJobId).then(() => loadData(selJobId, dataPage_));
    else loadData(selJobId, dataPage_);
  }
  if (name==='config') {
    if (selCollId) renderConfigFormCollection(collections_.find(c=>c.id===selCollId));
    else if (selJobId) renderConfigForm(jobs.find(j=>j.id===selJobId));
  }
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

// ── Run Now / Run Collection ──────────────────────────────────────────────────
async function runNow() {
  const btn = document.getElementById('btn-run');
  btn.disabled=true; btn.textContent='⟳ Running…';
  const jobId = selJobId;
  const r = await api.post(`/api/jobs/${jobId}/run`);
  switchTab('live');
  streamRun(r.run_id, jobId);
  setTimeout(() => loadJobs(), 1000);
}

async function runCollection(cid) {
  const coll = collections_.find(c=>c.id===cid);
  const btn  = document.getElementById('btn-run');
  btn.disabled=true; btn.textContent='⟳ Running…';
  const r = await api.post(`/api/collections/${cid}/run`, {});
  if (r.run_id) {
    for (const src of (coll?.sources || []))
      if (src.job_id) activeRunIds_[src.job_id] = r.run_id;
    const firstJobId = coll?.sources?.[0]?.job_id;
    switchTab('live');
    streamRun(r.run_id, firstJobId || selJobId);
  }
  setTimeout(() => loadJobs(), 1000);
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
  document.getElementById('job-detail').style.display = 'none';
  document.getElementById('welcome').style.display    = 'flex';
  await loadJobs();
  await loadStats();
}

// ── SSE run stream ────────────────────────────────────────────────────────────
function streamRun(runId, jobId) {
  if (jobId == null) jobId = selJobId;
  if (activeESes_[jobId]) { try { activeESes_[jobId].close(); } catch(e) {} }
  activeRunIds_[jobId] = runId;

  function _dom(fn) { if (selJobId === jobId) fn(); }

  _dom(() => {
    document.getElementById('terminal').innerHTML = '';
    document.getElementById('log-done').style.display = 'none';
    document.getElementById('live-title').textContent = `Run #${runId} — streaming…`;
  });

  const es = new EventSource(`/api/runs/${runId}/stream`);
  activeESes_[jobId] = es;

  _dom(() => {
    const cancelBtn = document.getElementById('btn-cancel-run');
    if (cancelBtn) cancelBtn.style.display = 'inline-flex';
  });

  es.onmessage = e => {
    const d = JSON.parse(e.data);
    if (d.type === 'log') {
      _dom(() => {
        const term = document.getElementById('terminal');
        const line = document.createElement('div');
        line.className = 'log-line ' + lineClass(d.line);
        line.textContent = d.line;
        term.appendChild(line); term.scrollTop = term.scrollHeight;
      });
    } else if (d.type === 'done') {
      es.close();
      delete activeESes_[jobId];
      _dom(() => {
        const cancelBtn = document.getElementById('btn-cancel-run');
        if (cancelBtn) cancelBtn.style.display = 'none';
        const runId2 = activeRunIds_[jobId];
        document.getElementById('live-title').textContent = `Run #${runId2} — ${d.status}`;
        const done = document.getElementById('log-done');
        done.className   = `log-done ${d.status}`;
        done.textContent = d.status === 'success' ? '✓ Completed successfully' : '✗ Run failed';
        done.style.display = 'block';
      });
      loadJobs(); loadRuns(jobId === selJobId ? selJobId : null);
      _startSmartPoll();
    }
  };
  es.onerror = () => {
    _dom(() => {
      document.getElementById('live-title').textContent = `Run #${runId} — connection closed`;
      const cancelBtn = document.getElementById('btn-cancel-run');
      if (cancelBtn) cancelBtn.style.display = 'none';
    });
  };
}

async function cancelRun() {
  let runId = activeRunIds_[selJobId];
  if (!runId && selCollId) {
    const coll = collections_.find(c => c.id === selCollId);
    for (const src of (coll?.sources || []))
      if (activeRunIds_[src.job_id]) { runId = activeRunIds_[src.job_id]; break; }
  }
  if (!runId) { showToast('!', 'Nothing to cancel', 'No active run found', 0); hideToast(3000); return; }
  if (!confirm(`Cancel run #${runId}?`)) return;
  try {
    await api.post(`/api/runs/${runId}/cancel`, {});
    showToast('✕', 'Cancelling…', `Sent cancel signal to run #${runId}`, 50);
    hideToast(3000);
  } catch(e) {
    showToast('!', 'Cancel failed', String(e), 0); hideToast(4000);
  }
}

function reconnectLiveLog(jobId) {
  const runId = activeRunIds_[jobId];
  if (!runId) return;
  const es = activeESes_[jobId];
  const term  = document.getElementById('terminal');
  const done  = document.getElementById('log-done');
  const title = document.getElementById('live-title');
  if (es && es.readyState !== EventSource.CLOSED) {
    title.textContent = `Run #${runId} — streaming…`;
    done.style.display = 'none';
  } else {
    title.textContent = `Run #${runId} — finished`;
    done.style.display = 'none';
    streamRun(runId, jobId);
  }
}

function lineClass(line) {
  if (!line) return '';
  if (line.startsWith('━━━ CHUNK'))                                           return 'chunk-hdr';
  if (line.includes('[✓]') || line.includes('Done'))                          return 'ok';
  if (line.includes('[✗]') || line.includes('ERROR') || line.includes('failed')) return 'err';
  if (line.includes('[!]') || line.includes('warning'))                        return 'warn';
  if (line.includes('[→]') || line.includes('[net]') || line.includes('[bsearch]')) return 'info';
  return '';
}

function clearLog() {
  document.getElementById('terminal').innerHTML = '';
  document.getElementById('log-done').style.display = 'none';
}

// ── Background Jobs tracker ───────────────────────────────────────────────────
let bgJobs_      = [];
let bgJobSeq_    = 0;
let bgPanelOpen_ = false;

function bgJobStart(icon, name) {
  const id = ++bgJobSeq_;
  bgJobs_.push({id, icon, name, pct:0, msg:'Starting…', status:'running', ts:Date.now()});
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

  btn.classList.toggle('has-running', running.length > 0);
  btn.classList.toggle('has-error',   !running.length && bgJobs_.some(j => j.status === 'error'));

  if (running.length > 0) {
    icon.textContent  = '⚙';
    icon.className    = 'bj-spinner';
    label.textContent = running.length === 1 ? running[0].name.slice(0,18) : `${running.length} running`;
    count.style.display = 'inline';
    count.textContent   = running.length;
  } else {
    icon.textContent    = '⚙';
    icon.className      = '';
    label.textContent   = 'Jobs';
    count.style.display = bgJobs_.length ? 'inline' : 'none';
    count.textContent   = bgJobs_.length;
    if (count.style.display !== 'none')
      count.style.background = bgJobs_.some(j=>j.status==='error') ? 'var(--red)' : 'var(--bg3)';
  }

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
