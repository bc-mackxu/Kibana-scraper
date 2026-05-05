// ── Dashboard ─────────────────────────────────────────────────────────────────
async function loadDashboard() {
  try {
    const s = await api.get('/api/stats');
    const jCount  = s.total_jobs || 0;
    const rCount  = s.total_rows || 0;
    const analyzed = s.analyzed_rows || 0;
    const pct = rCount ? Math.round(analyzed / rCount * 100) : 0;

    const jEl = document.getElementById('dash-jobs');
    const rEl = document.getElementById('dash-rows');
    const aEl = document.getElementById('dash-analyzed');
    if (jEl) jEl.textContent = jCount.toLocaleString();
    if (rEl) rEl.textContent = rCount.toLocaleString();
    if (aEl) aEl.textContent = pct + '%';
    const aSubEl = document.getElementById('dash-analyzed-sub');
    if (aSubEl) aSubEl.textContent = `${analyzed.toLocaleString()} of ${rCount.toLocaleString()} rows`;
    const jSubEl = document.getElementById('dash-jobs-sub');
    if (jSubEl) {
      const cols = (collections_ || []).length;
      jSubEl.textContent = cols ? `${cols} collection${cols !== 1 ? 's' : ''}` : 'No collections yet';
    }

    // Severity bars
    const sevBarsEl  = document.getElementById('dash-sev-bars');
    const sevStacked = document.getElementById('dash-sev-stacked');
    if (sevBarsEl && s.severity_counts) {
      const sc = s.severity_counts;
      const total = Object.values(sc).reduce((a, b) => a + b, 0) || 1;
      const sevs = [
        { k: 'critical', color: 'var(--red)',    label: 'Critical' },
        { k: 'high',     color: 'var(--orange)', label: 'High' },
        { k: 'medium',   color: 'var(--yellow)', label: 'Medium' },
        { k: 'low',      color: 'var(--blue)',   label: 'Low' },
        { k: 'info',     color: 'var(--tx2)',    label: 'Info' },
      ];
      sevBarsEl.innerHTML = sevs.filter(s => sc[s.k]).map(({ k, color, label }) => {
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
      if (sevStacked) sevStacked.innerHTML = sevs.filter(s => sc[s.k]).map(({ k, color }) => {
        const pct = Math.round((sc[k] || 0) / total * 100);
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

    await loadDashboardRecentRuns();
  } catch(e) { /* non-critical */ }
}

async function loadDashboardRecentRuns() {
  const el = document.getElementById('dash-recent-runs');
  if (!el || !jobs.length) return;
  try {
    const jobsToCheck = jobs.slice(0, 6);
    const allRuns = (await Promise.all(
      jobsToCheck.map(j => api.get(`/api/jobs/${j.id}/runs`).then(runs =>
        runs.map(r => ({ ...r, _jobName: j.name, _jobId: j.id }))
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

// ── Quick filter presets ──────────────────────────────────────────────────────
function applyPreset(name) {
  document.getElementById('data-search').value = '';
  document.getElementById('data-from').value = '';
  document.getElementById('data-to').value = '';
  fieldFilters_ = [];
  relFilter_ = '';
  ['all', 'relevant', 'irrelevant', 'unchecked'].forEach(k => {
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
    const relPillMap = { all: '', relevant: 'active-green', irrelevant: 'active-red', unchecked: 'active-orange' };
    Object.entries(relPillMap).forEach(([k, cls]) => {
      const e = document.getElementById('rpill-' + k);
      if (e) e.className = 'rel-pill' + (k === 'relevant' ? ' ' + cls : '');
    });
  } else if (name === 'unanalyzed') {
    setRelFilter('unchecked');
    return;
  } else if (name === 'last1h') {
    document.getElementById('data-from').value = isoLocal(new Date(now - 3600_000));
    document.getElementById('data-to').value   = isoLocal(now);
  } else if (name === 'last24h') {
    document.getElementById('data-from').value = isoLocal(new Date(now - 86400_000));
    document.getElementById('data-to').value   = isoLocal(now);
  }

  renderExcludeChips();
  dataPage_ = 1;
  loadData(selJobId, 1);
}
