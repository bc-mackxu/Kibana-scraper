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
  const ta  = document.getElementById('mcp-prompt-text');
  const btn = document.getElementById('mcp-copy-btn');
  ta.select();
  navigator.clipboard.writeText(ta.value).then(() => {
    btn.textContent = '✓ Copied!';
    setTimeout(() => { btn.textContent = '📋 Copy prompt'; }, 2000);
  });
}

// ── Analyze (SSE) ─────────────────────────────────────────────────────────────
function startAnalysis() {
  if (!selJobId) return;
  if (!anthropicReady_) { showMcpPromptModal('analyze'); return; }

  const btn       = document.getElementById('btn-analyze');
  const pbar      = document.getElementById('analyze-pbar');
  const fill      = document.getElementById('analyze-bar-fill');
  const statusTxt = document.getElementById('analyze-status');
  btn.disabled=true; btn.textContent='⟳ Analyzing…';
  pbar.classList.add('active'); fill.style.width='0%';
  statusTxt.style.display='block'; statusTxt.textContent='Starting…';
  showToast('🤖', 'Analyzing logs…', 'Sending to Claude Haiku', 0);

  const _bjId = bgJobStart('🤖', 'Analyze with AI');
  const es = new EventSource(`/api/jobs/${selJobId}/analyze/stream?limit=30`);
  es.onmessage = e => {
    const d = JSON.parse(e.data);
    if (d.type === 'progress') {
      const pct = d.total ? Math.round(d.done/d.total*100) : 0;
      fill.style.width = pct+'%';
      statusTxt.textContent = `${d.done}/${d.total}`;
      showToast('🤖', 'Analyzing…', `${d.done} of ${d.total} logs processed`, pct);
      bgJobProgress(_bjId, pct, `${d.done} / ${d.total} logs`);
    } else if (d.type === 'done') {
      es.close();
      btn.disabled=false; btn.textContent='🤖 Analyze with AI';
      fill.style.width='100%'; pbar.classList.remove('active');
      const msg = d.analyzed===0 ? 'All logs already analyzed' : `${d.analyzed} analyzed · ${d.relevant} checkout-related`;
      statusTxt.textContent = msg;
      showToast('✓', 'Analysis complete', msg, 100); hideToast(4000);
      bgJobDone(_bjId, msg);
      loadData(selJobId, dataPage_);
    }
  };
  es.onerror = () => {
    es.close();
    btn.disabled=false; btn.textContent='🤖 Analyze with AI';
    statusTxt.textContent='Error — check console'; pbar.classList.remove('active');
    showToast('✗', 'Analysis error', 'Check the server logs', 0); hideToast(5000);
    bgJobError(_bjId, 'Server error — check logs');
  };
}

// ── Auto-Classify (SSE) ───────────────────────────────────────────────────────
function startAutoClassify() {
  if (!selJobId) return;
  if (!anthropicReady_) { showMcpPromptModal('classify'); return; }

  const btn  = document.getElementById('btn-autoclassify');
  const pbar = document.getElementById('autoclassify-pbar');
  const fill = document.getElementById('autoclassify-bar-fill');
  btn.disabled=true; btn.textContent='⟳ Classifying…';
  pbar.classList.add('active'); fill.style.width='0%';
  showToast('⚡', 'Auto-Classify', 'Building pattern lookup…', 0);

  const _bjId2 = bgJobStart('⚡', 'Auto-Classify');
  const es = new EventSource(`/api/jobs/${selJobId}/auto-classify/stream`);
  es.onmessage = e => {
    const d = JSON.parse(e.data);
    if (d.type === 'lookup_built') {
      showToast('⚡', 'Auto-Classify', `${d.patterns} patterns learned`, 5);
      bgJobProgress(_bjId2, 5, `${d.patterns} patterns learned`);
    } else if (d.type === 'progress') {
      const pct = d.total ? Math.round(d.done/d.total*100) : 0;
      fill.style.width = pct+'%';
      showToast('⚡', 'Auto-Classify', `${d.done} / ${d.total} checked`, pct);
      bgJobProgress(_bjId2, pct, `${d.done} / ${d.total} records`);
    } else if (d.type === 'done') {
      es.close();
      btn.disabled=false; btn.textContent='⚡ Auto-Classify';
      fill.style.width='100%'; pbar.classList.remove('active');
      const msg = d.classified===0 ? (d.message||'Nothing to classify') : `Classified ${d.classified} · Skipped ${d.skipped} (no match)`;
      showToast('✓', 'Auto-Classify done', msg, 100); hideToast(4000);
      bgJobDone(_bjId2, msg);
      loadData(selJobId, dataPage_);
    }
  };
  es.onerror = () => {
    es.close();
    btn.disabled=false; btn.textContent='⚡ Auto-Classify';
    pbar.classList.remove('active');
    showToast('✗', 'Auto-Classify error', 'Check server logs', 0); hideToast(5000);
    bgJobError(_bjId2, 'Server error');
  };
}

// ── Settings ──────────────────────────────────────────────────────────────────
async function openSettings() {
  const s = await api.get('/api/settings');
  document.getElementById('ak-status').textContent = s.anthropic_key_set ? '✓ set' : '';
  document.getElementById('gh-status').textContent = s.github_token_set  ? '✓ set' : '';
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
  api.get('/api/settings').then(s => { anthropicReady_ = !!s.anthropic_key_set; });
  showToast('✓', 'Saved', 'Settings saved', 100); hideToast(2500);
}

// ── AI Summary ────────────────────────────────────────────────────────────────
async function showSummary() {
  if (!selJobId || !lastDataRows_ || !lastDataRows_.length) return;
  const btn = document.getElementById('btn-summary');
  btn.disabled=true; btn.textContent='⟳ Summarizing…';
  const ids = lastDataRows_.map(r => r.id);
  try {
    const res = await api.post(`/api/jobs/${selJobId}/summarize`, { ids });
    document.getElementById('summary-text').textContent  = res.summary || 'No summary available.';
    document.getElementById('summary-count').textContent = res.count || ids.length;
    document.getElementById('summary-modal').classList.add('open');
  } catch(e) {
    showToast('✗', 'Summary failed', String(e), 0); hideToast(4000);
  }
  btn.disabled=false; btn.textContent='📋 Summary';
}

// ── Code refs modal ───────────────────────────────────────────────────────────
function openCodeRefs(rowId) {
  const row = (lastDataRows_||[]).find(r => r.id === rowId);
  if (!row) return;
  const sev = row.severity || 'info';
  document.getElementById('cr-title').textContent = 'AI Analysis — Log #' + rowId;
  document.getElementById('cr-rel-label').textContent =
    row.relevance==='relevant' ? '✓ Checkout / payment related' :
    row.relevance==='irrelevant' ? '✗ Not related to checkout' : '';
  document.getElementById('cr-severity-badge').innerHTML = sevBadge(sev, rowId, false);
  document.getElementById('cr-summary').textContent = row.ai_summary || 'No summary available.';
  let refs = row.code_refs;
  if (typeof refs==='string') { try { refs=JSON.parse(refs); } catch(e) { refs=[]; } }
  const refsEl = document.getElementById('cr-refs');
  if (!refs || !refs.length) {
    refsEl.innerHTML = row.relevance==='relevant'
      ? `<div style="color:var(--tx2);font-size:12px;display:flex;align-items:center;gap:8px;">
           No GitHub code references found.
           <button class="btn btn-ghost btn-sm" onclick="fetchCodeRefs(${rowId})">🔍 Search GitHub</button>
         </div>`
      : '<div style="color:var(--tx2);font-size:12px;">Not checkout-related — no code search performed.</div>';
  } else {
    refsEl.innerHTML = '<div class="coderef-section-label" style="margin-bottom:8px;">Code References</div>' +
      refs.map(r=>`<div class="coderef-item">
        <a href="${esc(r.url)}" target="_blank">📄 ${esc(r.path)}</a>
        <div class="coderef-repo">${esc(r.repo)}</div>
      </div>`).join('');
  }
  document.getElementById('coderef-modal').classList.add('open');
}

async function fetchCodeRefs(rowId) {
  const settings = await api.get('/api/settings');
  if (!settings.github_token_set) {
    document.getElementById('coderef-modal').classList.remove('open');
    openSettings();
  }
}
