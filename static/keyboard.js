// ── Modal close on backdrop ───────────────────────────────────────────────────
['modal', 'gh-modal', 'coderef-modal', 'similar-modal', 'summary-modal', 'group-modal',
 'saved-search-modal', 'export-modal', 'clusters-modal', 'shortcuts-modal', 'mcp-modal'].forEach(id => {
  const el = document.getElementById(id);
  if (!el) return;
  el.addEventListener('click', e => {
    if (e.target.id === id) {
      if (id === 'modal') closeModal();
      else {
        document.getElementById(id).classList.remove('open');
        if (id === 'similar-modal') _similarIds_ = [];
      }
    }
  });
});

// ── Keyboard shortcuts ────────────────────────────────────────────────────────
document.addEventListener('keydown', e => {
  const tag = document.activeElement?.tagName?.toLowerCase();
  const isInput = tag === 'input' || tag === 'textarea' || tag === 'select';
  const anyModalOpen = [...document.querySelectorAll('.modal-overlay.open')].length > 0;

  if (e.key === 'Escape') {
    document.querySelectorAll('.modal-overlay.open').forEach(m => m.classList.remove('open'));
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

// ── Boot ──────────────────────────────────────────────────────────────────────
init();
