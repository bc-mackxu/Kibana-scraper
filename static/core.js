// ── State ─────────────────────────────────────────────────────────────────────
let jobs = [], selJobId = null, editJobId = null;
let collections_ = [], selCollId = null;
let dataPage_ = 1;
let activeRunIds_ = {};   // {jobId: runId}
let activeESes_   = {};   // {jobId: EventSource}
let sortField_ = 'timestamp', sortDir_ = 'desc';
let dataFields_ = [], dataSelectedFields_ = null, lastDataRows_ = null;
let relFilter_ = '';
let anthropicReady_ = false;   // set after first /api/settings fetch
let fieldPanelCollapsed_ = false;
let perPage_ = 50;
let selectedRows_ = new Set();
let regexMode_ = false;
let crossSourceMode_ = false;
let fieldSearch_ = '';
const LINKED_SEP = '\x1E';   // separates "SourceLabel\x1Efield_name" for linked fields
let linkedJobFields_ = [];
let corrDataCache_ = {};
let fieldFilters_ = [];   // [{field, value, op}]

const DATA_DB_MAP = { '@timestamp': r => r.timestamp };
const DEFAULT_FIELDS = ['@timestamp'];
const PRIORITY_FIELDS = [
  '@timestamp','error_level','message','store_id',
  'request_uri','server_name','domain','http_status_code',
  'remote_addr','request_method'
];

// ── API ───────────────────────────────────────────────────────────────────────
const api = {
  get:    url      => fetch(url).then(r => r.json()),
  post:   (url, b) => fetch(url, {method:'POST',  headers:{'Content-Type':'application/json'}, body:JSON.stringify(b)}).then(r=>r.json()),
  put:    (url, b) => fetch(url, {method:'PUT',   headers:{'Content-Type':'application/json'}, body:JSON.stringify(b)}).then(r=>r.json()),
  delete: url      => fetch(url, {method:'DELETE'}).then(r=>r.json()),
  del:    async (url) => { const r = await fetch(url, {method:'DELETE', headers:{'Content-Type':'application/json'}}); if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); },
};

// ── Utilities ─────────────────────────────────────────────────────────────────
function esc(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}
function timeAgo(dt){const d=Math.floor((Date.now()-new Date(dt+'Z'))/1000);if(d<60)return`${d}s ago`;if(d<3600)return`${Math.floor(d/60)}m ago`;if(d<86400)return`${Math.floor(d/3600)}h ago`;return`${Math.floor(d/86400)}d ago`;}
function fmtDate(dt){return new Date(dt+'Z').toLocaleString();}
function elapsed(a,b){const s=Math.round((new Date(b+'Z')-new Date(a+'Z'))/1000);if(s<60)return`${s}s`;return`${Math.floor(s/60)}m ${s%60}s`;}
function timeOptions(sel){return['Last 15 minutes','Last 1 hour','Last 24 hours','Last 7 days','Last 30 days','Last 90 days','Last 1 year'].map(o=>`<option ${o===sel?'selected':''}>${o}</option>`).join('');}
function chunkOptions(sel){
  const opts=[['0','Disabled (single pass)'],['1','1h per chunk'],['2','2h per chunk'],['4','4h per chunk'],['6','6h per chunk'],['12','12h per chunk'],['24','24h per chunk'],['48','48h per chunk']];
  return opts.map(([v,l])=>`<option value="${v}" ${String(sel)===v?'selected':''}>${l}</option>`).join('');
}

// ── Toast notification ────────────────────────────────────────────────────────
let toastTimer_ = null;
function showToast(icon, title, body, pct) {
  const t = document.getElementById('toast');
  document.getElementById('toast-icon').textContent  = icon;
  document.getElementById('toast-title').textContent = title;
  document.getElementById('toast-body').textContent  = body;
  document.getElementById('toast-fill').style.width  = (pct||0)+'%';
  t.classList.add('visible');
  if (toastTimer_) clearTimeout(toastTimer_);
}
function hideToast(delay=3000) {
  toastTimer_ = setTimeout(() => document.getElementById('toast').classList.remove('visible'), delay);
}

// ── Shared view-reset helper ──────────────────────────────────────────────────
// Call when switching jobs / collections to clear stale per-job UI state.
// NOTE: updateBulkBar() and renderExcludeChips() are defined in data-table.js
// and are always available at call time (all scripts loaded before init() runs).
function _resetJobView() {
  dataFields_=[]; dataSelectedFields_=null; lastDataRows_=null; dataPage_=1;
  linkedJobFields_=[]; corrDataCache_={};
  selectedRows_.clear(); updateBulkBar();
  document.getElementById('data-search').value='';
  document.getElementById('data-from').value='';
  document.getElementById('data-to').value='';
  fieldFilters_=[]; renderExcludeChips();
}
