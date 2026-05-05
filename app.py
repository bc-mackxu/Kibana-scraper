#!/usr/bin/env python3
"""
Kibana Scraper Web App
Run: python3 app.py   (opens at http://localhost:8765)
"""

import asyncio
import json
import os
import subprocess
import sys
import time
import threading
from datetime import datetime, timedelta
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from analyzer import analyze_batch, search_github
from db import qone, qall, exe, init_db, DB_PATH

# ─── Config ───────────────────────────────────────────────────────────────────

SCRAPER_DIR = Path(__file__).parent
SCRAPER_CMD = [sys.executable, "-u", str(SCRAPER_DIR / "kibana_scraper.py")]
STATIC_DIR  = SCRAPER_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)

# ─── Keys config (persisted to keys.json) ─────────────────────────────────────

KEYS_FILE = SCRAPER_DIR / "keys.json"

def _load_keys():
    if KEYS_FILE.exists():
        try:
            data = json.loads(KEYS_FILE.read_text())
            for k, v in data.items():
                if v:
                    os.environ.setdefault(k, v)
        except Exception:
            pass

def _save_keys(updates: dict):
    data = {}
    if KEYS_FILE.exists():
        try: data = json.loads(KEYS_FILE.read_text())
        except: pass
    data.update({k: v for k, v in updates.items() if v})
    KEYS_FILE.write_text(json.dumps(data, indent=2))
    for k, v in updates.items():
        if v: os.environ[k] = v

_load_keys()

app = FastAPI(title="Kibana Scraper")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# In-memory state for active runs
_run_logs:   dict[int, list[str]] = {}  # run_id → lines
_run_status: dict[int, str]       = {}  # run_id → "running"|"success"|"failed"

scheduler = BackgroundScheduler(timezone="UTC")

# ─── Models ───────────────────────────────────────────────────────────────────

class JobIn(BaseModel):
    name:          str
    kibana_url:    str
    time_range:    str  = "Last 7 days"
    max_pages:     int  = Field(50,  ge=1,  le=9999)
    schedule_hour: int  = Field(2,   ge=0,  le=23)
    enabled:       bool = True
    chunk_hours:   int  = Field(0,   ge=0)   # 0 = no chunking


# ─── Time-range helpers ───────────────────────────────────────────────────────

_TR_HOURS = {
    "last 15 minutes": 0.25,
    "last 1 hour":     1,
    "last 24 hours":   24,
    "last 7 days":     168,
    "last 30 days":    720,
    "last 90 days":    2160,
    "last 1 year":     8760,
}

def _range_to_hours(time_range: str) -> float:
    """Convert 'Last N days/hours' string to hours as float."""
    key = time_range.strip().lower()
    if key in _TR_HOURS:
        return _TR_HOURS[key]
    import re
    m = re.match(r'last\s+(\d+(?:\.\d+)?)\s+(minute|hour|day|week|month|year)s?', key)
    if m:
        n, unit = float(m.group(1)), m.group(2)
        return n * {"minute": 1/60, "hour": 1, "day": 24,
                    "week": 168, "month": 720, "year": 8760}[unit]
    return 168  # fallback: 7 days


def _build_chunks(time_range: str, chunk_hours: int) -> list[tuple[str, str]]:
    """Return [(from_iso, to_iso), ...] windows covering time_range, oldest first."""
    total_h  = _range_to_hours(time_range)
    chunk_h  = max(1, chunk_hours)
    end_dt   = datetime.now()
    start_dt = end_dt - timedelta(hours=total_h)
    windows  = []
    cur = start_dt
    while cur < end_dt:
        w_end = min(cur + timedelta(hours=chunk_h), end_dt)
        windows.append((
            cur.strftime("%Y-%m-%dT%H:%M:%S"),
            w_end.strftime("%Y-%m-%dT%H:%M:%S"),
        ))
        cur = w_end
    return windows

# ─── Run execution ────────────────────────────────────────────────────────────

def _last_timestamp(job_id: int):
    """Return the latest scraped timestamp for this job, or None."""
    row = qone("SELECT MAX(timestamp) AS ts FROM log_events WHERE job_id=?", (job_id,))
    return row["ts"] if row and row["ts"] else None


def _run_one_window(run_id: int, job: dict, time_args: list, rows_saved_ref: list):
    """Run scraper for a single time window, appending to _run_logs[run_id].
    rows_saved_ref is a mutable 1-element list [total] so caller can accumulate."""
    cmd = SCRAPER_CMD + [
        "scrape", job["kibana_url"],
        *time_args,
        "--pages",   str(job["max_pages"]),
        "--run-id",  str(run_id),
        "--job-id",  str(job["id"]),
        "--db-path", str(DB_PATH),
    ]
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, cwd=str(SCRAPER_DIR),
        )
        for line in proc.stdout:
            line = line.rstrip()
            _run_logs[run_id].append(line)
            if "rows saved (total" in line or "Total rows saved:" in line:
                for tok in line.split():
                    if tok.isdigit():
                        rows_saved_ref[0] = int(tok)
        proc.wait()
        return proc.returncode == 0
    except Exception as e:
        _run_logs[run_id].append(f"[ERROR] {e}")
        return False


def _do_run(run_id: int, job: dict):
    _run_logs[run_id]   = []
    _run_status[run_id] = "running"

    last_ts     = _last_timestamp(job["id"])
    chunk_hours = int(job.get("chunk_hours") or 0)
    rows_ref    = [0]       # mutable accumulator for total saved
    any_failed  = False

    if last_ts:
        # ── Incremental mode (always single window) ──────────────────────────
        from_dt = last_ts.strftime("%Y-%m-%dT%H:%M:%S") if hasattr(last_ts, "strftime") \
                  else str(last_ts).replace(" ", "T")[:19]
        to_dt   = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        _run_logs[run_id].append(f"[→] Incremental: {from_dt} → {to_dt}")
        ok = _run_one_window(run_id, job, ["--from", from_dt, "--to", to_dt], rows_ref)
        if not ok:
            any_failed = True

    elif chunk_hours > 0:
        # ── Chunked full scan ─────────────────────────────────────────────────
        windows = _build_chunks(job["time_range"], chunk_hours)
        total   = len(windows)
        _run_logs[run_id].append(
            f"[→] Chunked scan: {job['time_range']} → {total} chunks × {chunk_hours}h"
        )
        chunk_saved = [0]
        MAX_RETRIES = 2
        for i, (w_from, w_to) in enumerate(windows, 1):
            _run_logs[run_id].append(
                f"\n━━━ CHUNK {i}/{total} ━━━  {w_from}  →  {w_to}"
            )
            chunk_saved[0] = 0
            ok = False
            for attempt in range(1, MAX_RETRIES + 1):
                ok = _run_one_window(run_id, job, ["--from", w_from, "--to", w_to], chunk_saved)
                if ok:
                    break
                if attempt < MAX_RETRIES:
                    wait_s = 15 * attempt
                    _run_logs[run_id].append(
                        f"[!] Chunk {i} attempt {attempt} failed — retrying in {wait_s}s…"
                    )
                    time.sleep(wait_s)
                    chunk_saved[0] = 0   # reset for retry

            rows_ref[0] += chunk_saved[0]
            if ok:
                _run_logs[run_id].append(
                    f"[✓] Chunk {i}/{total} done — {chunk_saved[0]} rows  (total {rows_ref[0]})"
                )
            else:
                _run_logs[run_id].append(f"[✗] Chunk {i}/{total} FAILED after {MAX_RETRIES} attempts — continuing…")
                any_failed = True

            if i < total:
                time.sleep(5)

    else:
        # ── Single full scan ──────────────────────────────────────────────────
        _run_logs[run_id].append(f"[→] Full scan: {job['time_range']} (no prior data)")
        ok = _run_one_window(run_id, job, ["--time", job["time_range"]], rows_ref)
        if not ok:
            any_failed = True

    status = "failed" if any_failed else "success"
    _run_status[run_id] = status
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log = "\n".join(_run_logs[run_id])

    exe("UPDATE job_runs SET status=?, finished_at=?, rows_saved=?, log_output=? WHERE id=?",
        (status, now, rows_ref[0], log, run_id))
    exe("UPDATE scrape_jobs SET last_run_at=?, last_run_status=?, last_rows_saved=? WHERE id=?",
        (now, status, rows_ref[0], job["id"]))


def _start_run(job_id: int) -> int:
    job = qone("SELECT * FROM scrape_jobs WHERE id=? AND enabled=1", (job_id,))
    if not job:
        return -1
    run_id = exe("INSERT INTO job_runs (job_id) VALUES (?)", (job_id,))
    t = threading.Thread(target=_do_run, args=(run_id, job), daemon=True)
    t.start()
    return run_id


def _refresh_scheduler():
    for j in scheduler.get_jobs():
        scheduler.remove_job(j.id)
    for job in qall("SELECT * FROM scrape_jobs WHERE enabled=1"):
        scheduler.add_job(
            _start_run, CronTrigger(hour=job["schedule_hour"], minute=0),
            args=[job["id"]], id=f"j{job['id']}", replace_existing=True,
        )

# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get("/")
def index():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/static/{filename}")
def static_file(filename: str):
    p = STATIC_DIR / filename
    if not p.exists():
        raise HTTPException(404, "Not found")
    return FileResponse(str(p))


@app.get("/api/stats")
def stats():
    return {
        "total_jobs": (qone("SELECT COUNT(*) c FROM scrape_jobs") or {}).get("c", 0),
        "total_rows": (qone("SELECT COUNT(*) c FROM log_events")  or {}).get("c", 0),
        "recent_runs": qall(
            "SELECT jr.id, jr.status, jr.rows_saved, jr.started_at, jr.finished_at, sj.name "
            "FROM job_runs jr JOIN scrape_jobs sj ON jr.job_id=sj.id "
            "ORDER BY jr.id DESC LIMIT 8"
        ),
    }


@app.get("/api/jobs")
def list_jobs():
    jobs = qall("SELECT * FROM scrape_jobs ORDER BY created_at DESC")
    for j in jobs:
        last = qone("SELECT id, status FROM job_runs WHERE job_id=? ORDER BY id DESC LIMIT 1", (j["id"],))
        j["is_running"] = bool(last and _run_status.get(last["id"]) == "running")
    return jobs


@app.post("/api/jobs", status_code=201)
def create_job(job: JobIn):
    jid = exe(
        "INSERT INTO scrape_jobs (name,kibana_url,time_range,max_pages,schedule_hour,enabled,chunk_hours) "
        "VALUES (?,?,?,?,?,?,?)",
        (job.name, job.kibana_url, job.time_range, job.max_pages,
         job.schedule_hour, int(job.enabled), job.chunk_hours),
    )
    _refresh_scheduler()
    return {"id": jid}


@app.put("/api/jobs/{jid}")
def update_job(jid: int, job: JobIn):
    exe("UPDATE scrape_jobs SET name=?,kibana_url=?,time_range=?,max_pages=?,"
        "schedule_hour=?,enabled=?,chunk_hours=? WHERE id=?",
        (job.name, job.kibana_url, job.time_range, job.max_pages,
         job.schedule_hour, int(job.enabled), job.chunk_hours, jid))
    _refresh_scheduler()
    return {"ok": True}


@app.get("/api/jobs/{jid}/chunks/preview")
def preview_chunks(jid: int):
    """Return how many time-windows the next full-scan run would create."""
    job = qone("SELECT time_range, chunk_hours FROM scrape_jobs WHERE id=?", (jid,))
    if not job:
        raise HTTPException(404, "Job not found")
    ch = int(job.get("chunk_hours") or 0)
    if ch == 0:
        return {"chunk_hours": 0, "chunks": 1, "windows": []}
    windows = _build_chunks(job["time_range"], ch)
    return {"chunk_hours": ch, "chunks": len(windows), "windows": windows}


@app.delete("/api/jobs/{jid}/data")
def clear_job_data(jid: int):
    """Clear all log data for a job without deleting the job itself."""
    try:
        exe("DELETE FROM log_correlations WHERE log_id IN (SELECT id FROM log_events WHERE job_id=?)", (jid,))
        exe("DELETE FROM log_correlations WHERE corr_id IN (SELECT id FROM log_events WHERE job_id=?)", (jid,))
    except Exception:
        pass
    exe("DELETE FROM log_events WHERE job_id=?", (jid,))
    exe("DELETE FROM job_runs WHERE job_id=?", (jid,))
    exe("UPDATE scrape_jobs SET last_run_at=NULL, last_run_status=NULL, last_rows_saved=0 WHERE id=?", (jid,))
    return {"ok": True}


@app.delete("/api/jobs/{jid}")
def delete_job(jid: int):
    """Delete a job and ALL related data: events, runs, group memberships, correlations."""
    try:
        exe("DELETE FROM log_correlations WHERE log_id IN (SELECT id FROM log_events WHERE job_id=?)", (jid,))
        exe("DELETE FROM log_correlations WHERE corr_id IN (SELECT id FROM log_events WHERE job_id=?)", (jid,))
    except Exception:
        pass
    try:
        exe("DELETE FROM log_group_keys WHERE job_id_a=? OR job_id_b=?", (jid, jid))
        exe("DELETE FROM log_group_members WHERE job_id=?", (jid,))
    except Exception:
        pass
    exe("DELETE FROM log_events WHERE job_id=?", (jid,))
    exe("DELETE FROM job_runs WHERE job_id=?", (jid,))
    exe("DELETE FROM scrape_jobs WHERE id=?", (jid,))
    _refresh_scheduler()
    return {"ok": True}


@app.post("/api/jobs/{jid}/run")
def trigger_run(jid: int):
    job = qone("SELECT * FROM scrape_jobs WHERE id=?", (jid,))
    if not job:
        raise HTTPException(404, "Job not found")
    run_id = exe("INSERT INTO job_runs (job_id) VALUES (?)", (jid,))
    t = threading.Thread(target=_do_run, args=(run_id, job), daemon=True)
    t.start()
    return {"run_id": run_id}


@app.get("/api/runs/{run_id}/stream")
async def stream_run(run_id: int):
    async def gen():
        pos = 0
        yield f"data: {json.dumps({'type':'connected'})}\n\n"
        while True:
            lines = _run_logs.get(run_id)
            if lines is not None:
                while pos < len(lines):
                    yield f"data: {json.dumps({'type':'log','line':lines[pos]})}\n\n"
                    pos += 1
                st = _run_status.get(run_id)
                if st and st != "running":
                    yield f"data: {json.dumps({'type':'done','status':st})}\n\n"
                    return
            else:
                row = qone("SELECT * FROM job_runs WHERE id=?", (run_id,))
                if row and row["status"] != "running":
                    for line in (row["log_output"] or "").split("\n"):
                        yield f"data: {json.dumps({'type':'log','line':line})}\n\n"
                    yield f"data: {json.dumps({'type':'done','status':row['status']})}\n\n"
                    return
            await asyncio.sleep(0.15)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/jobs/{jid}/runs")
def job_runs(jid: int):
    return qall(
        "SELECT id, started_at, finished_at, status, rows_saved "
        "FROM job_runs WHERE job_id=? ORDER BY id DESC LIMIT 30", (jid,)
    )


@app.get("/api/jobs/{jid}/fields")
def job_fields(jid: int):
    """Return sorted list of all field names found in raw_json for this job."""
    rows = qall("SELECT raw_json FROM log_events WHERE job_id=? LIMIT 30", (jid,))
    fields: set[str] = set()
    for row in rows:
        rj = row.get("raw_json")
        if not rj:
            continue
        data = rj if isinstance(rj, dict) else json.loads(rj)
        fields.update(data.keys())
    first = ["@timestamp"] if "@timestamp" in fields else []
    others = sorted(f for f in fields if f != "@timestamp")
    return first + others


@app.get("/api/jobs/{jid}/group-fields")
def get_group_fields(jid: int):
    """Return field lists for all jobs linked to this job via correlation groups."""
    _ensure_groups_schema()
    groups = qall("SELECT group_id FROM log_group_members WHERE job_id=?", (jid,)) or []
    if not groups:
        return {"linked": []}
    group_ids = [g['group_id'] for g in groups]
    fmt = ','.join(['?'] * len(group_ids))
    members = qall(
        f"SELECT lgm.job_id, lgm.label, sj.name AS job_name "
        f"FROM log_group_members lgm JOIN scrape_jobs sj ON sj.id=lgm.job_id "
        f"WHERE lgm.group_id IN ({fmt}) AND lgm.job_id != ?",
        tuple(group_ids) + (jid,)
    ) or []
    linked = []
    seen_jobs = set()
    for m in members:
        if m['job_id'] in seen_jobs:
            continue
        seen_jobs.add(m['job_id'])
        sample = qall(
            "SELECT raw_json FROM log_events WHERE job_id=? AND raw_json IS NOT NULL LIMIT 200",
            (m['job_id'],)
        ) or []
        fields = set()
        for r in sample:
            rj = r['raw_json']
            if isinstance(rj, str):
                try: rj = json.loads(rj)
                except: continue
            if isinstance(rj, dict):
                fields.update(rj.keys())
        linked.append({
            'job_id': m['job_id'],
            'label': m.get('label') or m.get('job_name'),
            'fields': sorted(fields)
        })
    return {"linked": linked}


class BatchCorrRequest(BaseModel):
    ids: list[int]

@app.post("/api/logs/correlated-batch")
def get_correlated_batch(req: BatchCorrRequest):
    """Return precomputed correlated records for multiple log IDs in one call."""
    _ensure_correlations_schema()
    if not req.ids:
        return {}
    fmt = ','.join(['?'] * len(req.ids))
    links = qall(
        f"SELECT lc.log_id, lc.corr_id, lc.source_label, lc.matched_keys, "
        f"le.timestamp, le.relevance, le.severity, le.ai_summary, le.raw_json "
        f"FROM log_correlations lc "
        f"JOIN log_events le ON le.id = lc.corr_id "
        f"WHERE lc.log_id IN ({fmt})",
        tuple(req.ids)
    ) or []
    result = {}
    for lnk in links:
        lid = lnk['log_id']
        if lid not in result:
            result[lid] = []
        mk = lnk.get('matched_keys') or []
        if isinstance(mk, str):
            try: mk = json.loads(mk)
            except: mk = []
        rj = lnk['raw_json']
        if isinstance(rj, str):
            try: rj = json.loads(rj)
            except: rj = {}
        result[lid].append({
            'id':            lnk['corr_id'],
            'timestamp':     str(lnk['timestamp'] or ''),
            'relevance':     lnk['relevance'],
            'severity':      lnk['severity'],
            'ai_summary':    lnk['ai_summary'],
            'raw_json':      rj,
            '_source_label': lnk['source_label'],
            '_matched_keys': mk,
        })
    return {str(k): v for k, v in result.items()}


@app.get("/api/jobs/{jid}/data")
def job_data(
    jid: int,
    page: int = 1,
    per_page: int = 100,
    search: str = "",
    from_date: str = "",
    to_date: str = "",
    relevance: str = "",
    regex: bool = False,
    sort_by: str = "timestamp",
    sort_dir: str = "desc",
    field_filters: str = "",   # JSON: [{"field":"x","value":"y","op":"eq"|"neq"}]
):
    import re as _re2
    offset = (page - 1) * per_page
    conditions: list[str] = ["job_id=?"]
    args: tuple = (jid,)

    if search:
        if regex:
            conditions.append("CAST(raw_json AS TEXT) REGEXP ?")
            args += (search,)
        else:
            conditions.append("raw_json LIKE ?")
            args += (f"%{search}%",)
    if from_date:
        conditions.append("timestamp >= ?")
        args += (from_date,)
    if to_date:
        conditions.append("timestamp <= ?")
        args += (to_date,)
    if relevance:
        conditions.append("relevance=?")
        args += (relevance,)

    # Field-specific filters: [{"field":"http_status_code","value":"500","op":"eq"}]
    if field_filters:
        try:
            ff_list = json.loads(field_filters)
            for ff in ff_list:
                field = ff.get("field", "")
                value = ff.get("value", "")
                op    = ff.get("op", "eq")  # "eq" or "neq"
                if not field or not _re2.match(r'^[A-Za-z0-9_.@\-]+$', field):
                    continue  # safety: reject weird field names
                json_path0 = f"$.{field}[0]"
                json_path  = f"$.{field}"
                # Values may be stored as scalars OR as single-element arrays [v].
                # CAST to TEXT so numeric JSON values (e.g. 500) match string filters ("500").
                coalesce = "CAST(COALESCE(json_extract(raw_json, ?), json_extract(raw_json, ?)) AS TEXT)"
                if op == "neq":
                    conditions.append(
                        f"({coalesce} != ? OR json_extract(raw_json, ?) IS NULL)"
                    )
                    args += (json_path0, json_path, value, json_path)
                else:
                    conditions.append(f"{coalesce} = ?")
                    args += (json_path0, json_path, value)
        except Exception:
            pass

    # Build ORDER BY — real columns vs JSON-extracted fields
    _REAL_COLS = {"timestamp", "severity", "relevance", "id", "scraped_at"}
    dir_sql = "ASC" if sort_dir.lower() == "asc" else "DESC"
    if sort_by in _REAL_COLS:
        order_sql = f"{sort_by} {dir_sql}"
    else:
        import re as _re
        if _re.match(r'^[A-Za-z0-9_.\-]+$', sort_by):
            order_sql = f"json_extract(raw_json, '$.{sort_by}') {dir_sql}, timestamp DESC"
        else:
            order_sql = "timestamp DESC"

    where = " AND ".join(conditions)
    total = (qone(f"SELECT COUNT(*) c FROM log_events WHERE {where}", args) or {}).get("c", 0)
    rows  = qall(
        f"SELECT id, timestamp, run_id, raw_json, scraped_at, "
        f"relevance, severity, ai_summary, code_refs "
        f"FROM log_events WHERE {where} ORDER BY {order_sql} LIMIT ? OFFSET ?",
        args + (per_page, offset),
    )
    return {"rows": rows, "total": total, "page": page, "per_page": per_page}


@app.get("/api/jobs/{jid}/analyze/stream")
async def analyze_stream(jid: int, limit: int = Query(default=30, le=100)):
    """Stream AI analysis progress for unanalyzed logs in this job."""
    async def gen():
        yield f"data: {json.dumps({'type':'start'})}\n\n"

        rows = qall(
            "SELECT id, raw_json FROM log_events "
            "WHERE job_id=? AND (relevance='unchecked' OR relevance IS NULL) "
            "ORDER BY timestamp DESC LIMIT ?",
            (jid, limit),
        )
        if not rows:
            yield f"data: {json.dumps({'type':'done','analyzed':0,'message':'All logs already analyzed'})}\n\n"
            return

        total        = len(rows)
        analyzed     = 0
        relevant_ct  = 0
        batch_size   = 10
        github_token = os.environ.get("GITHUB_TOKEN", "")

        for i in range(0, total, batch_size):
            batch = rows[i : i + batch_size]
            yield f"data: {json.dumps({'type':'progress','done':analyzed,'total':total})}\n\n"

            results = analyze_batch(batch)

            for res in results:
                idx = res.get("id")
                if idx is None or idx >= len(batch):
                    continue
                row_id   = batch[idx]["id"]
                relevant = bool(res.get("relevant"))
                severity = res.get("severity", "info")
                summary  = res.get("summary", "")
                terms    = res.get("github_terms", []) if relevant else []

                code_refs = search_github(terms, github_token) if terms else []
                if relevant:
                    relevant_ct += 1

                exe(
                    "UPDATE log_events SET relevance=?, severity=?, "
                    "ai_summary=?, code_refs=? WHERE id=?",
                    ("relevant" if relevant else "irrelevant",
                     severity, summary, json.dumps(code_refs), row_id),
                )

            analyzed += len(batch)
            yield f"data: {json.dumps({'type':'progress','done':analyzed,'total':total})}\n\n"
            await asyncio.sleep(0.05)

        yield f"data: {json.dumps({'type':'done','analyzed':analyzed,'relevant':relevant_ct,'total':total})}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ─── Auto-classify endpoint ────────────────────────────────────────────────────

def _extract_ctx(raw_json) -> tuple:
    """Extract (message, error_level, store_id, domain) from a raw_json dict."""
    if isinstance(raw_json, str):
        try:
            raw_json = json.loads(raw_json)
        except Exception:
            raw_json = {}

    def _f(key):
        v = raw_json.get(key, "")
        if isinstance(v, list): v = v[0] if v else ""
        return str(v or "").strip()[:300]

    msg = _f("message")
    err = _f("error_level").lower()
    sid = _f("store_id")
    dom = _f("domain")
    return (msg, err, sid, dom)


def _ctx_keys(msg, err, sid, dom):
    """Return lookup keys from most-specific to least-specific."""
    return [
        (msg, err, sid, dom),
        (msg, err, sid, ""),
        (msg, err, "", dom),
        (msg, err, "", ""),
        (msg, "", "", ""),
    ]


@app.get("/api/jobs/{jid}/auto-classify/stream")
async def auto_classify_stream(jid: int):
    """
    SSE stream: build lookup from analyzed records, then apply to unchecked ones.
    Uses (message, error_level, store_id, domain) composite key with fallback tiers.
    """
    async def gen():
        yield f"data: {json.dumps({'type':'start'})}\n\n"

        analyzed_rows = qall(
            "SELECT raw_json, relevance, severity FROM log_events "
            "WHERE job_id=? AND relevance IS NOT NULL AND relevance != 'unchecked'",
            (jid,),
        )

        lookup: dict = {}
        for row in analyzed_rows:
            rel = row.get("relevance")
            if rel not in ("relevant", "irrelevant"):
                continue
            sev = row.get("severity") or "info"
            msg, err, sid, dom = _extract_ctx(row.get("raw_json"))
            if not msg:
                continue
            key = (msg, err, sid, dom)
            if key not in lookup:
                lookup[key] = {"relevant": 0, "irrelevant": 0, "severities": []}
            lookup[key][rel] += 1
            lookup[key]["severities"].append(sev)

        yield f"data: {json.dumps({'type':'lookup_built','patterns': len(lookup)})}\n\n"

        if not lookup:
            yield f"data: {json.dumps({'type':'done','classified':0,'skipped':0,'message':'No analyzed records to learn from'})}\n\n"
            return

        unchecked = qall(
            "SELECT id, raw_json FROM log_events "
            "WHERE job_id=? AND (relevance IS NULL OR relevance='unchecked') "
            "ORDER BY timestamp DESC",
            (jid,),
        )
        total = len(unchecked)
        if total == 0:
            yield f"data: {json.dumps({'type':'done','classified':0,'skipped':0,'message':'No unchecked records'})}\n\n"
            return

        yield f"data: {json.dumps({'type':'progress','done':0,'total':total})}\n\n"

        SEVER_ORDER = {"critical": 4, "high": 3, "medium": 2, "low": 1, "info": 0}

        classified = 0
        skipped    = 0
        batch_ids_rel  = []
        batch_ids_irr  = []
        batch_sev: dict[int, str] = {}

        for i, row in enumerate(unchecked):
            msg, err, sid, dom = _extract_ctx(row.get("raw_json"))
            entry = None
            for key in _ctx_keys(msg, err, sid, dom):
                if key in lookup:
                    entry = lookup[key]
                    break

            if entry is None:
                skipped += 1
            else:
                rel = "relevant" if entry["relevant"] >= entry["irrelevant"] else "irrelevant"
                sevs = entry["severities"]
                sev = max(set(sevs), key=lambda s: (sevs.count(s), SEVER_ORDER.get(s, 0)))

                rid = row["id"]
                if rel == "relevant":
                    batch_ids_rel.append(rid)
                else:
                    batch_ids_irr.append(rid)
                batch_sev[rid] = sev
                classified += 1

            if (i + 1) % 100 == 0:
                yield f"data: {json.dumps({'type':'progress','done':i+1,'total':total})}\n\n"
                await asyncio.sleep(0.01)

        def _update_batch(ids, rel_val):
            if not ids:
                return
            for rid in ids:
                sev = batch_sev.get(rid, "info")
                exe(
                    "UPDATE log_events SET relevance=?, severity=? "
                    "WHERE id=? AND (relevance IS NULL OR relevance='unchecked')",
                    (rel_val, sev, rid),
                )

        _update_batch(batch_ids_rel, "relevant")
        _update_batch(batch_ids_irr, "irrelevant")

        yield f"data: {json.dumps({'type':'done','classified':classified,'skipped':skipped,'total':total})}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


class GroupCreate(BaseModel):
    name: str
    description: str = ""

class GroupMemberReq(BaseModel):
    job_id: int
    label: str = ""

class GroupKeyReq(BaseModel):
    job_id_a: int
    field_a: str
    job_id_b: int
    field_b: str


def _ensure_groups_schema():
    exe("""CREATE TABLE IF NOT EXISTS log_groups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT DEFAULT '',
        collection_id INTEGER DEFAULT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )""")
    exe("""CREATE TABLE IF NOT EXISTS log_group_members (
        group_id INTEGER NOT NULL,
        job_id INTEGER NOT NULL,
        label TEXT DEFAULT '',
        PRIMARY KEY (group_id, job_id)
    )""")
    exe("""CREATE TABLE IF NOT EXISTS log_group_keys (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id INTEGER NOT NULL,
        job_id_a INTEGER NOT NULL,
        field_a TEXT NOT NULL,
        job_id_b INTEGER NOT NULL,
        field_b TEXT NOT NULL,
        confidence REAL DEFAULT 0.0,
        source TEXT DEFAULT 'auto'
    )""")


# ─── Collections (multi-source jobs) ─────────────────────────────────────────

class CollectionSourceIn(BaseModel):
    label:      str
    kibana_url: str

class CollectionIn(BaseModel):
    name:         str
    time_range:   str  = "Last 7 days"
    chunk_hours:  int  = 0
    schedule_hour:int  = 2
    enabled:      bool = True
    sources:      list[CollectionSourceIn] = []
    corr_keys:    list[str] = []


def _collection_detail(cid: int) -> dict | None:
    c = qone("SELECT * FROM scrape_collections WHERE id=?", (cid,))
    if not c:
        return None
    c["sources"] = qall(
        "SELECT cs.*, sj.last_run_at, sj.last_run_status, sj.last_rows_saved, "
        "       (sj.last_run_status = 'running') AS is_running "
        "FROM collection_sources cs "
        "LEFT JOIN scrape_jobs sj ON sj.id = cs.job_id "
        "WHERE cs.collection_id=? ORDER BY cs.sort_order, cs.id", (cid,)
    ) or []
    c["corr_keys"] = [r["field_name"] for r in
                      qall("SELECT field_name FROM collection_corr_keys WHERE collection_id=? ORDER BY id", (cid,))]
    return c


def _ensure_source_job(src: dict, coll: dict) -> int:
    """Get or create a scrape_jobs row for this collection source. Returns job_id."""
    if src.get("job_id"):
        exe("UPDATE scrape_jobs SET name=?, kibana_url=?, time_range=?, chunk_hours=?, "
            "schedule_hour=?, enabled=? WHERE id=?",
            (src["label"], src["kibana_url"], coll["time_range"],
             coll.get("chunk_hours", 0), coll.get("schedule_hour", 2),
             int(coll.get("enabled", True)), src["job_id"]))
        return src["job_id"]
    jid = exe(
        "INSERT INTO scrape_jobs (name,kibana_url,time_range,max_pages,schedule_hour,enabled,chunk_hours) "
        "VALUES (?,?,?,9999,?,0,?)",
        (src["label"], src["kibana_url"], coll["time_range"],
         coll.get("schedule_hour", 2), coll.get("chunk_hours", 0)),
    )
    exe("UPDATE collection_sources SET job_id=? WHERE id=?", (jid, src["id"]))
    return jid


def _sync_collection_group(cid: int, name: str, job_ids: list[int], keys: list[str]):
    """Ensure a log_group exists for this collection and sync members + keys."""
    _ensure_groups_schema()
    g = qone("SELECT id FROM log_groups WHERE collection_id=?", (cid,))
    if g:
        gid = g["id"]
    else:
        gid = exe("INSERT INTO log_groups (name, collection_id) VALUES (?,?)", (name, cid))

    existing_members = {r["job_id"] for r in
                        qall("SELECT job_id FROM log_group_members WHERE group_id=?", (gid,))}
    for jid in job_ids:
        if jid not in existing_members:
            exe("INSERT INTO log_group_members (group_id,job_id,label) "
                "SELECT ?,?,name FROM scrape_jobs WHERE id=?", (gid, jid, jid))
    for jid in existing_members - set(job_ids):
        exe("DELETE FROM log_group_members WHERE group_id=? AND job_id=?", (gid, jid))

    exe("DELETE FROM log_group_keys WHERE group_id=?", (gid,))
    from itertools import combinations as _combos
    for ja, jb in _combos(job_ids, 2):
        for field in keys:
            exe("INSERT INTO log_group_keys (group_id,job_id_a,field_a,job_id_b,field_b,confidence,source) "
                "VALUES (?,?,?,?,?,1.0,'manual')", (gid, ja, field, jb, field))
    return gid


@app.get("/api/collections")
def list_collections():
    rows = qall("SELECT id FROM scrape_collections ORDER BY id") or []
    return [_collection_detail(r["id"]) for r in rows]


@app.post("/api/collections", status_code=201)
def create_collection(body: CollectionIn):
    cid = exe(
        "INSERT INTO scrape_collections (name,time_range,chunk_hours,schedule_hour,enabled) "
        "VALUES (?,?,?,?,?)",
        (body.name, body.time_range, body.chunk_hours, body.schedule_hour, int(body.enabled)),
    )
    job_ids = []
    for i, src in enumerate(body.sources):
        src_id = exe(
            "INSERT INTO collection_sources (collection_id,label,kibana_url,sort_order) VALUES (?,?,?,?)",
            (cid, src.label, src.kibana_url, i),
        )
        coll_dict = {"time_range": body.time_range, "chunk_hours": body.chunk_hours,
                     "schedule_hour": body.schedule_hour, "enabled": body.enabled}
        jid = _ensure_source_job({"label": src.label, "kibana_url": src.kibana_url, "id": src_id, "job_id": None}, coll_dict)
        job_ids.append(jid)
    for field in body.corr_keys:
        exe("INSERT INTO collection_corr_keys (collection_id,field_name) VALUES (?,?)", (cid, field))
    if len(job_ids) >= 2:
        _sync_collection_group(cid, body.name, job_ids, body.corr_keys)
    _refresh_scheduler()
    return {"id": cid}


@app.put("/api/collections/{cid}")
def update_collection(cid: int, body: CollectionIn):
    exe("UPDATE scrape_collections SET name=?,time_range=?,chunk_hours=?,schedule_hour=?,enabled=? WHERE id=?",
        (body.name, body.time_range, body.chunk_hours, body.schedule_hour, int(body.enabled), cid))

    existing_srcs = {r["id"]: r for r in
                     qall("SELECT * FROM collection_sources WHERE collection_id=?", (cid,))}
    job_ids = []
    coll_dict = {"time_range": body.time_range, "chunk_hours": body.chunk_hours,
                 "schedule_hour": body.schedule_hour, "enabled": body.enabled}
    new_src_ids = set()
    for i, src in enumerate(body.sources):
        src_id = exe(
            "INSERT INTO collection_sources (collection_id,label,kibana_url,sort_order) VALUES (?,?,?,?)",
            (cid, src.label, src.kibana_url, i),
        )
        new_src_ids.add(src_id)
        jid = _ensure_source_job({"label": src.label, "kibana_url": src.kibana_url, "id": src_id, "job_id": None}, coll_dict)
        job_ids.append(jid)
    for old_id, old_src in existing_srcs.items():
        if old_id not in new_src_ids:
            if old_src.get("job_id"):
                exe("DELETE FROM scrape_jobs WHERE id=?", (old_src["job_id"],))
            exe("DELETE FROM collection_sources WHERE id=?", (old_id,))

    exe("DELETE FROM collection_corr_keys WHERE collection_id=?", (cid,))
    for field in body.corr_keys:
        exe("INSERT INTO collection_corr_keys (collection_id,field_name) VALUES (?,?)", (cid, field))

    if len(job_ids) >= 2:
        _sync_collection_group(cid, body.name, job_ids, body.corr_keys)
    _refresh_scheduler()
    return {"ok": True}


@app.delete("/api/collections/{cid}")
def delete_collection(cid: int):
    srcs = qall("SELECT job_id FROM collection_sources WHERE collection_id=?", (cid,))
    for s in (srcs or []):
        if s["job_id"]:
            try:
                exe("DELETE FROM log_correlations WHERE log_id IN (SELECT id FROM log_events WHERE job_id=?)", (s["job_id"],))
            except: pass
            exe("DELETE FROM log_events WHERE job_id=?", (s["job_id"],))
            exe("DELETE FROM job_runs WHERE job_id=?", (s["job_id"],))
            exe("DELETE FROM scrape_jobs WHERE id=?", (s["job_id"],))
    exe("DELETE FROM collection_sources WHERE collection_id=?", (cid,))
    exe("DELETE FROM collection_corr_keys WHERE collection_id=?", (cid,))
    g = qone("SELECT id FROM log_groups WHERE collection_id=?", (cid,))
    if g:
        exe("DELETE FROM log_group_keys WHERE group_id=?", (g["id"],))
        exe("DELETE FROM log_group_members WHERE group_id=?", (g["id"],))
        exe("DELETE FROM log_groups WHERE id=?", (g["id"],))
    exe("DELETE FROM scrape_collections WHERE id=?", (cid,))
    return {"ok": True}


@app.delete("/api/collections/{cid}/data")
def clear_collection_data(cid: int):
    """Clear all log data for every source in a collection, keeping jobs & config intact."""
    srcs = qall("SELECT job_id FROM collection_sources WHERE collection_id=?", (cid,)) or []
    for s in srcs:
        jid = s.get("job_id")
        if not jid:
            continue
        try:
            exe("DELETE FROM log_correlations WHERE log_id IN (SELECT id FROM log_events WHERE job_id=?)", (jid,))
            exe("DELETE FROM log_correlations WHERE corr_id IN (SELECT id FROM log_events WHERE job_id=?)", (jid,))
        except Exception:
            pass
        exe("DELETE FROM log_events WHERE job_id=?", (jid,))
        exe("DELETE FROM job_runs WHERE job_id=?", (jid,))
        exe("UPDATE scrape_jobs SET last_run_at=NULL, last_run_status=NULL, last_rows_saved=0 WHERE id=?", (jid,))
    exe("UPDATE scrape_collections SET last_run_at=NULL, last_run_status=NULL WHERE id=?", (cid,))
    return {"ok": True}


@app.post("/api/collections/{cid}/run")
def run_collection(cid: int):
    """Start a run for all sources in this collection, sequentially."""
    coll = qone("SELECT * FROM scrape_collections WHERE id=?", (cid,))
    if not coll:
        raise HTTPException(404, "Collection not found")
    srcs = qall("SELECT * FROM collection_sources WHERE collection_id=? ORDER BY sort_order, id", (cid,))
    if not srcs:
        raise HTTPException(400, "No sources configured")

    first_job_id = srcs[0]["job_id"] if srcs[0].get("job_id") else None
    if not first_job_id:
        raise HTTPException(400, "Sources not linked to jobs — save the collection first")

    run_id = exe("INSERT INTO job_runs (job_id) VALUES (?)", (first_job_id,))
    _run_logs[run_id]   = []
    _run_status[run_id] = "running"

    def _do_collection_run():
        _run_logs[run_id].append(f"[→] Collection: {coll['name']} — {len(srcs)} sources")
        any_failed = False
        total_saved = 0
        ok = False

        for i, src in enumerate(srcs, 1):
            jid = src.get("job_id")
            if not jid:
                _run_logs[run_id].append(f"[!] Source '{src['label']}' has no job_id — skipping")
                continue
            job = qone("SELECT * FROM scrape_jobs WHERE id=?", (jid,))
            if not job:
                continue

            _run_logs[run_id].append(f"\n{'━'*50}")
            _run_logs[run_id].append(f"[→] Source {i}/{len(srcs)}: {src['label']}")
            _run_logs[run_id].append(f"{'━'*50}")
            exe("UPDATE scrape_jobs SET last_run_status='running' WHERE id=?", (jid,))

            rows_ref = [0]
            chunk_hours = int(coll.get("chunk_hours") or 0)
            last_ts = _last_timestamp(jid)

            if last_ts:
                from_dt = last_ts.strftime("%Y-%m-%dT%H:%M:%S") if hasattr(last_ts, "strftime") \
                          else str(last_ts).replace(" ", "T")[:19]
                to_dt   = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                _run_logs[run_id].append(f"[→] Incremental: {from_dt} → {to_dt}")
                ok = _run_one_window(run_id, job, ["--from", from_dt, "--to", to_dt], rows_ref)
                if not ok: any_failed = True
            elif chunk_hours > 0:
                windows = _build_chunks(coll["time_range"], chunk_hours)
                _run_logs[run_id].append(f"[→] Chunked: {len(windows)} × {chunk_hours}h windows")
                chunk_saved = [0]
                for wi, (w_from, w_to) in enumerate(windows, 1):
                    _run_logs[run_id].append(f"\n  ── chunk {wi}/{len(windows)}: {w_from} → {w_to}")
                    chunk_saved[0] = 0
                    ok = _run_one_window(run_id, job, ["--from", w_from, "--to", w_to], chunk_saved)
                    rows_ref[0] += chunk_saved[0]
                    if ok:
                        _run_logs[run_id].append(f"  [✓] {chunk_saved[0]} rows")
                    else:
                        _run_logs[run_id].append(f"  [✗] chunk failed")
                        any_failed = True
                    if wi < len(windows):
                        time.sleep(5)
            else:
                _run_logs[run_id].append(f"[→] Full scan: {coll['time_range']}")
                ok = _run_one_window(run_id, job, ["--time", coll["time_range"]], rows_ref)
                if not ok: any_failed = True

            total_saved += rows_ref[0]
            src_status = "success" if ok else "failed"
            _run_logs[run_id].append(f"[✓] Source {i} done — {rows_ref[0]} rows ({src_status})")

            now_src = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            exe("UPDATE scrape_jobs SET last_run_at=?, last_run_status=?, last_rows_saved=? WHERE id=?",
                (now_src, src_status, rows_ref[0], jid))
            src_run_log = "\n".join(
                l for l in _run_logs[run_id]
                if l.startswith(f"[→] Source {i}/") or "chunk" in l.lower() or "rows" in l.lower()
            )
            exe("INSERT INTO job_runs (job_id, status, finished_at, rows_saved, log_output) "
                "VALUES (?, ?, ?, ?, ?)",
                (jid, src_status, now_src, rows_ref[0], src_run_log))

        # Auto-build correlations if group exists
        g = qone("SELECT id FROM log_groups WHERE collection_id=?", (cid,))
        if g and not any_failed:
            _run_logs[run_id].append(f"\n[→] Auto-building correlations for group {g['id']}…")
            try:
                job_ids = [s["job_id"] for s in srcs if s.get("job_id")]
                keys = [r["field_name"] for r in
                        qall("SELECT field_name FROM collection_corr_keys WHERE collection_id=?", (cid,))]
                if len(job_ids) >= 2 and keys:
                    _run_logs[run_id].append(f"  Keys: {', '.join(keys)}")
                    _run_logs[run_id].append("  [✓] Correlations queued — use 'Build Correlations' in Groups for full rebuild")
            except Exception as e:
                _run_logs[run_id].append(f"  [!] Correlation hint failed: {e}")

        status = "failed" if any_failed else "success"
        _run_status[run_id] = status
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log = "\n".join(_run_logs[run_id])
        exe("UPDATE job_runs SET status=?,finished_at=?,rows_saved=?,log_output=? WHERE id=?",
            (status, now, total_saved, log, run_id))
        exe("UPDATE scrape_collections SET last_run_at=?,last_run_status=? WHERE id=?",
            (now, status, cid))

    t = threading.Thread(target=_do_collection_run, daemon=True)
    t.start()
    return {"run_id": run_id, "collection_id": cid}


@app.get("/api/groups")
def list_groups():
    _ensure_groups_schema()
    groups = qall("SELECT * FROM log_groups ORDER BY id") or []
    for g in groups:
        g['members'] = qall(
            "SELECT lgm.job_id, lgm.label, sj.name AS job_name "
            "FROM log_group_members lgm JOIN scrape_jobs sj ON sj.id=lgm.job_id "
            "WHERE lgm.group_id=?", (g['id'],)
        ) or []
        g['keys'] = qall(
            "SELECT lgk.*, sa.name AS job_name_a, sb.name AS job_name_b "
            "FROM log_group_keys lgk "
            "LEFT JOIN scrape_jobs sa ON sa.id=lgk.job_id_a "
            "LEFT JOIN scrape_jobs sb ON sb.id=lgk.job_id_b "
            "WHERE lgk.group_id=? ORDER BY lgk.confidence DESC", (g['id'],)
        ) or []
    return groups

@app.post("/api/groups")
def create_group(req: GroupCreate):
    _ensure_groups_schema()
    gid = exe("INSERT INTO log_groups (name, description) VALUES (?,?)",
              (req.name, req.description))
    return {"id": gid, "name": req.name}

@app.delete("/api/groups/{gid}")
def delete_group(gid: int):
    exe("DELETE FROM log_group_keys WHERE group_id=?", (gid,))
    exe("DELETE FROM log_group_members WHERE group_id=?", (gid,))
    exe("DELETE FROM log_groups WHERE id=?", (gid,))
    return {"ok": True}

@app.post("/api/groups/{gid}/members")
def add_group_member(gid: int, req: GroupMemberReq):
    _ensure_groups_schema()
    exe("INSERT OR IGNORE INTO log_group_members (group_id, job_id, label) VALUES (?,?,?)",
        (gid, req.job_id, req.label))
    return {"ok": True}

@app.delete("/api/groups/{gid}/members/{job_id}")
def remove_group_member(gid: int, job_id: int):
    exe("DELETE FROM log_group_members WHERE group_id=? AND job_id=?", (gid, job_id))
    return {"ok": True}

@app.post("/api/groups/{gid}/keys")
def add_group_key(gid: int, req: GroupKeyReq):
    _ensure_groups_schema()
    kid = exe(
        "INSERT INTO log_group_keys (group_id,job_id_a,field_a,job_id_b,field_b,confidence,source) "
        "VALUES (?,?,?,?,?,1.0,'manual')",
        (gid, req.job_id_a, req.field_a, req.job_id_b, req.field_b)
    )
    return {"id": kid}

@app.delete("/api/groups/{gid}/keys/{kid}")
def delete_group_key(gid: int, kid: int):
    exe("DELETE FROM log_group_keys WHERE id=? AND group_id=?", (kid, gid))
    return {"ok": True}


# ─── Pre-computed correlations ─────────────────────────────────────────────────

def _ensure_correlations_schema():
    exe("""CREATE TABLE IF NOT EXISTS log_correlations (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        log_id       INTEGER NOT NULL,
        corr_id      INTEGER NOT NULL,
        group_id     INTEGER NOT NULL,
        source_label TEXT DEFAULT '',
        matched_keys TEXT DEFAULT NULL,
        UNIQUE (log_id, corr_id, group_id)
    )""")
    exe("CREATE INDEX IF NOT EXISTS idx_corr_log_id ON log_correlations (log_id)")
    exe("CREATE INDEX IF NOT EXISTS idx_corr_corr_id ON log_correlations (corr_id)")


@app.get("/api/groups/{gid}/build-correlations/stream")
async def build_correlations_stream(gid: int):
    """
    SSE: pre-compute ALL correlations for this group and store in log_correlations.
    Uses AND logic — all keys for a job-pair must match.
    """
    async def gen():
        _ensure_groups_schema()
        _ensure_correlations_schema()
        yield f"data: {json.dumps({'type':'start'})}\n\n"

        members = qall(
            "SELECT lgm.job_id, lgm.label, sj.name AS job_name "
            "FROM log_group_members lgm JOIN scrape_jobs sj ON sj.id=lgm.job_id "
            "WHERE lgm.group_id=?", (gid,)
        ) or []

        if len(members) < 2:
            yield f"data: {json.dumps({'type':'done','linked':0,'message':'Need at least 2 jobs'})}\n\n"
            return

        keys = qall("SELECT * FROM log_group_keys WHERE group_id=?", (gid,)) or []
        if not keys:
            yield f"data: {json.dumps({'type':'done','linked':0,'message':'No correlation keys defined'})}\n\n"
            return

        from collections import defaultdict
        pair_map: dict = defaultdict(list)
        for k in keys:
            ja, jb = k['job_id_a'], k['job_id_b']
            if ja > jb: ja, jb = jb, ja; k = {**k, 'job_id_a': ja, 'job_id_b': jb, 'field_a': k['field_b'], 'field_b': k['field_a']}
            pair_map[(ja, jb)].append(k)

        member_label = {m['job_id']: (m.get('label') or m.get('job_name') or f"Job {m['job_id']}") for m in members}

        total_linked = 0

        import sqlite3 as _sq3
        from db import DB_PATH as _DB_PATH

        for (jid_a, jid_b), pair_keys in pair_map.items():
            label_a = member_label.get(jid_a, f"Job {jid_a}")
            label_b = member_label.get(jid_b, f"Job {jid_b}")

            cnt_a = (qone("SELECT COUNT(*) c FROM log_events WHERE job_id=?", (jid_a,)) or {}).get("c", 0)
            cnt_b = (qone("SELECT COUNT(*) c FROM log_events WHERE job_id=?", (jid_b,)) or {}).get("c", 0)
            yield f"data: {json.dumps({'type':'progress','message':f'Loading {label_a} ({cnt_a:,} rows)...'})}\n\n"
            await asyncio.sleep(0.01)

            # ── Efficient approach: extract only the matching field values via SQL ──
            # Never load full raw_json; only pull id + extracted field values.
            def _extract_field_vals(job_id, fields):
                """Return {composite_key: [row_ids]} index for a job.
                composite_key = tuple of field values (one per field in `fields`).
                """
                # Build SELECT projections: CAST(COALESCE(array-form, scalar-form) AS TEXT)
                projections = []
                proj_args = []
                for f in fields:
                    p0, p = f"$.{f}[0]", f"$.{f}"
                    projections.append(
                        "CAST(COALESCE(json_extract(raw_json,?),json_extract(raw_json,?)) AS TEXT)"
                    )
                    proj_args += [p0, p]
                select_sql = (
                    f"SELECT id, {', '.join(projections)} "
                    f"FROM log_events WHERE job_id=?"
                )
                _c = _sq3.connect(str(_DB_PATH), check_same_thread=False, timeout=60)
                _c.execute("PRAGMA journal_mode=WAL")
                try:
                    cur = _c.execute(select_sql, proj_args + [job_id])
                    index: dict = defaultdict(list)
                    while True:
                        chunk = cur.fetchmany(2000)
                        if not chunk:
                            break
                        for row in chunk:
                            row_id = row[0]
                            vals = tuple(str(v or "").strip() for v in row[1:])
                            if any(vals):  # skip rows where all fields are empty
                                index[vals].append(row_id)
                finally:
                    _c.close()
                return index

            fields_a = [k['field_a'] for k in pair_keys]
            fields_b = [k['field_b'] for k in pair_keys]

            yield f"data: {json.dumps({'type':'progress','message':f'Indexing {label_a}...'})}\n\n"
            await asyncio.sleep(0.01)
            idx_a = _extract_field_vals(jid_a, fields_a)

            yield f"data: {json.dumps({'type':'progress','message':f'Indexing {label_b}...'})}\n\n"
            await asyncio.sleep(0.01)
            idx_b = _extract_field_vals(jid_b, fields_b)

            yield f"data: {json.dumps({'type':'progress','message':f'Matching {len(idx_a):,} × {len(idx_b):,} unique keys...'})}\n\n"
            await asyncio.sleep(0.01)

            # Clear old correlations for this pair
            exe("DELETE FROM log_correlations WHERE group_id=? AND log_id IN "
                "(SELECT id FROM log_events WHERE job_id=?)", (gid, jid_a))
            exe("DELETE FROM log_correlations WHERE group_id=? AND log_id IN "
                "(SELECT id FROM log_events WHERE job_id=?)", (gid, jid_b))

            linked = 0
            insert_batch: list = []

            for key_vals, ids_a in idx_a.items():
                if not any(key_vals):
                    continue
                ids_b = idx_b.get(key_vals, [])
                if not ids_b:
                    continue
                matched_json = json.dumps([
                    f"{k['field_a']}={str(v)[:60]}"
                    for k, v in zip(pair_keys, key_vals)
                    if v
                ])
                for id_a in ids_a:
                    for id_b in ids_b:
                        insert_batch.append((id_a, id_b, gid, label_b, matched_json))
                        insert_batch.append((id_b, id_a, gid, label_a, matched_json))
                        linked += 1

                if len(insert_batch) >= 2000:
                    _c2 = _sq3.connect(str(_DB_PATH), check_same_thread=False, timeout=60)
                    _c2.execute("PRAGMA journal_mode=WAL")
                    try:
                        _c2.executemany(
                            "INSERT OR IGNORE INTO log_correlations "
                            "(log_id,corr_id,group_id,source_label,matched_keys) VALUES (?,?,?,?,?)",
                            insert_batch
                        )
                        _c2.commit()
                    finally:
                        _c2.close()
                    insert_batch = []
                    yield f"data: {json.dumps({'type':'progress','message':f'{linked:,} pairs linked so far...'})}\n\n"
                    await asyncio.sleep(0.01)

            # Flush remaining
            if insert_batch:
                _c2 = _sq3.connect(str(_DB_PATH), check_same_thread=False, timeout=60)
                _c2.execute("PRAGMA journal_mode=WAL")
                try:
                    _c2.executemany(
                        "INSERT OR IGNORE INTO log_correlations "
                        "(log_id,corr_id,group_id,source_label,matched_keys) VALUES (?,?,?,?,?)",
                        insert_batch
                    )
                    _c2.commit()
                finally:
                    _c2.close()

            total_linked += linked
            yield f"data: {json.dumps({'type':'pair_done','label_a':label_a,'label_b':label_b,'linked':linked})}\n\n"

        yield f"data: {json.dumps({'type':'done','linked':total_linked})}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})


@app.get("/api/logs/{log_id}/correlated")
def get_correlated_logs(log_id: int):
    """Return pre-computed correlated records for this log entry."""
    _ensure_correlations_schema()

    links = qall(
        "SELECT lc.corr_id, lc.source_label, lc.matched_keys, "
        "le.timestamp, le.relevance, le.severity, le.ai_summary, le.raw_json "
        "FROM log_correlations lc "
        "JOIN log_events le ON le.id = lc.corr_id "
        "WHERE lc.log_id = ?",
        (log_id,)
    ) or []

    result = []
    for lnk in links:
        mk = lnk.get('matched_keys') or []
        if isinstance(mk, str):
            try: mk = json.loads(mk)
            except: mk = []
        result.append({
            'id':           lnk['corr_id'],
            'timestamp':    lnk['timestamp'],
            'relevance':    lnk['relevance'],
            'severity':     lnk['severity'],
            'ai_summary':   lnk['ai_summary'],
            'raw_json':     lnk['raw_json'],
            '_source_label':  lnk['source_label'],
            '_matched_keys':  mk,
        })
    return result


@app.get("/api/groups/{gid}/detect-keys/stream")
async def detect_keys_stream(gid: int, sample_size: int = Query(default=50, le=200)):
    """
    SSE stream: auto-detect correlation fields between all job pairs in this group.
    """
    async def gen():
        _ensure_groups_schema()
        yield f"data: {json.dumps({'type':'start'})}\n\n"

        members = qall(
            "SELECT lgm.job_id, lgm.label, sj.name AS job_name "
            "FROM log_group_members lgm JOIN scrape_jobs sj ON sj.id=lgm.job_id "
            "WHERE lgm.group_id=?", (gid,)
        ) or []

        if len(members) < 2:
            yield f"data: {json.dumps({'type':'done','message':'Need at least 2 jobs in the group','keys_added':0})}\n\n"
            return

        yield f"data: {json.dumps({'type':'progress','message':f'Sampling records from {len(members)} jobs...'})}\n\n"

        job_samples = {}
        for m in members:
            rows = qall(
                "SELECT id, raw_json FROM log_events WHERE job_id=? "
                "AND raw_json IS NOT NULL ORDER BY RANDOM() LIMIT ?",
                (m['job_id'], sample_size)
            ) or []
            parsed = []
            for r in rows:
                rj = r['raw_json']
                if isinstance(rj, str):
                    try: rj = json.loads(rj)
                    except: continue
                if isinstance(rj, dict):
                    parsed.append(rj)
            job_samples[m['job_id']] = {
                'label': m.get('label') or m.get('job_name'),
                'records': parsed
            }

        import itertools, re
        UUID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)
        HEX_RE  = re.compile(r'^[0-9a-f]{16,}$', re.I)
        ID_FIELDS = re.compile(r'(_id|_token|_key|request|trace|correlation|session)', re.I)

        def extract_values(records, field):
            vals = set()
            for r in records:
                v = r.get(field, "")
                if isinstance(v, list): v = v[0] if v else ""
                v = str(v or "").strip()
                if v and len(v) > 3:
                    vals.add(v)
            return vals

        keys_added = 0
        heuristic_keys = []

        for (jid_a, data_a), (jid_b, data_b) in itertools.combinations(job_samples.items(), 2):
            recs_a = data_a['records']
            recs_b = data_b['records']
            if not recs_a or not recs_b:
                continue

            fields_a = set().union(*[r.keys() for r in recs_a[:20]])
            fields_b = set().union(*[r.keys() for r in recs_b[:20]])
            common_fields = fields_a & fields_b

            label_a = data_a['label']
            label_b = data_b['label']
            nf = len(common_fields)
            yield f"data: {json.dumps({'type':'progress','message':f'Checking {nf} common fields between {label_a} and {label_b}...'})}\n\n"
            await asyncio.sleep(0.01)

            candidates = []
            for field in common_fields:
                vals_a = extract_values(recs_a, field)
                vals_b = extract_values(recs_b, field)
                overlap = vals_a & vals_b
                if not overlap:
                    continue
                score = len(overlap)
                if ID_FIELDS.search(field): score *= 3
                uuid_count = sum(1 for v in overlap if UUID_RE.match(v) or HEX_RE.match(v))
                score += uuid_count * 2
                candidates.append((field, field, score, len(overlap)))

            id_fields_a = [f for f in fields_a if ID_FIELDS.search(f)]
            id_fields_b = [f for f in fields_b if ID_FIELDS.search(f)]
            for fa in id_fields_a:
                for fb in id_fields_b:
                    if fa == fb: continue
                    vals_a = extract_values(recs_a, fa)
                    vals_b = extract_values(recs_b, fb)
                    overlap = vals_a & vals_b
                    if len(overlap) >= 2:
                        score = len(overlap) * 2
                        candidates.append((fa, fb, score, len(overlap)))

            candidates.sort(key=lambda x: -x[2])
            for field_a, field_b, score, overlap_ct in candidates[:3]:
                confidence = min(1.0, score / 20.0)
                existing = qone(
                    "SELECT id FROM log_group_keys WHERE group_id=? "
                    "AND ((job_id_a=? AND field_a=? AND job_id_b=? AND field_b=?) "
                    "OR (job_id_a=? AND field_a=? AND job_id_b=? AND field_b=?))",
                    (gid, jid_a, field_a, jid_b, field_b, jid_b, field_b, jid_a, field_a)
                )
                if not existing:
                    exe(
                        "INSERT INTO log_group_keys "
                        "(group_id,job_id_a,field_a,job_id_b,field_b,confidence,source) "
                        "VALUES (?,?,?,?,?,?,'auto')",
                        (gid, jid_a, field_a, jid_b, field_b, confidence)
                    )
                    keys_added += 1
                    heuristic_keys.append(f"{field_a} ↔ {field_b} (overlap:{overlap_ct})")
                    yield f"data: {json.dumps({'type':'key_found','field_a':field_a,'field_b':field_b,'overlap':overlap_ct,'source':'heuristic'})}\n\n"

        anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if anthropic_key and len(members) >= 2:
            yield f"data: {json.dumps({'type':'progress','message':'Running AI analysis...'})}\n\n"
            await asyncio.sleep(0.01)

            samples_for_ai = {}
            for m in members:
                jid = m['job_id']
                recs = job_samples[jid]['records'][:5]
                field_samples = {}
                for r in recs:
                    for k, v in r.items():
                        if k not in field_samples:
                            sv = v[0] if isinstance(v, list) else v
                            if sv and not isinstance(sv, dict):
                                field_samples[k] = str(sv)[:80]
                samples_for_ai[m.get('label') or m.get('job_name')] = field_samples

            prompt = (
                "You are analyzing two types of log records to find correlation fields.\n\n"
                "Here are sample field:value pairs from each log type:\n\n"
            )
            for label, fields in samples_for_ai.items():
                prompt += f"=== {label} ===\n"
                for k, v in list(fields.items())[:30]:
                    prompt += f"  {k}: {v}\n"
                prompt += "\n"
            prompt += (
                "Identify up to 3 field pairs that could correlate records between these log types. "
                "A good correlation field has similar or identical values in both logs for the same request/event. "
                "Look for: request IDs, trace IDs, session tokens, IP addresses, timestamps, order IDs, user IDs.\n\n"
                "RESPOND WITH ONLY a JSON array, nothing else. Example:\n"
                '[{"field_a":"request_id","log_a":"syslog","field_b":"http_request_id","log_b":"nginx","reason":"UUID request identifier"},{"field_a":"client_ip","log_a":"syslog","field_b":"remote_addr","log_b":"nginx","reason":"Client IP address"}]\n'
                "If no good correlation exists, return []."
            )

            try:
                import anthropic as _ant
                client = _ant.Anthropic()
                resp = client.messages.create(
                    model="claude-haiku-4-5",
                    max_tokens=500,
                    messages=[{"role": "user", "content": prompt}]
                )
                ai_text = resp.content[0].text.strip()
                import re as _re
                m_json = _re.search(r'\[.*\]', ai_text, _re.DOTALL)
                if m_json:
                    ai_keys = json.loads(m_json.group())
                    label_to_jid = {
                        (m.get('label') or m.get('job_name')): m['job_id']
                        for m in members
                    }
                    for ak in ai_keys:
                        fa = ak.get('field_a','')
                        fb = ak.get('field_b','')
                        la = ak.get('log_a','')
                        lb = ak.get('log_b','')
                        jid_a = label_to_jid.get(la)
                        jid_b = label_to_jid.get(lb)
                        if not (fa and fb and jid_a and jid_b):
                            continue
                        existing = qone(
                            "SELECT id FROM log_group_keys WHERE group_id=? "
                            "AND ((job_id_a=? AND field_a=? AND job_id_b=? AND field_b=?) "
                            "OR (job_id_a=? AND field_a=? AND job_id_b=? AND field_b=?))",
                            (gid, jid_a, fa, jid_b, fb, jid_b, fb, jid_a, fa)
                        )
                        if not existing:
                            exe(
                                "INSERT INTO log_group_keys "
                                "(group_id,job_id_a,field_a,job_id_b,field_b,confidence,source) "
                                "VALUES (?,?,?,?,?,0.9,'ai')",
                                (gid, jid_a, fa, jid_b, fb)
                            )
                            keys_added += 1
                            yield f"data: {json.dumps({'type':'key_found','field_a':fa,'field_b':fb,'source':'ai','reason':ak.get('reason','')})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'type':'progress','message':f'AI analysis skipped: {str(e)[:100]}'})}\n\n"

        yield f"data: {json.dumps({'type':'done','keys_added':keys_added})}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})


@app.get("/api/settings")
def get_settings():
    return {
        "github_token_set":    bool(os.environ.get("GITHUB_TOKEN")),
        "anthropic_key_set":   bool(os.environ.get("ANTHROPIC_API_KEY")),
    }


@app.post("/api/settings/keys")
def save_keys(payload: dict):
    updates = {}
    if payload.get("github_token"):
        updates["GITHUB_TOKEN"] = payload["github_token"].strip()
    if payload.get("anthropic_key"):
        updates["ANTHROPIC_API_KEY"] = payload["anthropic_key"].strip()
    if updates:
        _save_keys(updates)
        if "ANTHROPIC_API_KEY" in updates:
            import analyzer
            try:
                import anthropic as _ant
                analyzer._client = _ant.Anthropic()
                analyzer._available = True
            except Exception:
                pass
    return {"ok": True}


# ─── Log marking endpoints ─────────────────────────────────────────────────────

class MarkRequest(BaseModel):
    relevance: str      # "relevant" | "irrelevant"
    severity: str = "info"
    summary: str = ""

class MarkBulkRequest(BaseModel):
    ids: list[int]
    relevance: str
    severity: str = "info"

@app.post("/api/logs/{log_id}/mark")
def mark_single_log(log_id: int, req: MarkRequest):
    summary_val = req.summary if req.summary else (
        "Manually marked as relevant." if req.relevance == "relevant" else "Manually marked as irrelevant."
    )
    exe(
        "UPDATE log_events SET relevance=?, severity=?, "
        "ai_summary=CASE WHEN ?!='' THEN ? ELSE ai_summary END WHERE id=?",
        (req.relevance, req.severity, summary_val, summary_val, log_id)
    )
    return {"ok": True}

@app.post("/api/logs/mark-bulk")
def mark_bulk_logs(req: MarkBulkRequest):
    if not req.ids:
        return {"ok": True, "count": 0}
    placeholders = ",".join(["?"] * len(req.ids))
    exe(
        f"UPDATE log_events SET relevance=?, severity=COALESCE(NULLIF(severity,''), ?) WHERE id IN ({placeholders})",
        [req.relevance, req.severity] + list(req.ids)
    )
    return {"ok": True, "count": len(req.ids)}

@app.get("/api/logs/similar")
def find_similar_logs(job_id: int, message: str, exclude_id: int = 0, limit: int = 500):
    rows = qall(
        "SELECT id, relevance FROM log_events "
        "WHERE job_id=? AND id != ? "
        "AND (json_extract(raw_json, '$.message') = ? "
        "     OR json_extract(raw_json, '$.message[0]') = ?) "
        "LIMIT ?",
        (job_id, exclude_id, message, message, limit)
    )
    return rows

@app.post("/api/jobs/{jid}/summarize")
async def summarize_logs(jid: int, payload: dict):
    ids = payload.get("ids", [])
    if not ids:
        return {"summary": "No records to summarize.", "count": 0}

    cap = ids[:50]
    placeholders = ",".join(["?"] * len(cap))
    rows = qall(
        f"SELECT id, relevance, severity, ai_summary, raw_json "
        f"FROM log_events WHERE id IN ({placeholders})",
        cap
    )

    entries = []
    for r in rows:
        rj = r.get("raw_json") or {}
        if isinstance(rj, str):
            try: rj = json.loads(rj)
            except: rj = {}
        msg = rj.get("message", "")
        if isinstance(msg, list): msg = msg[0] if msg else ""
        sev = (r.get("severity") or "?").upper()
        text = r.get("ai_summary") or str(msg)[:200]
        entries.append(f"[{sev}] {text}")

    try:
        import anthropic as _ant
        client = _ant.Anthropic()
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=600,
            messages=[{
                "role": "user",
                "content": (
                    f"Summarize these {len(entries)} log entries in 3-5 bullet points. "
                    "Focus on patterns, main issues, and actionable insights:\n\n"
                    + "\n".join(entries)
                )
            }]
        )
        summary = response.content[0].text
    except Exception as e:
        summary = f"Summary unavailable: {e}"

    return {"summary": summary, "count": len(rows)}


# ─── Histogram ────────────────────────────────────────────────────────────────

@app.get("/api/jobs/{jid}/histogram")
def job_histogram(
    jid: int,
    search: str = "",
    from_date: str = "",
    to_date: str = "",
    relevance: str = "",
    regex: bool = False,
    field_filters: str = "",
):
    """Return time-bucketed row counts for a bar chart."""
    import re as _re2
    conditions: list[str] = ["job_id=?", "timestamp IS NOT NULL"]
    args: tuple = (jid,)

    if search:
        if regex:
            conditions.append("CAST(raw_json AS TEXT) REGEXP ?")
            args += (search,)
        else:
            conditions.append("raw_json LIKE ?")
            args += (f"%{search}%",)
    if from_date:
        conditions.append("timestamp >= ?")
        args += (from_date,)
    if to_date:
        conditions.append("timestamp <= ?")
        args += (to_date,)
    if relevance:
        conditions.append("relevance=?")
        args += (relevance,)
    if field_filters:
        try:
            ff_list = json.loads(field_filters)
            for ff in ff_list:
                field = ff.get("field", "")
                value = ff.get("value", "")
                op    = ff.get("op", "eq")
                if not field or not _re2.match(r'^[A-Za-z0-9_.@\-]+$', field):
                    continue
                json_path0 = f"$.{field}[0]"
                json_path  = f"$.{field}"
                coalesce = "CAST(COALESCE(json_extract(raw_json, ?), json_extract(raw_json, ?)) AS TEXT)"
                if op == "neq":
                    conditions.append(
                        f"({coalesce} != ? OR json_extract(raw_json, ?) IS NULL)"
                    )
                    args += (json_path0, json_path, value, json_path)
                else:
                    conditions.append(f"{coalesce} = ?")
                    args += (json_path0, json_path, value)
        except Exception:
            pass

    where = " AND ".join(conditions)

    # Get time range to pick bucket size
    bounds = qone(
        f"SELECT MIN(timestamp) as mn, MAX(timestamp) as mx, COUNT(*) as total "
        f"FROM log_events WHERE {where}", args
    ) or {}
    total = bounds.get("total", 0)
    mn_ts = bounds.get("mn", "")
    mx_ts = bounds.get("mx", "")

    if not total or not mn_ts or not mx_ts:
        return {"buckets": [], "total": 0, "bucket_label": "", "from": "", "to": ""}

    from datetime import datetime as _dt
    try:
        mn_dt = _dt.fromisoformat(str(mn_ts))
        mx_dt = _dt.fromisoformat(str(mx_ts))
    except Exception:
        return {"buckets": [], "total": total, "bucket_label": "", "from": mn_ts, "to": mx_ts}

    span_hours = max((mx_dt - mn_dt).total_seconds() / 3600, 0.01)

    # Target ~50 bars
    if span_hours <= 2:          # ≤2h → 5min buckets
        bucket_label = "5 min"
        bucket_sql = "strftime('%Y-%m-%dT%H:', timestamp) || printf('%02d', (CAST(strftime('%M', timestamp) AS INTEGER) / 5) * 5) || ':00'"
    elif span_hours <= 12:       # ≤12h → 30min buckets
        bucket_label = "30 min"
        bucket_sql = "strftime('%Y-%m-%dT%H:', timestamp) || CASE WHEN CAST(strftime('%M', timestamp) AS INTEGER) < 30 THEN '00:00' ELSE '30:00' END"
    elif span_hours <= 48:       # ≤2d → 1h buckets
        bucket_label = "1 hour"
        bucket_sql = "strftime('%Y-%m-%dT%H:00:00', timestamp)"
    elif span_hours <= 240:      # ≤10d → 6h buckets
        bucket_label = "6 hours"
        bucket_sql = "strftime('%Y-%m-%dT', timestamp) || printf('%02d', (CAST(strftime('%H', timestamp) AS INTEGER) / 6) * 6) || ':00:00'"
    elif span_hours <= 1680:     # ≤10w → 1d buckets
        bucket_label = "1 day"
        bucket_sql = "strftime('%Y-%m-%d', timestamp)"
    else:                        # >10w → 1 week buckets
        bucket_label = "1 week"
        bucket_sql = "strftime('%Y-W%W', timestamp)"

    rows = qall(
        f"SELECT {bucket_sql} as bucket, COUNT(*) as count "
        f"FROM log_events WHERE {where} "
        f"GROUP BY bucket ORDER BY bucket",
        args,
    )
    return {
        "buckets": [{"t": r["bucket"], "c": r["count"]} for r in (rows or [])],
        "total": total,
        "bucket_label": bucket_label,
        "from": mn_ts,
        "to": mx_ts,
    }


# ─── Startup ──────────────────────────────────────────────────────────────────

@app.on_event("startup")
def startup():
    init_db()
    _refresh_scheduler()
    if not scheduler.running:
        scheduler.start()
    print(f"✓ SQLite DB: {DB_PATH}")
    print("✓ Scheduler started")


if __name__ == "__main__":
    import uvicorn
    import webbrowser, threading as _th
    _th.Timer(1.2, lambda: webbrowser.open("http://localhost:8765")).start()
    uvicorn.run(app, host="0.0.0.0", port=8765)
