"""Job CRUD + run/cancel/stream/runs/data/fields/histogram/export/clusters endpoints."""

import asyncio
import csv
import io
import json
import re

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from db import qone, qall, exe, parse_raw_json
from routers._run_engine import (
    _run_logs, _run_status, _run_procs,
    _start_run, _refresh_scheduler,
    _build_chunks, _range_to_hours,
)
from routers._filters import _build_log_filter

router = APIRouter()

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

_STREAM_TIMEOUT_S = 4 * 3600   # 4-hour hard cap on SSE streams


# ─── Routes ───────────────────────────────────────────────────────────────────

@router.get("/api/jobs")
def list_jobs():
    jobs = qall("SELECT * FROM scrape_jobs ORDER BY created_at DESC")
    for j in jobs:
        last = qone("SELECT id, status FROM job_runs WHERE job_id=? ORDER BY id DESC LIMIT 1", (j["id"],))
        j["is_running"] = bool(last and _run_status.get(last["id"]) == "running")
    return jobs


@router.post("/api/jobs", status_code=201)
def create_job(job: JobIn):
    jid = exe(
        "INSERT INTO scrape_jobs (name,kibana_url,time_range,max_pages,schedule_hour,enabled,chunk_hours) "
        "VALUES (?,?,?,?,?,?,?)",
        (job.name, job.kibana_url, job.time_range, job.max_pages,
         job.schedule_hour, int(job.enabled), job.chunk_hours),
    )
    _refresh_scheduler()
    return {"id": jid}


@router.put("/api/jobs/{jid}")
def update_job(jid: int, job: JobIn):
    exe("UPDATE scrape_jobs SET name=?,kibana_url=?,time_range=?,max_pages=?,"
        "schedule_hour=?,enabled=?,chunk_hours=? WHERE id=?",
        (job.name, job.kibana_url, job.time_range, job.max_pages,
         job.schedule_hour, int(job.enabled), job.chunk_hours, jid))
    _refresh_scheduler()
    return {"ok": True}


@router.get("/api/jobs/{jid}/chunks/preview")
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


@router.delete("/api/jobs/{jid}/data")
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


@router.delete("/api/jobs/{jid}")
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


@router.post("/api/jobs/{jid}/run")
def trigger_run(jid: int):
    # Guard against duplicate concurrent runs for the same job
    last = qone("SELECT id FROM job_runs WHERE job_id=? ORDER BY id DESC LIMIT 1", (jid,))
    if last and _run_status.get(last["id"]) == "running":
        raise HTTPException(409, "A run is already active for this job")
    run_id = _start_run(jid)
    if run_id == -1:
        raise HTTPException(404, "Job not found or disabled")
    return {"run_id": run_id}


@router.get("/api/runs/{run_id}/stream")
async def stream_run(run_id: int):
    async def gen():
        pos = 0
        deadline = asyncio.get_event_loop().time() + _STREAM_TIMEOUT_S
        yield f"data: {json.dumps({'type':'connected'})}\n\n"
        while True:
            if asyncio.get_event_loop().time() > deadline:
                yield f"data: {json.dumps({'type':'done','status':'timeout'})}\n\n"
                return
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


@router.post("/api/runs/{run_id}/cancel")
def cancel_run(run_id: int):
    """Kill the scraper subprocess for a running job (F7)."""
    import time as _t
    proc = _run_procs.get(run_id)
    if proc is None:
        st = _run_status.get(run_id)
        if st and st != "running":
            return {"ok": False, "message": f"Run already {st}"}
        return {"ok": False, "message": "No active process found"}
    try:
        proc.terminate()
        _t.sleep(0.5)
        if proc.poll() is None:
            proc.kill()
    except Exception as e:
        return {"ok": False, "message": str(e)}
    _run_logs.get(run_id, []).append("[✗] Run cancelled by user")
    _run_status[run_id] = "failed"
    from datetime import datetime
    exe("UPDATE job_runs SET status='failed', finished_at=? WHERE id=?",
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), run_id))
    _run_procs.pop(run_id, None)
    return {"ok": True}


@router.get("/api/jobs/{jid}/runs")
def job_runs(jid: int):
    return qall(
        "SELECT id, started_at, finished_at, status, rows_saved "
        "FROM job_runs WHERE job_id=? ORDER BY id DESC LIMIT 30", (jid,)
    )


@router.get("/api/jobs/{jid}/fields")
def job_fields(jid: int):
    """Return sorted field names from a stratified sample (first+last+random). B7 fix."""
    total = (qone("SELECT COUNT(*) c FROM log_events WHERE job_id=?", (jid,)) or {}).get("c", 0)
    if total == 0:
        return []
    # Stratified: first 30, last 30, up to 40 from the middle
    rows = qall("SELECT raw_json FROM log_events WHERE job_id=? ORDER BY id ASC  LIMIT 30", (jid,))
    rows += qall("SELECT raw_json FROM log_events WHERE job_id=? ORDER BY id DESC LIMIT 30", (jid,))
    if total > 60:
        mid_offset = max(0, total // 2 - 20)
        rows += qall("SELECT raw_json FROM log_events WHERE job_id=? ORDER BY id ASC LIMIT 40 OFFSET ?",
                     (jid, mid_offset))
    fields: set = set()
    for row in rows:
        rj = row.get("raw_json")
        if not rj:
            continue
        try:
            data = rj if isinstance(rj, dict) else json.loads(rj)
            fields.update(data.keys())
        except Exception:
            pass
    first = ["@timestamp"] if "@timestamp" in fields else []
    others = sorted(f for f in fields if f != "@timestamp")
    return first + others


@router.get("/api/jobs/{jid}/group-fields")
def get_group_fields(jid: int):
    """Return field lists for all jobs linked to this job via correlation groups."""
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


@router.get("/api/jobs/{jid}/data")
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
    field_filters: str = "",
    cross_source: bool = False,
    job_ids: str = "",   # comma-separated job IDs for multi-source (All Sources) mode
):
    offset = (page - 1) * per_page
    parsed_ids = [int(x) for x in job_ids.split(',') if x.strip().isdigit()] if job_ids else []
    multi = len(parsed_ids) > 1
    where, args = _build_log_filter(
        jid, search=search, from_date=from_date, to_date=to_date,
        relevance=relevance, field_filters=field_filters,
        regex=regex, cross_source=cross_source,
        job_ids=parsed_ids if multi else None,
    )
    # Use table alias "le" when cross_source may have injected "le.id" references;
    # multi-source mode uses a plain table reference (no alias needed)
    use_alias = cross_source and not multi
    tbl = "log_events le" if use_alias else "log_events"
    col = "le." if use_alias else ""

    _REAL_COLS = {"timestamp", "severity", "relevance", "id", "scraped_at"}
    dir_sql = "ASC" if sort_dir.lower() == "asc" else "DESC"
    if sort_by in _REAL_COLS:
        order_sql = f"{col}{sort_by} {dir_sql}"
    elif re.match(r'^[A-Za-z0-9_.\-]+$', sort_by):
        order_sql = f"json_extract({col}raw_json, '$.{sort_by}') {dir_sql}, {col}timestamp DESC"
    else:
        order_sql = f"{col}timestamp DESC"

    total = (qone(f"SELECT COUNT(*) c FROM {tbl} WHERE {where}", args) or {}).get("c", 0)
    rows  = qall(
        f"SELECT {col}id, {col}timestamp, {col}run_id, {col}raw_json, {col}scraped_at, "
        f"{col}relevance, {col}severity, {col}ai_summary, {col}code_refs, {col}job_id "
        f"FROM {tbl} WHERE {where} ORDER BY {order_sql} LIMIT ? OFFSET ?",
        args + (per_page, offset),
    )
    # Attach source label when in multi-source mode so the UI can badge each row
    if multi:
        job_name_map = {r["id"]: r["name"] for r in (qall("SELECT id, name FROM scrape_jobs") or [])}
        for row in (rows or []):
            row["_source"] = job_name_map.get(row.get("job_id"), "")
    return {"rows": rows, "total": total, "page": page, "per_page": per_page}


@router.get("/api/jobs/{jid}/histogram")
def job_histogram(
    jid: int,
    search: str = "",
    from_date: str = "",
    to_date: str = "",
    relevance: str = "",
    regex: bool = False,
    field_filters: str = "",
    cross_source: bool = False,
    job_ids: str = "",   # comma-separated job IDs for multi-source (All Sources) mode
):
    """Return time-bucketed row counts for a bar chart."""
    from datetime import datetime as _dt
    parsed_ids = [int(x) for x in job_ids.split(',') if x.strip().isdigit()] if job_ids else []
    multi = len(parsed_ids) > 1
    where, full_args = _build_log_filter(
        jid, search=search, from_date=from_date, to_date=to_date,
        relevance=relevance, field_filters=field_filters,
        regex=regex, cross_source=cross_source,
        extra_conditions=["timestamp IS NOT NULL"],
        job_ids=parsed_ids if multi else None,
    )
    tbl = "log_events le" if (cross_source and not multi) else "log_events"

    # Get time range to pick bucket size
    bounds = qone(
        f"SELECT MIN(timestamp) as mn, MAX(timestamp) as mx, COUNT(*) as total "
        f"FROM {tbl} WHERE {where}", full_args
    ) or {}
    total = bounds.get("total", 0)
    mn_ts = bounds.get("mn", "")
    mx_ts = bounds.get("mx", "")

    if not total or not mn_ts or not mx_ts:
        return {"buckets": [], "total": 0, "bucket_label": "", "from": "", "to": ""}

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
        f"FROM log_events le WHERE {where} "
        f"GROUP BY bucket ORDER BY bucket",
        full_args,
    )
    return {
        "buckets": [{"t": r["bucket"], "c": r["count"]} for r in (rows or [])],
        "total": total,
        "bucket_label": bucket_label,
        "from": mn_ts,
        "to": mx_ts,
    }


@router.get("/api/jobs/{jid}/export")
def export_job_data(
    jid: int,
    fmt: str = "csv",
    search: str = "",
    from_date: str = "",
    to_date: str = "",
    relevance: str = "",
    field_filters: str = "",
    limit: int = Query(default=10000, le=50000),
):
    """Download filtered log data as CSV or JSON."""
    where, args = _build_log_filter(
        jid, search=search, from_date=from_date, to_date=to_date,
        relevance=relevance, field_filters=field_filters,
    )
    rows = qall(
        f"SELECT id, timestamp, relevance, severity, ai_summary, raw_json "
        f"FROM log_events WHERE {where} ORDER BY timestamp DESC LIMIT ?",
        args + (limit,)
    ) or []

    if fmt == "json":
        out = []
        for r in rows:
            rj = parse_raw_json(r.get("raw_json"))
            out.append({"id": r["id"], "timestamp": r["timestamp"],
                        "relevance": r["relevance"], "severity": r["severity"],
                        "ai_summary": r["ai_summary"], **rj})
        content = json.dumps(out, indent=2, default=str)
        return StreamingResponse(iter([content]),
                                 media_type="application/json",
                                 headers={"Content-Disposition": f"attachment; filename=logs_job{jid}.json"})

    # CSV
    buf = io.StringIO()
    all_fields: list = []
    seen_f: set = set()
    for r in rows:
        for k in parse_raw_json(r.get("raw_json")).keys():
            if k not in seen_f:
                seen_f.add(k); all_fields.append(k)
    base_cols = ["id", "timestamp", "relevance", "severity", "ai_summary"]
    writer = csv.DictWriter(buf, fieldnames=base_cols + all_fields, extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        rj = parse_raw_json(r.get("raw_json"))
        flat = {k: (v[0] if isinstance(v, list) and v else v) for k, v in rj.items()}
        writer.writerow({**{"id": r["id"], "timestamp": r["timestamp"],
                            "relevance": r["relevance"], "severity": r["severity"],
                            "ai_summary": r["ai_summary"]}, **flat})
    content = buf.getvalue()
    return StreamingResponse(iter([content]),
                             media_type="text/csv",
                             headers={"Content-Disposition": f"attachment; filename=logs_job{jid}.csv"})


@router.get("/api/jobs/{jid}/clusters")
def get_clusters(
    jid: int,
    limit: int = Query(default=5000, le=20000),
    relevance: str = "",
    field: str = "message",
):
    """Group logs by message pattern, return top clusters with count + sample (F1)."""
    import re as _re3
    from collections import defaultdict as _dd
    conditions = ["job_id=?", f"json_extract(raw_json,'$.{field}[0]') IS NOT NULL OR json_extract(raw_json,'$.{field}') IS NOT NULL"]
    args: tuple = (jid,)
    if relevance:
        conditions.append("relevance=?"); args += (relevance,)
    where = " AND ".join(conditions)
    rows = qall(
        f"SELECT id, timestamp, relevance, severity, ai_summary, "
        f"CAST(COALESCE(json_extract(raw_json,'$.{field}[0]'),json_extract(raw_json,'$.{field}')) AS TEXT) AS msg_val "
        f"FROM log_events WHERE {where} ORDER BY timestamp DESC LIMIT ?",
        args + (limit,)
    ) or []

    def _normalize(msg: str) -> str:
        """Strip highly variable parts to cluster similar messages."""
        s = str(msg or "")
        s = _re3.sub(r'\b[0-9a-f]{8,}\b', 'HEX', s, flags=_re3.I)   # hex IDs
        s = _re3.sub(r'\b\d{4,}\b', 'NUM', s)                          # long numbers
        s = _re3.sub(r'https?://[^\s]+', 'URL', s)                     # URLs
        s = _re3.sub(r'\b[\w.+-]+@[\w.]+\b', 'EMAIL', s)               # emails
        s = _re3.sub(r'\s+', ' ', s).strip()
        return s[:200]

    clusters: dict = _dd(lambda: {"count": 0, "sample_id": None, "sample_ts": None,
                                   "sample_msg": None, "severity": "info",
                                   "sample_summary": None})
    sev_rank = {"critical": 4, "high": 3, "medium": 2, "low": 1, "info": 0}
    for r in rows:
        key = _normalize(r.get("msg_val", ""))
        if not key:
            continue
        cl = clusters[key]
        cl["count"] += 1
        sev = r.get("severity") or "info"
        if sev_rank.get(sev, 0) >= sev_rank.get(cl["severity"], 0):
            cl["severity"] = sev
        if cl["sample_id"] is None:
            cl["sample_id"]      = r["id"]
            cl["sample_ts"]      = r.get("timestamp")
            cl["sample_msg"]     = r.get("msg_val", "")[:300]
            cl["sample_summary"] = r.get("ai_summary")
        cl["pattern"] = key

    result = sorted(clusters.values(), key=lambda x: -x["count"])
    return {"clusters": result[:200], "total_rows": len(rows)}
