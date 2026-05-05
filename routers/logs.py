"""Log marking, tags, similar, correlated-batch, and saved searches endpoints."""

import json

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from db import qone, qall, exe

router = APIRouter()

# ─── Models ───────────────────────────────────────────────────────────────────

class MarkRequest(BaseModel):
    relevance: str      # "relevant" | "irrelevant"
    severity: str = "info"
    summary: str = ""

class MarkBulkRequest(BaseModel):
    ids: list
    relevance: str
    severity: str = "info"

class BatchCorrRequest(BaseModel):
    ids: list

class SavedSearchIn(BaseModel):
    job_id:       int   = None
    name:         str
    search:       str   = ""
    from_date:    str   = ""
    to_date:      str   = ""
    relevance:    str   = ""
    field_filters:str   = "[]"
    sort_by:      str   = "timestamp"
    sort_dir:     str   = "desc"
    is_preset:    bool  = False


# ─── Routes ───────────────────────────────────────────────────────────────────

@router.get("/api/logs/{log_id}/correlated")
def get_correlated_logs(log_id: int):
    """Return pre-computed correlated records for this log entry."""
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


@router.post("/api/logs/correlated-batch")
def get_correlated_batch(req: BatchCorrRequest):
    """Return precomputed correlated records for multiple log IDs in one call."""
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


@router.post("/api/logs/{log_id}/mark")
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


@router.post("/api/logs/mark-bulk")
def mark_bulk_logs(req: MarkBulkRequest):
    if not req.ids:
        return {"ok": True, "count": 0}
    placeholders = ",".join(["?"] * len(req.ids))
    exe(
        f"UPDATE log_events SET relevance=?, severity=COALESCE(NULLIF(severity,''), ?) WHERE id IN ({placeholders})",
        [req.relevance, req.severity] + list(req.ids)
    )
    return {"ok": True, "count": len(req.ids)}


@router.get("/api/logs/similar")
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


@router.get("/api/logs/{log_id}/tags")
def get_log_tags(log_id: int):
    return [r["tag"] for r in (qall("SELECT tag FROM log_tags WHERE log_id=? ORDER BY tag", (log_id,)) or [])]


@router.post("/api/logs/{log_id}/tags")
def add_log_tag(log_id: int, payload: dict):
    tag = (payload.get("tag") or "").strip()[:50]
    if not tag:
        raise HTTPException(400, "tag required")
    exe("INSERT OR IGNORE INTO log_tags (log_id, tag) VALUES (?,?)", (log_id, tag))
    return {"ok": True}


@router.delete("/api/logs/{log_id}/tags/{tag}")
def remove_log_tag(log_id: int, tag: str):
    exe("DELETE FROM log_tags WHERE log_id=? AND tag=?", (log_id, tag))
    return {"ok": True}


@router.get("/api/jobs/{jid}/tags")
def list_job_tags(jid: int):
    """Return all distinct tags used across a job's logs, with counts."""
    return qall(
        "SELECT lt.tag, COUNT(*) as count FROM log_tags lt "
        "JOIN log_events le ON le.id=lt.log_id WHERE le.job_id=? "
        "GROUP BY lt.tag ORDER BY count DESC", (jid,)
    ) or []


@router.get("/api/saved-searches")
def list_saved_searches(job_id: int = None):
    if job_id:
        return qall("SELECT * FROM saved_searches WHERE job_id=? OR job_id IS NULL ORDER BY name", (job_id,)) or []
    return qall("SELECT * FROM saved_searches ORDER BY name") or []


@router.post("/api/saved-searches", status_code=201)
def create_saved_search(body: SavedSearchIn):
    sid = exe(
        "INSERT INTO saved_searches (job_id,name,search,from_date,to_date,relevance,"
        "field_filters,sort_by,sort_dir,is_preset) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (body.job_id, body.name, body.search, body.from_date, body.to_date,
         body.relevance, body.field_filters, body.sort_by, body.sort_dir, int(body.is_preset))
    )
    return {"id": sid}


@router.delete("/api/saved-searches/{sid}")
def delete_saved_search(sid: int):
    exe("DELETE FROM saved_searches WHERE id=?", (sid,))
    return {"ok": True}
