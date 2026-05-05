"""Keys, session health, stats, and DB maintenance endpoints."""

import json
import os
import time

from fastapi import APIRouter, Query
from pydantic import BaseModel

from db import qone, qall, exe, DB_PATH
from routers._run_engine import SCRAPER_DIR

router = APIRouter()

# ─── Keys helpers ─────────────────────────────────────────────────────────────

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


# ─── Routes ───────────────────────────────────────────────────────────────────

@router.get("/api/stats")
def stats():
    severity_rows = qall(
        "SELECT severity, COUNT(*) as cnt FROM log_events "
        "WHERE severity IS NOT NULL GROUP BY severity"
    ) or []
    severity_map = {r["severity"]: r["cnt"] for r in severity_rows}
    rel_rows = qall(
        "SELECT relevance, COUNT(*) as cnt FROM log_events GROUP BY relevance"
    ) or []
    rel_map = {(r["relevance"] or "unchecked"): r["cnt"] for r in rel_rows}
    total_rows    = (qone("SELECT COUNT(*) c FROM log_events") or {}).get("c", 0)
    analyzed_rows = (qone(
        "SELECT COUNT(*) c FROM log_events WHERE relevance IS NOT NULL AND relevance != 'unchecked'"
    ) or {}).get("c", 0)
    return {
        "total_jobs":     (qone("SELECT COUNT(*) c FROM scrape_jobs") or {}).get("c", 0),
        "total_rows":     total_rows,
        "analyzed_rows":  analyzed_rows,       # frontend reads s.analyzed_rows
        "severity_counts": severity_map,       # frontend reads s.severity_counts
        "severity":       severity_map,        # keep old key for backwards compat
        "relevance":      rel_map,
        "recent_runs":    qall(
            "SELECT jr.id, jr.status, jr.rows_saved, jr.started_at, jr.finished_at, sj.name "
            "FROM job_runs jr JOIN scrape_jobs sj ON jr.job_id=sj.id "
            "ORDER BY jr.id DESC LIMIT 8"
        ),
        "top_jobs": qall(
            "SELECT sj.id, sj.name, COUNT(le.id) as rows "
            "FROM scrape_jobs sj LEFT JOIN log_events le ON le.job_id=sj.id "
            "GROUP BY sj.id ORDER BY rows DESC LIMIT 5"
        ) or [],
    }


@router.get("/api/settings")
def get_settings():
    import analyzer as _az
    ai_ok = bool(_az._get_client())
    system_prompt = (qone("SELECT value FROM ai_config WHERE key='system_prompt'") or {}).get("value", "")
    return {
        "github_token_set":  bool(os.environ.get("GITHUB_TOKEN")),
        "anthropic_key_set": bool(os.environ.get("ANTHROPIC_API_KEY")),
        "anthropic_ready":   ai_ok,
        "system_prompt":     system_prompt,
    }


class KeysPayload(BaseModel):
    github_token:  str = ""
    anthropic_key: str = ""
    system_prompt: str | None = None


@router.post("/api/settings/keys")
def save_keys(payload: KeysPayload):
    updates = {}
    if payload.github_token:
        updates["GITHUB_TOKEN"] = payload.github_token.strip()
    if payload.anthropic_key:
        updates["ANTHROPIC_API_KEY"] = payload.anthropic_key.strip()
    if updates:
        _save_keys(updates)
    # Always update system prompt if provided (even without key changes)
    if payload.system_prompt is not None:
        exe("INSERT OR REPLACE INTO ai_config (key, value) VALUES ('system_prompt', ?)",
            (payload.system_prompt.strip(),))
    return {"ok": True}


@router.get("/api/session/health")
def session_health():
    """Check whether session.json exists and roughly how old it is (F9)."""
    session_file = SCRAPER_DIR / "session.json"
    if not session_file.exists():
        return {"status": "missing", "message": "No session.json — run: python3 kibana_scraper.py login <url>"}
    age_hours = (time.time() - session_file.stat().st_mtime) / 3600
    if age_hours > 20:
        return {"status": "stale", "age_hours": round(age_hours, 1),
                "message": f"Session is {round(age_hours,1)}h old — may need re-login"}
    return {"status": "ok", "age_hours": round(age_hours, 1)}


@router.get("/api/db/stats")
def db_stats():
    """Return DB file size, row counts per job, and run log sizes (F6)."""
    size_bytes = DB_PATH.stat().st_size if DB_PATH.exists() else 0
    job_counts = qall(
        "SELECT sj.id, sj.name, COUNT(le.id) as rows, "
        "SUM(CASE WHEN le.relevance='relevant' THEN 1 ELSE 0 END) as relevant_rows, "
        "SUM(CASE WHEN le.relevance IS NULL OR le.relevance='unchecked' THEN 1 ELSE 0 END) as unanalyzed "
        "FROM scrape_jobs sj LEFT JOIN log_events le ON le.job_id=sj.id "
        "GROUP BY sj.id ORDER BY rows DESC"
    ) or []
    run_log_size = qone("SELECT SUM(LENGTH(log_output)) as s FROM job_runs") or {}
    return {
        "db_size_mb": round(size_bytes / 1_048_576, 2),
        "jobs": job_counts,
        "run_log_kb": round((run_log_size.get("s") or 0) / 1024, 1),
        "total_events": (qone("SELECT COUNT(*) c FROM log_events") or {}).get("c", 0),
    }


@router.post("/api/db/vacuum")
def db_vacuum():
    """Run VACUUM to reclaim space and rebuild indexes."""
    try:
        from db import _db as _get_db
        conn = _get_db()
        conn.execute("VACUUM")
        conn.commit()
        size_after = DB_PATH.stat().st_size if DB_PATH.exists() else 0
        return {"ok": True, "db_size_mb": round(size_after / 1_048_576, 2)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.post("/api/db/prune-runs")
def prune_run_logs(keep: int = Query(default=10)):
    """Remove log_output from old job_runs, keeping the N most recent per job (D3)."""
    jobs = qall("SELECT id FROM scrape_jobs") or []
    pruned = 0
    for j in jobs:
        old_ids = qall(
            "SELECT id FROM job_runs WHERE job_id=? ORDER BY id DESC LIMIT -1 OFFSET ?",
            (j["id"], keep)
        ) or []
        for r in old_ids:
            exe("UPDATE job_runs SET log_output=NULL WHERE id=?", (r["id"],))
            pruned += 1
    return {"ok": True, "pruned": pruned}
