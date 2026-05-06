"""Collection CRUD + run endpoints."""

import threading
import time
from datetime import datetime
from itertools import combinations as _combos

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from db import qone, qall, exe
from routers._run_engine import (
    _run_logs, _run_status,
    _run_one_window, _last_timestamp,
    _build_chunks, _evict_old_runs,
    _refresh_scheduler,
)

router = APIRouter()

# ─── Models ───────────────────────────────────────────────────────────────────

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


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _collection_detail(cid: int):
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


def _sync_collection_group(cid: int, name: str, job_ids: list, keys: list):
    """Ensure a log_group exists for this collection and sync members + keys."""
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
    for ja, jb in _combos(job_ids, 2):
        for field in keys:
            exe("INSERT INTO log_group_keys (group_id,job_id_a,field_a,job_id_b,field_b,confidence,source) "
                "VALUES (?,?,?,?,?,1.0,'manual')", (gid, ja, field, jb, field))
    return gid


# ─── Routes ───────────────────────────────────────────────────────────────────

@router.get("/api/collections")
def list_collections():
    rows = qall("SELECT id FROM scrape_collections ORDER BY id") or []
    return [_collection_detail(r["id"]) for r in rows]


@router.post("/api/collections", status_code=201)
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


@router.put("/api/collections/{cid}")
def update_collection(cid: int, body: CollectionIn):
    """B5 fix: properly UPDATE existing sources instead of delete+insert."""
    exe("UPDATE scrape_collections SET name=?,time_range=?,chunk_hours=?,schedule_hour=?,enabled=? WHERE id=?",
        (body.name, body.time_range, body.chunk_hours, body.schedule_hour, int(body.enabled), cid))

    coll_dict = {"time_range": body.time_range, "chunk_hours": body.chunk_hours,
                 "schedule_hour": body.schedule_hour, "enabled": body.enabled}

    # Match incoming sources to existing ones by position (sort_order)
    existing_srcs = qall(
        "SELECT * FROM collection_sources WHERE collection_id=? ORDER BY sort_order, id", (cid,)
    ) or []
    existing_by_pos = {r["sort_order"]: r for r in existing_srcs}
    existing_ids_all = {r["id"] for r in existing_srcs}

    job_ids = []
    touched_src_ids = set()
    for i, src in enumerate(body.sources):
        existing = existing_by_pos.get(i)
        if existing:
            # Update in place — preserve job_id linkage
            exe("UPDATE collection_sources SET label=?, kibana_url=?, sort_order=? WHERE id=?",
                (src.label, src.kibana_url, i, existing["id"]))
            touched_src_ids.add(existing["id"])
            jid = _ensure_source_job({**existing, "label": src.label, "kibana_url": src.kibana_url}, coll_dict)
        else:
            # New source
            new_src_id = exe(
                "INSERT INTO collection_sources (collection_id,label,kibana_url,sort_order) VALUES (?,?,?,?)",
                (cid, src.label, src.kibana_url, i),
            )
            touched_src_ids.add(new_src_id)
            jid = _ensure_source_job({"label": src.label, "kibana_url": src.kibana_url,
                                       "id": new_src_id, "job_id": None}, coll_dict)
        job_ids.append(jid)

    # Remove sources that were dropped
    for src_id in existing_ids_all - touched_src_ids:
        old_src = next((r for r in existing_srcs if r["id"] == src_id), None)
        if old_src and old_src.get("job_id"):
            exe("DELETE FROM scrape_jobs WHERE id=?", (old_src["job_id"],))
        exe("DELETE FROM collection_sources WHERE id=?", (src_id,))

    exe("DELETE FROM collection_corr_keys WHERE collection_id=?", (cid,))
    for field in body.corr_keys:
        exe("INSERT INTO collection_corr_keys (collection_id,field_name) VALUES (?,?)", (cid, field))

    if len(job_ids) >= 2:
        _sync_collection_group(cid, body.name, job_ids, body.corr_keys)
    _refresh_scheduler()
    return {"ok": True}


@router.delete("/api/collections/{cid}")
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


@router.delete("/api/collections/{cid}/data")
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


@router.post("/api/collections/{cid}/run")
def run_collection(cid: int):
    """Start a run for all sources in this collection, sequentially."""
    from routers.groups import _build_correlations_sync
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
            # B6 fix: don't insert a duplicate per-source job_runs row;
            # the master run_id row already tracks the whole collection run.

        # Auto-build correlations if group has keys defined
        g = qone("SELECT id FROM log_groups WHERE collection_id=?", (cid,))
        if g:
            gid = g['id']
            keys = qall("SELECT * FROM log_group_keys WHERE group_id=?", (gid,)) or []
            if keys:
                _run_logs[run_id].append(f"\n{'━'*50}")
                _run_logs[run_id].append(f"[🔗] Auto-building correlations (group '{g.get('id')}', {len(keys)} key(s))…")
                try:
                    linked = _build_correlations_sync(
                        gid,
                        log_fn=lambda msg: _run_logs[run_id].append(msg)
                    )
                    _run_logs[run_id].append(f"[✓] Correlations done — {linked:,} pairs linked")
                except Exception as e:
                    _run_logs[run_id].append(f"[!] Correlation build failed: {e}")
            else:
                _run_logs[run_id].append("\n[→] No correlation keys defined — skipping auto-link")

        status = "failed" if any_failed else "success"
        _run_status[run_id] = status
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log = "\n".join(_run_logs[run_id])
        # Cap log_output at 50 KB to match the single-job run limit (P3-6)
        log_capped = log if len(log) <= 51200 else log[-51200:]
        exe("UPDATE job_runs SET status=?,finished_at=?,rows_saved=?,log_output=? WHERE id=?",
            (status, now, total_saved, log_capped, run_id))
        exe("UPDATE scrape_collections SET last_run_at=?,last_run_status=? WHERE id=?",
            (now, status, cid))
        _evict_old_runs()

    t = threading.Thread(target=_do_collection_run, daemon=True)
    t.start()
    return {"run_id": run_id, "collection_id": cid}
