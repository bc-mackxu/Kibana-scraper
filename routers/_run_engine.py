"""Shared run-engine state and execution logic."""

import json
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from db import qone, qall, exe, DB_PATH

# ─── Config ───────────────────────────────────────────────────────────────────

SCRAPER_DIR = Path(__file__).parent.parent
SCRAPER_CMD = [sys.executable, "-u", str(SCRAPER_DIR / "kibana_scraper.py")]

# ─── In-memory state for active runs ─────────────────────────────────────────

_run_logs:   dict = {}  # run_id → list[str]
_run_status: dict = {}  # run_id → "running"|"success"|"failed"
_run_procs:  dict = {}  # run_id → subprocess.Popen (for cancellation)

_RUN_LOG_MAX_LINES = 5000   # cap per run to prevent unbounded memory
_RUN_KEEP_FINISHED = 50     # keep at most this many finished run entries in memory

# ─── Scheduler ────────────────────────────────────────────────────────────────

scheduler = BackgroundScheduler(timezone="UTC")


# ─── Time-range helpers (also needed by run engine) ──────────────────────────

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
    import re
    key = time_range.strip().lower()
    if key in _TR_HOURS:
        return _TR_HOURS[key]
    m = re.match(r'last\s+(\d+(?:\.\d+)?)\s+(minute|hour|day|week|month|year)s?', key)
    if m:
        n, unit = float(m.group(1)), m.group(2)
        return n * {"minute": 1/60, "hour": 1, "day": 24,
                    "week": 168, "month": 720, "year": 8760}[unit]
    return 168  # fallback: 7 days


def _build_chunks(time_range: str, chunk_hours: int) -> list:
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


# ─── Run helpers ──────────────────────────────────────────────────────────────

def _evict_old_runs():
    """Drop finished run entries beyond the keep limit (B2 memory leak fix).
    Sort by run_id (ascending) so oldest entries are evicted first."""
    finished = sorted(rid for rid, st in _run_status.items() if st != "running")
    for rid in finished[:-_RUN_KEEP_FINISHED]:
        _run_logs.pop(rid, None)
        _run_status.pop(rid, None)
        _run_procs.pop(rid, None)


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
        _run_procs[run_id] = proc
        logs = _run_logs[run_id]
        for line in proc.stdout:
            line = line.rstrip()
            logs.append(line)
            # cap log size to avoid unbounded memory (B2)
            if len(logs) > _RUN_LOG_MAX_LINES:
                del logs[0]
            # Parse "…rows saved  (total 250,)" — split on "(total " to get the count
            if "rows saved" in line or "Total rows saved:" in line:
                    for marker in ("(total ", "Total rows saved:"):
                        if marker in line:
                            try:
                                rows_saved_ref[0] = int(
                                    line.split(marker)[1].strip().rstrip(",)").split()[0]
                                )
                            except (IndexError, ValueError):
                                pass
                            break
        proc.wait()
        _run_procs.pop(run_id, None)
        return proc.returncode == 0
    except Exception as e:
        _run_logs[run_id].append(f"[ERROR] {e}")
        _run_procs.pop(run_id, None)
        return False


def _do_run(run_id: int, job: dict):
    _run_logs[run_id]   = []
    _run_status[run_id] = "running"
    try:
        _do_run_inner(run_id, job)
    except Exception as exc:
        _run_logs[run_id].append(f"[FATAL] Unhandled error: {exc}")
        _run_status[run_id] = "failed"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_capped = "\n".join(_run_logs[run_id])[-51200:]
        try:
            exe("UPDATE job_runs SET status='failed', finished_at=?, log_output=? WHERE id=?",
                (now, log_capped, run_id))
        except Exception:
            pass
        _evict_old_runs()


def _do_run_inner(run_id: int, job: dict):
    last_ts     = _last_timestamp(job["id"])
    chunk_hours = int(job.get("chunk_hours") or 0)
    rows_ref    = [0]       # mutable accumulator for total saved
    any_failed  = False

    if last_ts:
        # ── Incremental mode (always single window) ──────────────────────────
        from_dt = datetime.fromisoformat(str(last_ts).replace(" ", "T")).strftime("%Y-%m-%dT%H:%M:%S")
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

    # Cap log_output at 50 KB to avoid bloating the DB (D3)
    log_capped = log if len(log) <= 51200 else log[-51200:]
    exe("UPDATE job_runs SET status=?, finished_at=?, rows_saved=?, log_output=? WHERE id=?",
        (status, now, rows_ref[0], log_capped, run_id))
    exe("UPDATE scrape_jobs SET last_run_at=?, last_run_status=?, last_rows_saved=? WHERE id=?",
        (now, status, rows_ref[0], job["id"]))
    _evict_old_runs()


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
