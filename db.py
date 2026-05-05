"""
Shared SQLite layer for kibana-scraper.

All three entry-points (app.py, kibana_scraper.py, mcp_server.py) import from here
so the DB path and helpers stay in one place.
"""

import re
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "kibana_data.db"


# ─── Connection ───────────────────────────────────────────────────────────────

def _db():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False, timeout=30)
    conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    # SQLite doesn't have REGEXP built in — register a Python implementation
    conn.create_function(
        "REGEXP", 2,
        lambda pattern, value: bool(re.search(pattern, str(value) if value else "")) if pattern else False,
    )
    return conn


def qone(sql, args=()):
    c = _db()
    try:
        return c.execute(sql, args).fetchone()
    finally:
        c.close()


def qall(sql, args=()):
    c = _db()
    try:
        return c.execute(sql, args).fetchall()
    finally:
        c.close()


def exe(sql, args=()):
    c = _db()
    try:
        cur = c.execute(sql, args)
        c.commit()
        return cur.lastrowid
    finally:
        c.close()


# ─── Schema ───────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS scrape_jobs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    name           TEXT    NOT NULL,
    kibana_url     TEXT    NOT NULL,
    time_range     TEXT    DEFAULT 'Last 7 days',
    max_pages      INTEGER DEFAULT 50,
    schedule_hour  INTEGER DEFAULT 2,
    enabled        INTEGER DEFAULT 1,
    chunk_hours    INTEGER DEFAULT 0,
    last_run_at    DATETIME DEFAULT NULL,
    last_run_status TEXT   DEFAULT NULL,
    last_rows_saved INTEGER DEFAULT 0,
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS job_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      INTEGER NOT NULL,
    started_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    finished_at DATETIME DEFAULT NULL,
    status      TEXT    DEFAULT 'running',
    rows_saved  INTEGER DEFAULT 0,
    log_output  TEXT    DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS log_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    es_id      TEXT    DEFAULT NULL,
    es_index   TEXT    DEFAULT NULL,
    timestamp  DATETIME DEFAULT NULL,
    job_id     INTEGER DEFAULT NULL,
    run_id     INTEGER DEFAULT NULL,
    raw_json   TEXT    DEFAULT NULL,
    scraped_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    relevance  TEXT    DEFAULT NULL,
    severity   TEXT    DEFAULT NULL,
    ai_summary TEXT    DEFAULT NULL,
    code_refs  TEXT    DEFAULT NULL,
    UNIQUE (es_id)
);

CREATE INDEX IF NOT EXISTS idx_log_events_job_id   ON log_events (job_id);
CREATE INDEX IF NOT EXISTS idx_log_events_timestamp ON log_events (timestamp);

CREATE TABLE IF NOT EXISTS scrape_collections (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    name           TEXT    NOT NULL,
    time_range     TEXT    DEFAULT 'Last 7 days',
    chunk_hours    INTEGER DEFAULT 0,
    schedule_hour  INTEGER DEFAULT 2,
    enabled        INTEGER DEFAULT 1,
    last_run_at    DATETIME DEFAULT NULL,
    last_run_status TEXT   DEFAULT NULL,
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS collection_sources (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    collection_id INTEGER NOT NULL,
    label         TEXT    NOT NULL,
    kibana_url    TEXT    NOT NULL,
    sort_order    INTEGER DEFAULT 0,
    job_id        INTEGER DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS collection_corr_keys (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    collection_id INTEGER NOT NULL,
    field_name    TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS log_groups (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT    NOT NULL,
    description   TEXT    DEFAULT '',
    collection_id INTEGER DEFAULT NULL,
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS log_group_members (
    group_id INTEGER NOT NULL,
    job_id   INTEGER NOT NULL,
    label    TEXT    DEFAULT '',
    PRIMARY KEY (group_id, job_id)
);

CREATE TABLE IF NOT EXISTS log_group_keys (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id   INTEGER NOT NULL,
    job_id_a   INTEGER NOT NULL,
    field_a    TEXT    NOT NULL,
    job_id_b   INTEGER NOT NULL,
    field_b    TEXT    NOT NULL,
    confidence REAL    DEFAULT 0.0,
    source     TEXT    DEFAULT 'auto'
);

CREATE TABLE IF NOT EXISTS log_correlations (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    log_id       INTEGER NOT NULL,
    corr_id      INTEGER NOT NULL,
    group_id     INTEGER NOT NULL,
    source_label TEXT    DEFAULT '',
    matched_keys TEXT    DEFAULT NULL,
    UNIQUE (log_id, corr_id, group_id)
);

CREATE INDEX IF NOT EXISTS idx_correlations_log_id  ON log_correlations (log_id);
CREATE INDEX IF NOT EXISTS idx_correlations_corr_id ON log_correlations (corr_id);
"""


def init_db():
    """Create all tables if they don't exist yet. Safe to call multiple times."""
    c = _db()
    try:
        c.executescript(_SCHEMA)
        c.commit()
    finally:
        c.close()
