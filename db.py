"""
Shared SQLite layer for kibana-scraper.

All three entry-points (app.py, kibana_scraper.py, mcp_server.py) import from here
so the DB path and helpers stay in one place.
"""

import re
import sqlite3
import threading
from pathlib import Path

DB_PATH = Path(__file__).parent / "kibana_data.db"

# ─── Thread-local connection pool ─────────────────────────────────────────────
# One persistent connection per thread avoids the overhead of open/close on every
# qone/qall/exe call while still being safe under Python's threading model.

_local = threading.local()


def _db() -> sqlite3.Connection:
    conn = getattr(_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False, timeout=30)
        conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA cache_size=-8000")   # 8 MB page cache per connection
        conn.create_function(
            "REGEXP", 2,
            lambda pattern, value: bool(re.search(pattern, str(value) if value else "")) if pattern else False,
        )
        _local.conn = conn
    return conn


def qone(sql, args=()):
    return _db().execute(sql, args).fetchone()


def qall(sql, args=()):
    return _db().execute(sql, args).fetchall()


def exe(sql, args=()):
    conn = _db()
    cur  = conn.execute(sql, args)
    conn.commit()
    return cur.lastrowid


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

CREATE INDEX IF NOT EXISTS idx_log_events_job_id        ON log_events (job_id);
CREATE INDEX IF NOT EXISTS idx_log_events_timestamp     ON log_events (timestamp);
CREATE INDEX IF NOT EXISTS idx_log_events_job_relevance ON log_events (job_id, relevance);
CREATE INDEX IF NOT EXISTS idx_log_events_job_severity  ON log_events (job_id, severity);
CREATE INDEX IF NOT EXISTS idx_log_events_job_ts_desc   ON log_events (job_id, timestamp DESC);

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

CREATE TABLE IF NOT EXISTS saved_searches (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id     INTEGER DEFAULT NULL,
    name       TEXT    NOT NULL,
    search     TEXT    DEFAULT '',
    from_date  TEXT    DEFAULT '',
    to_date    TEXT    DEFAULT '',
    relevance  TEXT    DEFAULT '',
    field_filters TEXT DEFAULT '[]',
    sort_by    TEXT    DEFAULT 'timestamp',
    sort_dir   TEXT    DEFAULT 'desc',
    is_preset  INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ai_config (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    key         TEXT    NOT NULL UNIQUE,
    value       TEXT    DEFAULT ''
);

CREATE TABLE IF NOT EXISTS log_tags (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    log_id  INTEGER NOT NULL,
    tag     TEXT    NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (log_id, tag)
);

CREATE INDEX IF NOT EXISTS idx_log_tags_log_id ON log_tags (log_id);
CREATE INDEX IF NOT EXISTS idx_log_tags_tag    ON log_tags (tag);

CREATE TABLE IF NOT EXISTS schema_version (
    version  INTEGER PRIMARY KEY,
    applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
"""

# Incremental migrations applied on top of the base schema
_MIGRATIONS: list[tuple[int, str]] = [
    # (version, sql)  — add new ones at the end; never edit existing ones
]


def init_db():
    """Create all tables and run any pending migrations. Safe to call multiple times."""
    conn = _db()
    conn.executescript(_SCHEMA)
    conn.commit()

    # Apply any pending migrations
    current = (conn.execute("SELECT MAX(version) FROM schema_version").fetchone() or {})
    current_v = current.get("MAX(version)") or 0
    for ver, sql in _MIGRATIONS:
        if ver > current_v:
            try:
                conn.executescript(sql)
                conn.execute("INSERT OR IGNORE INTO schema_version (version) VALUES (?)", (ver,))
                conn.commit()
            except Exception as e:
                print(f"[db] Migration {ver} failed: {e}")
