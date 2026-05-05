-- SQLite schema for kibana-scraper
-- Auto-generated from db.py:_SCHEMA — this is the single source of truth
-- Run "python3 db.py" to initialize the database

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
