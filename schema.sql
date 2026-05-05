CREATE DATABASE IF NOT EXISTS kibana_data CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE kibana_data;

-- Generic log events table (replaces nginx_logs for new scrapes)
CREATE TABLE IF NOT EXISTS log_events (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    es_id       VARCHAR(100) UNIQUE,
    es_index    VARCHAR(200),
    `timestamp` DATETIME(3),
    job_id      INT NOT NULL,
    run_id      INT NULL,
    raw_json    JSON,
    scraped_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_job_id  (job_id),
    INDEX idx_job_ts  (job_id, `timestamp`),
    INDEX idx_run_id  (run_id)
);

CREATE TABLE IF NOT EXISTS nginx_logs (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    es_id         VARCHAR(100)  UNIQUE,
    es_index      VARCHAR(200),
    `timestamp`   DATETIME(3),
    store_id      VARCHAR(100),
    request_uri   TEXT,
    server_name   VARCHAR(200),
    remote_addr   VARCHAR(50),
    http_status   SMALLINT,
    request_method VARCHAR(20),
    request_time  FLOAT,
    http_user_agent TEXT,
    http_referer  TEXT,
    role          VARCHAR(100),
    raw_json      JSON,
    run_id        INT NULL,
    scraped_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_store_id   (store_id),
    INDEX idx_timestamp  (`timestamp`),
    INDEX idx_server     (server_name),
    INDEX idx_run_id     (run_id)
);

CREATE TABLE IF NOT EXISTS scrape_jobs (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    name            VARCHAR(200) NOT NULL,
    kibana_url      TEXT NOT NULL,
    time_range      VARCHAR(100) DEFAULT 'Last 7 days',
    max_pages       INT DEFAULT 50,
    schedule_hour   INT DEFAULT 2,
    enabled         TINYINT(1) DEFAULT 1,
    last_run_at     DATETIME NULL,
    last_run_status VARCHAR(50) NULL,
    last_rows_saved INT DEFAULT 0,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS job_runs (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    job_id      INT NOT NULL,
    started_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at DATETIME NULL,
    status      VARCHAR(50) DEFAULT 'running',
    rows_saved  INT DEFAULT 0,
    log_output  LONGTEXT,
    INDEX idx_job_id (job_id)
);
