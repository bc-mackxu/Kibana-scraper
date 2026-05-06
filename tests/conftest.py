"""
Shared fixtures for kibana-scraper tests.

Each test gets a fresh in-file SQLite database (tmp_path/test.db) so tests are
fully isolated.  The `db` module uses a thread-local connection cached against
db.DB_PATH; patching the path and clearing the cache before every test is
enough to redirect all queries.
"""
import json
import pytest


@pytest.fixture()
def patched_db(tmp_path, monkeypatch):
    """Return the db module pointed at a fresh, initialised SQLite file."""
    import db as db_module

    db_path = tmp_path / "test.db"

    # Clear any cached connection from a previous test
    db_module._local.__dict__.pop("conn", None)
    monkeypatch.setattr(db_module, "DB_PATH", db_path)
    # Clear again after the patch so next qone/qall/exe opens against new path
    db_module._local.__dict__.pop("conn", None)

    db_module.init_db()

    yield db_module

    # Cleanup: drop the connection so tmp_path can be removed on Windows
    db_module._local.__dict__.pop("conn", None)


# ── Seed helpers ──────────────────────────────────────────────────────────────

def seed_job(db_module, name="Test Job", url="https://example.com") -> int:
    """Insert a minimal scrape_jobs row and return its id."""
    return db_module.exe(
        "INSERT INTO scrape_jobs (name, kibana_url) VALUES (?, ?)",
        (name, url),
    )


def seed_log(db_module, job_id: int, raw_json: dict, **kwargs) -> int:
    """Insert a log_events row and return its id."""
    ts        = kwargs.get("timestamp", "2024-01-01 00:00:00")
    severity  = kwargs.get("severity",  "info")
    relevance = kwargs.get("relevance", None)
    es_id     = kwargs.get("es_id",     None)
    return db_module.exe(
        """INSERT INTO log_events
               (job_id, timestamp, severity, relevance, raw_json, es_id)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (job_id, ts, severity, relevance, json.dumps(raw_json), es_id),
    )
