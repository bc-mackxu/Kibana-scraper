"""
Functional tests for the FastAPI application.

Each test class uses the `client` fixture which spins up the app against a
fresh in-memory SQLite database so tests are fully isolated.
"""
import json
import pytest
from fastapi.testclient import TestClient


# ── Fixture ───────────────────────────────────────────────────────────────────

@pytest.fixture()
def client(patched_db):
    """Return a TestClient wired to a fresh test DB."""
    import app as app_module
    with TestClient(app_module.app) as c:
        yield c


# ── Jobs CRUD ─────────────────────────────────────────────────────────────────

class TestJobsCRUD:
    def test_list_jobs_empty(self, client):
        r = client.get("/api/jobs")
        assert r.status_code == 200
        assert r.json() == []

    def test_create_job(self, client):
        r = client.post("/api/jobs", json={"name": "My Job", "kibana_url": "https://k.example.com"})
        assert r.status_code == 201
        data = r.json()
        assert data["id"] > 0

    def test_list_jobs_after_create(self, client):
        client.post("/api/jobs", json={"name": "J1", "kibana_url": "https://a.com"})
        client.post("/api/jobs", json={"name": "J2", "kibana_url": "https://b.com"})
        r = client.get("/api/jobs")
        names = [j["name"] for j in r.json()]
        assert "J1" in names and "J2" in names

    def test_delete_job(self, client):
        r = client.post("/api/jobs", json={"name": "ToDelete", "kibana_url": "https://x.com"})
        jid = r.json()["id"]
        r2 = client.delete(f"/api/jobs/{jid}")
        assert r2.status_code == 200
        ids = [j["id"] for j in client.get("/api/jobs").json()]
        assert jid not in ids


# ── Job data endpoint ─────────────────────────────────────────────────────────

class TestJobData:
    def test_data_empty(self, client, patched_db):
        from tests.conftest import seed_job
        jid = seed_job(patched_db)
        r = client.get(f"/api/jobs/{jid}/data")
        assert r.status_code == 200
        d = r.json()
        assert d["total"] == 0
        assert d["rows"] == []

    def test_data_returns_rows(self, client, patched_db):
        from tests.conftest import seed_job, seed_log
        jid = seed_job(patched_db)
        seed_log(patched_db, jid, {"message": "hello world"})
        seed_log(patched_db, jid, {"message": "another entry"})
        r = client.get(f"/api/jobs/{jid}/data")
        assert r.json()["total"] == 2

    def test_data_search_filter(self, client, patched_db):
        from tests.conftest import seed_job, seed_log
        jid = seed_job(patched_db)
        seed_log(patched_db, jid, {"message": "Export failure detected"})
        seed_log(patched_db, jid, {"message": "Normal log entry"})
        r = client.get(f"/api/jobs/{jid}/data", params={"search": "Export failure"})
        assert r.json()["total"] == 1

    def test_data_regex_filter(self, client, patched_db):
        from tests.conftest import seed_job, seed_log
        jid = seed_job(patched_db)
        seed_log(patched_db, jid, {"message": "Error 500 timeout"})
        seed_log(patched_db, jid, {"message": "Status OK"})
        r = client.get(f"/api/jobs/{jid}/data", params={"search": "Error.*timeout", "regex": "true"})
        assert r.json()["total"] == 1

    def test_data_from_date_filter(self, client, patched_db):
        from tests.conftest import seed_job, seed_log
        jid = seed_job(patched_db)
        seed_log(patched_db, jid, {"message": "old"}, timestamp="2023-01-01 00:00:00")
        seed_log(patched_db, jid, {"message": "new"}, timestamp="2024-06-01 00:00:00")
        r = client.get(f"/api/jobs/{jid}/data", params={"from_date": "2024-01-01 00:00:00"})
        assert r.json()["total"] == 1

    def test_data_relevance_filter(self, client, patched_db):
        from tests.conftest import seed_job, seed_log
        jid = seed_job(patched_db)
        seed_log(patched_db, jid, {"message": "a"}, relevance="relevant")
        seed_log(patched_db, jid, {"message": "b"}, relevance="irrelevant")
        r = client.get(f"/api/jobs/{jid}/data", params={"relevance": "relevant"})
        assert r.json()["total"] == 1


class TestJobDataFilters:
    def test_field_filter_eq(self, client, patched_db):
        from tests.conftest import seed_job, seed_log
        jid = seed_job(patched_db)
        seed_log(patched_db, jid, {"status": "200", "message": "ok"})
        seed_log(patched_db, jid, {"status": "500", "message": "err"})
        ff = json.dumps([{"field": "status", "value": "200", "op": "eq"}])
        r = client.get(f"/api/jobs/{jid}/data", params={"field_filters": ff})
        assert r.json()["total"] == 1

    def test_field_filter_neq(self, client, patched_db):
        from tests.conftest import seed_job, seed_log
        jid = seed_job(patched_db)
        seed_log(patched_db, jid, {"status": "200"})
        seed_log(patched_db, jid, {"status": "500"})
        ff = json.dumps([{"field": "status", "value": "500", "op": "neq"}])
        r = client.get(f"/api/jobs/{jid}/data", params={"field_filters": ff})
        assert r.json()["total"] == 1

    def test_field_filter_keyword_suffix(self, client, patched_db):
        from tests.conftest import seed_job, seed_log
        jid = seed_job(patched_db)
        seed_log(patched_db, jid, {"host": "web01"})
        seed_log(patched_db, jid, {"host": "web02"})
        # .keyword suffix should be stripped
        ff = json.dumps([{"field": "host.keyword", "value": "web01", "op": "eq"}])
        r = client.get(f"/api/jobs/{jid}/data", params={"field_filters": ff})
        assert r.json()["total"] == 1

    def test_invalid_field_filter_ignored(self, client, patched_db):
        from tests.conftest import seed_job, seed_log
        jid = seed_job(patched_db)
        seed_log(patched_db, jid, {"msg": "x"})
        r = client.get(f"/api/jobs/{jid}/data", params={"field_filters": "not-json"})
        assert r.status_code == 200
        assert r.json()["total"] == 1  # filter ignored → all rows returned


class TestPagination:
    def test_per_page(self, client, patched_db):
        from tests.conftest import seed_job, seed_log
        jid = seed_job(patched_db)
        for i in range(10):
            seed_log(patched_db, jid, {"msg": f"entry {i}"})
        r = client.get(f"/api/jobs/{jid}/data", params={"per_page": 3, "page": 1})
        d = r.json()
        assert d["total"] == 10
        assert len(d["rows"]) == 3

    def test_page_2(self, client, patched_db):
        from tests.conftest import seed_job, seed_log
        jid = seed_job(patched_db)
        for i in range(10):
            seed_log(patched_db, jid, {"msg": f"entry {i}"})
        r1 = client.get(f"/api/jobs/{jid}/data", params={"per_page": 5, "page": 1})
        r2 = client.get(f"/api/jobs/{jid}/data", params={"per_page": 5, "page": 2})
        ids1 = {row["id"] for row in r1.json()["rows"]}
        ids2 = {row["id"] for row in r2.json()["rows"]}
        assert ids1.isdisjoint(ids2)


class TestMultiSource:
    def test_job_ids_returns_from_both(self, client, patched_db):
        from tests.conftest import seed_job, seed_log
        jid1 = seed_job(patched_db, name="Nginx")
        jid2 = seed_job(patched_db, name="Syslog")
        seed_log(patched_db, jid1, {"message": "nginx log"})
        seed_log(patched_db, jid2, {"message": "syslog entry"})
        r = client.get(f"/api/jobs/{jid1}/data", params={"job_ids": f"{jid1},{jid2}"})
        assert r.json()["total"] == 2

    def test_job_ids_with_search(self, client, patched_db):
        from tests.conftest import seed_job, seed_log
        jid1 = seed_job(patched_db, name="Nginx")
        jid2 = seed_job(patched_db, name="Syslog")
        seed_log(patched_db, jid1, {"message": "Export failure in nginx"})
        seed_log(patched_db, jid1, {"message": "Normal nginx"})
        seed_log(patched_db, jid2, {"message": "Export failure in syslog"})
        seed_log(patched_db, jid2, {"message": "Normal syslog"})
        r = client.get(
            f"/api/jobs/{jid1}/data",
            params={"job_ids": f"{jid1},{jid2}", "search": "Export failure"},
        )
        assert r.json()["total"] == 2  # one from each source

    def test_rows_have_source_label(self, client, patched_db):
        from tests.conftest import seed_job, seed_log
        jid1 = seed_job(patched_db, name="Nginx")
        jid2 = seed_job(patched_db, name="Syslog")
        seed_log(patched_db, jid1, {"message": "nginx log"})
        seed_log(patched_db, jid2, {"message": "syslog log"})
        r = client.get(f"/api/jobs/{jid1}/data", params={"job_ids": f"{jid1},{jid2}"})
        sources = {row.get("_source") for row in r.json()["rows"]}
        assert "Nginx" in sources
        assert "Syslog" in sources

    def test_single_source_no_label(self, client, patched_db):
        from tests.conftest import seed_job, seed_log
        jid = seed_job(patched_db, name="Nginx")
        seed_log(patched_db, jid, {"message": "hello"})
        r = client.get(f"/api/jobs/{jid}/data")
        row = r.json()["rows"][0]
        assert "_source" not in row or row["_source"] is None or row["_source"] == ""


# ── Histogram ─────────────────────────────────────────────────────────────────

class TestHistogram:
    def test_histogram_returns_buckets(self, client, patched_db):
        from tests.conftest import seed_job, seed_log
        jid = seed_job(patched_db)
        for ts in ["2024-01-01 10:00:00", "2024-01-01 11:00:00", "2024-01-02 10:00:00"]:
            seed_log(patched_db, jid, {"msg": "x"}, timestamp=ts)
        r = client.get(f"/api/jobs/{jid}/histogram")
        assert r.status_code == 200
        d = r.json()
        assert "buckets" in d
        assert len(d["buckets"]) >= 1
        total = sum(b.get("c", 0) for b in d["buckets"])
        assert total == 3

    def test_histogram_with_search(self, client, patched_db):
        from tests.conftest import seed_job, seed_log
        jid = seed_job(patched_db)
        seed_log(patched_db, jid, {"message": "Export failure"}, timestamp="2024-01-01 10:00:00")
        seed_log(patched_db, jid, {"message": "Normal"}, timestamp="2024-01-01 11:00:00")
        r = client.get(f"/api/jobs/{jid}/histogram", params={"search": "Export failure"})
        d = r.json()
        total = sum(b.get("c", 0) for b in d.get("buckets", []))
        assert total == 1


# ── Collections ───────────────────────────────────────────────────────────────

class TestCollections:
    def test_create_collection(self, client):
        payload = {
            "name": "My Collection",
            "sources": [
                {"label": "Nginx",  "kibana_url": "https://kibana.example.com/nginx"},
                {"label": "Syslog", "kibana_url": "https://kibana.example.com/syslog"},
            ],
            "corr_keys": ["request_id"],
        }
        r = client.post("/api/collections", json=payload)
        assert r.status_code in (200, 201)
        data = r.json()
        assert data["id"] > 0
        # Verify it shows up in the collection list with the right name
        listed = client.get("/api/collections").json()
        assert any(c["name"] == "My Collection" for c in listed)

    def test_list_collections(self, client):
        client.post("/api/collections", json={
            "name": "Coll A",
            "sources": [{"label": "S1", "kibana_url": "https://k.example.com"}],
        })
        r = client.get("/api/collections")
        assert r.status_code == 200
        names = [c["name"] for c in r.json()]
        assert "Coll A" in names

    def test_delete_collection(self, client):
        r = client.post("/api/collections", json={
            "name": "Temp",
            "sources": [{"label": "S1", "kibana_url": "https://k.example.com"}],
        })
        cid = r.json()["id"]
        r2 = client.delete(f"/api/collections/{cid}")
        assert r2.status_code == 200
        ids = [c["id"] for c in client.get("/api/collections").json()]
        assert cid not in ids


# ── Stats ─────────────────────────────────────────────────────────────────────

class TestStats:
    def test_global_stats(self, client, patched_db):
        from tests.conftest import seed_job, seed_log
        jid = seed_job(patched_db)
        seed_log(patched_db, jid, {"msg": "a"}, severity="error", relevance="relevant")
        seed_log(patched_db, jid, {"msg": "b"}, severity="info",  relevance="irrelevant")
        r = client.get("/api/stats")
        assert r.status_code == 200
        d = r.json()
        # Key names from the actual /api/stats response
        assert "total_rows" in d or "analyzed_rows" in d
        assert d.get("total_rows", d.get("analyzed_rows", 0)) >= 0

    def test_stats_has_severity_breakdown(self, client, patched_db):
        from tests.conftest import seed_job, seed_log
        jid = seed_job(patched_db)
        seed_log(patched_db, jid, {"msg": "a"}, severity="error")
        seed_log(patched_db, jid, {"msg": "b"}, severity="info")
        r = client.get("/api/stats")
        assert r.status_code == 200
        d = r.json()
        assert "severity" in d or "severity_counts" in d
