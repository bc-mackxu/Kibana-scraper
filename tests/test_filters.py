"""Unit tests for routers/_filters.py :: _build_log_filter."""
import json
import pytest

from routers._filters import _build_log_filter


# ── Helpers ───────────────────────────────────────────────────────────────────

def _w(where):
    """Normalise whitespace for easier assertion."""
    return " ".join(where.split())


# ── Basic conditions ──────────────────────────────────────────────────────────

class TestBasicConditions:
    def test_job_id_only(self):
        where, args = _build_log_filter(job_id=1)
        assert "job_id=?" in where
        assert args == (1,)

    def test_search_adds_like(self):
        where, args = _build_log_filter(job_id=1, search="foo")
        assert "raw_json LIKE ?" in where
        assert "%foo%" in args

    def test_regex_mode(self):
        where, args = _build_log_filter(job_id=1, search="err.*", regex=True)
        assert "REGEXP ?" in where
        assert "err.*" in args

    def test_date_range(self):
        where, args = _build_log_filter(job_id=1, from_date="2024-01-01", to_date="2024-12-31")
        assert "timestamp >= ?" in where
        assert "timestamp <= ?" in where
        assert "2024-01-01" in args
        assert "2024-12-31" in args

    def test_relevance_filter(self):
        where, args = _build_log_filter(job_id=1, relevance="relevant")
        assert "relevance=?" in where
        assert "relevant" in args

    def test_extra_conditions(self):
        where, args = _build_log_filter(job_id=1, extra_conditions=["timestamp IS NOT NULL"])
        assert "timestamp IS NOT NULL" in where

    def test_no_search_no_cross_source_clause(self):
        where, args = _build_log_filter(job_id=1, cross_source=True)
        # No search → cross-source subquery should NOT appear
        assert "log_correlations" not in where


# ── Field filters ─────────────────────────────────────────────────────────────

class TestFieldFilters:
    def _ff(self, field, value, op="eq"):
        return json.dumps([{"field": field, "value": value, "op": op}])

    def test_eq_filter(self):
        where, args = _build_log_filter(job_id=1, field_filters=self._ff("status", "200"))
        assert "COALESCE" in where
        assert "200" in args

    def test_neq_filter(self):
        where, args = _build_log_filter(job_id=1, field_filters=self._ff("status", "500", op="neq"))
        assert "!=" in where
        assert "500" in args

    def test_raw_keyword_suffix_stripped(self):
        where, args = _build_log_filter(job_id=1, field_filters=self._ff("message.raw", "hello"))
        # Should become $.message, not $.message.raw
        assert "$.message" in str(args)
        assert ".raw" not in str(args)

    def test_keyword_suffix_stripped(self):
        where, args = _build_log_filter(job_id=1, field_filters=self._ff("host.keyword", "web01"))
        assert ".keyword" not in str(args)
        assert "$.host" in str(args)

    def test_sql_injection_rejected(self):
        # Field names with special chars should be silently skipped
        where, args = _build_log_filter(job_id=1, field_filters=self._ff("bad field;drop", "x"))
        # Only the job_id condition should remain
        assert where.strip() == "job_id=?"

    def test_invalid_json_ignored(self):
        where, args = _build_log_filter(job_id=1, field_filters="not-valid-json")
        assert where.strip() == "job_id=?"

    def test_multiple_filters_stacked(self):
        ff = json.dumps([
            {"field": "status", "value": "200"},
            {"field": "method", "value": "GET"},
        ])
        where, args = _build_log_filter(job_id=1, field_filters=ff)
        # Each filter adds one CAST(COALESCE(...)) condition → 2 total
        assert where.count("COALESCE") == 2
        # Both field paths should appear
        assert "$.status" in str(args)
        assert "$.method" in str(args)


# ── Cross-source ──────────────────────────────────────────────────────────────

class TestCrossSource:
    def test_search_adds_cross_subquery(self):
        where, args = _build_log_filter(job_id=1, search="foo", cross_source=True)
        assert "log_correlations" in where
        assert "lc.log_id" in where

    def test_regex_cross_source(self):
        where, args = _build_log_filter(job_id=1, search="err.*", regex=True, cross_source=True)
        assert "REGEXP" in where
        assert "log_correlations" in where

    def test_field_filter_cross_source(self):
        ff = json.dumps([{"field": "status", "value": "500"}])
        where, args = _build_log_filter(job_id=1, field_filters=ff, cross_source=True)
        assert "log_correlations" in where
        assert "ce.raw_json" in where

    def test_cross_source_without_search_no_subquery(self):
        # cross_source with no search/field_filters should not add the subquery
        where, args = _build_log_filter(job_id=1, cross_source=True)
        assert "log_correlations" not in where


# ── Multi-source ──────────────────────────────────────────────────────────────

class TestMultiSource:
    def test_job_ids_uses_in_clause(self):
        where, args = _build_log_filter(job_id=1, job_ids=[10, 11])
        assert "job_id IN (?,?)" in where
        assert 10 in args
        assert 11 in args

    def test_job_ids_disables_cross_source(self):
        where, args = _build_log_filter(job_id=1, job_ids=[10, 11], search="foo", cross_source=True)
        # Multi-source disables cross_source — should NOT add log_correlations subquery
        assert "log_correlations" not in where

    def test_single_job_in_list_falls_back(self):
        where, args = _build_log_filter(job_id=1, job_ids=[5])
        # Single ID in list: len(job_ids) > 1 is False → falls back to regular job_id=?
        # The job_id parameter is used, not the single-element list
        assert "job_id=?" in where
        assert args[0] == 1  # original job_id is used

    def test_empty_job_ids_uses_eq(self):
        where, args = _build_log_filter(job_id=1, job_ids=[])
        assert "job_id=?" in where
        assert args[0] == 1
