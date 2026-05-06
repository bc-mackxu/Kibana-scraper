"""Shared query filter builder used by jobs, histogram, and export endpoints."""

import json
import re


def _build_log_filter(
    job_id: int,
    search: str = "",
    from_date: str = "",
    to_date: str = "",
    relevance: str = "",
    field_filters: str = "",
    regex: bool = False,
    cross_source: bool = False,
    extra_conditions: list = None,   # e.g. ["timestamp IS NOT NULL"]
    job_ids: list = None,            # multi-source: search across all listed job IDs
) -> tuple:
    """
    Build a WHERE clause + args tuple for log_events queries.
    Always adds job_id=? (or job_id IN (...)) as the first condition.

    When job_ids is provided (multi-source mode), searches across all those jobs
    and disables cross-source correlation (not needed — data is already merged).

    When cross_source=True and a search/field-filter is given, also matches rows
    whose correlated partner records satisfy the same condition.

    Returns (where_clause, args) — caller adds ORDER BY / LIMIT / OFFSET.
    """
    # Multi-source mode: override job_id filter with IN clause; disable correlation
    if job_ids and len(job_ids) > 1:
        placeholders = ','.join('?' * len(job_ids))
        conditions: list = [f"job_id IN ({placeholders})"]
        args: tuple = tuple(job_ids)
        cross_source = False          # already seeing all sources — correlation N/A
    else:
        conditions: list = ["job_id=?"]
        args: tuple = (job_id,)

    if extra_conditions:
        conditions.extend(extra_conditions)

    cross_ids_clause = ""
    cross_args: tuple = ()

    if search:
        if regex:
            conditions.append("CAST(raw_json AS TEXT) REGEXP ?")
            args += (search,)
            if cross_source:
                cross_ids_clause = (
                    "OR le.id IN ("
                    "  SELECT lc.log_id FROM log_correlations lc"
                    "  JOIN log_events ce ON ce.id = lc.corr_id"
                    "  WHERE lc.log_id IN (SELECT id FROM log_events WHERE job_id=?)"
                    "  AND CAST(ce.raw_json AS TEXT) REGEXP ?"
                    ")"
                )
                cross_args = (job_id, search)
        else:
            conditions.append("raw_json LIKE ?")
            args += (f"%{search}%",)
            if cross_source:
                cross_ids_clause = (
                    "OR le.id IN ("
                    "  SELECT lc.log_id FROM log_correlations lc"
                    "  JOIN log_events ce ON ce.id = lc.corr_id"
                    "  WHERE lc.log_id IN (SELECT id FROM log_events WHERE job_id=?)"
                    "  AND ce.raw_json LIKE ?"
                    ")"
                )
                cross_args = (job_id, f"%{search}%")

    if from_date:
        conditions.append("timestamp >= ?")
        args += (from_date,)
    if to_date:
        conditions.append("timestamp <= ?")
        args += (to_date,)
    if relevance:
        conditions.append("relevance=?")
        args += (relevance,)

    if field_filters:
        try:
            for ff in json.loads(field_filters):
                field = ff.get("field", "")
                value = ff.get("value", "")
                op    = ff.get("op", "eq")
                if not field or not re.match(r'^[A-Za-z0-9_.@\-]+$', field):
                    continue
                # Strip Elasticsearch sub-field suffixes (.raw, .keyword) —
                # these aren't present in the stored _source JSON.
                field = re.sub(r'\.(raw|keyword)$', '', field)
                p0      = f"$.{field}[0]"
                p       = f"$.{field}"
                coalesce    = "CAST(COALESCE(json_extract(raw_json,?),json_extract(raw_json,?)) AS TEXT)"
                ce_coalesce = "CAST(COALESCE(json_extract(ce.raw_json,?),json_extract(ce.raw_json,?)) AS TEXT)"
                if op == "neq":
                    direct_cond = f"({coalesce} != ? OR json_extract(raw_json,?) IS NULL)"
                    direct_args = (p0, p, value, p)
                    cross_cond  = f"({ce_coalesce} != ? OR json_extract(ce.raw_json,?) IS NULL)"
                    cross_cond_args = (p0, p, value, p)
                else:
                    direct_cond = f"{coalesce} = ?"
                    direct_args = (p0, p, value)
                    cross_cond  = f"{ce_coalesce} = ?"
                    cross_cond_args = (p0, p, value)
                if cross_source:
                    cross_sub = (
                        "le.id IN ("
                        "  SELECT lc.log_id FROM log_correlations lc"
                        "  JOIN log_events ce ON ce.id = lc.corr_id"
                        "  WHERE lc.log_id IN (SELECT id FROM log_events WHERE job_id=?)"
                        f"  AND {cross_cond}"
                        ")"
                    )
                    conditions.append(f"({direct_cond} OR {cross_sub})")
                    args += direct_args + (job_id,) + cross_cond_args
                else:
                    conditions.append(direct_cond)
                    args += direct_args
        except Exception:
            pass

    # Build final WHERE: if cross_source active, merge search + cross subquery
    if cross_ids_clause and cross_args:
        # Find the search condition index (always right after job_id + any extra_conditions)
        n_prefix = 1 + len(extra_conditions or [])
        search_cond = conditions[n_prefix]          # LIKE or REGEXP
        search_arg  = args[n_prefix]                # the search value
        rest_conds  = conditions[n_prefix + 1:]
        rest_args   = args[n_prefix + 1:]
        merged = conditions[:n_prefix] + [f"({search_cond} {cross_ids_clause})"] + rest_conds
        where  = " AND ".join(merged)
        full_args = args[:n_prefix] + (search_arg,) + cross_args + rest_args
    else:
        where     = " AND ".join(conditions)
        full_args = args

    return where, full_args
