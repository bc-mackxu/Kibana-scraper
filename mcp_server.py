#!/usr/bin/env python3
"""
Kibana Scraper MCP Server

Exposes SQLite log data and AI analysis as tools for Claude Desktop.

Setup in Claude Desktop config (~/.claude/claude_desktop_config.json):
{
  "mcpServers": {
    "kibana-scraper": {
      "command": "python3",
      "args": ["/Users/mack.xu/Documents/Hackathon/2026/kibana-scraper/mcp_server.py"],
      "env": {}
    }
  }
}

Then restart Claude Desktop. Claude can then call:
  - list_jobs()
  - get_stats(job_id?)
  - query_logs(job_id, limit, severity, relevant_only, search)
  - fetch_logs_for_analysis(job_id, limit, offset)
  - save_analysis_results(results)
  - get_analysis_summary(job_id)
"""

import json
import os
import sys
from pathlib import Path

# Load keys.json so ANTHROPIC_API_KEY / GITHUB_TOKEN are available
SCRAPER_DIR = Path(__file__).parent
KEYS_FILE = SCRAPER_DIR / "keys.json"
if KEYS_FILE.exists():
    try:
        for k, v in json.loads(KEYS_FILE.read_text()).items():
            if v:
                os.environ.setdefault(k, v)
    except Exception:
        pass

from db import qone, qall, exe, init_db   # noqa: E402
from analyzer import analyze_batch, search_github  # noqa: E402

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    print("mcp package not installed. Run: pip install mcp", file=sys.stderr)
    sys.exit(1)

# ─── MCP Server ───────────────────────────────────────────────────────────────

mcp = FastMCP("kibana-scraper")

# Ensure schema on startup
init_db()


def _ensure_analysis_columns():
    """Add analysis columns if they don't exist (backward compat guard)."""
    for col, defn in [
        ("relevance",  "TEXT DEFAULT NULL"),
        ("severity",   "TEXT DEFAULT NULL"),
        ("ai_summary", "TEXT DEFAULT NULL"),
        ("code_refs",  "TEXT DEFAULT NULL"),
    ]:
        try:
            exe(f"ALTER TABLE log_events ADD COLUMN {col} {defn}")
        except Exception:
            pass  # column already exists


@mcp.tool()
def list_jobs() -> str:
    """List all Kibana scrape jobs with their log counts and last scrape time."""
    jobs = qall("SELECT id, name, kibana_url, time_range, enabled FROM scrape_jobs ORDER BY id")
    if not jobs:
        return "No jobs found."

    lines = []
    for j in jobs:
        count = (qone("SELECT COUNT(*) AS n FROM log_events WHERE job_id=?", (j["id"],)) or {}).get("n", 0)
        last  = (qone("SELECT MAX(timestamp) AS ts FROM log_events WHERE job_id=?", (j["id"],)) or {}).get("ts")
        analyzed = (qone(
            "SELECT COUNT(*) AS n FROM log_events WHERE job_id=? AND relevance IS NOT NULL",
            (j["id"],)
        ) or {}).get("n", 0)
        status = "✓ enabled" if j["enabled"] else "✗ disabled"
        lines.append(
            f"Job {j['id']}: {j['name']} [{status}]\n"
            f"  URL: {j['kibana_url'][:80]}...\n"
            f"  Logs: {count:,} total, {analyzed:,} analyzed\n"
            f"  Last scraped: {last or 'never'}"
        )
    return "\n\n".join(lines)


@mcp.tool()
def get_stats(job_id: int = 0) -> str:
    """
    Get statistics for a job (or all jobs if job_id=0).
    Shows total logs, severity breakdown, relevance breakdown.
    """
    if job_id:
        total    = (qone("SELECT COUNT(*) AS n FROM log_events WHERE job_id=?", (job_id,)) or {}).get("n", 0)
        analyzed = (qone("SELECT COUNT(*) AS n FROM log_events WHERE job_id=? AND relevance IS NOT NULL", (job_id,)) or {}).get("n", 0)
        sev_rows = qall("SELECT severity, COUNT(*) AS n FROM log_events WHERE job_id=? AND severity IS NOT NULL GROUP BY severity ORDER BY n DESC", (job_id,))
        rel_rows = qall("SELECT relevance, COUNT(*) AS n FROM log_events WHERE job_id=? AND relevance IS NOT NULL GROUP BY relevance ORDER BY n DESC", (job_id,))
        scope = f"Job {job_id}"
    else:
        total    = (qone("SELECT COUNT(*) AS n FROM log_events") or {}).get("n", 0)
        analyzed = (qone("SELECT COUNT(*) AS n FROM log_events WHERE relevance IS NOT NULL") or {}).get("n", 0)
        sev_rows = qall("SELECT severity, COUNT(*) AS n FROM log_events WHERE severity IS NOT NULL GROUP BY severity ORDER BY n DESC")
        rel_rows = qall("SELECT relevance, COUNT(*) AS n FROM log_events WHERE relevance IS NOT NULL GROUP BY relevance ORDER BY n DESC")
        scope = "All jobs"

    lines = [f"=== {scope} Stats ===",
             f"Total logs: {total:,}",
             f"Analyzed:   {analyzed:,} ({100*analyzed//total if total else 0}%)"]

    if rel_rows:
        lines.append("\nRelevance:")
        for r in rel_rows:
            lines.append(f"  {r['relevance']}: {r['n']:,}")

    if sev_rows:
        lines.append("\nSeverity:")
        for r in sev_rows:
            lines.append(f"  {r['severity']}: {r['n']:,}")

    return "\n".join(lines)


@mcp.tool()
def query_logs(
    job_id: int,
    limit: int = 20,
    severity: str = "",
    relevant_only: bool = False,
    search: str = "",
) -> str:
    """
    Query log entries from the database.

    Args:
        job_id: Which job's logs to query (required)
        limit: Max rows to return (default 20, max 200)
        severity: Filter by severity: critical, high, medium, low, info
        relevant_only: If true, only return checkout-relevant logs
        search: Text to search in raw_json (case-insensitive)
    """
    limit = min(limit, 200)
    conditions = ["job_id=?"]
    params: list = [job_id]

    if severity:
        conditions.append("severity=?")
        params.append(severity)
    if relevant_only:
        conditions.append("relevance='relevant'")
    if search:
        conditions.append("raw_json LIKE ?")
        params.append(f"%{search}%")

    where = " AND ".join(conditions)
    rows = qall(
        f"SELECT id, timestamp, relevance, severity, ai_summary, raw_json "
        f"FROM log_events WHERE {where} ORDER BY timestamp DESC LIMIT ?",
        params + [limit]
    )

    if not rows:
        return "No logs found matching the criteria."

    lines = [f"Found {len(rows)} log(s):"]
    for row in rows:
        rj = row.get("raw_json") or {}
        if isinstance(rj, str):
            try:
                rj = json.loads(rj)
            except Exception:
                rj = {}

        msg = rj.get("message", "")
        if isinstance(msg, list):
            msg = msg[0] if msg else ""
        msg = str(msg)[:200]

        sev_label = f"[{row['severity'].upper()}]" if row.get("severity") else ""
        rel_label = "✓" if row.get("relevance") == "relevant" else ("✗" if row.get("relevance") == "irrelevant" else "?")
        summary = row.get("ai_summary") or ""

        lines.append(
            f"\n--- Log #{row['id']} {sev_label} {rel_label} @ {row.get('timestamp','?')} ---\n"
            f"  Message: {msg}\n"
            + (f"  AI Summary: {summary}\n" if summary else "")
        )
    return "\n".join(lines)


@mcp.tool()
def fetch_logs_for_analysis(job_id: int, limit: int = 50, offset: int = 0) -> str:
    """
    Fetch unanalyzed logs from the database so YOU (Claude) can classify them.

    Returns a JSON array of log objects. For each log, YOU should determine:
      - relevant: true if related to checkout, cart, payment, or order processing
      - severity: "critical" | "high" | "medium" | "low" | "info"
      - summary: one concise English sentence describing what happened
      - github_terms: 1-2 class/method names or error codes to search in source code

    After analyzing, call save_analysis_results() with your classifications.

    Args:
        job_id: Which job's logs to fetch
        limit: How many logs to fetch (default 50, max 100)
        offset: Skip this many rows (for pagination through all logs)
    """
    _ensure_analysis_columns()
    limit = min(limit, 100)

    rows = qall(
        "SELECT id, timestamp, raw_json FROM log_events "
        "WHERE job_id=? AND relevance IS NULL "
        "ORDER BY timestamp DESC LIMIT ? OFFSET ?",
        (job_id, limit, offset)
    )

    total_pending = (qone(
        "SELECT COUNT(*) AS n FROM log_events WHERE job_id=? AND relevance IS NULL",
        (job_id,)
    ) or {}).get("n", 0)

    if not rows:
        return json.dumps({
            "status": "no_more_logs",
            "message": f"No unanalyzed logs remaining for job {job_id}.",
            "total_pending": 0,
            "logs": []
        })

    logs = []
    for row in rows:
        rj = row.get("raw_json") or {}
        if isinstance(rj, str):
            try:
                rj = json.loads(rj)
            except Exception:
                rj = {}

        def _field(key):
            v = rj.get(key, "")
            if isinstance(v, list): v = v[0] if v else ""
            if isinstance(v, dict): return json.dumps(v)[:200]
            return str(v or "")[:300]

        logs.append({
            "id": row["id"],
            "timestamp": str(row.get("timestamp", "")),
            "error_level": _field("error_level"),
            "message": _field("message"),
            "request_uri": _field("request_uri"),
            "store_id": _field("store_id"),
            "domain": _field("domain"),
        })

    return json.dumps({
        "status": "ok",
        "job_id": job_id,
        "fetched": len(logs),
        "total_pending": total_pending,
        "instruction": (
            "Analyze each log entry below. For each, decide: "
            "(1) relevant=true if related to checkout/cart/payment/order, else false. "
            "(2) severity: critical/high/medium/low/info. "
            "(3) summary: one sentence. "
            "(4) github_terms: 1-2 specific class/method names or error codes from the message. "
            "Then call save_analysis_results() with ALL results in one call."
        ),
        "logs": logs
    }, ensure_ascii=False, indent=2)


@mcp.tool()
def save_analysis_results(results: list) -> str:
    """
    Save YOUR analysis results back to the database.

    Call this after analyzing logs from fetch_logs_for_analysis().

    Args:
        results: List of objects, each with:
          - id (int): log row id from fetch_logs_for_analysis
          - relevant (bool): true if checkout/payment related
          - severity (str): "critical" | "high" | "medium" | "low" | "info"
          - summary (str): one-sentence description
          - github_terms (list[str]): optional, 1-2 terms to search in GitHub source

    Example:
        [
          {"id": 1234, "relevant": true, "severity": "high",
           "summary": "PayPal checkout failed with PAYMENTS-335 error",
           "github_terms": ["PAYMENTS-335", "finalizeOrder"]},
          {"id": 1235, "relevant": false, "severity": "info",
           "summary": "Routine agent heartbeat log", "github_terms": []}
        ]
    """
    if not results:
        return "No results provided."

    github_token = os.environ.get("GITHUB_TOKEN", "")
    saved = 0
    relevant_count = 0
    errors = []

    for res in results:
        try:
            row_id   = int(res["id"])
            relevant = bool(res.get("relevant", False))
            severity = str(res.get("severity", "info"))
            summary  = str(res.get("summary", ""))
            terms    = res.get("github_terms", [])

            relevance_val = "relevant" if relevant else "irrelevant"
            if relevant:
                relevant_count += 1

            code_refs = []
            if relevant and terms and github_token:
                code_refs = search_github(terms, github_token)

            exe(
                "UPDATE log_events SET relevance=?, severity=?, ai_summary=?, code_refs=? WHERE id=?",
                (relevance_val, severity, summary, json.dumps(code_refs), row_id)
            )
            saved += 1
        except Exception as e:
            errors.append(str(e))

    lines = [
        f"✓ Saved {saved} results to database.",
        f"  Checkout-relevant: {relevant_count}",
        f"  Irrelevant: {saved - relevant_count}",
    ]
    if errors:
        lines.append(f"  Errors: {len(errors)} — {errors[:3]}")
    lines.append("Refresh http://localhost:8765 to see updated results in the Web UI.")
    return "\n".join(lines)


@mcp.tool()
def get_analysis_summary(job_id: int) -> str:
    """
    Get a human-readable summary of analyzed logs for a job.
    Shows critical/high severity logs and their AI summaries.
    """
    critical = qall(
        "SELECT id, timestamp, severity, ai_summary, code_refs "
        "FROM log_events WHERE job_id=? AND severity IN ('critical','high') AND relevance='relevant' "
        "ORDER BY timestamp DESC LIMIT 20",
        (job_id,)
    )

    medium_count = (qall(
        "SELECT COUNT(*) AS n FROM log_events "
        "WHERE job_id=? AND severity='medium' AND relevance='relevant'",
        (job_id,)
    ) or [{}])[0].get("n", 0)

    stats = qone(
        "SELECT "
        "  COUNT(*) AS total,"
        "  SUM(CASE WHEN relevance='relevant' THEN 1 ELSE 0 END) AS relevant,"
        "  SUM(CASE WHEN relevance='irrelevant' THEN 1 ELSE 0 END) AS irrelevant "
        "FROM log_events WHERE job_id=? AND relevance IS NOT NULL",
        (job_id,)
    ) or {}

    lines = [
        f"=== Analysis Summary — Job {job_id} ===",
        f"Analyzed: {stats.get('total',0):,} logs",
        f"  Checkout-relevant: {stats.get('relevant',0):,}",
        f"  Irrelevant: {stats.get('irrelevant',0):,}",
        f"  Medium severity: {medium_count:,}",
        "",
        "=== Critical / High Severity Issues ==="
    ]

    if not critical:
        lines.append("No critical or high severity issues found.")
    else:
        for row in critical:
            refs = row.get("code_refs") or []
            if isinstance(refs, str):
                try:
                    refs = json.loads(refs)
                except Exception:
                    refs = []

            lines.append(
                f"\n[{row['severity'].upper()}] @ {row.get('timestamp','?')}\n"
                f"  {row.get('ai_summary','(no summary)')}"
            )
            for ref in refs[:3]:
                lines.append(f"  → {ref.get('repo','?')}/{ref.get('path','?')}")
                lines.append(f"    {ref.get('url','')}")

    return "\n".join(lines)


if __name__ == "__main__":
    mcp.run()
