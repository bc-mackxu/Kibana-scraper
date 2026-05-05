"""Analyze/auto-classify/summarize stream endpoints."""

import asyncio
import json
import os

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from analyzer import analyze_batch, search_github
from db import qone, qall, exe

router = APIRouter()


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _extract_ctx(raw_json) -> tuple:
    """Extract (message, error_level, store_id, domain) from a raw_json dict."""
    if isinstance(raw_json, str):
        try:
            raw_json = json.loads(raw_json)
        except Exception:
            raw_json = {}

    def _f(key):
        v = raw_json.get(key, "")
        if isinstance(v, list): v = v[0] if v else ""
        return str(v or "").strip()[:300]

    msg = _f("message")
    err = _f("error_level").lower()
    sid = _f("store_id")
    dom = _f("domain")
    return (msg, err, sid, dom)


def _ctx_keys(msg, err, sid, dom):
    """Return lookup keys from most-specific to least-specific."""
    return [
        (msg, err, sid, dom),
        (msg, err, sid, ""),
        (msg, err, "", dom),
        (msg, err, "", ""),
        (msg, "", "", ""),
    ]


# ─── Routes ───────────────────────────────────────────────────────────────────

@router.get("/api/jobs/{jid}/analyze/stream")
async def analyze_stream(jid: int, limit: int = Query(default=30, le=100)):
    """Stream AI analysis progress for unanalyzed logs in this job."""
    async def gen():
        yield f"data: {json.dumps({'type':'start'})}\n\n"

        rows = qall(
            "SELECT id, raw_json FROM log_events "
            "WHERE job_id=? AND (relevance='unchecked' OR relevance IS NULL) "
            "ORDER BY timestamp DESC LIMIT ?",
            (jid, limit),
        )
        if not rows:
            yield f"data: {json.dumps({'type':'done','analyzed':0,'message':'All logs already analyzed'})}\n\n"
            return

        total        = len(rows)
        analyzed     = 0
        relevant_ct  = 0
        batch_size   = 10
        github_token = os.environ.get("GITHUB_TOKEN", "")

        for i in range(0, total, batch_size):
            batch = rows[i : i + batch_size]
            yield f"data: {json.dumps({'type':'progress','done':analyzed,'total':total})}\n\n"

            results = analyze_batch(batch)

            for res in results:
                idx = res.get("id")
                if idx is None or idx >= len(batch):
                    continue
                row_id   = batch[idx]["id"]
                relevant = bool(res.get("relevant"))
                severity = res.get("severity", "info")
                summary  = res.get("summary", "")
                terms    = res.get("github_terms", []) if relevant else []

                code_refs = search_github(terms, github_token) if terms else []
                if relevant:
                    relevant_ct += 1

                exe(
                    "UPDATE log_events SET relevance=?, severity=?, "
                    "ai_summary=?, code_refs=? WHERE id=?",
                    ("relevant" if relevant else "irrelevant",
                     severity, summary, json.dumps(code_refs), row_id),
                )

            analyzed += len(batch)
            yield f"data: {json.dumps({'type':'progress','done':analyzed,'total':total})}\n\n"
            await asyncio.sleep(0.05)

        yield f"data: {json.dumps({'type':'done','analyzed':analyzed,'relevant':relevant_ct,'total':total})}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.get("/api/jobs/{jid}/auto-classify/stream")
async def auto_classify_stream(jid: int):
    """
    SSE stream: build lookup from analyzed records, then apply to unchecked ones.
    Uses (message, error_level, store_id, domain) composite key with fallback tiers.
    """
    async def gen():
        yield f"data: {json.dumps({'type':'start'})}\n\n"

        analyzed_rows = qall(
            "SELECT raw_json, relevance, severity FROM log_events "
            "WHERE job_id=? AND relevance IS NOT NULL AND relevance != 'unchecked'",
            (jid,),
        )

        lookup: dict = {}
        for row in analyzed_rows:
            rel = row.get("relevance")
            if rel not in ("relevant", "irrelevant"):
                continue
            sev = row.get("severity") or "info"
            msg, err, sid, dom = _extract_ctx(row.get("raw_json"))
            if not msg:
                continue
            key = (msg, err, sid, dom)
            if key not in lookup:
                lookup[key] = {"relevant": 0, "irrelevant": 0, "severities": []}
            lookup[key][rel] += 1
            lookup[key]["severities"].append(sev)

        yield f"data: {json.dumps({'type':'lookup_built','patterns': len(lookup)})}\n\n"

        if not lookup:
            yield f"data: {json.dumps({'type':'done','classified':0,'skipped':0,'message':'No analyzed records to learn from'})}\n\n"
            return

        unchecked = qall(
            "SELECT id, raw_json FROM log_events "
            "WHERE job_id=? AND (relevance IS NULL OR relevance='unchecked') "
            "ORDER BY timestamp DESC",
            (jid,),
        )
        total = len(unchecked)
        if total == 0:
            yield f"data: {json.dumps({'type':'done','classified':0,'skipped':0,'message':'No unchecked records'})}\n\n"
            return

        yield f"data: {json.dumps({'type':'progress','done':0,'total':total})}\n\n"

        SEVER_ORDER = {"critical": 4, "high": 3, "medium": 2, "low": 1, "info": 0}

        classified = 0
        skipped    = 0
        batch_ids_rel  = []
        batch_ids_irr  = []
        batch_sev: dict = {}

        for i, row in enumerate(unchecked):
            msg, err, sid, dom = _extract_ctx(row.get("raw_json"))
            entry = None
            for key in _ctx_keys(msg, err, sid, dom):
                if key in lookup:
                    entry = lookup[key]
                    break

            if entry is None:
                skipped += 1
            else:
                rel = "relevant" if entry["relevant"] >= entry["irrelevant"] else "irrelevant"
                sevs = entry["severities"]
                sev = max(set(sevs), key=lambda s: (sevs.count(s), SEVER_ORDER.get(s, 0)))

                rid = row["id"]
                if rel == "relevant":
                    batch_ids_rel.append(rid)
                else:
                    batch_ids_irr.append(rid)
                batch_sev[rid] = sev
                classified += 1

            if (i + 1) % 100 == 0:
                yield f"data: {json.dumps({'type':'progress','done':i+1,'total':total})}\n\n"
                await asyncio.sleep(0.01)

        def _update_batch(ids: list, rel_val: str):
            """Bulk-update: group IDs by severity, one UPDATE per (rel, sev) pair."""
            if not ids:
                return
            by_sev: dict = {}
            for rid in ids:
                sev = batch_sev.get(rid, "info")
                by_sev.setdefault(sev, []).append(rid)
            for sev, sev_ids in by_sev.items():
                placeholders = ",".join("?" * len(sev_ids))
                exe(
                    f"UPDATE log_events SET relevance=?, severity=? "
                    f"WHERE id IN ({placeholders}) "
                    f"AND (relevance IS NULL OR relevance='unchecked')",
                    (rel_val, sev, *sev_ids),
                )

        _update_batch(batch_ids_rel, "relevant")
        _update_batch(batch_ids_irr, "irrelevant")

        yield f"data: {json.dumps({'type':'done','classified':classified,'skipped':skipped,'total':total})}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.post("/api/jobs/{jid}/summarize")
async def summarize_logs(jid: int, payload: dict):
    ids = payload.get("ids", [])
    if not ids:
        return {"summary": "No records to summarize.", "count": 0}

    cap = ids[:50]
    placeholders = ",".join(["?"] * len(cap))
    rows = qall(
        f"SELECT id, relevance, severity, ai_summary, raw_json "
        f"FROM log_events WHERE id IN ({placeholders})",
        cap
    )

    entries = []
    for r in rows:
        rj = r.get("raw_json") or {}
        if isinstance(rj, str):
            try: rj = json.loads(rj)
            except: rj = {}
        msg = rj.get("message", "")
        if isinstance(msg, list): msg = msg[0] if msg else ""
        sev = (r.get("severity") or "?").upper()
        text = r.get("ai_summary") or str(msg)[:200]
        entries.append(f"[{sev}] {text}")

    try:
        import anthropic as _ant
        client = _ant.Anthropic()
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=600,
            messages=[{
                "role": "user",
                "content": (
                    f"Summarize these {len(entries)} log entries in 3-5 bullet points. "
                    "Focus on patterns, main issues, and actionable insights:\n\n"
                    + "\n".join(entries)
                )
            }]
        )
        summary = response.content[0].text
    except Exception as e:
        summary = f"Summary unavailable: {e}"

    return {"summary": summary, "count": len(rows)}
