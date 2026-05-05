"""Group CRUD + correlations + detect-keys SSE endpoints."""

import asyncio
import concurrent.futures
import itertools
import json
import os
import re

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from db import qone, qall, exe, DB_PATH

router = APIRouter()

# ─── Models ───────────────────────────────────────────────────────────────────

class GroupCreate(BaseModel):
    name: str
    description: str = ""

class GroupMemberReq(BaseModel):
    job_id: int
    label: str = ""

class GroupKeyReq(BaseModel):
    job_id_a: int
    field_a: str
    job_id_b: int
    field_b: str


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _build_correlations_sync(gid: int, log_fn=None):
    """
    Synchronously build correlations for group `gid`.
    log_fn(msg) is called with progress strings (optional).
    Returns total pairs linked.
    """
    import sqlite3 as _sq3
    from collections import defaultdict as _dd

    def _log(msg):
        if log_fn:
            log_fn(msg)

    members = qall(
        "SELECT lgm.job_id, lgm.label, sj.name AS job_name "
        "FROM log_group_members lgm JOIN scrape_jobs sj ON sj.id=lgm.job_id "
        "WHERE lgm.group_id=?", (gid,)
    ) or []
    if len(members) < 2:
        _log("  [!] Need at least 2 jobs in group — skipping")
        return 0

    keys = qall("SELECT * FROM log_group_keys WHERE group_id=?", (gid,)) or []
    if not keys:
        _log("  [!] No correlation keys defined — skipping")
        return 0

    pair_map: dict = _dd(list)
    for k in keys:
        ja, jb = k['job_id_a'], k['job_id_b']
        if ja > jb:
            ja, jb = jb, ja
            k = {**k, 'job_id_a': ja, 'job_id_b': jb, 'field_a': k['field_b'], 'field_b': k['field_a']}
        pair_map[(ja, jb)].append(k)

    member_label = {m['job_id']: (m.get('label') or m.get('job_name') or f"Job {m['job_id']}") for m in members}
    total_linked = 0

    for (jid_a, jid_b), pair_keys in pair_map.items():
        label_a = member_label.get(jid_a, f"Job {jid_a}")
        label_b = member_label.get(jid_b, f"Job {jid_b}")

        cnt_a = (qone("SELECT COUNT(*) c FROM log_events WHERE job_id=?", (jid_a,)) or {}).get("c", 0)
        cnt_b = (qone("SELECT COUNT(*) c FROM log_events WHERE job_id=?", (jid_b,)) or {}).get("c", 0)
        _log(f"  [→] Correlating {label_a} ({cnt_a:,}) ↔ {label_b} ({cnt_b:,}) on {len(pair_keys)} key(s)…")

        def _extract(job_id, fields):
            projections = []
            proj_args = []
            for f in fields:
                projections.append(
                    "CAST(COALESCE(json_extract(raw_json,?),json_extract(raw_json,?)) AS TEXT)"
                )
                proj_args += [f"$.{f}[0]", f"$.{f}"]
            sql = f"SELECT id, {', '.join(projections)} FROM log_events WHERE job_id=?"
            _c = _sq3.connect(str(DB_PATH), check_same_thread=False, timeout=60)
            _c.execute("PRAGMA journal_mode=WAL")
            try:
                cur = _c.execute(sql, proj_args + [job_id])
                index: dict = _dd(list)
                while True:
                    chunk = cur.fetchmany(2000)
                    if not chunk:
                        break
                    for row in chunk:
                        vals = tuple(str(v or "").strip() for v in row[1:])
                        if any(vals):
                            index[vals].append(row[0])
            finally:
                _c.close()
            return index

        fields_a = [k['field_a'] for k in pair_keys]
        fields_b = [k['field_b'] for k in pair_keys]

        idx_a = _extract(jid_a, fields_a)
        idx_b = _extract(jid_b, fields_b)
        _log(f"  [→] Matching {len(idx_a):,} × {len(idx_b):,} unique keys…")

        exe("DELETE FROM log_correlations WHERE group_id=? AND log_id IN "
            "(SELECT id FROM log_events WHERE job_id=?)", (gid, jid_a))
        exe("DELETE FROM log_correlations WHERE group_id=? AND log_id IN "
            "(SELECT id FROM log_events WHERE job_id=?)", (gid, jid_b))

        linked = 0
        insert_batch: list = []

        _c2 = _sq3.connect(str(DB_PATH), check_same_thread=False, timeout=60)
        _c2.execute("PRAGMA journal_mode=WAL")
        try:
            for key_vals, ids_a in idx_a.items():
                if not any(key_vals):
                    continue
                ids_b = idx_b.get(key_vals, [])
                if not ids_b:
                    continue
                matched_json = json.dumps([
                    f"{k['field_a']}={str(v)[:60]}"
                    for k, v in zip(pair_keys, key_vals) if v
                ])
                for id_a in ids_a:
                    for id_b in ids_b:
                        insert_batch.append((id_a, id_b, gid, label_b, matched_json))
                        insert_batch.append((id_b, id_a, gid, label_a, matched_json))
                        linked += 1
                if len(insert_batch) >= 2000:
                    _c2.executemany(
                        "INSERT OR IGNORE INTO log_correlations "
                        "(log_id,corr_id,group_id,source_label,matched_keys) VALUES (?,?,?,?,?)",
                        insert_batch
                    )
                    _c2.commit()
                    insert_batch = []
                    _log(f"  [→] {linked:,} pairs so far…")
            if insert_batch:
                _c2.executemany(
                    "INSERT OR IGNORE INTO log_correlations "
                    "(log_id,corr_id,group_id,source_label,matched_keys) VALUES (?,?,?,?,?)",
                    insert_batch
                )
                _c2.commit()
        finally:
            _c2.close()

        _log(f"  [✓] {label_a} ↔ {label_b}: {linked:,} pairs linked")
        total_linked += linked

    return total_linked


# ─── Routes ───────────────────────────────────────────────────────────────────

@router.get("/api/groups")
def list_groups():
    groups = qall("SELECT * FROM log_groups ORDER BY id") or []
    for g in groups:
        g['members'] = qall(
            "SELECT lgm.job_id, lgm.label, sj.name AS job_name "
            "FROM log_group_members lgm JOIN scrape_jobs sj ON sj.id=lgm.job_id "
            "WHERE lgm.group_id=?", (g['id'],)
        ) or []
        g['keys'] = qall(
            "SELECT lgk.*, sa.name AS job_name_a, sb.name AS job_name_b "
            "FROM log_group_keys lgk "
            "LEFT JOIN scrape_jobs sa ON sa.id=lgk.job_id_a "
            "LEFT JOIN scrape_jobs sb ON sb.id=lgk.job_id_b "
            "WHERE lgk.group_id=? ORDER BY lgk.confidence DESC", (g['id'],)
        ) or []
    return groups


@router.post("/api/groups")
def create_group(req: GroupCreate):
    gid = exe("INSERT INTO log_groups (name, description) VALUES (?,?)",
              (req.name, req.description))
    return {"id": gid, "name": req.name}


@router.delete("/api/groups/{gid}")
def delete_group(gid: int):
    exe("DELETE FROM log_group_keys WHERE group_id=?", (gid,))
    exe("DELETE FROM log_group_members WHERE group_id=?", (gid,))
    exe("DELETE FROM log_groups WHERE id=?", (gid,))
    return {"ok": True}


@router.post("/api/groups/{gid}/members")
def add_group_member(gid: int, req: GroupMemberReq):
    exe("INSERT OR IGNORE INTO log_group_members (group_id, job_id, label) VALUES (?,?,?)",
        (gid, req.job_id, req.label))
    return {"ok": True}


@router.delete("/api/groups/{gid}/members/{job_id}")
def remove_group_member(gid: int, job_id: int):
    exe("DELETE FROM log_group_members WHERE group_id=? AND job_id=?", (gid, job_id))
    return {"ok": True}


@router.post("/api/groups/{gid}/keys")
def add_group_key(gid: int, req: GroupKeyReq):
    kid = exe(
        "INSERT INTO log_group_keys (group_id,job_id_a,field_a,job_id_b,field_b,confidence,source) "
        "VALUES (?,?,?,?,?,1.0,'manual')",
        (gid, req.job_id_a, req.field_a, req.job_id_b, req.field_b)
    )
    return {"id": kid}


@router.delete("/api/groups/{gid}/keys/{kid}")
def delete_group_key(gid: int, kid: int):
    exe("DELETE FROM log_group_keys WHERE id=? AND group_id=?", (kid, gid))
    return {"ok": True}


@router.get("/api/groups/{gid}/build-correlations/stream")
async def build_correlations_stream(gid: int):
    """SSE wrapper around _build_correlations_sync for the Groups UI button."""
    async def gen():
        yield f"data: {json.dumps({'type':'start'})}\n\n"
        loop = asyncio.get_event_loop()

        progress_buf: list = []

        def _on_log(msg):
            progress_buf.append(msg)

        # Run the sync function in a thread so we don't block the event loop
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            fut = loop.run_in_executor(pool, lambda: _build_correlations_sync(gid, _on_log))
            while not fut.done():
                while progress_buf:
                    msg = progress_buf.pop(0)
                    yield f"data: {json.dumps({'type':'progress','message':msg.strip()})}\n\n"
                await asyncio.sleep(0.2)
            # Drain any remaining messages
            while progress_buf:
                msg = progress_buf.pop(0)
                yield f"data: {json.dumps({'type':'progress','message':msg.strip()})}\n\n"
            total_linked = await fut

        yield f"data: {json.dumps({'type':'done','linked':total_linked})}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})


@router.get("/api/groups/{gid}/detect-keys/stream")
async def detect_keys_stream(gid: int, sample_size: int = Query(default=50, le=200)):
    """
    SSE stream: auto-detect correlation fields between all job pairs in this group.
    """
    async def gen():

        yield f"data: {json.dumps({'type':'start'})}\n\n"

        members = qall(
            "SELECT lgm.job_id, lgm.label, sj.name AS job_name "
            "FROM log_group_members lgm JOIN scrape_jobs sj ON sj.id=lgm.job_id "
            "WHERE lgm.group_id=?", (gid,)
        ) or []

        if len(members) < 2:
            yield f"data: {json.dumps({'type':'done','message':'Need at least 2 jobs in the group','keys_added':0})}\n\n"
            return

        yield f"data: {json.dumps({'type':'progress','message':f'Sampling records from {len(members)} jobs...'})}\n\n"

        job_samples = {}
        for m in members:
            rows = qall(
                "SELECT id, raw_json FROM log_events WHERE job_id=? "
                "AND raw_json IS NOT NULL ORDER BY RANDOM() LIMIT ?",
                (m['job_id'], sample_size)
            ) or []
            parsed = []
            for r in rows:
                rj = r['raw_json']
                if isinstance(rj, str):
                    try: rj = json.loads(rj)
                    except: continue
                if isinstance(rj, dict):
                    parsed.append(rj)
            job_samples[m['job_id']] = {
                'label': m.get('label') or m.get('job_name'),
                'records': parsed
            }

        UUID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)
        HEX_RE  = re.compile(r'^[0-9a-f]{16,}$', re.I)
        ID_FIELDS = re.compile(r'(_id|_token|_key|request|trace|correlation|session)', re.I)

        def extract_values(records, field):
            vals = set()
            for r in records:
                v = r.get(field, "")
                if isinstance(v, list): v = v[0] if v else ""
                v = str(v or "").strip()
                if v and len(v) > 3:
                    vals.add(v)
            return vals

        keys_added = 0
        heuristic_keys = []

        for (jid_a, data_a), (jid_b, data_b) in itertools.combinations(job_samples.items(), 2):
            recs_a = data_a['records']
            recs_b = data_b['records']
            if not recs_a or not recs_b:
                continue

            fields_a = set().union(*[r.keys() for r in recs_a[:20]])
            fields_b = set().union(*[r.keys() for r in recs_b[:20]])
            common_fields = fields_a & fields_b

            label_a = data_a['label']
            label_b = data_b['label']
            nf = len(common_fields)
            yield f"data: {json.dumps({'type':'progress','message':f'Checking {nf} common fields between {label_a} and {label_b}...'})}\n\n"
            await asyncio.sleep(0.01)

            candidates = []
            for field in common_fields:
                vals_a = extract_values(recs_a, field)
                vals_b = extract_values(recs_b, field)
                overlap = vals_a & vals_b
                if not overlap:
                    continue
                score = len(overlap)
                if ID_FIELDS.search(field): score *= 3
                uuid_count = sum(1 for v in overlap if UUID_RE.match(v) or HEX_RE.match(v))
                score += uuid_count * 2
                candidates.append((field, field, score, len(overlap)))

            id_fields_a = [f for f in fields_a if ID_FIELDS.search(f)]
            id_fields_b = [f for f in fields_b if ID_FIELDS.search(f)]
            for fa in id_fields_a:
                for fb in id_fields_b:
                    if fa == fb: continue
                    vals_a = extract_values(recs_a, fa)
                    vals_b = extract_values(recs_b, fb)
                    overlap = vals_a & vals_b
                    if len(overlap) >= 2:
                        score = len(overlap) * 2
                        candidates.append((fa, fb, score, len(overlap)))

            candidates.sort(key=lambda x: -x[2])
            for field_a, field_b, score, overlap_ct in candidates[:3]:
                confidence = min(1.0, score / 20.0)
                existing = qone(
                    "SELECT id FROM log_group_keys WHERE group_id=? "
                    "AND ((job_id_a=? AND field_a=? AND job_id_b=? AND field_b=?) "
                    "OR (job_id_a=? AND field_a=? AND job_id_b=? AND field_b=?))",
                    (gid, jid_a, field_a, jid_b, field_b, jid_b, field_b, jid_a, field_a)
                )
                if not existing:
                    exe(
                        "INSERT INTO log_group_keys "
                        "(group_id,job_id_a,field_a,job_id_b,field_b,confidence,source) "
                        "VALUES (?,?,?,?,?,?,'auto')",
                        (gid, jid_a, field_a, jid_b, field_b, confidence)
                    )
                    keys_added += 1
                    heuristic_keys.append(f"{field_a} ↔ {field_b} (overlap:{overlap_ct})")
                    yield f"data: {json.dumps({'type':'key_found','field_a':field_a,'field_b':field_b,'overlap':overlap_ct,'source':'heuristic'})}\n\n"

        anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if anthropic_key and len(members) >= 2:
            yield f"data: {json.dumps({'type':'progress','message':'Running AI analysis...'})}\n\n"
            await asyncio.sleep(0.01)

            samples_for_ai = {}
            for m in members:
                jid = m['job_id']
                recs = job_samples[jid]['records'][:5]
                field_samples = {}
                for r in recs:
                    for k, v in r.items():
                        if k not in field_samples:
                            sv = v[0] if isinstance(v, list) else v
                            if sv and not isinstance(sv, dict):
                                field_samples[k] = str(sv)[:80]
                samples_for_ai[m.get('label') or m.get('job_name')] = field_samples

            prompt = (
                "You are analyzing two types of log records to find correlation fields.\n\n"
                "Here are sample field:value pairs from each log type:\n\n"
            )
            for label, fields in samples_for_ai.items():
                prompt += f"=== {label} ===\n"
                for k, v in list(fields.items())[:30]:
                    prompt += f"  {k}: {v}\n"
                prompt += "\n"
            prompt += (
                "Identify up to 3 field pairs that could correlate records between these log types. "
                "A good correlation field has similar or identical values in both logs for the same request/event. "
                "Look for: request IDs, trace IDs, session tokens, IP addresses, timestamps, order IDs, user IDs.\n\n"
                "RESPOND WITH ONLY a JSON array, nothing else. Example:\n"
                '[{"field_a":"request_id","log_a":"syslog","field_b":"http_request_id","log_b":"nginx","reason":"UUID request identifier"},{"field_a":"client_ip","log_a":"syslog","field_b":"remote_addr","log_b":"nginx","reason":"Client IP address"}]\n'
                "If no good correlation exists, return []."
            )

            try:
                import anthropic as _ant
                import re as _re
                client = _ant.Anthropic()
                resp = client.messages.create(
                    model="claude-haiku-4-5",
                    max_tokens=500,
                    messages=[{"role": "user", "content": prompt}]
                )
                ai_text = resp.content[0].text.strip()
                m_json = _re.search(r'\[.*\]', ai_text, _re.DOTALL)
                if m_json:
                    ai_keys = json.loads(m_json.group())
                    label_to_jid = {
                        (m.get('label') or m.get('job_name')): m['job_id']
                        for m in members
                    }
                    for ak in ai_keys:
                        fa = ak.get('field_a','')
                        fb = ak.get('field_b','')
                        la = ak.get('log_a','')
                        lb = ak.get('log_b','')
                        jid_a = label_to_jid.get(la)
                        jid_b = label_to_jid.get(lb)
                        if not (fa and fb and jid_a and jid_b):
                            continue
                        existing = qone(
                            "SELECT id FROM log_group_keys WHERE group_id=? "
                            "AND ((job_id_a=? AND field_a=? AND job_id_b=? AND field_b=?) "
                            "OR (job_id_a=? AND field_a=? AND job_id_b=? AND field_b=?))",
                            (gid, jid_a, fa, jid_b, fb, jid_b, fb, jid_a, fa)
                        )
                        if not existing:
                            exe(
                                "INSERT INTO log_group_keys "
                                "(group_id,job_id_a,field_a,job_id_b,field_b,confidence,source) "
                                "VALUES (?,?,?,?,?,0.9,'ai')",
                                (gid, jid_a, fa, jid_b, fb)
                            )
                            keys_added += 1
                            yield f"data: {json.dumps({'type':'key_found','field_a':fa,'field_b':fb,'source':'ai','reason':ak.get('reason','')})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'type':'progress','message':f'AI analysis skipped: {str(e)[:100]}'})}\n\n"

        yield f"data: {json.dumps({'type':'done','keys_added':keys_added})}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})
