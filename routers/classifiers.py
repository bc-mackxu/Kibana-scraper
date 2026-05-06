"""Classifier CRUD + embedding-based log classification job endpoints."""

import asyncio
import json
import math
import struct

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from db import qone, qall, exe

router = APIRouter()

# ─── Ollama helpers ───────────────────────────────────────────────────────────

def _get_embedding(text: str) -> bytes:
    try:
        import ollama
        resp = ollama.embeddings(model="nomic-embed-text", prompt=text[:4000])
        emb = resp["embedding"]
        return struct.pack(f"{len(emb)}f", *emb)
    except Exception as e:
        raise HTTPException(500, f"Embedding failed (is ollama running?): {e}")


def _cosine_similarity(a_bytes: bytes, b_bytes: bytes) -> float:
    try:
        n = len(a_bytes) // 4
        a = struct.unpack(f"{n}f", a_bytes)
        b = struct.unpack(f"{n}f", b_bytes)
        dot = sum(x * y for x, y in zip(a, b))
        na = math.sqrt(sum(x * x for x in a))
        nb = math.sqrt(sum(y * y for y in b))
        if na == 0 or nb == 0:
            return 0.0
        return max(0.0, min(1.0, dot / (na * nb)))
    except Exception:
        return 0.0


def _classifier_text(c: dict) -> str:
    parts = [c["name"], c["description"]]
    if c.get("keywords"):
        parts.append(f"Keywords: {c['keywords']}")
    if c.get("positive_examples"):
        parts.append(f"Positive examples: {c['positive_examples']}")
    if c.get("negative_examples"):
        parts.append(f"Should NOT match: {c['negative_examples']}")
    return "\n".join(parts)


def _log_text(raw_json) -> str:
    if isinstance(raw_json, str):
        try:
            raw_json = json.loads(raw_json)
        except Exception:
            return str(raw_json)[:2000]
    if not isinstance(raw_json, dict):
        return ""

    def _get(key):
        v = raw_json.get(key, "")
        if isinstance(v, list):
            v = v[0] if v else ""
        return str(v or "").strip()

    parts = [_get("message"), _get("error_level"), _get("request_uri"),
             _get("domain"), _get("server_name")]
    return " ".join(p for p in parts if p)[:2000]


def _llm_reason(log_text: str, clf_name: str, clf_desc: str, confidence: float) -> str:
    try:
        import ollama
        prompt = (
            f"Classifier: {clf_name}\n"
            f"Description: {clf_desc}\n"
            f"Log: {log_text[:400]}\n\n"
            f"In one concise sentence, explain why this log matches the classifier "
            f"(similarity {confidence:.2f})."
        )
        resp = ollama.chat(
            model="llama3.1:8b",
            messages=[{"role": "user", "content": prompt}],
            options={"num_predict": 80},
        )
        return resp["message"]["content"].strip()
    except Exception:
        return f"Similarity score: {confidence:.2f}"


# ─── Models ───────────────────────────────────────────────────────────────────

class ClassifierIn(BaseModel):
    name: str
    description: str
    positive_examples: str = ""
    negative_examples: str = ""
    keywords: str = ""
    enabled: bool = True


# ─── Routes: CRUD ─────────────────────────────────────────────────────────────

@router.get("/api/classifiers")
def list_classifiers():
    rows = qall(
        "SELECT id, name, description, positive_examples, negative_examples, "
        "keywords, enabled FROM classifiers ORDER BY id"
    )
    return rows or []


@router.post("/api/classifiers", status_code=201)
def create_classifier(body: ClassifierIn):
    emb = _get_embedding(_classifier_text(body.dict()))
    cid = exe(
        "INSERT INTO classifiers (name, description, positive_examples, "
        "negative_examples, keywords, enabled, embedding) VALUES (?,?,?,?,?,?,?)",
        (body.name, body.description, body.positive_examples,
         body.negative_examples, body.keywords, int(body.enabled), emb),
    )
    return {"id": cid}


@router.put("/api/classifiers/{cid}")
def update_classifier(cid: int, body: ClassifierIn):
    if not qone("SELECT id FROM classifiers WHERE id=?", (cid,)):
        raise HTTPException(404, "Not found")
    emb = _get_embedding(_classifier_text(body.dict()))
    exe(
        "UPDATE classifiers SET name=?, description=?, positive_examples=?, "
        "negative_examples=?, keywords=?, enabled=?, embedding=? WHERE id=?",
        (body.name, body.description, body.positive_examples,
         body.negative_examples, body.keywords, int(body.enabled), emb, cid),
    )
    return {"ok": True}


@router.delete("/api/classifiers/{cid}")
def delete_classifier(cid: int):
    exe("DELETE FROM classifiers WHERE id=?", (cid,))
    exe("DELETE FROM log_classification_results WHERE classifier_id=?", (cid,))
    return {"ok": True}


# ─── Routes: per-log results ──────────────────────────────────────────────────

@router.get("/api/logs/{log_id}/classifications")
def get_log_classifications(log_id: int):
    return qall(
        "SELECT lcr.classifier_id, lcr.matched, lcr.confidence, lcr.reason, "
        "c.name, c.enabled "
        "FROM log_classification_results lcr "
        "JOIN classifiers c ON c.id = lcr.classifier_id "
        "WHERE lcr.log_id=? ORDER BY lcr.confidence DESC",
        (log_id,),
    ) or []


@router.post("/api/logs/classifications-batch")
def get_classifications_batch(payload: dict):
    """Return classifier scores for multiple log IDs in one call.
    Returns {log_id: [{classifier_id, name, confidence, matched}]}
    """
    ids = payload.get("ids", [])
    if not ids:
        return {}
    fmt = ",".join(["?"] * len(ids))
    rows = qall(
        f"SELECT lcr.log_id, lcr.classifier_id, lcr.matched, lcr.confidence, c.name "
        f"FROM log_classification_results lcr "
        f"JOIN classifiers c ON c.id = lcr.classifier_id "
        f"WHERE lcr.log_id IN ({fmt}) ORDER BY lcr.log_id, lcr.confidence DESC",
        tuple(ids),
    ) or []
    result: dict = {}
    for row in rows:
        lid = str(row["log_id"])
        if lid not in result:
            result[lid] = []
        result[lid].append({
            "classifier_id": row["classifier_id"],
            "name":          row["name"],
            "confidence":    row["confidence"],
            "matched":       bool(row["matched"]),
        })
    return result


# ─── Routes: classification run (SSE stream) ─────────────────────────────────

@router.get("/api/classifiers/run/stream")
async def classify_run_stream(
    mode: str = Query("missing"),        # "all" | "missing" | "range"
    from_date: str = Query(""),
    to_date: str = Query(""),
    job_id: int = Query(None),
    threshold: float = Query(0.5),
    with_reasons: bool = Query(False),
):
    async def gen():
        yield f"data: {json.dumps({'type': 'start'})}\n\n"

        classifiers = qall(
            "SELECT id, name, description, embedding FROM classifiers WHERE enabled=1"
        ) or []

        if not classifiers:
            yield f"data: {json.dumps({'type': 'done', 'processed': 0, 'message': 'No enabled classifiers'})}\n\n"
            return

        # Drop classifiers with no embedding stored
        classifiers = [c for c in classifiers if c.get("embedding")]
        if not classifiers:
            yield f"data: {json.dumps({'type': 'done', 'processed': 0, 'message': 'Classifiers have no embeddings — try re-saving them'})}\n\n"
            return

        yield f"data: {json.dumps({'type': 'info', 'message': f'{len(classifiers)} classifiers loaded'})}\n\n"

        # ── Build log query ──────────────────────────────────────────────────
        conditions = []
        args: list = []

        if job_id:
            conditions.append("job_id=?")
            args.append(job_id)

        if mode == "range":
            if from_date:
                conditions.append("timestamp >= ?")
                args.append(from_date)
            if to_date:
                conditions.append("timestamp <= ?")
                args.append(to_date)
        elif mode == "missing":
            active_ids = ",".join(str(c["id"]) for c in classifiers)
            n_clf = len(classifiers)
            conditions.append(
                f"(SELECT COUNT(DISTINCT lcr.classifier_id) "
                f"FROM log_classification_results lcr "
                f"WHERE lcr.log_id=log_events.id AND lcr.classifier_id IN ({active_ids})) < {n_clf}"
            )

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        log_rows = qall(
            f"SELECT id, raw_json FROM log_events {where} ORDER BY timestamp DESC",
            tuple(args),
        ) or []

        total = len(log_rows)
        if total == 0:
            yield f"data: {json.dumps({'type': 'done', 'processed': 0, 'message': 'No logs to process'})}\n\n"
            return

        yield f"data: {json.dumps({'type': 'progress', 'done': 0, 'total': total})}\n\n"

        processed = 0
        matched_total = 0
        upsert_sql = (
            "INSERT OR REPLACE INTO log_classification_results "
            "(log_id, classifier_id, matched, confidence, reason) VALUES (?,?,?,?,?)"
        )
        insert_ignore_sql = (
            "INSERT OR IGNORE INTO log_classification_results "
            "(log_id, classifier_id, matched, confidence, reason) VALUES (?,?,?,?,?)"
        )
        write_sql = upsert_sql if mode in ("all", "range") else insert_ignore_sql

        try:
            import ollama
        except ImportError:
            yield f"data: {json.dumps({'type': 'error', 'message': 'ollama package not installed'})}\n\n"
            return

        for log_row in log_rows:
            text = _log_text(log_row.get("raw_json"))
            if not text.strip():
                processed += 1
                continue

            try:
                resp = ollama.embeddings(model="nomic-embed-text", prompt=text)
                log_emb = struct.pack(f"{len(resp['embedding'])}f", *resp["embedding"])
            except Exception as e:
                yield f"data: {json.dumps({'type': 'warning', 'message': f'Embedding error: {e}'})}\n\n"
                processed += 1
                continue

            for clf in classifiers:
                sim = _cosine_similarity(log_emb, clf["embedding"])
                matched = sim >= threshold
                reason = ""
                if matched and with_reasons:
                    reason = _llm_reason(text, clf["name"], clf.get("description", ""), sim)

                exe(write_sql, (log_row["id"], clf["id"], int(matched), sim, reason))
                if matched:
                    matched_total += 1

            processed += 1
            if processed % 10 == 0 or processed == total:
                yield f"data: {json.dumps({'type': 'progress', 'done': processed, 'total': total, 'matched': matched_total})}\n\n"
                await asyncio.sleep(0.01)

        yield f"data: {json.dumps({'type': 'done', 'processed': processed, 'matched': matched_total, 'total': total})}\n\n"

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
