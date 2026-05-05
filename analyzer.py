"""
AI analysis: Claude for log classification, GitHub for code references.
"""

import json
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor

import requests

# Thread pool for blocking GitHub requests (keeps async routes unblocked)
_github_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="github")


def _get_client():
    """Return an Anthropic client if the API key is available, else None."""
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not key:
        return None
    try:
        import anthropic
        return anthropic.Anthropic(api_key=key)
    except Exception:
        return None


def _get_ai_prompt_prefix() -> str:
    """Return the user-configured system prompt prefix, or the default."""
    try:
        from db import qone
        row = qone("SELECT value FROM ai_config WHERE key='system_prompt'")
        if row and row.get("value"):
            return row["value"].strip()
    except Exception:
        pass
    return "You are analyzing BigCommerce production logs to find checkout/payment issues."


def _extract(raw_json: dict, name: str) -> str:
    v = raw_json.get(name, "")
    if isinstance(v, list):
        v = v[0] if v else ""
    if isinstance(v, dict):
        return json.dumps(v)[:200]
    return str(v or "")[:300]


def analyze_batch(rows: list[dict]) -> list[dict]:
    """
    Analyze up to ~20 log rows with Claude Haiku in a single API call.
    Returns a list of result dicts matching the input order by 'id' index.
    """
    client = _get_client()
    if not client or not rows:
        return []

    system_prompt = _get_ai_prompt_prefix()

    lines = []
    for i, row in enumerate(rows):
        rj = row.get("raw_json") or {}
        if isinstance(rj, str):
            try:
                rj = json.loads(rj)
            except Exception:
                rj = {}
        parts = []
        for f in ("error_level", "domain", "request_uri", "store_id"):
            v = _extract(rj, f)
            if v:
                parts.append(f"{f}={v!r}")
        msg = _extract(rj, "message")
        if msg:
            parts.append(f"message={msg!r}")
        lines.append(f"[{i}] " + " | ".join(parts))

    prompt = f"""{system_prompt}

For each log entry below, return a JSON object with:
- "id": the integer index shown in brackets
- "relevant": true if related to the system's core functionality; false otherwise
- "severity": one of "critical" (system down/data loss), "high" (major feature broken), "medium" (degraded), "low" (minor), "info"
- "summary": one concise English sentence describing what happened
- "github_terms": array of 1-2 specific class names, method names, or error codes from the message to search in source code (only for relevant logs, else [])

Return ONLY a valid JSON array, no other text. Example:
[{{"id":0,"relevant":true,"severity":"high","summary":"Checkout payment failed","github_terms":["setExternalCheckoutPayment"]}}]

Logs:
{chr(10).join(lines)}"""

    try:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=3000,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        m = re.search(r"\[[\s\S]*\]", text)
        if m:
            return json.loads(m.group())
    except Exception as e:
        print(f"[analyzer] Claude error: {e}")
    return []


def search_github(terms: list[str], token: str) -> list[dict]:
    """Search org:bigcommerce GitHub for source code matching the given terms.
    Runs in a thread pool so it doesn't block the async event loop."""
    if not terms or not token:
        return []

    def _do_search():
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        results = []
        seen: set[str] = set()
        for term in terms[:2]:
            term = term.strip()
            if not term or len(term) < 4:
                continue
            try:
                r = requests.get(
                    "https://api.github.com/search/code",
                    headers=headers,
                    params={"q": f"{term} org:bigcommerce", "per_page": 5},
                    timeout=12,
                )
                if r.status_code == 200:
                    for item in r.json().get("items", [])[:5]:
                        url = item["html_url"]
                        if url not in seen:
                            seen.add(url)
                            results.append({
                                "repo": item["repository"]["full_name"],
                                "path": item["path"],
                                "url":  url,
                            })
                elif r.status_code == 403:
                    print("[github] Rate limited — stopping search")
                    break
            except Exception as e:
                print(f"[github] Search error for {term!r}: {e}")
        return results[:6]

    # Run in thread pool — caller can await the future or just call synchronously
    fut = _github_pool.submit(_do_search)
    try:
        return fut.result(timeout=20)
    except Exception:
        return []
