"""
Kibana → SQLite scraper (Playwright + network interception)

Commands:
  python kibana_scraper.py login  <kibana_url>
  python kibana_scraper.py scrape <kibana_url> [options]

Examples:
  python kibana_scraper.py login  https://kibana.bigcommerce.net
  python kibana_scraper.py scrape "https://kibana.bigcommerce.net/goto/bae0d8b0-..." \\
      --time "Last 24 hours" --pages 20
"""

import argparse
import base64
import json
import sqlite3
import sys
import time
import threading
import zlib
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from db import DB_PATH, init_db

SESSION_FILE = Path("session.json")
OUTPUT_DIR   = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)


# ─── Session ──────────────────────────────────────────────────────────────────

def save_session(context):
    SESSION_FILE.write_text(json.dumps(context.storage_state()))
    print(f"[✓] Session saved → {SESSION_FILE}")


def make_context(playwright, headless=True):
    browser = playwright.chromium.launch(headless=headless, slow_mo=30)
    if SESSION_FILE.exists():
        state = json.loads(SESSION_FILE.read_text())
        ctx = browser.new_context(storage_state=state)
        print("[✓] Loaded saved session")
    else:
        ctx = browser.new_context()
        print("[!] No session — run login first")
    return browser, ctx


# ─── SQLite ───────────────────────────────────────────────────────────────────

def get_db(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or str(DB_PATH)
    conn = sqlite3.connect(path, check_same_thread=False, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


INSERT_SQL = """
INSERT OR IGNORE INTO log_events
  (es_id, es_index, timestamp, job_id, run_id, raw_json)
VALUES
  (:es_id, :es_index, :timestamp, :job_id, :run_id, :raw_json)
"""


def parse_ts(val):
    """Parse ISO8601 timestamp to a SQLite-friendly string."""
    if not val:
        return None
    try:
        val = str(val).replace("Z", "+00:00")
        dt = datetime.fromisoformat(val)
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    except Exception:
        return None


def hit_to_row(hit: dict) -> dict:
    src  = hit.get("_source") or {}
    flds = hit.get("fields") or {}
    ts_raw = flds.get("@timestamp") or src.get("@timestamp")
    if isinstance(ts_raw, list):
        ts_raw = ts_raw[0] if ts_raw else None
    return {
        "es_id":     hit.get("_id"),
        "es_index":  hit.get("_index"),
        "timestamp": parse_ts(ts_raw),
        "job_id":    None,  # filled by save_hits_to_db
        "run_id":    None,  # filled by save_hits_to_db
        "raw_json":  json.dumps(flds or src, ensure_ascii=False),
    }


def save_hits_to_db(conn: sqlite3.Connection, hits: list[dict],
                    run_id: int | None = None,
                    job_id: int | None = None) -> int:
    if not hits:
        return 0
    rows = [hit_to_row(h) for h in hits]
    for r in rows:
        r["run_id"] = run_id
        r["job_id"] = job_id
    conn.executemany(INSERT_SQL, rows)
    conn.commit()
    return len(rows)


# ─── Network interception — capture ES search responses ──────────────────────

class HitCollector:
    """Thread-safe buffer for ES hits intercepted from network responses."""

    def __init__(self):
        self._hits: list[dict] = []
        self._lock = threading.Lock()

    def handle_response(self, response):
        """Playwright response handler — runs on browser thread."""
        url = response.url
        if "/internal/bsearch" not in url:
            return
        print(f"    [bsearch] response received, status={response.status}")
        try:
            body = response.body()
            text = body.decode("utf-8", errors="replace")
        except Exception as e:
            print(f"    [bsearch] body error: {e}")
            return

        self._parse_bsearch(text)

    def handle_route(self, route, request):
        """Route-based interception — captures bsearch response body reliably."""
        response = route.fetch()
        if "/internal/bsearch" in request.url:
            try:
                text = response.text()
                print(f"    [bsearch] route captured, len={len(text)}")
                self._parse_bsearch(text, debug=True)
            except Exception as e:
                print(f"    [bsearch] route error: {e}")
        route.fulfill(response=response)

    def _decode_line(self, line: str) -> dict | None:
        """Decode one bsearch line: base64+zlib → JSON."""
        line = line.strip()
        if not line:
            return None
        if line.startswith("{"):
            try:
                return json.loads(line)
            except Exception:
                return None
        try:
            raw = base64.b64decode(line + "==")
            return json.loads(zlib.decompress(raw).decode("utf-8"))
        except Exception:
            return None

    def _parse_bsearch(self, text: str, debug=False):
        new_hits = []
        lines = [l for l in text.splitlines() if l.strip()]
        for line in lines:
            obj = self._decode_line(line)
            if obj is None:
                continue
            result = obj.get("result", obj)
            is_running = result.get("isRunning", False)
            raw = result.get("rawResponse", {})
            hits_obj = raw.get("hits", {})
            batch = hits_obj.get("hits", [])
            total_raw = hits_obj.get("total", {})
            total = total_raw.get("value", total_raw) if isinstance(total_raw, dict) else total_raw
            if debug:
                print(f"    [parse] isRunning={is_running} hits={len(batch)} total={total}")
                if total and total != {} and not batch:
                    print(f"    [parse] FULL OBJ: {json.dumps(obj)[:800]}")
            if batch:
                new_hits.extend(batch)

        if new_hits:
            with self._lock:
                self._hits.extend(new_hits)
            print(f"    [✓] captured {len(new_hits)} hits (total buffered: {len(self._hits)})")

    def drain(self) -> list[dict]:
        with self._lock:
            out, self._hits = self._hits, []
        return out

    def total(self) -> int:
        with self._lock:
            return len(self._hits)


# ─── Kibana UI helpers ────────────────────────────────────────────────────────

def wait_for_results(page, timeout=30_000):
    """Wait until Kibana stops the loading spinner."""
    try:
        page.wait_for_selector(
            '[data-test-subj="globalLoadingIndicator-hidden"]',
            timeout=timeout
        )
    except PlaywrightTimeout:
        pass
    page.wait_for_load_state("networkidle", timeout=timeout)


def set_time_range(page, label: str):
    print(f"[→] Time range: {label}")
    try:
        page.click('[data-test-subj="superDatePickerToggleQuickMenuButton"]', timeout=6_000)
    except PlaywrightTimeout:
        page.click('[data-test-subj="superDatePickerstartDatePopoverButton"]', timeout=6_000)
    page.get_by_text(label, exact=True).first.click(timeout=10_000)
    wait_for_results(page)
    print(f"[✓] Time set to: {label}")


def _inject_time_in_url(current_url: str, from_iso: str, to_iso: str) -> str | None:
    """Inject absolute time range into a Kibana Discover URL hash.

    Handles two URL formats:
      1. Compressed:  #/?_g=h@75c69a9&_a=h@ec2e19a   (Kibana short-URL state)
      2. Full rison:  #/?_g=(filters:!(),time:(from:now-7d,to:now))&_a=(...)

    In both cases, replaces (or injects) the _g global-state with an explicit
    time-range block, keeping _a intact so the index/query config is preserved.

    Returns the modified URL, or None if the URL has no discoverable Kibana hash.
    """
    import re as _re
    if '#' not in current_url:
        return None
    base, fragment = current_url.split('#', 1)
    try:
        from urllib.parse import unquote
        fragment_dec = unquote(fragment)
    except Exception:
        fragment_dec = fragment

    new_g = f"_g=(time:(from:'{from_iso}',to:'{to_iso}'))"

    # ── Case 1: compressed _g=h@<hex> format ──────────────────────────────────
    if _re.search(r'_g=h@[0-9a-f]+', fragment_dec, _re.I):
        new_fragment = _re.sub(r'_g=h@[0-9a-f]+', new_g, fragment_dec, flags=_re.I)
        print(f"[→] Replaced compressed _g hash with explicit time rison")
        return base + '#' + new_fragment

    # ── Case 2: full rison _g=(...) format ────────────────────────────────────
    if '_g=(' not in fragment_dec:
        return None

    new_time = f"time:(from:'{from_iso}',to:'{to_iso}')"

    if 'time:(' in fragment_dec:
        idx = fragment_dec.find('time:(')
        depth = 0
        i = idx + len('time:(') - 1
        while i < len(fragment_dec):
            if fragment_dec[i] == '(':
                depth += 1
            elif fragment_dec[i] == ')':
                depth -= 1
                if depth == 0:
                    break
            i += 1
        new_fragment = fragment_dec[:idx] + new_time + fragment_dec[i + 1:]
    else:
        new_fragment = fragment_dec.replace('_g=(', '_g=(' + new_time + ',', 1)

    return base + '#' + new_fragment


def set_custom_time(page, from_str: str, to_str: str):
    """Set an absolute custom time range.

    Primary strategy: modify the Kibana Discover URL hash in Python, then
    navigate to the new URL.  Falls back to the UI date-picker approach.
    """
    print(f"[→] Custom time: {from_str}  →  {to_str}")

    from_iso = from_str.strip().replace(' ', 'T')
    to_iso   = to_str.strip().replace(' ', 'T')

    # ── Strategy 1: navigate to URL with time injected into _g hash ────────────
    try:
        current_url = page.url
        print(f"[→] Current URL: {current_url[:120]}...")
        new_url = _inject_time_in_url(current_url, from_iso, to_iso)
        if new_url:
            print(f"[→] Navigating to time-modified URL...")
            page.goto(new_url, wait_until="networkidle", timeout=60_000)
            page.wait_for_timeout(3_000)
            wait_for_results(page)
            try:
                page.click('[data-test-subj="querySubmitButton"]', timeout=5_000)
                wait_for_results(page)
            except PlaywrightTimeout:
                pass
            print(f"[✓] Time set via URL navigation: {from_iso} → {to_iso}")
            return
        else:
            print(f"[!] URL has no _g=( hash — falling back to UI (url={current_url[:80]})")
    except Exception as e:
        print(f"[!] URL navigation method failed ({e}) — falling back to UI")

    # ── Strategy 2: UI approach ────────────────────────────────────────────────
    print("[→] Trying UI date picker...")
    page.wait_for_timeout(2_000)

    opened = False
    for attempt in range(3):
        try:
            page.wait_for_selector(
                '[data-test-subj="superDatePickerstartDatePopoverButton"]',
                state='visible', timeout=5_000
            )
            page.click('[data-test-subj="superDatePickerstartDatePopoverButton"]')
            opened = True
            break
        except PlaywrightTimeout:
            if attempt < 2:
                page.wait_for_timeout(2_000)

    if not opened:
        print("[!] Absolute date button not found — trying relative→absolute switch")
        for sel in [
            '[data-test-subj="superDatePickerShowDatesButton"]',
            '[data-test-subj="superDatePickerToggleQuickMenuButton"]',
        ]:
            try:
                page.click(sel, timeout=5_000)
                page.wait_for_timeout(600)
                opened = True
                break
            except PlaywrightTimeout:
                continue

        if opened:
            for label in ['Absolute', 'absolute']:
                try:
                    page.get_by_role('tab', name=label).click(timeout=3_000)
                    break
                except Exception:
                    pass
            page.wait_for_timeout(400)

    try:
        page.wait_for_selector(
            '[data-test-subj="superDatePickerAbsoluteDateInput"]',
            state='visible', timeout=8_000
        )
    except PlaywrightTimeout:
        print("[!] Absolute date inputs still not visible — skipping custom time")
        return

    inputs = page.query_selector_all('[data-test-subj="superDatePickerAbsoluteDateInput"]')
    if len(inputs) < 1:
        print("[!] No date inputs found — skipping custom time")
        return

    inputs[0].triple_click()
    inputs[0].fill(from_str)
    page.keyboard.press('Escape')

    if len(inputs) < 2:
        try:
            page.click('[data-test-subj="superDatePickerendDatePopoverButton"]', timeout=4_000)
            page.wait_for_timeout(400)
            inputs = page.query_selector_all('[data-test-subj="superDatePickerAbsoluteDateInput"]')
        except PlaywrightTimeout:
            pass

    if len(inputs) >= 2:
        inputs[-1].triple_click()
        inputs[-1].fill(to_str)
        page.keyboard.press('Escape')

    page.click('[data-test-subj="querySubmitButton"]', timeout=10_000)
    wait_for_results(page)


def click_next_page(page) -> bool:
    for sel in [
        '[data-test-subj="pagination-button-next"]',
        'button[aria-label="Next page"]',
        'button[aria-label="下一页"]',
    ]:
        btn = page.query_selector(sel)
        if btn and btn.is_enabled():
            btn.click()
            wait_for_results(page)
            return True
    return False


def refresh_page(page):
    """Click Kibana Refresh button to trigger a new search."""
    try:
        page.click('[data-test-subj="querySubmitButton"]', timeout=5_000)
        wait_for_results(page)
    except PlaywrightTimeout:
        pass


# ─── Commands ─────────────────────────────────────────────────────────────────

def cmd_login(url: str):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=50)
        ctx = browser.new_context()
        page = ctx.new_page()
        print(f"[→] Opening {url}")
        print("[!] Please complete SSO login in the browser window.")
        print("[!] Script will auto-save session once Kibana fully loads...")

        page.goto(url, wait_until="domcontentloaded", timeout=90_000)

        deadline = time.time() + 300
        while time.time() < deadline:
            current = page.url
            print(f"    current url: {current}")
            if "kibana.bigcommerce.net/app/" in current:
                print("[✓] Detected Kibana /app/ — waiting for page to settle...")
                try:
                    page.wait_for_load_state("networkidle", timeout=20_000)
                except PlaywrightTimeout:
                    pass
                print(f"[✓] Logged in!")
                break
            time.sleep(3)
        else:
            print("[!] Timed out — saving session anyway")

        save_session(ctx)
        time.sleep(2)
        browser.close()


def cmd_scrape(args):
    db_path = args.db_path or str(DB_PATH)

    # Ensure schema exists
    init_db()

    try:
        conn = get_db(db_path)
        print(f"[✓] SQLite connected → {db_path}")
    except Exception as e:
        print(f"[✗] SQLite connection failed: {e}")
        sys.exit(1)

    collector = HitCollector()
    total_saved = 0

    with sync_playwright() as p:
        browser, ctx = make_context(p, headless=not args.show_browser)
        page = ctx.new_page()

        page.route("**/internal/bsearch**", collector.handle_route)
        page.on("response", collector.handle_response)

        print(f"[→] Navigating to {args.url}")
        page.goto(args.url, wait_until="networkidle", timeout=90_000)

        if "login" in page.url.lower() or "sso" in page.url.lower():
            print("[!] Session expired. Run `login` again.")
            browser.close()
            conn.close()
            sys.exit(1)

        # Wait for client-side redirect (goto URLs redirect via JS to Discover)
        for _ in range(15):
            resolved = page.url
            if "/discover#" in resolved or "/discover?" in resolved:
                break
            page.wait_for_timeout(1_000)
        else:
            pass

        print(f"[✓] Resolved URL: {page.url[:120]}")
        wait_for_results(page)

        # Drain stale hits from initial page load before applying our time window
        stale = collector.drain()
        if stale:
            print(f"[→] Discarded {len(stale)} stale hits from initial load")

        # Apply time range — then wait for the new bsearch to complete
        if args.from_dt and args.to_dt:
            set_custom_time(page, args.from_dt, args.to_dt)
        elif args.time:
            set_time_range(page, args.time)
        else:
            refresh_page(page)

        print("[→] Waiting for async search to complete...")
        page.wait_for_timeout(8000)

        page_num = 0
        while True:
            page_num += 1
            time.sleep(1)

            hits = collector.drain()
            if not hits:
                print(f"    [wait] async search still running, waiting 5s more...")
                page.wait_for_timeout(5000)
                hits = collector.drain()

            if hits:
                saved = save_hits_to_db(conn, hits, run_id=args.run_id, job_id=args.job_id)
                total_saved += saved
                print(f"[✓] Page {page_num}: {saved} rows saved (total {total_saved})")
            else:
                print(f"[!] Page {page_num}: no hits captured")

            if page_num >= args.pages:
                print(f"[✓] Reached max pages ({args.pages})")
                break

            if not click_next_page(page):
                print("[✓] No more pages")
                break

        # Final drain in case last page was slow
        page.wait_for_timeout(2000)
        hits = collector.drain()
        if hits:
            saved = save_hits_to_db(conn, hits, run_id=args.run_id, job_id=args.job_id)
            total_saved += saved
            print(f"[✓] Final flush: {saved} rows (total {total_saved})")

        browser.close()

    conn.close()
    print(f"\n[✓] Done. Total rows saved: {total_saved}")


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Kibana → SQLite scraper")
    sub = parser.add_subparsers(dest="cmd")

    # login
    lp = sub.add_parser("login", help="SSO login, saves session.json")
    lp.add_argument("url", help="Kibana base URL")

    # scrape
    sp = sub.add_parser("scrape", help="Scrape Kibana Discover → SQLite")
    sp.add_argument("url", help="Kibana Discover URL (or goto URL)")
    sp.add_argument("--time",    default=None,   help='Quick range e.g. "Last 7 days"')
    sp.add_argument("--from",    dest="from_dt", default=None, help="Abs start ISO8601")
    sp.add_argument("--to",      dest="to_dt",   default=None, help="Abs end ISO8601")
    sp.add_argument("--pages",   type=int, default=50,  help="Max pages (default 50)")
    sp.add_argument("--show-browser", action="store_true")
    sp.add_argument("--db-path",      default=None,  help="Path to SQLite DB file")
    sp.add_argument("--run-id",       type=int, default=None, help="job_runs.id to tag rows")
    sp.add_argument("--job-id",       type=int, default=None, help="scrape_jobs.id to tag rows")

    # Legacy MySQL flags — silently ignored so old invocations don't crash
    sp.add_argument("--db-host",     default=None, help=argparse.SUPPRESS)
    sp.add_argument("--db-port",     type=int, default=None, help=argparse.SUPPRESS)
    sp.add_argument("--db-user",     default=None, help=argparse.SUPPRESS)
    sp.add_argument("--db-password", default=None, help=argparse.SUPPRESS)
    sp.add_argument("--db-name",     default=None, help=argparse.SUPPRESS)

    args = parser.parse_args()

    if args.cmd == "login":
        cmd_login(args.url)
    elif args.cmd == "scrape":
        cmd_scrape(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
