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
        self.es_total: int = 0        # total hits reported by ES (for % progress)
        self.total_inserted: int = 0  # cumulative rows saved to DB

    def handle_response(self, response):
        """Playwright response handler — runs on browser thread."""
        url = response.url
        if "/internal/bsearch" not in url:
            return
        try:
            body = response.body()
            text = body.decode("utf-8", errors="replace")
        except Exception as e:
            print(f"    [bsearch] body error: {e}")
            return
        self._parse_bsearch(text)

    def handle_route(self, route, request):
        """Route-based interception — captures bsearch response body reliably."""
        try:
            response = route.fetch()
        except Exception as e:
            # Browser closed mid-request — silently abort, don't crash the handler
            err = str(e)
            if "disposed" in err or "destroyed" in err or "closed" in err:
                try:
                    route.abort()
                except Exception:
                    pass
                return
            print(f"    [bsearch] route fetch error: {e}")
            try:
                route.abort()
            except Exception:
                pass
            return

        if "/internal/bsearch" in request.url:
            try:
                text = response.text()
                self._parse_bsearch(text, debug=True)
            except Exception as e:
                print(f"    [bsearch] parse error: {e}")
        try:
            route.fulfill(response=response)
        except Exception:
            pass  # page already closed — safe to ignore

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
            if debug and not is_running and isinstance(total, int) and total > 0:
                print(f"    [bsearch] ES total={total:,}  hits_in_batch={len(batch)}")
            # Capture ES total for progress % (only trust non-zero, non-running values)
            if isinstance(total, int) and total > 0 and not is_running:
                with self._lock:
                    if total > self.es_total:
                        self.es_total = total
            if batch:
                new_hits.extend(batch)

        if new_hits:
            with self._lock:
                self._hits.extend(new_hits)

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


def read_time_range_from_page(page) -> str | None:
    """Read the currently active time range label from Kibana's date picker."""
    # Try quick-range text first (e.g. "Last 24 hours")
    for sel in [
        '[data-test-subj="superDatePickerToggleQuickMenuButton"]',
        '[data-test-subj="superDatePickerShowDatesButton"]',
    ]:
        try:
            el = page.query_selector(sel)
            if el:
                txt = el.inner_text().strip()
                if txt and txt not in ("show dates", ""):
                    return txt
        except Exception:
            pass
    # Try absolute start/end labels
    try:
        start_el = page.query_selector('[data-test-subj="superDatePickerstartDatePopoverButton"]')
        end_el   = page.query_selector('[data-test-subj="superDatePickerendDatePopoverButton"]')
        if start_el and end_el:
            s = start_el.inner_text().strip()
            e = end_el.inner_text().strip()
            if s or e:
                return f"{s} → {e}"
    except Exception:
        pass
    return None


# ─── Commands ─────────────────────────────────────────────────────────────────

def cmd_login(url: str):
    import signal
    TIMEOUT = 120  # seconds

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=50)
        ctx = browser.new_context()
        page = ctx.new_page()
        print(f"[→] Opening {url}")
        print("[!] Complete SSO login in the browser window.")
        print(f"[!] Session saves automatically after {TIMEOUT}s, or press ^C once to save now.\n")

        page.goto(url, wait_until="domcontentloaded", timeout=90_000)

        deadline = time.time() + TIMEOUT
        try:
            while True:
                remaining = int(deadline - time.time())
                if remaining <= 0:
                    print(f"\n[!] {TIMEOUT}s elapsed — saving session now...")
                    break

                # Check all open pages for a successful Kibana /app/ URL
                for pg in ctx.pages:
                    try:
                        current = pg.url
                    except Exception:
                        continue
                    if "kibana" in current and "/app/" in current:
                        print(f"\n[✓] Detected Kibana: {current[:80]}")
                        try:
                            pg.wait_for_load_state("networkidle", timeout=10_000)
                        except PlaywrightTimeout:
                            pass
                        print("[✓] Logged in — saving session...")
                        deadline = 0  # break outer loop
                        break

                if deadline == 0:
                    break

                # Countdown line (overwrites itself)
                urls = []
                for pg in ctx.pages:
                    try: urls.append(pg.url[:70])
                    except: pass
                url_str = " | ".join(urls) if urls else "(loading...)"
                print(f"  [{remaining:>2}s remaining]  {url_str}     ", end="\r", flush=True)
                time.sleep(1)

        except KeyboardInterrupt:
            print("\n[!] ^C received — saving session now...")

        # Ignore further ^C during the save so it can't be interrupted
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        try:
            save_session(ctx)
        except Exception as e:
            print(f"[!] Save error: {e}")
        finally:
            signal.signal(signal.SIGINT, signal.SIG_DFL)

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

        # ── Verify active time range ──────────────────────────────────────────
        page.wait_for_timeout(1500)
        active_time = read_time_range_from_page(page)
        if active_time:
            print(f"[✓] Active time range: {active_time}")
        else:
            print(f"[!] Could not read active time range from page")

        print("[→] Waiting for async search to complete...")
        page.wait_for_timeout(8000)

        # Track rows already in DB to compute "new vs duplicate" at the end
        existing_before = (conn.execute(
            "SELECT COUNT(*) FROM log_events WHERE job_id=?", (args.job_id,)
        ).fetchone() or [0])[0] if args.job_id else 0

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

                # Progress %: based on ES total if known, else page count
                es_total = collector.es_total
                if es_total > 0:
                    pct = min(100, round(total_saved / es_total * 100))
                    bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
                    print(f"[✓] Page {page_num}: +{saved} rows  |  {total_saved:,}/{es_total:,} ({pct}%) [{bar}]")
                else:
                    pages_pct = min(100, round(page_num / args.pages * 100))
                    print(f"[✓] Page {page_num}: +{saved} rows saved  (total {total_saved:,})  [{pages_pct}% of max pages]")
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
            print(f"[✓] Final flush: +{saved} rows (total {total_saved:,})")

        # Unregister route handler BEFORE closing to avoid "context disposed" race
        try:
            page.unroute("**/internal/bsearch**")
        except Exception:
            pass

        browser.close()

    # ── End summary ───────────────────────────────────────────────────────────
    new_rows = 0
    corr_count = 0
    if args.job_id:
        row = conn.execute("SELECT COUNT(*) FROM log_events WHERE job_id=?", (args.job_id,)).fetchone()
        total_in_db = (row or [0])[0]
        new_rows = total_in_db - existing_before
        # Count correlated pairs involving this job's logs
        row2 = conn.execute(
            "SELECT COUNT(*) FROM log_correlations lc "
            "JOIN log_events le ON le.id=lc.log_id WHERE le.job_id=?",
            (args.job_id,)
        ).fetchone()
        corr_count = (row2 or [0])[0]

    conn.close()

    print(f"\n{'━'*55}")
    print(f"  Run complete")
    print(f"{'━'*55}")
    print(f"  Attempted to save : {total_saved:>8,} rows")
    if args.job_id:
        dup_rows = total_saved - new_rows
        print(f"  New rows in DB    : {new_rows:>8,}")
        print(f"  Duplicates skipped: {dup_rows:>8,}")
        print(f"  Total in DB (job) : {total_in_db:>8,}")
        if corr_count:
            print(f"  Correlated records: {corr_count:>8,}")
        else:
            print(f"  Correlated records:        – (run Build Correlations)")
    print(f"{'━'*55}\n")


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
