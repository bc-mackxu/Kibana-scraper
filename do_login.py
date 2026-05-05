"""Run this directly in a terminal: python3 do_login.py"""
import json, time, sys
from pathlib import Path
from playwright.sync_api import sync_playwright

URL = "https://kibana.bigcommerce.net"
SESSION_FILE = Path("session.json")
WAIT_SECONDS = 120   # adjust if SSO takes longer

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    ctx = browser.new_context()
    page = ctx.new_page()
    page.goto(URL, wait_until="domcontentloaded", timeout=60_000)

    print(f"Browser opened. Complete SSO login.")
    print(f"Session will be saved automatically in {WAIT_SECONDS}s after Kibana loads.")

    deadline = time.time() + WAIT_SECONDS
    while time.time() < deadline:
        url = page.url
        sys.stdout.write(f"\r  URL: {url[:80]:<80}")
        sys.stdout.flush()
        if "kibana.bigcommerce.net/app/" in url:
            print(f"\nKibana detected, saving session...")
            time.sleep(3)
            break
        time.sleep(2)
    else:
        print("\nTimeout reached, saving session anyway...")

    SESSION_FILE.write_text(json.dumps(ctx.storage_state()))
    print(f"Session saved → {SESSION_FILE}")
    browser.close()
