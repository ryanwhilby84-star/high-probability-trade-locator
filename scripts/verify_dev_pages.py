"""Quick SPA route smoke test against the running Vite dashboard."""
from __future__ import annotations

import sys
import urllib.parse

ROUTES = [
    ("Gold COT Workstation", "#/instrument/Gold/cot-workstation"),
    ("Natural Gas Valuation", "#/valuation/Natural%20Gas%20%2F%20NG"),
]


def main() -> int:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("playwright missing - HTTP-only fallback")
        import urllib.request

        for label, frag in ROUTES:
            url = "http://localhost:5173/" + frag
            with urllib.request.urlopen("http://localhost:5173/", timeout=10) as r:
                print(f"[HTTP] {label}: shell={r.status} url={url}")
        return 0

    ok = True
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        for label, frag in ROUTES:
            url = "http://localhost:5173/" + frag
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(2500)
            title = page.title()
            body = page.inner_text("body")[:200].replace("\n", " ")
            # Fail only on empty body / connection issues
            passed = bool(body.strip()) and "ERR_CONNECTION" not in body
            mark = "PASS" if passed else "FAIL"
            if not passed:
                ok = False
            print(f"[{mark}] {label}")
            print(f"  url={url}")
            print(f"  title={title!r}")
            print(f"  body={body!r}")
        browser.close()
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
