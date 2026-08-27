"""Prove Backend quote == Frontend store == Rendered page mid.

Instruments (canonical internal keys / user labels):
  Gold
  Natural Gas / NG
  Crude Oil / CL          (WTI)
  Euro FX / 6E            (EUR/USD)
  S&P 500 / ES

Requires: Current Price Service :8787, Vite :5173
"""
from __future__ import annotations

import json
import sys
import urllib.parse
from pathlib import Path

INSTRUMENTS = [
    ("Gold", "Gold"),
    ("Natural Gas / NG", "Natural Gas"),
    ("Crude Oil / CL", "WTI"),
    ("Euro FX / 6E", "EUR/USD"),
    ("S&P 500 / ES", "S&P 500"),
]

DASH = "http://localhost:5173"
OUT = Path("data/audits/frontend_live_parity.json")


def nearly_eq(a, b, rel=1e-9, abs_tol=1e-6) -> bool:
    if a is None or b is None:
        return a is None and b is None
    try:
        fa, fb = float(a), float(b)
    except (TypeError, ValueError):
        return False
    return abs(fa - fb) <= max(abs_tol, rel * max(abs(fa), abs(fb), 1.0))


def main() -> int:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: playwright not installed", file=sys.stderr)
        return 2

    results = []
    all_ok = True

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(f"{DASH}/", wait_until="domcontentloaded", timeout=30000)

        for key, label in INSTRUMENTS:
            enc = urllib.parse.quote(key, safe="")
            url = f"{DASH}/#/instrument/{enc}/cot-workstation"
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(3000)

            # Wait until store is exposed and live badge appears (or timeout)
            try:
                page.wait_for_function(
                    """() => !!(window.__HPTL_LIVE_PRICE_STORE__ && window.__HPTL_CURRENT_PRICE_STREAM__)""",
                    timeout=10000,
                )
            except Exception:
                pass

            try:
                page.wait_for_selector("[data-testid='live-mid']", timeout=15000)
            except Exception:
                pass

            # Wait for WS connected + live mid in DOM, then sample all three together.
            try:
                page.wait_for_function(
                    """(marketId) => {
                      const stream = window.__HPTL_CURRENT_PRICE_STREAM__;
                      const el = document.querySelector('[data-testid="live-mid"]');
                      return !!(
                        stream &&
                        stream.getConnectionState() === 'connected' &&
                        el &&
                        el.getAttribute('data-live-mid')
                      );
                    }""",
                    key,
                    timeout=20000,
                )
            except Exception:
                pass

            snap = page.evaluate(
                """async (marketId) => {
                  const store = window.__HPTL_LIVE_PRICE_STORE__;
                  const stream = window.__HPTL_CURRENT_PRICE_STREAM__;
                  if (!store || !stream) {
                    return { error: 'store_not_exposed' };
                  }

                  const near = (a, b) => {
                    if (a == null || b == null) return false;
                    const fa = Number(a), fb = Number(b);
                    if (!Number.isFinite(fa) || !Number.isFinite(fb)) return false;
                    return Math.abs(fa - fb) <= Math.max(1e-6, 1e-9 * Math.max(Math.abs(fa), Math.abs(fb), 1));
                  };

                  // Poll until HTTP snapshot mid matches the live store mid (same tick).
                  let backend = null;
                  let quote = null;
                  let streamPrice = null;
                  for (let i = 0; i < 40; i++) {
                    const resp = await fetch('/api/prices', { cache: 'no-store' });
                    const doc = resp.ok ? await resp.json() : null;
                    backend = doc?.prices?.[marketId] ?? null;
                    streamPrice = stream.getPrice(marketId);
                    quote = store.getQuote(marketId);
                    const storeMid = quote?.mid ?? streamPrice?.mid ?? null;
                    if (near(backend?.mid, storeMid) && stream.getConnectionState() === 'connected') {
                      break;
                    }
                    await new Promise((r) => setTimeout(r, 100));
                  }

                  const status = store.getStatus(marketId);
                  const el = document.querySelector('[data-testid="live-mid"]');
                  const renderedMid = el ? Number(el.getAttribute('data-live-mid')) : null;
                  const renderedText = el ? el.textContent.trim() : null;
                  const storeMid = quote?.mid ?? streamPrice?.mid ?? null;

                  return {
                    backend_mid: backend?.mid ?? null,
                    backend_status: backend?.status ?? null,
                    backend_symbol: backend?.provider_symbol ?? null,
                    store_mid: storeMid,
                    store_status: status ?? null,
                    stream_mid: streamPrice?.mid ?? null,
                    rendered_mid: Number.isFinite(renderedMid) ? renderedMid : null,
                    rendered_text: renderedText,
                    connection: stream.getConnectionState?.() ?? null,
                  };
                }""",
                key,
            )

            be = snap.get("backend_mid")
            st = snap.get("store_mid")
            rd = snap.get("rendered_mid")
            ok = (
                snap.get("error") is None
                and nearly_eq(be, st)
                and nearly_eq(st, rd)
                and be is not None
            )
            if not ok:
                all_ok = False

            row = {
                "label": label,
                "internal_key": key,
                "ok": ok,
                **snap,
            }
            results.append(row)

            mark = "PASS" if ok else "FAIL"
            print(f"[{mark}] {label} ({key})")
            print(f"  1 backend : mid={be} status={snap.get('backend_status')} symbol={snap.get('backend_symbol')}")
            print(f"  2 store   : mid={st} status={snap.get('store_status')} stream_mid={snap.get('stream_mid')} conn={snap.get('connection')}")
            print(f"  3 rendered: mid={rd} text={snap.get('rendered_text')!r}")
            if snap.get("error"):
                print(f"  ERROR: {snap['error']}")

        browser.close()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({"ok": all_ok, "results": results}, indent=2), encoding="utf-8")
    print("=" * 60)
    print(f"OVERALL: {'PASS' if all_ok else 'FAIL'}  wrote {OUT}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
