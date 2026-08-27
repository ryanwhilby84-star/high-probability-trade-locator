"""Verify NG/Gold price camera + NG live price after repair."""
from __future__ import annotations

import json
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path("data/audits/gold_market_clearing_valuation")
NG_URL = "http://127.0.0.1:5173/#/valuation/Natural%20Gas%20%2F%20NG"
GOLD_URL = "http://127.0.0.1:5173/#/valuation/Gold"


def zoom_price(page):
    box = page.locator("[data-panel='price'] .ws-chart-pane-canvas").bounding_box()
    assert box
    before = page.evaluate(
        """() => {
          const sync = window.__NGVW_SYNC__ || window.__GOLDVW_SYNC__ || null;
          return sync && sync.sharedVisibleRange ? {...sync.sharedVisibleRange} : null;
        }"""
    )
    page.mouse.move(box["x"] + box["width"] * 0.55, box["y"] + box["height"] * 0.4)
    for _ in range(5):
        page.mouse.wheel(0, -420)
        page.wait_for_timeout(100)
    page.wait_for_timeout(400)
    after = page.evaluate(
        """() => {
          const sync = window.__NGVW_SYNC__ || window.__GOLDVW_SYNC__ || null;
          return sync && sync.sharedVisibleRange ? {...sync.sharedVisibleRange} : null;
        }"""
    )
    return before, after, box


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 1100})

        # --- Natural Gas ---
        page.goto(NG_URL, wait_until="networkidle", timeout=90000)
        page.wait_for_selector("[data-testid='ngvw-page']", timeout=45000)
        page.wait_for_selector("[data-testid='ws-price-camera-controls']", timeout=20000)
        # Wait for poll to replace stale snapshot
        page.wait_for_function(
            """() => {
              const el = document.querySelector('[data-testid=ngvw-market-price]');
              const status = document.querySelector('[data-testid=ngvw-price-status]');
              if (!el || !status) return false;
              const txt = el.textContent || '';
              const st = (status.textContent || '').trim();
              return st === 'POLLING' || st === 'LIVE';
            }""",
            timeout=45000,
        )
        page.wait_for_timeout(1500)
        ng_price = page.inner_text("[data-testid='ngvw-market-price']")
        ng_status = page.inner_text("[data-testid='ngvw-price-status']")
        ng_compare = page.inner_text("[data-testid='ngvw-comparison-status']")
        ng_hb = page.inner_text("[data-testid='ngvw-heartbeat']")
        ng_diag = page.evaluate(
            """() => {
              const pre = document.querySelector('[data-testid=ngvw-live-diag]');
              if (!pre) return null;
              try { return JSON.parse(pre.textContent); } catch { return { raw: pre.textContent }; }
            }"""
        )
        before, after, _ = zoom_price(page)
        page.screenshot(path=str(OUT / "ng_price_camera_zoomed.png"), full_page=False)
        # Polling must not reset camera — wait one poll cycle tick (simulate by waiting and checking range)
        range_after_zoom = after
        page.wait_for_timeout(3500)
        after_wait = page.evaluate(
            """() => {
              const sync = window.__NGVW_SYNC__ || null;
              return sync && sync.sharedVisibleRange ? {...sync.sharedVisibleRange} : null;
            }"""
        )
        page.locator("[data-testid='ws-price-camera-reset']").click()
        page.wait_for_timeout(400)

        # --- Gold ---
        page.goto(GOLD_URL, wait_until="networkidle", timeout=90000)
        page.wait_for_selector("[data-testid='goldvw-page']", timeout=45000)
        page.wait_for_selector("[data-testid='ws-price-camera-controls']", timeout=20000)
        page.wait_for_timeout(2000)
        g_before, g_after, _ = zoom_price(page)
        page.screenshot(path=str(OUT / "gold_price_camera_zoomed.png"), full_page=False)

        browser.close()

    proof = {
        "natural_gas": {
            "market_price_display": ng_price,
            "price_status": ng_status,
            "comparison_status": ng_compare,
            "heartbeat": ng_hb,
            "diag": ng_diag,
            "zoom_before": before,
            "zoom_after": after,
            "zoomed": before != after,
            "camera_preserved_during_wait": range_after_zoom == after_wait,
            "range_after_wait": after_wait,
            "screenshot": str(OUT / "ng_price_camera_zoomed.png"),
            "camera_controls": True,
        },
        "gold": {
            "zoom_before": g_before,
            "zoom_after": g_after,
            "zoomed": g_before != g_after,
            "screenshot": str(OUT / "gold_price_camera_zoomed.png"),
            "camera_controls": True,
        },
    }
    (OUT / "ng_camera_and_price_proof.json").write_text(json.dumps(proof, indent=2), encoding="utf-8")
    print(json.dumps(proof, indent=2))
    if not (proof["natural_gas"]["zoomed"] and proof["gold"]["zoomed"]):
        raise SystemExit(2)
    if ng_status not in ("POLLING", "LIVE"):
        raise SystemExit(3)


if __name__ == "__main__":
    main()
