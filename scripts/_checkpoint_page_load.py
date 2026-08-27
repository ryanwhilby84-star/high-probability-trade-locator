"""Confirm NG + Gold valuation pages load in restored (no-camera) state."""
from __future__ import annotations

import json
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path("data/audits/gold_market_clearing_valuation/checkpoint_page_load.json")


def check(page, url: str, page_sel: str) -> dict:
    page.goto(url, wait_until="networkidle", timeout=90000)
    page.wait_for_selector(page_sel, timeout=45000)
    page.wait_for_selector("[data-panel='price'] .ws-chart-pane-canvas", timeout=20000)
    page.wait_for_timeout(2000)
    body = page.inner_text("body")
    box = page.locator("[data-panel='price'] .ws-chart-pane-canvas").bounding_box()
    camera = page.locator("[data-testid='ws-price-camera-controls']").count()
    return {
        "url": url,
        "ok": bool(box) and "CURRENT" in body and "VALUATION" in body,
        "has_price_chart": box is not None and box["height"] >= 250,
        "has_deviation_or_valuation": "Valuation Deviation" in body or "Fair Value" in body,
        "camera_controls": camera,
        "bbox_top": box["y"] if box else None,
        "bbox_height": box["height"] if box else None,
    }


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 1100})
        ng = check(
            page,
            "http://127.0.0.1:5173/#/valuation/Natural%20Gas%20%2F%20NG",
            "[data-testid='ngvw-page']",
        )
        gold = check(
            page,
            "http://127.0.0.1:5173/#/valuation/Gold",
            "[data-testid='goldvw-page']",
        )
        browser.close()
    proof = {"natural_gas": ng, "gold": gold}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(proof, indent=2), encoding="utf-8")
    print(json.dumps(proof, indent=2))
    if not (ng["ok"] and gold["ok"] and ng["camera_controls"] == 0 and gold["camera_controls"] == 0):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
