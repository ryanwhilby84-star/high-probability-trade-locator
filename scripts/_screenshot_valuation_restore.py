"""Screenshot restored Gold + Natural Gas valuation pages (pre-camera)."""
from __future__ import annotations

import json
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT_DIR = Path("data/audits/gold_market_clearing_valuation")
GOLD_URL = "http://127.0.0.1:5173/#/valuation/Gold"
NG_URL = "http://127.0.0.1:5173/#/valuation/Natural%20Gas%20%2F%20NG"


def shot(page, url: str, out: Path, wait_sel: str) -> dict:
    page.goto(url, wait_until="networkidle", timeout=90000)
    page.wait_for_selector(wait_sel, timeout=45000)
    page.wait_for_timeout(3500)
    page.screenshot(path=str(out), full_page=True)
    body = page.inner_text("body")
    box = page.locator(wait_sel).bounding_box()
    return {
        "url": url,
        "screenshot": str(out),
        "wait_sel": wait_sel,
        "body_chars": len(body),
        "has_price_title": ("Weekly" in body and "Price" in body),
        "has_current_valuation": "CURRENT" in body and "VALUATION" in body,
        "has_deviation": "Valuation Deviation" in body or "valuation deviation" in body.lower(),
        "bbox_top": box["y"] if box else None,
        "bbox_height": box["height"] if box else None,
        "not_far_below": (box is None) or (box["y"] < 2000),
    }


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    gold_out = OUT_DIR / "gold_valuation_restored_pre_camera.png"
    ng_out = OUT_DIR / "ng_valuation_restored_pre_camera.png"
    proof_out = OUT_DIR / "valuation_pages_restored_proof.json"

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 1600})
        gold = shot(page, GOLD_URL, gold_out, "[data-testid='goldvw-page']")
        ng = shot(page, NG_URL, ng_out, "[data-testid='ngvw-page']")
        browser.close()

    proof = {"gold": gold, "natural_gas": ng}
    proof_out.write_text(json.dumps(proof, indent=2), encoding="utf-8")
    print(json.dumps(proof, indent=2))


if __name__ == "__main__":
    main()
