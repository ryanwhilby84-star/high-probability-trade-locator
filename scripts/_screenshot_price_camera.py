"""Screenshot NG + Gold valuation price charts after price-camera enablement."""
from __future__ import annotations

import json
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT_DIR = Path("data/audits/gold_market_clearing_valuation")
GOLD_URL = "http://127.0.0.1:5173/#/valuation/Gold"
NG_URL = "http://127.0.0.1:5173/#/valuation/Natural%20Gas%20%2F%20NG"


def inspect(page, url: str, out: Path, page_sel: str) -> dict:
    page.goto(url, wait_until="networkidle", timeout=90000)
    page.wait_for_selector(page_sel, timeout=45000)
    page.wait_for_selector("[data-testid='ws-price-camera-controls']", timeout=20000)
    page.wait_for_timeout(2500)

    # Prove camera controls exist only on price pane, then exercise zoom.
    price_pane = page.locator("[data-panel='price']")
    controls = price_pane.locator("[data-testid='ws-price-camera-controls']")
    canvas = price_pane.locator(".ws-chart-pane-canvas")
    box = canvas.bounding_box()
    assert box, "price canvas missing"

    # Wheel zoom in at chart center
    page.mouse.move(box["x"] + box["width"] * 0.55, box["y"] + box["height"] * 0.45)
    page.mouse.wheel(0, -480)
    page.wait_for_timeout(400)
    page.mouse.wheel(0, -480)
    page.wait_for_timeout(500)

    # Drag pan
    sx = box["x"] + box["width"] * 0.6
    sy = box["y"] + box["height"] * 0.5
    page.mouse.move(sx, sy)
    page.mouse.down()
    page.mouse.move(sx - 120, sy, steps=8)
    page.mouse.up()
    page.wait_for_timeout(400)

    page.screenshot(path=str(out), full_page=False)
    page.locator("[data-testid='ws-price-camera-reset']").click()
    page.wait_for_timeout(500)

    valuation_controls = page.locator("[data-panel='valuation'] [data-testid='ws-price-camera-controls']")
    body = page.inner_text("body")
    return {
        "url": url,
        "screenshot": str(out),
        "price_camera_controls": controls.count(),
        "valuation_camera_controls": valuation_controls.count(),
        "price_canvas": {
            "top": box["y"],
            "height": box["height"],
            "width": box["width"],
        },
        "has_price_title": "Weekly" in body and "Price" in body,
        "has_current_valuation": "CURRENT" in body and "VALUATION" in body,
        "has_deviation": "Valuation Deviation" in body,
        "layout_ok": box["y"] < 800 and 250 <= box["height"] <= 900,
    }


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ng_out = OUT_DIR / "ng_price_camera.png"
    gold_out = OUT_DIR / "gold_price_camera.png"
    proof_out = OUT_DIR / "price_camera_proof.json"

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 1100})
        ng = inspect(page, NG_URL, ng_out, "[data-testid='ngvw-page']")
        gold = inspect(page, GOLD_URL, gold_out, "[data-testid='goldvw-page']")
        browser.close()

    proof = {"natural_gas": ng, "gold": gold}
    proof_out.write_text(json.dumps(proof, indent=2), encoding="utf-8")
    print(json.dumps(proof, indent=2))


if __name__ == "__main__":
    main()
