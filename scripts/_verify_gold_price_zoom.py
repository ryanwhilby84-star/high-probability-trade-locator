from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path("data/audits/gold_market_clearing_valuation")


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 1100})
        page.goto("http://127.0.0.1:5173/#/valuation/Gold", wait_until="networkidle", timeout=90000)
        page.wait_for_selector("[data-testid='goldvw-page']", timeout=45000)
        page.wait_for_selector("[data-panel='price'] .ws-chart-pane-canvas", timeout=20000)
        page.wait_for_timeout(2500)

        before = page.evaluate(
            "() => (window.__GOLDVW_SYNC__ && window.__GOLDVW_SYNC__.sharedVisibleRange) || null"
        )
        box = page.locator("[data-panel='price'] .ws-chart-pane-canvas").bounding_box()
        assert box
        page.screenshot(path=str(OUT / "gold_before_zoom.png"), full_page=False)
        page.mouse.move(box["x"] + box["width"] * 0.55, box["y"] + box["height"] * 0.4)
        for _ in range(6):
            page.mouse.wheel(0, -400)
            page.wait_for_timeout(120)
        page.wait_for_timeout(500)
        after = page.evaluate(
            "() => (window.__GOLDVW_SYNC__ && window.__GOLDVW_SYNC__.sharedVisibleRange) || null"
        )
        page.screenshot(path=str(OUT / "gold_after_zoom.png"), full_page=False)
        page.locator("[data-testid='ws-price-camera-reset']").click()
        page.wait_for_timeout(600)
        reset = page.evaluate(
            "() => (window.__GOLDVW_SYNC__ && window.__GOLDVW_SYNC__.sharedVisibleRange) || null"
        )
        page.screenshot(path=str(OUT / "gold_after_reset.png"), full_page=False)
        print({"before": before, "after": after, "reset": reset, "zoomed": before != after})
        browser.close()


if __name__ == "__main__":
    main()
