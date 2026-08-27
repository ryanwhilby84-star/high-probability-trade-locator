from pathlib import Path
import json
import re

from playwright.sync_api import sync_playwright

OUT = Path("data/audits/gold_market_clearing_valuation/gold_valuation_page_repaired.png")
PROOF = Path("data/audits/gold_market_clearing_valuation/gold_valuation_ui_acceptance.json")
URL = "http://127.0.0.1:5173/#/valuation/Gold"


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 1800})
        page.goto(URL, wait_until="networkidle", timeout=90000)
        page.wait_for_selector("[data-testid='goldvw-page']", timeout=45000)
        page.wait_for_timeout(4000)
        page.screenshot(path=str(OUT), full_page=True)

        body = page.inner_text("body")
        market = None
        m = re.search(r"Market price\s*\$?([\d,]+\.?\d*)", body, re.I)
        if m:
            try:
                market = float(m.group(1).replace(",", ""))
            except ValueError:
                market = None

        proof = {
            "url": URL,
            "screenshot": str(OUT),
            "is_workstation": page.locator("[data-testid='goldvw-page']").count() == 1,
            "has_weekly_gold_price_title": "Weekly Gold Price" in body,
            "has_current_gold_valuation": "CURRENT GOLD VALUATION" in body,
            "has_deviation_toggle": page.locator("button", has_text="Valuation Deviation").count() > 0,
            "has_fair_value_toggle": page.locator("button", has_text="Fair Value").count() > 0,
            "has_live_card": page.locator("[data-testid='goldvw-live-card']").count() == 1,
            "market_price_parsed": market,
            "live_near_4090": market is not None and 3500 <= market <= 5000,
            "not_ngv_board": page.locator(".ngv-page").count() == 0,
        }
        PROOF.write_text(json.dumps(proof, indent=2), encoding="utf-8")
        browser.close()
    print("wrote", OUT)
    print(json.dumps(proof, indent=2))


if __name__ == "__main__":
    main()
