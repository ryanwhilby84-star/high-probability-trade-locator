"""Export TFF macro positioning (DXY + Treasury futures) for dashboard and FX valuation."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.cot.tff_macro_contracts import TFF_MACRO_SYMBOLS
from hptl.cot.tff_macro_loader import latest_tff_macro_snapshot, load_tff_macro_weeks
from hptl.fx.fx_macro_positioning import build_macro_positioning_document
from hptl.macro.dollar_positioning import score_dollar_positioning
from hptl.macro.treasury_positioning import score_treasury_positioning

CANONICAL_PATH = PROCESSED_DIR / "tff_macro_positioning_latest.json"
PUBLIC_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "tff_macro_positioning_latest.json"


def _instrument_cards(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []
    for iid, block in (snapshot.get("instruments") or {}).items():
        sym = TFF_MACRO_SYMBOLS.get(iid, iid)
        latest = block.get("latest") if block.get("available") else None
        cards.append(
            {
                "instrument_id": iid,
                "symbol": sym,
                "available": bool(block.get("available")),
                "report_date": latest.get("date") if latest else None,
                "positioning": latest,
                "weeks": block.get("weeks") if block.get("available") else [],
            }
        )
    return cards


def build_tff_macro_positioning_payload(*, weeks_by_inst: dict[str, list] | None = None) -> dict[str, Any]:
    weeks_by_inst = weeks_by_inst or load_tff_macro_weeks()
    snapshot = latest_tff_macro_snapshot(weeks_by_inst)
    macro = build_macro_positioning_document(snapshot)
    dxy = score_dollar_positioning(snapshot)
    treas = score_treasury_positioning(snapshot)

    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": snapshot.get("source"),
        "trader_group": snapshot.get("trader_group"),
        "instruments": _instrument_cards(snapshot),
        "macro_positioning": macro,
        "widgets": {
            "us_dollar_positioning": {
                "title": "US Dollar Positioning",
                **dxy.as_dict(),
            },
            "treasury_positioning": {
                "title": "Treasury Positioning",
                **treas.as_dict(),
            },
            "rates_yield_sentiment": macro.get("rates_yield_sentiment"),
        },
        "raw_snapshot": snapshot,
    }


def write_tff_macro_positioning_exports(payload: dict[str, Any] | None = None) -> Path:
    payload = payload or build_tff_macro_positioning_payload()
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    for path in (CANONICAL_PATH, PUBLIC_PATH):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return CANONICAL_PATH


def run() -> Path:
    path = write_tff_macro_positioning_exports()
    n = sum(1 for c in (json.loads(path.read_text()).get("instruments") or []) if c.get("available"))
    print(f"Wrote TFF macro positioning: {path} ({n} instruments with data)")
    return path


if __name__ == "__main__":
    run()
