"""Build valuation_latest.json for all COT-mapped instruments."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.confluence.build_decision_table import TARGET_MARKETS
from hptl.config import PROJECT_ROOT
from hptl.macro.macro_relationship_maps import MACRO_MAPS_PUBLIC_PATH
from hptl.prices.price_store import PUBLIC_PATH as PRICES_PUBLIC_PATH, load_price_store
from hptl.valuation.engine import compute_valuation

DATA_OUT = Path("data/valuation_latest.json")
PUBLIC_OUT = PROJECT_ROOT / "web-dashboard/public/data/valuation_latest.json"
DIST_OUT = PROJECT_ROOT / "web-dashboard/dist/data/valuation_latest.json"


def _load_macro_maps() -> dict[str, Any]:
    if not MACRO_MAPS_PUBLIC_PATH.exists():
        return {}
    doc = json.loads(MACRO_MAPS_PUBLIC_PATH.read_text(encoding="utf-8"))
    return doc.get("macro_relationship_maps") or {}


def build_valuation_latest() -> dict[str, Any]:
    prices_doc = load_price_store()
    instruments_px = prices_doc.get("instruments") or {}
    macro_maps = _load_macro_maps()

    instruments: dict[str, Any] = {}
    wired = 0
    for market in TARGET_MARKETS:
        px = instruments_px.get(market) or {}
        weekly = px.get("weekly") or []
        val = compute_valuation(
            market=market,
            weekly_bars=weekly,
            range_52w=px.get("range_52w"),
            macro_map=macro_maps.get(market),
            as_of_week=str(weekly[-1].get("date") if weekly else "")[:10] or None,
        )
        instruments[market] = val
        if val.get("wired"):
            wired += 1

    return {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parser": "hptl.valuation.export",
        "summary": {
            "total_instruments": len(TARGET_MARKETS),
            "wired_count": wired,
            "unavailable_count": len(TARGET_MARKETS) - wired,
        },
        "instruments": instruments,
    }


def write_valuation_exports(payload: dict[str, Any] | None = None) -> dict[str, Path]:
    payload = payload or build_valuation_latest()
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    for path in (DATA_OUT, PUBLIC_OUT, DIST_OUT):
        if path == DIST_OUT and not path.parent.parent.exists():
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return {"data": DATA_OUT, "public": PUBLIC_OUT, "dist": DIST_OUT}
