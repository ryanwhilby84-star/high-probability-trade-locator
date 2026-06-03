"""Build seasonality_latest.json for all COT-mapped instruments."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.confluence.build_decision_table import TARGET_MARKETS
from hptl.config import PROJECT_ROOT
from hptl.prices.price_store import load_price_store
from hptl.seasonality.engine import compute_seasonality

DATA_OUT = Path("data/seasonality_latest.json")
PUBLIC_OUT = PROJECT_ROOT / "web-dashboard/public/data/seasonality_latest.json"
DIST_OUT = PROJECT_ROOT / "web-dashboard/dist/data/seasonality_latest.json"


def build_seasonality_latest() -> dict[str, Any]:
    prices_doc = load_price_store()
    instruments_px = prices_doc.get("instruments") or {}

    instruments: dict[str, Any] = {}
    wired = 0
    for market in TARGET_MARKETS:
        px = instruments_px.get(market) or {}
        weekly = px.get("weekly") or []
        as_of = str(weekly[-1].get("date") if weekly else "")[:10] or None
        sea = compute_seasonality(market=market, weekly_bars=weekly, as_of_week=as_of)
        instruments[market] = sea
        if sea.get("wired"):
            wired += 1

    return {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parser": "hptl.seasonality.export",
        "summary": {
            "total_instruments": len(TARGET_MARKETS),
            "wired_count": wired,
            "unavailable_count": len(TARGET_MARKETS) - wired,
        },
        "instruments": instruments,
    }


def write_seasonality_exports(payload: dict[str, Any] | None = None) -> dict[str, Path]:
    payload = payload or build_seasonality_latest()
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    for path in (DATA_OUT, PUBLIC_OUT, DIST_OUT):
        if path == DIST_OUT and not path.parent.parent.exists():
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return {"data": DATA_OUT, "public": PUBLIC_OUT, "dist": DIST_OUT}
