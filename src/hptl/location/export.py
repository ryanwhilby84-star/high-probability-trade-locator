"""Build location_latest.json for all COT-mapped instruments."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.confluence.build_decision_table import TARGET_MARKETS
from hptl.config import PROJECT_ROOT
from hptl.location.engine import compute_location
from hptl.macro.macro_relationship_maps import MACRO_MAPS_PUBLIC_PATH
from hptl.prices.canonical_timeline import DERIVED_WEEKLY_ISO, load_canonical_timeline
from hptl.prices.price_store import load_price_store

DATA_OUT = Path("data/location_latest.json")
PUBLIC_OUT = PROJECT_ROOT / "web-dashboard/public/data/location_latest.json"
DIST_OUT = PROJECT_ROOT / "web-dashboard/dist/data/location_latest.json"


def _load_macro_maps() -> dict[str, Any]:
    if not MACRO_MAPS_PUBLIC_PATH.exists():
        return {}
    doc = json.loads(MACRO_MAPS_PUBLIC_PATH.read_text(encoding="utf-8"))
    return doc.get("macro_relationship_maps") or {}


def build_location_latest() -> dict[str, Any]:
    prices_doc = load_price_store()
    instruments_px = prices_doc.get("instruments") or {}
    macro_maps = _load_macro_maps()

    instruments: dict[str, Any] = {}
    wired = 0
    for market in TARGET_MARKETS:
        tl = load_canonical_timeline(market)
        if tl:
            weekly_pairs, _ = tl.derive_weekly_iso()
            weekly = [{"date": d, "close": c} for d, c in weekly_pairs]
            range_52w = tl.range_52w()
            as_of = weekly[-1].get("date") if weekly else tl.date_end
        else:
            px = instruments_px.get(market) or {}
            weekly = px.get("weekly") or []
            range_52w = px.get("range_52w")
            as_of = str(weekly[-1].get("date") if weekly else "")[:10] or None

        loc = compute_location(
            market=market,
            weekly_bars=weekly,
            range_52w=range_52w,
            macro_map=macro_maps.get(market),
            as_of_week=as_of,
        )
        if tl:
            loc["canonical_source"] = tl.canonical_source
            loc["canonical_symbol"] = tl.canonical_symbol
            loc["price_derivation"] = DERIVED_WEEKLY_ISO
            loc["proxy"] = tl.proxy
            loc["proxy_explanation"] = tl.proxy_explanation
        instruments[market] = loc
        if loc.get("wired"):
            wired += 1

    return {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parser": "hptl.location.export",
        "pillar": "location",
        "summary": {
            "total_instruments": len(TARGET_MARKETS),
            "wired_count": wired,
            "unavailable_count": len(TARGET_MARKETS) - wired,
        },
        "instruments": instruments,
    }


def write_location_exports(payload: dict[str, Any] | None = None) -> dict[str, Path]:
    payload = payload or build_location_latest()
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    for path in (DATA_OUT, PUBLIC_OUT, DIST_OUT):
        if path == DIST_OUT and not path.parent.parent.exists():
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return {"data": DATA_OUT, "public": PUBLIC_OUT, "dist": DIST_OUT}
