"""Rates + USD index valuation export and valuation_latest merge."""

from __future__ import annotations

from typing import Any

from hptl.valuation.rates_curve_fair_value_v1 import MODEL_ID as RATES_MODEL_ID
from hptl.valuation.rates_curve_fair_value_v1 import RATES_MARKETS, build_all_rates_valuations
from hptl.valuation.usd_broad_fair_value_v1 import DXY_MARKET, MODEL_ID as USD_MODEL_ID
from hptl.valuation.usd_broad_fair_value_v1 import build_usd_broad_valuation


def merge_rates_and_usd_into_valuation_latest(valuation_doc: dict[str, Any]) -> dict[str, Any]:
    rates_doc = build_all_rates_valuations()
    usd_doc = build_usd_broad_valuation()

    instruments = dict(valuation_doc.get("instruments") or {})
    for market in RATES_MARKETS:
        row = rates_doc.get("instruments", {}).get(market)
        if row:
            instruments[market] = {**row, "market": market, "valuation_pillar": "rates_curve_fair_value"}
    dxy_row = usd_doc.get("instruments", {}).get(DXY_MARKET)
    if dxy_row:
        instruments[DXY_MARKET] = {**dxy_row, "market": DXY_MARKET, "valuation_pillar": "usd_broad_fair_value"}

    wired = sum(1 for v in instruments.values() if v.get("wired"))
    rates_wired = sum(1 for m in RATES_MARKETS if (instruments.get(m) or {}).get("wired"))
    usd_wired = 1 if (instruments.get(DXY_MARKET) or {}).get("wired") else 0

    out = dict(valuation_doc)
    out["instruments"] = instruments
    summary = dict(out.get("summary") or {})
    summary["wired_count"] = wired
    summary["unavailable_count"] = len(instruments) - wired
    summary["rates_wired_count"] = rates_wired
    summary["usd_index_wired_count"] = usd_wired
    out["summary"] = summary
    out["rates_pillar_engine"] = RATES_MODEL_ID
    out["usd_index_pillar_engine"] = USD_MODEL_ID
    out["rates_valuation_summary"] = rates_doc.get("summary")
    out["usd_index_valuation_summary"] = usd_doc.get("summary")
    note = out.get("note") or ""
    extra = " Rates = rates_curve_fair_value_v1; USD index = usd_broad_fair_value_v1."
    if "rates_curve_fair_value_v1" not in note:
        out["note"] = note.rstrip() + extra
    return out
