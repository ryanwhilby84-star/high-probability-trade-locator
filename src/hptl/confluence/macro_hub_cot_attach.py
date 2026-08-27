"""Attach Macro Hub COT snapshots to confluence rows when legacy COT is not yet resolved.



Used as a fallback for newly wired direct-COT instruments (e.g. Bitcoin / 133741, USD Index / 098662) so

scanner eligibility and institutional reads can use pooled macro_hub data until the

next full legacy COT + confluence rebuild completes.

"""



from __future__ import annotations



import json

from functools import lru_cache

from pathlib import Path

from typing import Any



from hptl.config import EXPORTS_DIR, PROCESSED_DIR, PROJECT_ROOT



_MACRO_HUB_PATHS = (

    EXPORTS_DIR / "macro_hub_latest.json",

    PROCESSED_DIR / "macro_hub_latest.json",

    PROJECT_ROOT / "web-dashboard" / "public" / "data" / "macro_hub_latest.json",

)



# instrument_id -> (macro_hub section key, cot block key within section)

_MACRO_HUB_COT_SECTIONS: dict[str, tuple[str, str]] = {

    "Bitcoin": ("bitcoin", "cot"),

    "US Dollar Index / DX": ("usd", "cot"),

}



_CFTC_DEFAULTS: dict[str, str] = {

    "Bitcoin": "133741",

    "US Dollar Index / DX": "098662",

}





def _num(v: Any) -> float | None:

    if v is None or isinstance(v, bool):

        return None

    try:

        f = float(v)

    except (TypeError, ValueError):

        return None

    return f if f == f else None





def _cot_resolved(rec: dict[str, Any]) -> bool:

    bias = str(rec.get("cot_bias") or "").strip().upper()

    if not bias or bias == "N/A":

        return False

    reason = str(rec.get("missing_reason") or "")

    if "no mapped raw COT" in reason:

        return False

    return rec.get("net_value") is not None





def _bias_from_net(net: float | None) -> str:

    if net is None:

        return "N/A"

    if net > 0:

        return "Bullish"

    if net < 0:

        return "Bearish"

    return "Neutral"





@lru_cache(maxsize=1)

def _load_macro_hub_doc() -> dict[str, Any] | None:

    for path in _MACRO_HUB_PATHS:

        if path.exists():

            try:

                return json.loads(path.read_text(encoding="utf-8"))

            except (OSError, json.JSONDecodeError):

                continue

    return None





def macro_hub_cot_for_market(market_id: str) -> dict[str, Any] | None:

    doc = _load_macro_hub_doc()

    if not doc:

        return None

    section = _MACRO_HUB_COT_SECTIONS.get(market_id)

    if not section:

        return None

    block_key, cot_key = section

    cot = (doc.get(block_key) or {}).get(cot_key)

    if not isinstance(cot, dict):

        return None

    if cot.get("net") is None and cot.get("long") is None:

        return None

    return cot





def macro_hub_cot_fields_for_market(market_id: str) -> dict[str, Any]:

    cot = macro_hub_cot_for_market(market_id)

    if not cot:

        return {}

    long_v = _num(cot.get("long"))

    short_v = _num(cot.get("short"))

    net_v = _num(cot.get("net"))

    if net_v is None and long_v is not None and short_v is not None:

        net_v = long_v - short_v

    bias = _bias_from_net(net_v)

    report_date = str(cot.get("report_date") or "")[:10] or None

    return {

        "macro_hub_cot_attached": True,

        "macro_hub_cot": {

            "resolved": True,

            "source": cot.get("source"),

            "report_date": report_date,

            "cftc_code": cot.get("cftc_code") or _CFTC_DEFAULTS.get(market_id, ""),

        },

        "cot_bias": bias,

        "final_calculated_cot_bias": bias,

        "long_value": long_v,

        "short_value": short_v,

        "net_value": net_v,

        "open_interest": _num(cot.get("open_interest")),

        "one_week_net_change": _num(cot.get("weekly_net_change")),

        "four_week_net_change": _num(cot.get("four_week_net_change")),

        "positioning_status": "cot_available",

        "cot_status_label": "COT mapped (macro hub)",

        "positioning_source": str(cot.get("source") or "macro_hub_latest.json"),

        "missing_reason": None,

        "cot_reason": None,

        "latest_report_date": report_date or "N/A",

        "cot_report_date": report_date,

        "rolling_3y_history_context": {

            "net_percentile": _num(cot.get("net_percentile_3y")),

            "short_percentile": _num(cot.get("short_percentile_3y")),

            "oi_percentile": _num(cot.get("oi_percentile_3y")),

            "source": "macro_hub_latest.json",

        },

    }





def apply_macro_hub_cot_fallback(records: list[dict[str, Any]]) -> int:

    """Patch unresolved direct-COT rows on the latest calendar week only."""

    if not records:

        return 0

    latest_week = max(str(r.get("date") or "") for r in records)

    if not latest_week:

        return 0

    patched = 0

    for rec in records:

        if str(rec.get("date") or "") != latest_week:

            continue

        market = str(rec.get("market") or "")

        if market not in _MACRO_HUB_COT_SECTIONS:

            continue

        if _cot_resolved(rec):

            continue

        fields = macro_hub_cot_fields_for_market(market)

        if not fields:

            continue

        rec.update(fields)

        meta = dict(rec.get("instrument_meta") or {})

        meta["has_cot_mapping"] = True

        meta["positioning_status"] = "cot_available"

        meta["data_status"] = "cot_available"

        rec["instrument_meta"] = meta

        rec["data_status"] = "cot_available"

        patched += 1

    return patched


