"""Compare HTPL registry vs live OANDA + Alpha Vantage price coverage."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.alpha_vantage.alpha_adapter import fetch_alpha_vantage_coverage_metadata
from hptl.alpha_vantage.mappings import mapping_to_evidence, resolve_alpha_mapping
from hptl.markets.instrument_registry import InstrumentSpec, all_instrument_ids, load_registry
from hptl.oanda.oanda_adapter import fetch_oanda_coverage_metadata
from hptl.oanda.oanda_coverage_audit import (
    _friendly_name,
    resolve_lookup_symbol,
)

AUDIT_JSON_PATH = Path("data/price_coverage_audit.json")
PUBLIC_AUDIT_PATH = Path("web-dashboard/public/data/price_coverage_audit.json")


def _oanda_evidence(
    spec: InstrumentSpec,
    reg: dict[str, InstrumentSpec],
    oanda_meta: dict[str, Any],
) -> dict[str, Any]:
    names = oanda_meta["names_set"]
    symbol, source, tried = resolve_lookup_symbol(spec, reg, names)
    row_meta = (oanda_meta.get("by_name") or {}).get(symbol or "")
    ts = oanda_meta.get("last_successful_response")
    ok = bool(symbol and symbol in names)
    return {
        "source": "oanda",
        "symbol": symbol or spec.oanda_symbol,
        "endpoint": oanda_meta.get("endpoint", "/v3/accounts/{accountId}/instruments"),
        "mapping_source": source,
        "candidates_tried": tried,
        "oanda_display_name": (row_meta or {}).get("displayName"),
        "oanda_type": (row_meta or {}).get("type"),
        "last_successful_response": ts if ok else None,
        "coverage_status": "supported" if ok else "unsupported",
    }


def _alpha_supported(
    mapping: Any,
    av_meta: dict[str, Any],
) -> bool:
    if mapping is None:
        return False
    verified = set(av_meta.get("verified_functions") or [])
    cat_ts = av_meta.get("category_timestamps") or {}
    if mapping.function in verified:
        return True
    if mapping.category == "fx" and "fx" in cat_ts:
        return True
    if mapping.category == "commodity" and mapping.function in verified:
        return True
    if mapping.category == "index" and "index" in cat_ts:
        return True
    if mapping.category == "crypto" and "crypto" in cat_ts:
        return True
    if mapping.category == "rates" and ("rates" in cat_ts or mapping.function in verified):
        return True
    return False


def build_price_coverage_audit(
    *,
    oanda_meta: dict[str, Any] | None = None,
    av_meta: dict[str, Any] | None = None,
    av_probe_delay_sec: float = 12.0,
) -> dict[str, Any]:
    reg = load_registry()
    oanda_meta = oanda_meta or fetch_oanda_coverage_metadata()
    av_meta = av_meta or fetch_alpha_vantage_coverage_metadata(delay_sec=av_probe_delay_sec)

    instruments: list[dict[str, Any]] = []
    oanda_supported: list[str] = []
    alpha_supported: list[str] = []
    supported_by_both: list[str] = []
    unsupported: list[str] = []

    for iid in all_instrument_ids(tradeable_only=True):
        spec = reg[iid]
        oanda_ev = _oanda_evidence(spec, reg, oanda_meta)
        mapping = resolve_alpha_mapping(spec)
        alpha_ev = mapping_to_evidence(
            mapping,
            verified_functions=set(av_meta.get("verified_functions") or []),
            category_timestamps=av_meta.get("category_timestamps") or {},
            per_function_timestamps=av_meta.get("per_function_timestamps") or {},
        ) if mapping else {
            "source": "alpha_vantage",
            "symbol": None,
            "endpoint": "https://www.alphavantage.co/query",
            "function": None,
            "params": {},
            "category": None,
            "last_successful_response": None,
            "coverage_status": "unsupported",
        }

        if mapping and _alpha_supported(mapping, av_meta):
            alpha_ev["coverage_status"] = "supported"
            alpha_ev["last_successful_response"] = (
                av_meta.get("per_function_timestamps", {}).get(mapping.function)
                or av_meta.get("category_timestamps", {}).get(mapping.category)
                or av_meta.get("last_successful_response")
            )

        o_ok = oanda_ev["coverage_status"] == "supported"
        a_ok = alpha_ev["coverage_status"] == "supported"

        if o_ok:
            oanda_supported.append(iid)
        if a_ok:
            alpha_supported.append(iid)
        if o_ok and a_ok:
            supported_by_both.append(iid)
        if not o_ok and not a_ok:
            unsupported.append(iid)

        if o_ok and a_ok:
            overall = "supported_by_both"
        elif o_ok:
            overall = "oanda_only"
        elif a_ok:
            overall = "alpha_only"
        else:
            overall = "unsupported"

        instruments.append(
            {
                "htpl_instrument_id": iid,
                "friendly_name": _friendly_name(spec, oanda_ev.get("symbol")),
                "asset_class": spec.asset_class,
                "coverage_status": overall,
                "sources": [oanda_ev, alpha_ev],
            }
        )

    oanda_supported.sort()
    alpha_supported.sort()
    supported_by_both.sort()
    unsupported.sort()

    return {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_from": "hptl.prices.price_coverage_audit",
        "summary": {
            "htpl_tradeable_instruments": len(instruments),
            "oanda_supported_count": len(oanda_supported),
            "alpha_supported_count": len(alpha_supported),
            "supported_by_both_count": len(supported_by_both),
            "unsupported_count": len(unsupported),
            "oanda_only_count": len(oanda_supported) - len(supported_by_both),
            "alpha_only_count": len(alpha_supported) - len(supported_by_both),
        },
        "oanda_supported": oanda_supported,
        "alpha_supported": alpha_supported,
        "supported_by_both": supported_by_both,
        "unsupported": unsupported,
        "oanda_api": {
            "account_id": oanda_meta.get("account_id"),
            "api_host": oanda_meta.get("api_host"),
            "instrument_count": oanda_meta.get("instrument_count"),
            "last_successful_response": oanda_meta.get("last_successful_response"),
            "endpoint": oanda_meta.get("endpoint"),
        },
        "alpha_vantage_api": {
            "supported_categories": av_meta.get("supported_categories"),
            "verified_functions": av_meta.get("verified_functions"),
            "last_successful_response": av_meta.get("last_successful_response"),
            "category_probes": av_meta.get("category_probes"),
        },
        "instruments": instruments,
    }


def write_price_coverage_audit(
    payload: dict[str, Any],
    *,
    path: Path | None = None,
    public_path: Path | None = None,
) -> Path:
    out = path or AUDIT_JSON_PATH
    pub = public_path or PUBLIC_AUDIT_PATH
    out.parent.mkdir(parents=True, exist_ok=True)
    pub.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    out.write_text(text, encoding="utf-8")
    pub.write_text(text, encoding="utf-8")
    return out
