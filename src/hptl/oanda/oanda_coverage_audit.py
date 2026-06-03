"""Compare HTPL instrument registry against live OANDA v20 account instruments."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import get_oanda_account_id, get_oanda_api_host
from hptl.markets.instrument_registry import InstrumentSpec, all_instrument_ids, load_registry
from hptl.oanda.oanda_client import (
    fetch_account_instruments,
    instruments_by_name,
    instrument_names_set,
    resolve_account_id,
)

AUDIT_JSON_PATH = Path("data/oanda_coverage_audit.json")
PUBLIC_AUDIT_PATH = Path("web-dashboard/public/data/oanda_coverage_audit.json")

# Prefer these OANDA symbols when resolving PRIMARY boards with no registry symbol.
_PRIMARY_SYMBOL_PREFERENCES: dict[str, list[str]] = {
    "Gold": ["XAU_USD", "XAU_EUR", "XAU_GBP", "XAU_AUD", "XAU_CAD", "XAU_JPY", "XAU_CHF"],
    "Silver": ["XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_AUD"],
    "Copper / HG": ["XCUUSD"],
    "Crude Oil / CL": ["WTICOUSD", "BCOUSD"],
    "Natural Gas / NG": ["NATGAS_USD", "NATGASUSD", "NGASUSD"],
}


def _friendly_name(spec: InstrumentSpec, oanda_symbol: str | None) -> str:
    if spec.asset_class == "fx" and "/" in spec.id:
        return spec.id
    if oanda_symbol and "_" in oanda_symbol and len(oanda_symbol.split("_")) == 2:
        base, quote = oanda_symbol.split("_", 1)
        if len(base) in (3, 6) and len(quote) in (3, 6):
            return f"{base}/{quote}"
    aliases = {
        "NASDAQ / NQ": "NASDAQ",
        "S&P 500 / ES": "S&P 500",
        "Dow / YM": "Dow",
        "Euro FX / 6E": "EUR/USD",
        "British Pound / 6B": "GBP/USD",
        "Japanese Yen / 6J": "USD/JPY",
        "Swiss Franc / 6S": "USD/CHF",
        "Australian Dollar / 6A": "AUD/USD",
        "Canadian Dollar / 6C": "USD/CAD",
        "NZ Dollar / 6N": "NZD/USD",
        "Crude Oil / CL": "Crude Oil",
        "Natural Gas / NG": "Natural Gas",
        "West Texas Oil": "Crude Oil (WTI)",
        "US Nas 100": "NASDAQ",
        "US SPX 500": "S&P 500",
        "US Wall St 30": "Dow",
    }
    return aliases.get(spec.id, spec.display_name or spec.id)


def _proxy_symbols_for(spec: InstrumentSpec, reg: dict[str, InstrumentSpec]) -> list[str]:
    syms: list[str] = []
    for other in reg.values():
        if other.cot_proxy_of == spec.id and other.oanda_symbol:
            syms.append(other.oanda_symbol)
    # stable dedupe
    seen: set[str] = set()
    out: list[str] = []
    for s in syms:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def resolve_lookup_symbol(
    spec: InstrumentSpec,
    reg: dict[str, InstrumentSpec],
    oanda_names: set[str],
) -> tuple[str | None, str, list[str]]:
    """Return (symbol, mapping_source, candidates_tried)."""
    if spec.oanda_symbol:
        return spec.oanda_symbol, "registry", [spec.oanda_symbol]

    prefs = _PRIMARY_SYMBOL_PREFERENCES.get(spec.id, [])
    proxies = _proxy_symbols_for(spec, reg)
    candidates = prefs + proxies
    tried: list[str] = []
    for sym in candidates:
        if sym not in tried:
            tried.append(sym)
        if sym in oanda_names:
            if sym in prefs:
                return sym, "primary_preference", tried
            return sym, f"proxy_registry:{sym}", tried
    return None, "none", tried


def build_oanda_coverage_audit(
    *,
    account_id: str | None = None,
    instruments: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    reg = load_registry()
    aid = account_id or get_oanda_account_id() or resolve_account_id()
    oanda_rows = instruments if instruments is not None else fetch_account_instruments(aid)
    by_name = instruments_by_name(oanda_rows)
    oanda_names = instrument_names_set(oanda_rows)

    supported: list[dict[str, Any]] = []
    unsupported: list[dict[str, Any]] = []
    mappings: list[dict[str, Any]] = []

    for iid in all_instrument_ids(tradeable_only=True):
        spec = reg[iid]
        symbol, source, tried = resolve_lookup_symbol(spec, reg, oanda_names)
        friendly = _friendly_name(spec, symbol or spec.oanda_symbol)
        meta = by_name.get(symbol or "")

        row = {
            "htpl_instrument_id": spec.id,
            "friendly_name": friendly,
            "asset_class": spec.asset_class,
            "registry_oanda_symbol": spec.oanda_symbol,
            "resolved_oanda_symbol": symbol,
            "mapping_source": source,
            "candidates_tried": tried,
            "oanda_display_name": (meta or {}).get("displayName"),
            "oanda_type": (meta or {}).get("type"),
            "minimum_trade_size": (meta or {}).get("minimumTradeSize"),
            "display_precision": (meta or {}).get("displayPrecision"),
        }

        if symbol and symbol in oanda_names:
            row["status"] = "supported"
            supported.append(row)
        else:
            reason = "no_oanda_symbol_in_registry" if not spec.oanda_symbol and not tried else "symbol_not_on_oanda_account"
            if not spec.oanda_symbol and not tried:
                reason = "no_oanda_symbol_in_registry"
            elif spec.oanda_symbol and spec.oanda_symbol not in oanda_names:
                reason = "registry_symbol_not_on_oanda"
            elif tried and not symbol:
                reason = "no_matching_oanda_instrument"
            row["status"] = "unsupported"
            row["unsupported_reason"] = reason
            unsupported.append(row)

        mappings.append(row)

    supported.sort(key=lambda r: (r["asset_class"], r["friendly_name"]))
    unsupported.sort(key=lambda r: (r["asset_class"], r["friendly_name"]))

    return {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_from": "hptl.oanda.oanda_coverage_audit",
        "oanda_api_host": get_oanda_api_host(),
        "oanda_account_id": aid,
        "oanda_instruments_on_account": len(oanda_rows),
        "htpl_tradeable_instruments": len(mappings),
        "summary": {
            "supported_count": len(supported),
            "unsupported_count": len(unsupported),
        },
        "supported": supported,
        "unsupported": unsupported,
        "mappings": mappings,
    }


def write_oanda_coverage_audit(
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
