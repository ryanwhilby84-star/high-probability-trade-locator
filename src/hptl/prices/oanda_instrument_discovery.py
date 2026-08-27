"""OANDA instrument discovery — build the canonical Current Price mapping.

Queries the authenticated OANDA account for its available instruments and
resolves every HPTL instrument id to a *real* account instrument name. We never
trust hardcoded symbols: a candidate is only accepted if OANDA actually reports
it on the account. The output is the single source of truth consumed by
``hptl.prices.current_price_service``.

For each resolved instrument we store exactly what OANDA reports:

    display_name, internal_key, provider, provider_symbol, asset_type,
    currency, price_precision, supports_streaming

Run:  python -m hptl.prices.oanda_instrument_discovery
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT, get_oanda_account_id, get_oanda_api_host
from hptl.markets.instrument_registry import (
    InstrumentSpec,
    all_instrument_ids,
    load_registry,
)
from hptl.oanda.oanda_client import (
    fetch_account_instruments,
    instrument_names_set,
    instruments_by_name,
    resolve_account_id,
)
from hptl.oanda.oanda_coverage_audit import _PRIMARY_SYMBOL_PREFERENCES

MAPPING_PATH = PROJECT_ROOT / "data" / "config" / "current_price_instruments.json"
PUBLIC_MAPPING_PATH = (
    PROJECT_ROOT / "web-dashboard" / "public" / "data" / "current_price_instruments.json"
)

PROVIDER_OANDA = "oanda"

# Currency / quote-unit codes used to split OANDA symbols that lack an underscore
# (registry stores several as e.g. ``WTICOUSD`` / ``NAS100USD`` while the live
# account reports ``WTICO_USD`` / ``NAS100_USD``).
_QUOTE_CODES: tuple[str, ...] = (
    "USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD", "HKD", "SGD",
    "ZAR", "TRY", "MXN", "NOK", "SEK", "DKK", "PLN", "HUF", "CZK", "CNH",
    "INR", "THB", "SAR", "XAG", "XAU",
)

# Known-good overrides verified against the practice account. These are only used
# as *candidates*: they must still be present on the live account to be accepted.
_KNOWN_GOOD: dict[str, list[str]] = {
    "Gold": ["XAU_USD"],
    "Silver": ["XAG_USD"],
    "Crude Oil / CL": ["WTICO_USD", "BCO_USD"],
    "Copper / HG": ["XCU_USD"],
    "Sugar": ["SUGAR_USD"],
    "Soybeans": ["SOYBN_USD"],
    "Natural Gas / NG": ["NATGAS_USD"],
    "NASDAQ / NQ": ["NAS100_USD"],
    "S&P 500 / ES": ["SPX500_USD"],
    "Dow / YM": ["US30_USD"],
    "US Nas 100": ["NAS100_USD"],
    "US SPX 500": ["SPX500_USD"],
    "US Wall St 30": ["US30_USD"],
    "US Russ 2000": ["US2000_USD"],
    "Brent Crude Oil": ["BCO_USD"],
    "West Texas Oil": ["WTICO_USD"],
    "Bitcoin": ["BTC_USD"],
}


def _underscore_variants(symbol: str | None) -> list[str]:
    """Candidate OANDA names for a raw/registry symbol (handles missing ``_``)."""
    if not symbol:
        return []
    sym = symbol.strip()
    if not sym:
        return []
    out: list[str] = [sym]
    if "_" not in sym:
        # e.g. WTICOUSD -> WTICO_USD ; XCUUSD -> XCU_USD ; NAS100USD -> NAS100_USD
        for code in _QUOTE_CODES:
            if len(sym) > len(code) and sym.endswith(code):
                out.append(f"{sym[:-len(code)]}_{code}")
                break
        # 6-char FX split 3/3 (EURUSD -> EUR_USD)
        if len(sym) == 6:
            out.append(f"{sym[:3]}_{sym[3:]}")
    # stable dedupe
    seen: set[str] = set()
    dedup: list[str] = []
    for s in out:
        if s and s not in seen:
            seen.add(s)
            dedup.append(s)
    return dedup


def _proxy_symbols_for(spec: InstrumentSpec, reg: dict[str, InstrumentSpec]) -> list[str]:
    syms: list[str] = []
    for other in reg.values():
        if other.cot_proxy_of == spec.id and other.oanda_symbol:
            syms.append(other.oanda_symbol)
    return syms


def _candidate_symbols(spec: InstrumentSpec, reg: dict[str, InstrumentSpec]) -> list[str]:
    """Ordered candidate OANDA names, most-trusted first, with underscore variants."""
    raw: list[str] = []
    raw.extend(_KNOWN_GOOD.get(spec.id, []))
    if spec.oanda_symbol:
        raw.append(spec.oanda_symbol)
    raw.extend(_PRIMARY_SYMBOL_PREFERENCES.get(spec.id, []))
    raw.extend(_proxy_symbols_for(spec, reg))

    candidates: list[str] = []
    seen: set[str] = set()
    for sym in raw:
        for variant in _underscore_variants(sym):
            if variant not in seen:
                seen.add(variant)
                candidates.append(variant)
    return candidates


def _currency_from_symbol(symbol: str | None) -> str | None:
    if not symbol:
        return None
    if "_" in symbol:
        return symbol.rsplit("_", 1)[-1] or None
    for code in _QUOTE_CODES:
        if symbol.endswith(code):
            return code
    return None


def _precision_from_meta(meta: dict[str, Any] | None) -> int | None:
    if not meta:
        return None
    dp = meta.get("displayPrecision")
    try:
        return int(dp) if dp is not None else None
    except (TypeError, ValueError):
        return None


def resolve_instrument(
    spec: InstrumentSpec,
    reg: dict[str, InstrumentSpec],
    oanda_names: set[str],
    by_name: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Resolve one HPTL instrument to a live OANDA name + metadata."""
    candidates = _candidate_symbols(spec, reg)
    resolved: str | None = None
    for sym in candidates:
        if sym in oanda_names:
            resolved = sym
            break

    meta = by_name.get(resolved or "")
    provider_symbol = resolved
    currency = _currency_from_symbol(resolved) or _currency_from_symbol(spec.oanda_symbol)

    return {
        "internal_key": spec.id,
        "display_name": spec.display_name or spec.id,
        "provider": PROVIDER_OANDA if resolved else None,
        "provider_symbol": provider_symbol,
        "asset_type": spec.asset_class,
        "provider_asset_type": (meta or {}).get("type"),
        "oanda_display_name": (meta or {}).get("displayName"),
        "currency": currency,
        "price_precision": _precision_from_meta(meta),
        "pip_location": (meta or {}).get("pipLocation"),
        "supports_streaming": bool(resolved),  # OANDA streams every account instrument
        "tradeable": bool(spec.tradeable),
        "cot_proxy_of": spec.cot_proxy_of,
        "candidates_tried": candidates,
        "status": "mapped" if resolved else "unmapped",
    }


def discover_oanda_instruments(*, account_id: str | None = None) -> dict[str, Any]:
    """Query OANDA account + build the canonical Current Price mapping document."""
    reg = load_registry()
    aid = account_id or get_oanda_account_id() or resolve_account_id()
    rows = fetch_account_instruments(aid)
    oanda_names = instrument_names_set(rows)
    by_name = instruments_by_name(rows)

    mappings: dict[str, dict[str, Any]] = {}
    mapped = 0
    unmapped: list[str] = []
    for iid in all_instrument_ids(tradeable_only=True):
        spec = reg.get(iid)
        if not spec:
            continue
        row = resolve_instrument(spec, reg, oanda_names, by_name)
        mappings[iid] = row
        if row["status"] == "mapped":
            mapped += 1
        else:
            unmapped.append(iid)

    return {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_from": "hptl.prices.oanda_instrument_discovery",
        "provider": PROVIDER_OANDA,
        "oanda_api_host": get_oanda_api_host(),
        "oanda_account_id": aid,
        "oanda_instruments_on_account": len(rows),
        "summary": {
            "total_instruments": len(mappings),
            "mapped_count": mapped,
            "unmapped_count": len(unmapped),
        },
        "unmapped": sorted(unmapped),
        "instruments": mappings,
    }


def write_discovery(payload: dict[str, Any] | None = None) -> Path:
    doc = payload or discover_oanda_instruments()
    text = json.dumps(doc, indent=2, ensure_ascii=False)
    MAPPING_PATH.parent.mkdir(parents=True, exist_ok=True)
    MAPPING_PATH.write_text(text, encoding="utf-8")
    PUBLIC_MAPPING_PATH.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_MAPPING_PATH.write_text(text, encoding="utf-8")
    return MAPPING_PATH


def load_discovery(path: Path | None = None) -> dict[str, Any] | None:
    p = path or MAPPING_PATH
    if not p.is_file():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def main() -> int:
    payload = discover_oanda_instruments()
    path = write_discovery(payload)
    s = payload["summary"]
    print(
        f"Wrote {path} — {s['mapped_count']}/{s['total_instruments']} mapped "
        f"({s['unmapped_count']} unmapped) from account {payload['oanda_account_id']} "
        f"on {payload['oanda_api_host']}."
    )
    if payload["unmapped"]:
        print("Unmapped:", ", ".join(payload["unmapped"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
