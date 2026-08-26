"""Load price coverage audit and resolve primary fetch source per instrument."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hptl.markets.instrument_registry import InstrumentSpec, get_instrument, load_registry
from hptl.oanda.oanda_coverage_audit import resolve_lookup_symbol

COVERAGE_PATH = Path("data/price_coverage_audit.json")


def load_price_coverage(path: Path | None = None) -> dict[str, Any]:
    p = path or COVERAGE_PATH
    if not p.exists():
        raise FileNotFoundError(f"Price coverage audit missing at {p}. Run: python -m hptl.prices.run_price_coverage_audit")
    return json.loads(p.read_text(encoding="utf-8"))


def _instrument_coverage_row(audit: dict[str, Any], instrument_id: str) -> dict[str, Any] | None:
    for row in audit.get("instruments") or []:
        if row.get("htpl_instrument_id") == instrument_id:
            return row
    return None


def oanda_symbol_for(spec: InstrumentSpec, audit: dict[str, Any]) -> str | None:
    """Resolve OANDA symbol — registry/canonical wins over stale coverage audit rows."""
    if spec.oanda_symbol:
        return spec.oanda_symbol
    row = _instrument_coverage_row(audit, spec.id)
    if row:
        for src in row.get("sources") or []:
            if src.get("source") == "oanda" and src.get("symbol") and src.get("coverage_status") == "supported":
                return str(src["symbol"])
    if spec.id not in (audit.get("oanda_supported") or []):
        return None
    reg = load_registry()
    oanda_names = {str(s.get("symbol")) for r in audit.get("instruments") or [] for s in r.get("sources") or [] if s.get("source") == "oanda" and s.get("symbol")}
    sym, _, _ = resolve_lookup_symbol(spec, reg, oanda_names)
    return sym


def select_price_source(instrument_id: str, audit: dict[str, Any] | None = None) -> str | None:
    """Return canonical price source for an instrument."""
    doc = audit or load_price_coverage()
    spec = get_instrument(instrument_id)
    try:
        from hptl.prices.softs_futures_backfill import SOFTS_YAHOO
        if instrument_id in SOFTS_YAHOO: return "yahoo_futures"
    except Exception:
        pass
    try:
        from hptl.markets.usd_index_identity import is_ice_dx_price_id
        if is_ice_dx_price_id(instrument_id): return "yahoo_futures"
    except Exception:
        pass
    if spec and spec.oanda_symbol: return "oanda"
    if spec and instrument_id in set(doc.get("oanda_supported") or []) and oanda_symbol_for(spec, doc): return "oanda"
    if instrument_id in set(doc.get("alpha_supported") or []): return "alpha_vantage"
    if instrument_id in set(doc.get("fred_supported") or []): return "fred"
    try:
        from hptl.prices.fred_prices import fred_series_for
        if fred_series_for(instrument_id): return "fred"
    except Exception:
        pass
    return None


def supported_instrument_ids(audit: dict[str, Any] | None = None) -> list[str]:
    """All instruments the refresh can actually service.

    The generated coverage audit can lag the registry.  Canonical registry
    instruments with an explicit OANDA symbol therefore belong in the refresh
    universe even if the last audit omitted them.  This keeps core indices,
    metals, energy and FX from silently falling out of the nightly refresh.
    """
    doc = audit or load_price_coverage()
    ids = set(doc.get("oanda_supported") or []) | set(doc.get("alpha_supported") or []) | set(doc.get("fred_supported") or [])
    try:
        ids.update(iid for iid, spec in load_registry().items() if spec.oanda_symbol)
    except Exception:
        pass
    try:
        from hptl.prices.softs_futures_backfill import SOFTS_YAHOO
        ids.update(SOFTS_YAHOO)
    except Exception:
        pass
    try:
        from hptl.prices.fred_prices import FRED_INSTRUMENT_SERIES
        ids.update(FRED_INSTRUMENT_SERIES)
    except Exception:
        pass
    return sorted(ids)


def get_spec(instrument_id: str) -> InstrumentSpec | None:
    return get_instrument(instrument_id)
