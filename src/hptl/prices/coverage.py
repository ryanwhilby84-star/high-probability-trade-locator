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
        raise FileNotFoundError(
            f"Price coverage audit missing at {p}. Run: python -m hptl.prices.run_price_coverage_audit"
        )
    return json.loads(p.read_text(encoding="utf-8"))


def _instrument_coverage_row(audit: dict[str, Any], instrument_id: str) -> dict[str, Any] | None:
    for row in audit.get("instruments") or []:
        if row.get("htpl_instrument_id") == instrument_id:
            return row
    return None


def oanda_symbol_for(spec: InstrumentSpec, audit: dict[str, Any]) -> str | None:
    if spec.id not in (audit.get("oanda_supported") or []):
        return None
    row = _instrument_coverage_row(audit, spec.id)
    if row:
        for src in row.get("sources") or []:
            if (
                src.get("source") == "oanda"
                and src.get("symbol")
                and src.get("coverage_status") == "supported"
            ):
                return str(src["symbol"])
    if spec.oanda_symbol:
        return spec.oanda_symbol
    reg = load_registry()
    oanda_names = {
        str(s.get("symbol"))
        for r in audit.get("instruments") or []
        for s in r.get("sources") or []
        if s.get("source") == "oanda" and s.get("symbol")
    }
    sym, _, _ = resolve_lookup_symbol(spec, reg, oanda_names)
    return sym


def select_price_source(
    instrument_id: str,
    audit: dict[str, Any] | None = None,
) -> str | None:
    """Return ``oanda``, ``alpha_vantage``, or ``None`` (unsupported). Prefer OANDA when both."""
    doc = audit or load_price_coverage()
    spec = get_instrument(instrument_id)
    oanda_set = set(doc.get("oanda_supported") or [])
    if spec and instrument_id in oanda_set and oanda_symbol_for(spec, doc):
        return "oanda"
    alpha_set = set(doc.get("alpha_supported") or [])
    if instrument_id in alpha_set:
        return "alpha_vantage"
    return None


def supported_instrument_ids(audit: dict[str, Any] | None = None) -> list[str]:
    doc = audit or load_price_coverage()
    oanda_set = set(doc.get("oanda_supported") or [])
    alpha_set = set(doc.get("alpha_supported") or [])
    return sorted(oanda_set | alpha_set)


def get_spec(instrument_id: str) -> InstrumentSpec | None:
    return get_instrument(instrument_id)
