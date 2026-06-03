"""Price data integrity checks for Opportunity Engine pillars."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from hptl.alpha_vantage.mappings import _INDEX_SYMBOL, resolve_alpha_mapping
from hptl.markets.instrument_registry import InstrumentSpec, all_instrument_ids, get_instrument
from hptl.prices.coverage import load_price_coverage, oanda_symbol_for, select_price_source
from hptl.prices.price_store import PRICES_DIR, load_price_store

IntegrityStatus = Literal["PASS", "FAIL"]

# Cross-commodity substitutes — never score valuation/seasonality on these series.
REMOVED_PRICE_PROXIES: dict[str, str] = {
    "Soybeans": "CORN",
    "Cocoa": "COTTON",
}

METAL_SPOT_SHARED = "GOLD_SILVER_SPOT"

_NATIVE_COMMODITY: dict[str, str] = {
    "Crude Oil / CL": "WTI",
    "West Texas Oil": "WTI",
    "Brent Crude Oil": "BRENT",
    "Natural Gas / NG": "NATURAL_GAS",
    "Copper / HG": "COPPER",
    "Copper": "COPPER",
    "Wheat": "WHEAT",
    "Corn": "CORN",
    "Soybeans": "SOYBEANS",
    "Sugar": "SUGAR",
    "Coffee": "COFFEE",
    "Cocoa": "COCOA",
}


def _safe_filename(instrument_id: str) -> str:
    return re.sub(r"[^\w\-]+", "_", instrument_id.strip()).strip("_") or "instrument"


def _load_internal_raw(instrument_id: str) -> dict[str, Any]:
    path = PRICES_DIR / f"{_safe_filename(instrument_id)}.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _instrument_price_row(instrument_id: str, public: dict[str, Any] | None = None) -> dict[str, Any]:
    """Merge dashboard export with per-instrument processed store (source of truth)."""
    internal = _load_internal_raw(instrument_id)
    pub = (public or {}).get(instrument_id) or {}
    if not internal:
        return pub
    merged = dict(pub)
    for key in ("daily", "weekly", "range_52w", "history", "price", "error"):
        if internal.get(key) is not None:
            merged[key] = internal[key]
    return merged


def expected_price_source(instrument_id: str, cov: dict[str, Any] | None = None) -> str | None:
    doc = cov or load_price_coverage()
    if instrument_id in set(doc.get("oanda_supported") or []):
        spec = get_instrument(instrument_id)
        if spec and oanda_symbol_for(spec, doc):
            return "oanda"
    if instrument_id in REMOVED_PRICE_PROXIES:
        return None
    if instrument_id in {"Platinum", "Palladium"}:
        return None
    alpha_set = set(doc.get("alpha_supported") or [])
    if instrument_id not in alpha_set:
        return None
    spec = get_instrument(instrument_id)
    if spec is None:
        return None
    mapping = resolve_alpha_mapping(spec)
    if mapping is None:
        return None
    if mapping.function == METAL_SPOT_SHARED and spec.id not in {"Gold", "Silver"}:
        return None
    return "alpha_vantage"


def expected_symbol(
    spec: InstrumentSpec,
    expected_source: str | None,
    cov: dict[str, Any] | None = None,
) -> str | None:
    if expected_source == "oanda":
        doc = cov or load_price_coverage()
        return oanda_symbol_for(spec, doc) or spec.oanda_symbol
    if expected_source == "alpha_vantage":
        native = _NATIVE_COMMODITY.get(spec.id)
        if native:
            return native
        if spec.id in _INDEX_SYMBOL:
            return _INDEX_SYMBOL[spec.id]
        mapping = resolve_alpha_mapping(spec)
        if mapping:
            if mapping.category == "commodity":
                return mapping.function
            return mapping.params.get("symbol") or mapping.symbol
    return spec.oanda_symbol


def actual_fetch_meta(
    instrument_id: str,
    public: dict[str, Any] | None = None,
    cov: dict[str, Any] | None = None,
) -> tuple[str | None, str | None, int, int, str | None]:
    doc = cov or load_price_coverage()
    internal = _load_internal_raw(instrument_id)
    pub = (public or {}).get(instrument_id) or {}
    src = internal.get("_fetched_via")
    daily = internal.get("daily") or pub.get("daily") or []
    weekly = internal.get("weekly") or pub.get("weekly") or []
    err = internal.get("error") or pub.get("error")

    if not src and (daily or weekly):
        src = select_price_source(instrument_id, doc)

    actual_symbol: str | None = None
    spec = get_instrument(instrument_id)
    if src == "oanda" and spec:
        actual_symbol = oanda_symbol_for(spec, doc) or spec.oanda_symbol
    elif src == "alpha_vantage" and spec:
        mapping = resolve_alpha_mapping(spec)
        if mapping:
            if mapping.category == "commodity":
                actual_symbol = mapping.function
            else:
                actual_symbol = mapping.params.get("symbol") or mapping.symbol

    return src, actual_symbol, len(daily), len(weekly), err


def _proxy_violation(
    instrument_id: str,
    actual_symbol: str | None,
    actual_source: str | None,
) -> str | None:
    proxy = REMOVED_PRICE_PROXIES.get(instrument_id)
    if proxy and actual_symbol and actual_symbol.upper() == proxy.upper():
        return f"removed price proxy ({proxy})"

    spec = get_instrument(instrument_id)
    if spec and spec.id in {"Platinum", "Palladium"} and actual_symbol == METAL_SPOT_SHARED:
        return "removed shared GOLD_SILVER_SPOT proxy for metal"

    if spec and spec.id.startswith("Gold/") and actual_source == "alpha_vantage":
        return "cross pair should use OANDA, not AV spot proxy"
    if spec and spec.id.startswith("Silver/") and actual_source == "alpha_vantage":
        return "cross pair should use OANDA, not AV spot proxy"
    return None


@dataclass
class InstrumentIntegrity:
    instrument: str
    expected_symbol: str | None
    actual_symbol: str | None
    expected_source: str | None
    actual_source: str | None
    daily_bars: int
    weekly_bars: int
    valuation_available: bool
    seasonality_available: bool
    status: IntegrityStatus
    reasons: list[str] = field(default_factory=list)
    error: str | None = None

    @property
    def score_eligible(self) -> bool:
        return self.status == "PASS"

    def to_dict(self) -> dict[str, Any]:
        return {
            "instrument": self.instrument,
            "expected_symbol": self.expected_symbol,
            "actual_symbol": self.actual_symbol,
            "expected_source": self.expected_source or "none",
            "actual_source": self.actual_source or "none",
            "daily_bars": self.daily_bars,
            "weekly_bars": self.weekly_bars,
            "valuation_available": self.valuation_available,
            "seasonality_available": self.seasonality_available,
            "integrity": self.status,
            "reasons": self.reasons,
            "score_eligible": self.score_eligible,
            "error": self.error,
        }


def check_instrument_integrity(
    instrument_id: str,
    *,
    public: dict[str, Any] | None = None,
    cov: dict[str, Any] | None = None,
) -> InstrumentIntegrity:
    doc = cov or load_price_coverage()
    spec = get_instrument(instrument_id)
    if spec is None:
        return InstrumentIntegrity(
            instrument=instrument_id,
            expected_symbol=None,
            actual_symbol=None,
            expected_source=None,
            actual_source=None,
            daily_bars=0,
            weekly_bars=0,
            valuation_available=False,
            seasonality_available=False,
            status="FAIL",
            reasons=["unknown instrument"],
        )

    expected_src = expected_price_source(instrument_id, doc)
    expected_sym = expected_symbol(spec, expected_src, doc)
    actual_src, actual_sym, n_daily, n_weekly, err = actual_fetch_meta(
        instrument_id, public, doc
    )

    reasons: list[str] = []
    if expected_src is None:
        reasons.append("no supported native price source")

    if expected_sym and actual_sym and str(actual_sym).upper() != str(expected_sym).upper():
        if not (
            expected_src == "oanda"
            and spec.oanda_symbol
            and actual_sym.replace("_", "") == spec.oanda_symbol.replace("_", "")
        ):
            reasons.append(f"symbol mismatch (expected {expected_sym}, got {actual_sym})")
    elif expected_sym and not actual_sym:
        reasons.append("actual symbol missing")

    if expected_src and actual_src and expected_src != actual_src:
        reasons.append(f"source mismatch (expected {expected_src}, got {actual_src})")

    if n_daily <= 0:
        reasons.append("daily bars = 0")
    if n_weekly <= 0:
        reasons.append("weekly bars = 0")
    if err:
        reasons.append(str(err)[:160])

    proxy_reason = _proxy_violation(instrument_id, actual_sym, actual_src)
    if proxy_reason:
        reasons.append(proxy_reason)

    px = _instrument_price_row(instrument_id, public)
    weekly_for_pillars = px.get("weekly") or []

    status: IntegrityStatus = "PASS" if not reasons else "FAIL"

    if status == "PASS":
        from hptl.seasonality.engine import compute_seasonality
        from hptl.valuation.engine import compute_valuation

        val = compute_valuation(
            market=instrument_id,
            weekly_bars=weekly_for_pillars,
            range_52w=px.get("range_52w"),
        )
        sea = compute_seasonality(market=instrument_id, weekly_bars=weekly_for_pillars)
        val_ok = bool(val.get("wired"))
        sea_ok = bool(sea.get("wired"))
    else:
        val_ok = False
        sea_ok = False

    return InstrumentIntegrity(
        instrument=instrument_id,
        expected_symbol=expected_sym,
        actual_symbol=actual_sym,
        expected_source=expected_src,
        actual_source=actual_src,
        daily_bars=n_daily,
        weekly_bars=n_weekly,
        valuation_available=val_ok,
        seasonality_available=sea_ok,
        status=status,
        reasons=reasons,
        error=err,
    )


@lru_cache(maxsize=1)
def _public_instruments() -> dict[str, Any]:
    return (load_price_store().get("instruments") or {})


def build_integrity_report(
    instrument_ids: list[str] | None = None,
    *,
    tradeable_only: bool = True,
) -> dict[str, Any]:
    cov = load_price_coverage()
    public = _public_instruments()
    ids = instrument_ids or all_instrument_ids(tradeable_only=tradeable_only)
    rows = [check_instrument_integrity(iid, public=public, cov=cov).to_dict() for iid in ids]
    pass_n = sum(1 for r in rows if r["integrity"] == "PASS")
    return {
        "generated_from": "hptl.prices.data_integrity",
        "store_generated_at": load_price_store().get("generated_at"),
        "summary": {
            "total": len(rows),
            "pass": pass_n,
            "fail": len(rows) - pass_n,
            "score_eligible": pass_n,
        },
        "rows": rows,
    }


def unavailable_pillar_fields(*, reason: str = "Price data integrity check failed.") -> dict[str, Any]:
    return {
        "valuation_bias": "UNAVAILABLE",
        "valuation_score": None,
        "valuation_reason": reason,
        "valuation_wired": False,
        "valuation_price_percentile_52w": None,
        "seasonality_bias": "UNAVAILABLE",
        "seasonality_score": None,
        "seasonality_reason": reason,
        "seasonality_wired": False,
        "seasonality_calendar_month": None,
        "data_integrity": "FAIL",
    }


def integrity_status_for(instrument_id: str) -> InstrumentIntegrity:
    return check_instrument_integrity(instrument_id, public=_public_instruments())
