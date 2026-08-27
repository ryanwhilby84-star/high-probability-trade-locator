"""Canonical and derived daily FX spot histories for valuation regression."""
from __future__ import annotations

from typing import Any

from hptl.fx.currency_map import COT_CURRENCY_SOURCES
from hptl.fx.fx_valuation import resolve_pair_currencies
from hptl.prices.price_store import load_price_store

# Cross pairs synthesised from USD legs (no direct price_store instrument).
DERIVED_CROSS_SPECS: dict[str, dict[str, Any]] = {
    "EUR/JPY": {
        "formula": "EUR/USD × USD/JPY",
        "legs": ("EUR/USD", "USD/JPY"),
        "op": "multiply_usd_cross",
    },
    "AUD/JPY": {
        "formula": "AUD/USD × USD/JPY",
        "legs": ("AUD/USD", "USD/JPY"),
        "op": "multiply_usd_cross",
    },
    "NZD/JPY": {
        "formula": "NZD/USD × USD/JPY",
        "legs": ("NZD/USD", "USD/JPY"),
        "op": "multiply_usd_cross",
    },
    "GBP/JPY": {
        "formula": "GBP/USD × USD/JPY",
        "legs": ("GBP/USD", "USD/JPY"),
        "op": "multiply_usd_cross",
    },
    "EUR/GBP": {
        "formula": "EUR/USD ÷ GBP/USD",
        "legs": ("EUR/USD", "GBP/USD"),
        "op": "divide",
    },
    "EUR/AUD": {
        "formula": "EUR/USD ÷ AUD/USD",
        "legs": ("EUR/USD", "AUD/USD"),
        "op": "divide",
    },
}


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _spot_store_key_candidates(canonical: str, pair_id: str) -> list[str]:
    """Candidate price_store keys — COT major first, then pair id aliases."""
    keys: list[str] = []
    seen: set[str] = set()

    def add(key: str | None) -> None:
        if key and key not in seen:
            seen.add(key)
            keys.append(key)

    for _code, spec in COT_CURRENCY_SOURCES.items():
        if str(spec.get("quote")) == canonical:
            add(str(spec.get("market")))
    add(canonical)
    add(pair_id)
    return keys


def _best_instrument_record(
    pair_id: str,
    *,
    bar_type: str = "daily",
) -> tuple[dict[str, Any], str | None]:
    """Pick the price_store instrument with the deepest bar history for a pair."""
    resolved = resolve_pair_currencies(pair_id)
    if not resolved:
        return {}, None
    _base, _quote, canonical = resolved
    instruments = load_price_store().get("instruments") or {}
    best_key: str | None = None
    best_rec: dict[str, Any] = {}
    best_len = -1
    for key in _spot_store_key_candidates(canonical, pair_id):
        rec = instruments.get(key) or {}
        bars = rec.get(bar_type) or []
        if len(bars) > best_len:
            best_len = len(bars)
            best_key = key
            best_rec = rec
    return best_rec, best_key


def _direct_daily_spot(pair_id: str) -> tuple[list[dict[str, Any]], str | None]:
    resolved = resolve_pair_currencies(pair_id)
    if not resolved:
        return [], None
    rec, used_key = _best_instrument_record(pair_id, bar_type="daily")
    daily = rec.get("daily") or []
    out: list[dict[str, Any]] = []
    for bar in daily:
        if not isinstance(bar, dict):
            continue
        d = str(bar.get("date") or "")[:10]
        c = _num(bar.get("close"))
        if d and c is not None and c > 0:
            out.append({"date": d, "spot": c})
    out.sort(key=lambda x: x["date"])
    return out, used_key


def _weekly_fallback(pair_id: str) -> list[dict[str, Any]]:
    resolved = resolve_pair_currencies(pair_id)
    if not resolved:
        return []
    rec, _used_key = _best_instrument_record(pair_id, bar_type="weekly")
    weekly = rec.get("weekly") or []
    out: list[dict[str, Any]] = []
    for bar in weekly:
        if not isinstance(bar, dict):
            continue
        d = str(bar.get("date") or "")[:10]
        c = _num(bar.get("close"))
        if d and c is not None and c > 0:
            out.append({"date": d, "spot": c})
    out.sort(key=lambda x: x["date"])
    return out


def _merge_by_date(*series: list[dict[str, Any]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for s in series:
        for pt in s:
            out[str(pt["date"])[:10]] = float(pt["spot"])
    return out


def _derive_cross_daily(spec: dict[str, Any]) -> list[dict[str, Any]]:
    leg_a, leg_b = spec["legs"]
    a, _ = _direct_daily_spot(leg_a)
    b, _ = _direct_daily_spot(leg_b)
    if not a:
        a = _weekly_fallback(leg_a)
    if not b:
        b = _weekly_fallback(leg_b)
    map_a = _merge_by_date(a)
    map_b = _merge_by_date(b)
    dates = sorted(set(map_a.keys()) & set(map_b.keys()))
    out: list[dict[str, Any]] = []
    op = spec.get("op")
    for d in dates:
        va, vb = map_a[d], map_b[d]
        if va <= 0 or vb <= 0:
            continue
        if op == "multiply_usd_cross":
            spot = round(va * vb, 6)
        elif op == "divide":
            spot = round(va / vb, 6)
        else:
            continue
        out.append({"date": d, "spot": spot})
    return out


def get_daily_spot_series(pair_id: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return daily spot bars and metadata (direct or derived_cross)."""
    direct, store_key = _direct_daily_spot(pair_id)
    if direct:
        return direct, {
            "proxy_flag": None,
            "source": f"price_store daily ({store_key})",
        }

    spec = DERIVED_CROSS_SPECS.get(pair_id)
    if spec:
        derived = _derive_cross_daily(spec)
        if derived:
            return derived, {
                "proxy_flag": "derived_cross",
                "derivation_formula": spec["formula"],
                "source_legs": list(spec["legs"]),
                "source": f"derived_cross: {spec['formula']}",
            }

    weekly = _weekly_fallback(pair_id)
    return weekly, {
        "proxy_flag": "weekly_fallback" if weekly else None,
        "source": "price_store weekly (fallback)",
    }
