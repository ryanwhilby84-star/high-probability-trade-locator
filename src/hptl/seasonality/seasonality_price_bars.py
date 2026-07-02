"""Weekly price bar extraction — always derived from canonical daily when available."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from hptl.markets.instrument_registry import get_instrument
from hptl.prices.canonical_timeline import (
    DERIVED_WEEKLY_ISO,
    DERIVED_WEEKLY_NATIVE,
    CanonicalTimeline,
    build_canonical_timeline,
    load_canonical_timeline,
    resample_weekly_closes,
)


def _num(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def bars_from_ohlc(rows: list[dict[str, Any]]) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    seen: set[str] = set()
    for b in rows or []:
        d = str(b.get("date") or "")[:10]
        c = _num(b.get("close"))
        if d and c is not None and d not in seen:
            seen.add(d)
            out.append((d, c))
    out.sort(key=lambda t: t[0])
    return out


def history_years(bars: list[tuple[str, float]]) -> set[int]:
    years: set[int] = set()
    for date, _ in bars:
        try:
            years.add(datetime.strptime(str(date)[:10], "%Y-%m-%d").year)
        except ValueError:
            continue
    return years


MIN_ISO_WEEKS_PER_YEAR = 35


def iso_weeks_by_year(bars: list[tuple[str, float]]) -> dict[int, set[int]]:
    out: dict[int, set[int]] = {}
    for date, _ in bars:
        try:
            dt = datetime.strptime(str(date)[:10], "%Y-%m-%d")
            iso = dt.isocalendar()
            out.setdefault(int(iso[0]), set()).add(int(iso[1]))
        except ValueError:
            continue
    return out


def history_quality(bars: list[tuple[str, float]]) -> tuple[int, float, int]:
    yw = iso_weeks_by_year(bars)
    if not yw:
        return 0, 0.0, 0
    latest = max(yw.keys())
    hist = sorted(y for y in yw if y < latest)
    if not hist:
        hist = [latest]
    counts = [len(yw[y]) for y in hist]
    avg_wpy = sum(counts) / len(counts)
    last3 = hist[-3:]
    min_last3 = min(len(yw[y]) for y in last3)
    return len(hist), avg_wpy, min_last3


def select_weekly_closes(
    rec: dict[str, Any] | None,
) -> tuple[list[tuple[str, float]], str]:
    """Derive weekly closes from a store record — daily ISO resample is canonical rule."""
    if not rec:
        return [], "none"
    daily = rec.get("daily") or []
    if daily:
        return resample_weekly_closes(daily), DERIVED_WEEKLY_ISO
    weekly_native = bars_from_ohlc(rec.get("weekly") or [])
    if weekly_native:
        return weekly_native, DERIVED_WEEKLY_NATIVE
    return [], "none"


def weekly_closes_from_record(rec: dict[str, Any] | None) -> list[tuple[str, float]]:
    bars, _ = select_weekly_closes(rec)
    return bars


def weekly_closes_for_instrument(
    instrument_id: str,
) -> tuple[list[tuple[str, float]], str, CanonicalTimeline | None]:
    """Canonical entry point — one timeline per instrument."""
    tl = load_canonical_timeline(instrument_id)
    if not tl:
        return [], "none", None
    bars, method = tl.weekly_for_seasonality()
    return bars, method, tl


def record_has_price_bars(rec: dict[str, Any] | None) -> bool:
    return bool(weekly_closes_from_record(rec))


def _norm_key(s: str) -> str:
    return str(s or "").lower().replace(" ", " ").strip()


def resolve_price_record(
    market: str,
    instruments: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any] | None, str | None, str | None]:
    """Resolve price record for *market* (legacy — prefer weekly_closes_for_instrument)."""
    tl = build_canonical_timeline(market, instruments=instruments, apply_supplements=False)
    if tl and tl.resolved_store_key:
        from hptl.prices.data_integrity import _instrument_price_row

        rec = _instrument_price_row(tl.resolved_store_key, instruments)
        return rec, tl.resolved_store_key, None

    if market in instruments:
        rec = instruments[market]
        if record_has_price_bars(rec):
            return rec, market, None
        if rec.get("error"):
            return rec, market, "price_fetch_error"
        return rec, market, "missing_price_history"

    spec = get_instrument(market)
    if spec and spec.cot_proxy_of:
        parent = spec.cot_proxy_of
        if parent in instruments:
            rec = instruments[parent]
            if record_has_price_bars(rec):
                return rec, parent, None

    for iid, rec in instruments.items():
        other = get_instrument(iid)
        if other and other.cot_proxy_of == market and record_has_price_bars(rec):
            return rec, iid, None

    target = _norm_key(market)
    for key, rec in instruments.items():
        if _norm_key(key) == target and record_has_price_bars(rec):
            return rec, key, None

    base = _norm_key(str(market).split("/")[0])
    for key, rec in instruments.items():
        if _norm_key(str(key).split("/")[0]) == base and record_has_price_bars(rec):
            return rec, key, None

    if market in instruments:
        rec = instruments[market]
        err = rec.get("error")
        if err:
            return rec, market, "price_fetch_error"
        return rec, market, "missing_price_history"

    if spec is None:
        return None, None, "unsupported_instrument"

    return None, None, "mapping_failure"
