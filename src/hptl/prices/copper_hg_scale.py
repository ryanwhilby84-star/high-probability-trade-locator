"""Copper / HG price scale: Alpha Vantage USD/metric tonne -> COMEX HG chart scale."""

from __future__ import annotations

from typing import Any

from hptl.prices.models import (
    OhlcBar,
    PriceHistoryMeta,
    PriceSnapshot,
    Range52w,
    build_history_meta,
    compute_range_52w,
)

COPPER_HG_INSTRUMENT_ID = "Copper / HG"
LB_PER_METRIC_TONNE = 2204.6226218488
HG_DISPLAY_MULT = 1000.0
RAW_UNIT = "USD per metric tonne"
TRANSFORMED_UNIT = "USD/lb × 1000"
VALUE_KIND = "hg_chart_usd_per_lb_x1000"


def should_apply_copper_hg_av_scale(
    instrument_id: str,
    *,
    source: str | None,
    av_function: str | None,
) -> bool:
    return (
        instrument_id == COPPER_HG_INSTRUMENT_ID
        and source == "alpha_vantage"
        and (av_function or "").upper() == "COPPER"
    )


def metric_tonne_to_hg_chart(value: float) -> float:
    return value / LB_PER_METRIC_TONNE * HG_DISPLAY_MULT


def _scale_ohlc(value: float | None) -> float | None:
    if value is None:
        return None
    return metric_tonne_to_hg_chart(value)


def transform_bar(bar: OhlcBar) -> OhlcBar:
    raw_close = bar.get("close")
    out: OhlcBar = dict(bar)
    if raw_close is not None:
        out["raw_close"] = float(raw_close)
    for key in ("open", "high", "low", "close"):
        if bar.get(key) is not None:
            out[key] = _scale_ohlc(float(bar[key]))  # type: ignore[arg-type]
    return out


def _transform_snapshot(price: PriceSnapshot | None, *, latest_raw_close: float | None) -> PriceSnapshot | None:
    if price is None:
        return None
    mid = price.get("mid")
    if mid is None:
        return price
    raw_mid = float(mid)
    out: PriceSnapshot = dict(price)
    out["mid"] = metric_tonne_to_hg_chart(raw_mid)
    out["raw_mid"] = raw_mid
    if latest_raw_close is not None:
        out["raw_close"] = latest_raw_close
    return out


def build_price_scale_meta(*, raw_close: float, transformed_close: float) -> dict[str, Any]:
    return {
        "raw_close": raw_close,
        "raw_unit": RAW_UNIT,
        "transformed_close": transformed_close,
        "transformed_unit": TRANSFORMED_UNIT,
        "conversion_factor": LB_PER_METRIC_TONNE,
        "value_kind": VALUE_KIND,
        "source": "alpha_vantage",
        "source_function": "COPPER",
    }


def is_already_transformed(record: dict[str, Any]) -> bool:
    ps = record.get("price_scale") or {}
    return ps.get("value_kind") == VALUE_KIND


def apply_copper_hg_av_scale_to_record(
    record: dict[str, Any],
    *,
    source: str | None,
    av_function: str | None,
) -> dict[str, Any]:
    """Transform AV COPPER series to HG chart scale; preserve raw values."""
    iid = str(record.get("instrument_id") or "")
    if not should_apply_copper_hg_av_scale(iid, source=source, av_function=av_function):
        return record
    if is_already_transformed(record):
        return record

    daily = [transform_bar(b) for b in record.get("daily") or []]
    weekly = [transform_bar(b) for b in record.get("weekly") or []]
    bars = daily or weekly
    if not bars:
        return record

    latest_raw = float(bars[-1].get("raw_close") or bars[-1].get("close") or 0)
    # If bars were previously transformed without metadata, raw_close on bar is the display value.
    if latest_raw < 9000 and bars[-1].get("raw_close") is None:
        # Likely already HG-scale data — do not double-transform.
        return record

    latest_xformed = float(bars[-1]["close"])
    price = _transform_snapshot(record.get("price"), latest_raw_close=latest_raw)
    range_52w = compute_range_52w(daily)
    history: PriceHistoryMeta | None = None
    if daily or weekly:
        history = build_history_meta(daily, weekly, range_52w)

    record["daily"] = daily
    record["weekly"] = weekly
    record["price"] = price
    record["range_52w"] = range_52w
    record["history"] = history
    record["price_scale"] = build_price_scale_meta(
        raw_close=latest_raw,
        transformed_close=latest_xformed,
    )
    return record


def migrate_copper_hg_file_on_disk(path: Any) -> bool:
    """Apply HG scale transform to a stored Copper / HG JSON file if needed."""
    import json
    from pathlib import Path

    p = Path(path)
    if not p.exists():
        return False
    doc = json.loads(p.read_text(encoding="utf-8"))
    if doc.get("instrument_id") != COPPER_HG_INSTRUMENT_ID:
        return False
    if is_already_transformed(doc):
        return False
    src = doc.get("_fetched_via") or "alpha_vantage"
    apply_copper_hg_av_scale_to_record(doc, source=src, av_function="COPPER")
    if not is_already_transformed(doc):
        return False
    p.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    return True
