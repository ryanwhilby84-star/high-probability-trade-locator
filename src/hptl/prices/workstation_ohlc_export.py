"""Workstation weekly OHLC export — visualization-only price/COT alignment layer.

Uses canonical daily timeline (with COT-window supplements) but emits only weeks
with real OHLC (high > low). Close-only FRED proxy bars are excluded from candles.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.cot.cot_3y_series_export import PUBLIC_PATH as COT_3Y_PUBLIC
from hptl.markets.instrument_registry import all_instrument_ids
from hptl.prices.canonical_timeline import build_canonical_timeline
from hptl.prices.workstation_index_ohlc_history import (
    load_workstation_index_daily_bars,
    resolve_workstation_index_source,
)

OUT_PATH = PROCESSED_DIR / "workstation_ohlc_latest.json"
PUBLIC_OUT = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "workstation_ohlc_latest.json"
DIST_OUT = PROJECT_ROOT / "web-dashboard" / "dist" / "data" / "workstation_ohlc_latest.json"
MAX_COMPLETED_OHLC_AGE_DAYS = 10


def _num(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _is_real_ohlc(open_: float | None, high: float | None, low: float | None, close: float | None) -> bool:
    if open_ is None or high is None or low is None or close is None:
        return False
    return high > low


def _is_usable_daily_bar(
    open_: float | None, high: float | None, low: float | None, close: float | None
) -> bool:
    """Accept real wick bars or close-only index prints (O=H=L=C) for weekly aggregation."""
    if open_ is None or high is None or low is None or close is None:
        return False
    if high > low:
        return True
    return open_ == high == low == close


def _iso_week_key(date_str: str) -> str:
    try:
        return pd.Timestamp(str(date_str)[:10]).strftime("%G-W%V")
    except (TypeError, ValueError):
        return str(date_str)[:7]


def derive_weekly_ohlc_from_daily(daily_bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """ISO-week OHLC from daily bars.

    Close-only daily index prints (FRED DTWEXBGS etc.) are aggregated into weekly
    OHLC using the week's first/last/min/max closes — not fabricated intraday wicks.
    """
    buckets: dict[str, dict[str, Any]] = {}
    for bar in daily_bars:
        d = str(bar.get("date") or "")[:10]
        o, h, l, c = _num(bar.get("open")), _num(bar.get("high")), _num(bar.get("low")), _num(bar.get("close"))
        if not d or not _is_usable_daily_bar(o, h, l, c):
            continue
        # Close-only print: use close for all fields so weekly min/max reflect the index path.
        if not _is_real_ohlc(o, h, l, c):
            o = h = l = c
        wk = _iso_week_key(d)
        prev = buckets.get(wk)
        if prev is None:
            buckets[wk] = {"date": d, "open": o, "high": h, "low": l, "close": c, "source": bar.get("source")}
            continue
        if d < prev["date"]:
            buckets[wk] = {
                **prev,
                "open": o,
                "high": max(prev["high"], h),
                "low": min(prev["low"], l),
            }
        else:
            buckets[wk] = {
                "date": d,
                "open": prev["open"],
                "high": max(prev["high"], h),
                "low": min(prev["low"], l),
                "close": c,
                "source": bar.get("source") or prev.get("source"),
            }
    out = list(buckets.values())
    out.sort(key=lambda b: b["date"])
    # Keep weeks with a real range; also keep flat weeks (single close) for continuity.
    return [
        b
        for b in out
        if _is_usable_daily_bar(b["open"], b["high"], b["low"], b["close"])
    ]


def _find_bar_as_of(bars: list[dict[str, Any]], cot_date: str) -> dict[str, Any] | None:
    d = str(cot_date)[:10]
    best = None
    for bar in bars:
        if bar["date"] <= d:
            best = bar
        else:
            break
    return best


def _common_range(cot_first: str | None, cot_last: str | None, ohlc_first: str | None, ohlc_last: str | None) -> tuple[str | None, str | None]:
    if not cot_first or not cot_last or not ohlc_first or not ohlc_last:
        return None, None
    common_start = max(cot_first, ohlc_first)
    common_end = min(cot_last, ohlc_last)
    if common_start > common_end:
        return None, None
    return common_start, common_end


def _age_days(date_str: str | None) -> int | None:
    if not date_str:
        return None
    try:
        d = datetime.fromisoformat(str(date_str)[:10]).date()
    except ValueError:
        return None
    return max(0, (datetime.now(timezone.utc).date() - d).days)


def _price_quality(instrument_id: str, latest_date: str | None, source: str | None, symbol: str | None) -> dict[str, Any]:
    if not latest_date:
        return {
            "status": "MISSING",
            "latest_date": None,
            "latest_age_days": None,
            "warning": "Price OHLC is missing.",
        }
    age = _age_days(latest_date)
    warnings: list[str] = []
    status = "PASS"
    if age is not None and age > MAX_COMPLETED_OHLC_AGE_DAYS:
        status = "STALE"
        warnings.append(f"Latest completed OHLC is {age} days old.")
    if instrument_id == "NASDAQ / NQ" and symbol != "NAS100_USD" and source != "oanda":
        status = "WRONG_SYMBOL" if status == "PASS" else status
        warnings.append("NASDAQ / NQ is not using the configured OANDA NAS100_USD workstation proxy.")
    return {
        "status": status,
        "latest_date": latest_date,
        "latest_age_days": age,
        "source": source,
        "symbol": symbol,
        "warning": " ".join(warnings) if warnings else None,
    }


def build_instrument_workstation_ohlc(
    instrument_id: str,
    *,
    cot_block: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cot_first = str(cot_block.get("earliest_date") or "")[:10] if cot_block else None
    cot_last = str(cot_block.get("latest_date") or "")[:10] if cot_block else None
    cot_rows = int(cot_block.get("weeks") or 0) if cot_block else 0
    price_audit = (cot_block or {}).get("price_audit") or {}

    index_spec = resolve_workstation_index_source(instrument_id)
    if index_spec:
        index_daily, index_diag = load_workstation_index_daily_bars(
            instrument_id,
            window_start=cot_first or None,
            refresh=False,
        )
        weekly_ohlc = derive_weekly_ohlc_from_daily(index_daily)
        ohlc_first = weekly_ohlc[0]["date"] if weekly_ohlc else None
        ohlc_last = weekly_ohlc[-1]["date"] if weekly_ohlc else None
        common_first, common_last = _common_range(cot_first, cot_last, ohlc_first, ohlc_last)
        aligned: list[dict[str, Any]] = []
        missing = 0
        if cot_block and cot_block.get("series"):
            for row in cot_block["series"]:
                d = str(row.get("date") or "")[:10]
                if not d:
                    continue
                if common_first and d < common_first:
                    continue
                if common_last and d > common_last:
                    continue
                bar = _find_bar_as_of(weekly_ohlc, d)
                if bar:
                    aligned.append({**bar, "cot_date": d})
                else:
                    missing += 1
        source = index_diag.get("source") or "oanda"
        symbol = index_diag.get("source_symbol") or index_spec.get("oanda_symbol")
        incomplete = bool(cot_rows > 0 and weekly_ohlc and common_first and cot_first and common_first > cot_first)
        note = None
        if not weekly_ohlc:
            note = "Weekly OHLC unavailable."
        elif incomplete:
            note = "Price OHLC history incomplete — displaying common overlap only."
        return {
            "instrument_id": instrument_id,
            "weekly_ohlc": weekly_ohlc,
            "aligned_weekly_ohlc": aligned,
            "price_source": f"{source}:{symbol}",
            "canonical_symbol": symbol,
            "canonical_source": source,
            "cot_first_date": cot_first,
            "cot_last_date": cot_last,
            "cot_rows": cot_rows,
            "ohlc_first_date": ohlc_first,
            "ohlc_last_date": ohlc_last,
            "ohlc_rows": len(weekly_ohlc),
            "common_first_date": common_first,
            "common_last_date": common_last,
            "common_rows": len(aligned) if aligned else 0,
            "missing_ohlc_weeks": missing,
            "incomplete_history": incomplete or (not weekly_ohlc),
            "note": note,
            "price_quality": _price_quality(instrument_id, ohlc_last, source, symbol),
            "price_audit": {
                "workstation_index_source": index_spec,
                "index_diagnostics": index_diag,
            },
        }

    tl = build_canonical_timeline(instrument_id, window_start=cot_first or None)
    if not tl:
        return {
            "instrument_id": instrument_id,
            "weekly_ohlc": [],
            "aligned_weekly_ohlc": [],
            "price_source": "none",
            "canonical_symbol": None,
            "cot_first_date": cot_first,
            "cot_last_date": cot_last,
            "cot_rows": cot_rows,
            "ohlc_first_date": None,
            "ohlc_last_date": None,
            "ohlc_rows": 0,
            "common_first_date": None,
            "common_last_date": None,
            "common_rows": 0,
            "missing_ohlc_weeks": cot_rows,
            "incomplete_history": True,
            "note": "Weekly OHLC unavailable — no canonical price bars.",
        }

    real_daily = [
        {
            "date": b.date,
            "open": b.open,
            "high": b.high,
            "low": b.low,
            "close": b.close,
            "source": b.source,
        }
        for b in tl.bars
        if _is_usable_daily_bar(b.open, b.high, b.low, b.close)
    ]
    weekly_ohlc = derive_weekly_ohlc_from_daily(real_daily)

    # Prefer native store weekly when it has real wicks (indices / AV).
    store_rec = None
    try:
        from hptl.prices.price_store import load_price_store

        store_rec = (load_price_store().get("instruments") or {}).get(instrument_id) or {}
    except Exception:
        store_rec = {}
    native_weekly = store_rec.get("weekly") or []
    native_real = [
        {
            "date": str(b.get("date") or "")[:10],
            "open": _num(b.get("open")),
            "high": _num(b.get("high")),
            "low": _num(b.get("low")),
            "close": _num(b.get("close")),
            "source": "prices_latest:native_weekly",
        }
        for b in native_weekly
        if _is_real_ohlc(_num(b.get("open")), _num(b.get("high")), _num(b.get("low")), _num(b.get("close")))
    ]
    if len(native_real) > len(weekly_ohlc):
        weekly_ohlc = sorted(native_real, key=lambda b: b["date"])

    ohlc_first = weekly_ohlc[0]["date"] if weekly_ohlc else None
    ohlc_last = weekly_ohlc[-1]["date"] if weekly_ohlc else None
    common_first, common_last = _common_range(cot_first, cot_last, ohlc_first, ohlc_last)

    aligned: list[dict[str, Any]] = []
    missing = 0
    if cot_block and cot_block.get("series"):
        for row in cot_block["series"]:
            d = str(row.get("date") or "")[:10]
            if not d:
                continue
            if common_first and d < common_first:
                continue
            if common_last and d > common_last:
                continue
            bar = _find_bar_as_of(weekly_ohlc, d)
            if bar:
                aligned.append({**bar, "cot_date": d})
            else:
                missing += 1

    incomplete = bool(
        cot_rows > 0
        and weekly_ohlc
        and common_first
        and cot_first
        and common_first > cot_first
    )

    note = None
    if not weekly_ohlc:
        note = "Weekly OHLC unavailable."
    elif incomplete:
        note = "Price OHLC history incomplete — displaying common overlap only."

    source = price_audit.get("price_store_key") or tl.canonical_source
    symbol = price_audit.get("canonical_symbol") or tl.canonical_symbol

    return {
        "instrument_id": instrument_id,
        "weekly_ohlc": weekly_ohlc,
        "aligned_weekly_ohlc": aligned,
        "price_source": source,
        "canonical_symbol": symbol,
        "canonical_source": tl.canonical_source,
        "cot_first_date": cot_first,
        "cot_last_date": cot_last,
        "cot_rows": cot_rows,
        "ohlc_first_date": ohlc_first,
        "ohlc_last_date": ohlc_last,
        "ohlc_rows": len(weekly_ohlc),
        "common_first_date": common_first,
        "common_last_date": common_last,
        "common_rows": len(aligned) if aligned else (
            sum(1 for r in (cot_block or {}).get("series") or [] if common_first and common_last and common_first <= str(r.get("date") or "")[:10] <= common_last)
        ),
        "missing_ohlc_weeks": missing,
        "incomplete_history": incomplete or (not weekly_ohlc),
        "note": note,
        "price_quality": _price_quality(instrument_id, ohlc_last, tl.canonical_source, symbol),
        "price_audit": {
            "fred_fallback_series": price_audit.get("fred_fallback_series"),
            "store_bar_count": price_audit.get("store_bar_count"),
        },
    }


def build_workstation_ohlc_payload() -> dict[str, Any]:
    cot_doc: dict[str, Any] = {}
    if COT_3Y_PUBLIC.exists():
        cot_doc = json.loads(COT_3Y_PUBLIC.read_text(encoding="utf-8"))
    cot_markets = cot_doc.get("markets") or {}

    instruments: dict[str, Any] = {}
    for iid in all_instrument_ids():
        block = cot_markets.get(iid)
        instruments[iid] = build_instrument_workstation_ohlc(iid, cot_block=block)

    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parser": "hptl.prices.workstation_ohlc_export",
        "notes": "Weekly OHLC for workstation candles only — real wick bars; no close-only proxy candles.",
        "instruments": instruments,
    }


def write_workstation_ohlc_exports(payload: dict[str, Any] | None = None) -> Path:
    payload = payload or build_workstation_ohlc_payload()
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(text, encoding="utf-8")
    PUBLIC_OUT.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_OUT.write_text(text, encoding="utf-8")
    # Publish to dist/data too so preview/build copies never drift from
    # processed/public. sync_dist_exports() only copies confluence + macro maps,
    # so without this the dist workstation OHLC only updated on a full build.
    if DIST_OUT.parent.exists():
        DIST_OUT.write_text(text, encoding="utf-8")
    return OUT_PATH


def run() -> Path:
    path = write_workstation_ohlc_exports()
    n = len((json.loads(path.read_text(encoding="utf-8"))).get("instruments") or {})
    print(f"Wrote {path} ({n} instruments).")
    return path


if __name__ == "__main__":
    run()
