"""Forensic benchmark for the known Bernd DXY seasonal window.

This does NOT alter the production scanner. It compares Institutional Edge's
current canonical DXY history with the genuine ICE U.S. Dollar Index series
exposed by Yahoo as DX-Y.NYB.

Run from repository root:
    python scripts/audit_bernd_dxy_window.py

Known external benchmark from the mentor video:
    US Dollar Index / DX, 28 Sep -> 20 Oct, bullish, 15/15, recent 4/4.
"""

from __future__ import annotations

import math
import statistics
from bisect import bisect_left, bisect_right
from datetime import date, datetime, timezone

from hptl.seasonality_workstation.returns import load_daily_closes

INSTRUMENT = "US Dollar Index / DX"
ICE_YAHOO_SYMBOL = "DX-Y.NYB"
BENCHMARK_START = (9, 28)
BENCHMARK_END = (10, 20)
LOOKBACK = 15


def _rows(daily):
    out = []
    for d, close in daily:
        try:
            dt = date.fromisoformat(str(d)[:10])
            px = float(close)
        except Exception:
            continue
        if math.isfinite(px) and px > 0:
            out.append((dt, px))
    return sorted(out)


def _fetch_true_ice_dxy():
    """Fetch Yahoo's ICE U.S. Dollar Index daily closes for diagnosis only."""
    try:
        import requests
    except ImportError:
        return [], "requests_not_installed"
    period1 = int(datetime(2009, 1, 1, tzinfo=timezone.utc).timestamp())
    period2 = int(datetime.now(timezone.utc).timestamp())
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ICE_YAHOO_SYMBOL}"
    try:
        r = requests.get(url, params={"period1": period1, "period2": period2, "interval": "1d", "events": "history"}, timeout=25, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        result = ((r.json().get("chart") or {}).get("result") or [None])[0]
        if not result:
            return [], "empty_yahoo_result"
        timestamps = result.get("timestamp") or []
        quote = (((result.get("indicators") or {}).get("quote") or [{}])[0])
        closes = quote.get("close") or []
        out = []
        for ts, close in zip(timestamps, closes):
            if close is None:
                continue
            d = datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()
            try:
                px = float(close)
            except (TypeError, ValueError):
                continue
            if math.isfinite(px) and px > 0:
                out.append((d, px))
        return out, None
    except Exception as exc:
        return [], f"{type(exc).__name__}: {exc}"


def _window_return(rows, year, start_md, end_md):
    start = date(year, *start_md)
    end_year = year + (1 if end_md < start_md else 0)
    end = date(end_year, *end_md)
    left = bisect_left(rows, start, key=lambda row: row[0])
    right = bisect_right(rows, end, key=lambda row: row[0]) - 1
    if left >= len(rows) or right < left:
        return None
    start_dt, start_px = rows[left]
    end_dt, end_px = rows[right]
    return {"year": year, "target_start": start.isoformat(), "actual_start": start_dt.isoformat(), "target_end": end.isoformat(), "actual_end": end_dt.isoformat(), "return_pct": (end_px / start_px - 1.0) * 100.0}


def _evaluate(rows, years, start_md, end_md):
    samples = [r for year in years if (r := _window_return(rows, year, start_md, end_md)) is not None]
    returns = [s["return_pct"] for s in samples]
    wins = sum(r > 0 for r in returns)
    return {"start": start_md, "end": end_md, "n": len(returns), "wins": wins, "hit_rate": wins / len(returns) if returns else 0.0, "mean": statistics.fmean(returns) if returns else float("nan"), "median": statistics.median(returns) if returns else float("nan"), "recent4": sum(s["return_pct"] > 0 for s in samples[-4:]), "samples": samples}


def _md_label(md):
    return date(2000, *md).strftime("%d %b")


def _print_result(label, rows, years):
    exact = _evaluate(rows, years, BENCHMARK_START, BENCHMARK_END)
    print(f"\n=== {label} ===")
    print(f"history: {rows[0][0]} -> {rows[-1][0]} ({len(rows):,} rows)")
    print(f"28 Sep -> 20 Oct: {exact['wins']}/{exact['n']} ({exact['hit_rate']:.0%}) | mean {exact['mean']:+.3f}% | median {exact['median']:+.3f}% | recent {exact['recent4']}/4")
    for s in exact["samples"]:
        mark = "WIN " if s["return_pct"] > 0 else "LOSS"
        shifted = "" if (s["target_start"] == s["actual_start"] and s["target_end"] == s["actual_end"]) else " [aligned]"
        print(f"  {s['year']}: {s['return_pct']:+7.3f}% {mark} {s['actual_start']} -> {s['actual_end']}{shifted}")
    return exact


def main():
    years = list(range(date.today().year - LOOKBACK, date.today().year))
    print("\n=== DXY SOURCE FORENSIC AUDIT ===")
    print(f"sample years: {years[0]}-{years[-1]}")
    print("external benchmark: ICE DXY | 28 Sep -> 20 Oct | bullish | 15/15 | recent 4/4")

    canonical, source, error = load_daily_closes(INSTRUMENT)
    if error or not canonical:
        raise SystemExit(f"canonical DXY history unavailable: {error or 'no history'}")
    canonical_rows = _rows(canonical)
    print(f"canonical source label: {source}")
    current = _print_result("CURRENT INSTITUTIONAL EDGE SOURCE", canonical_rows, years)

    ice_daily, ice_error = _fetch_true_ice_dxy()
    if ice_error or not ice_daily:
        print(f"\nTRUE ICE DXY FETCH FAILED: {ice_error}")
        print("No production code was changed. Retry when internet access is available.")
        return
    ice_rows = _rows(ice_daily)
    ice = _print_result("TRUE ICE DXY (Yahoo DX-Y.NYB)", ice_rows, years)

    print("\n=== VERDICT ===")
    if ice["wins"] == 15 and ice["recent4"] == 4:
        print("SOURCE MISMATCH CONFIRMED: true ICE DXY reproduces the known 15/15 benchmark.")
        print("The seasonality maths survives this test; production DXY should be migrated to an ICE-compatible history before scanner tuning.")
    elif ice["wins"] > current["wins"]:
        print(f"SOURCE MATTERS: true ICE DXY improves the benchmark from {current['wins']}/{current['n']} to {ice['wins']}/{ice['n']}, but does not fully reproduce 15/15.")
        print("Keep the production scanner unchanged; the remaining difference is methodological/date-series construction and needs another forensic pass.")
    else:
        print(f"SOURCE ALONE DOES NOT EXPLAIN IT: true ICE DXY gives {ice['wins']}/{ice['n']} versus current {current['wins']}/{current['n']}.")
        print("Do not replace production data merely to chase the benchmark. Next investigate what Bernd's displayed dates/seasonal construction represent.")


if __name__ == "__main__":
    main()
