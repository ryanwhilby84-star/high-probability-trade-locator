"""Forensic benchmark for the known Bernd DXY seasonal window.

This does NOT alter the production scanner. It asks one narrow question:
using Institutional Edge's current daily price history and return convention,
what does the exact 28 Sep -> 20 Oct DXY window do over the last 15 completed
calendar occurrences, and which nearby windows are strongest?

Run from repository root:
    python scripts/audit_bernd_dxy_window.py

Known external benchmark from the mentor video:
    US Dollar Index / DX, 28 Sep -> 20 Oct, bullish, 15/15, recent 4/4.
"""

from __future__ import annotations

import math
import statistics
from bisect import bisect_left, bisect_right
from datetime import date

from hptl.seasonality_workstation.returns import load_daily_closes

INSTRUMENT = "US Dollar Index / DX"
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
    return {
        "year": year,
        "target_start": start.isoformat(),
        "actual_start": start_dt.isoformat(),
        "target_end": end.isoformat(),
        "actual_end": end_dt.isoformat(),
        "return_pct": (end_px / start_px - 1.0) * 100.0,
    }


def _evaluate(rows, years, start_md, end_md):
    samples = []
    for year in years:
        result = _window_return(rows, year, start_md, end_md)
        if result is not None:
            samples.append(result)
    returns = [s["return_pct"] for s in samples]
    wins = sum(r > 0 for r in returns)
    return {
        "start": start_md,
        "end": end_md,
        "n": len(returns),
        "wins": wins,
        "hit_rate": wins / len(returns) if returns else 0.0,
        "mean": statistics.fmean(returns) if returns else float("nan"),
        "median": statistics.median(returns) if returns else float("nan"),
        "recent4": sum(s["return_pct"] > 0 for s in samples[-4:]),
        "samples": samples,
    }


def _md_label(md):
    return date(2000, *md).strftime("%d %b")


def main():
    daily, source, error = load_daily_closes(INSTRUMENT)
    if error or not daily:
        raise SystemExit(f"DXY history unavailable: {error or 'no daily history'}")
    rows = _rows(daily)
    # Match the production engine: current year is excluded from the 15Y sample.
    asof = date.today()
    years = list(range(asof.year - LOOKBACK, asof.year))

    exact = _evaluate(rows, years, BENCHMARK_START, BENCHMARK_END)
    print("\n=== DXY KNOWN-ANSWER FORENSIC AUDIT ===")
    print(f"source: {source}")
    print(f"price history: {rows[0][0]} -> {rows[-1][0]} ({len(rows):,} rows)")
    print(f"sample years: {years[0]}-{years[-1]}")
    print("external benchmark: 28 Sep -> 20 Oct | bullish | 15/15 | recent 4/4")
    print(
        f"Institutional Edge: {_md_label(BENCHMARK_START)} -> {_md_label(BENCHMARK_END)} | "
        f"{exact['wins']}/{exact['n']} ({exact['hit_rate']:.0%}) | "
        f"mean {exact['mean']:+.3f}% | median {exact['median']:+.3f}% | "
        f"recent {exact['recent4']}/4"
    )

    print("\nYear-by-year exact benchmark:")
    for s in exact["samples"]:
        mark = "WIN " if s["return_pct"] > 0 else "LOSS"
        shifted = "" if (s["target_start"] == s["actual_start"] and s["target_end"] == s["actual_end"]) else "  [trading-day aligned]"
        print(
            f"  {s['year']}: {s['return_pct']:+7.3f}%  {mark}  "
            f"{s['actual_start']} -> {s['actual_end']}{shifted}"
        )

    print("\nNearby-window grid (20 Sep-05 Oct starts; 12 Oct-01 Nov ends), ranked by hit rate then mean:")
    candidates = []
    for start_day in range(20, 31):
        start_md = (9, start_day)
        for end_day in range(12, 32):
            candidates.append(_evaluate(rows, years, start_md, (10, end_day)))
        candidates.append(_evaluate(rows, years, start_md, (11, 1)))
    for start_day in range(1, 6):
        start_md = (10, start_day)
        for end_day in range(12, 32):
            candidates.append(_evaluate(rows, years, start_md, (10, end_day)))
        candidates.append(_evaluate(rows, years, start_md, (11, 1)))

    candidates = [c for c in candidates if c["n"] == LOOKBACK]
    candidates.sort(key=lambda c: (c["hit_rate"], c["recent4"], c["mean"]), reverse=True)
    for c in candidates[:25]:
        star = "  <== BERND" if c["start"] == BENCHMARK_START and c["end"] == BENCHMARK_END else ""
        print(
            f"  {_md_label(c['start'])} -> {_md_label(c['end'])}: "
            f"{c['wins']}/{c['n']} {c['hit_rate']:.0%} | mean {c['mean']:+.3f}% | "
            f"median {c['median']:+.3f}% | recent {c['recent4']}/4{star}"
        )

    print("\nDiagnosis:")
    if exact["wins"] == 15 and exact["recent4"] == 4:
        print("  MATCH: underlying DXY history/return calculation reproduces the known directional benchmark.")
        print("  Next target is scanner discovery/ranking: make the production scanner surface this window automatically.")
    else:
        losses = [str(s["year"]) for s in exact["samples"] if s["return_pct"] <= 0]
        print("  MISMATCH: the exact known window does not reproduce 15/15 yet.")
        print(f"  Disagreeing years: {', '.join(losses) if losses else 'sample count/data coverage'}")
        print("  Do NOT tune scanner ranking yet; inspect these exact years for price-series/date-alignment differences first.")


if __name__ == "__main__":
    main()
