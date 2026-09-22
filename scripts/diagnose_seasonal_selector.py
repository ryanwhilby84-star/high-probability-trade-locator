"""Diagnose why known-good seasonal windows persist while top scanner picks fail.

Research-only. Does not change production workstation code.

For each of the seven frozen external windows, compare its statistical fingerprint
with nearby/all calendar candidates under the same 15-year semantics. The goal is
not to tune a new production score yet; it is to identify discriminators worth
validating out-of-sample next.
"""
from __future__ import annotations

from datetime import date, timedelta
from statistics import median, pstdev

from hptl.seasonality_workstation.bernd_method_scanner import (
    LOOKBACK_YEARS,
    evaluate_window,
    prepare_market,
)

BENCHMARKS = [
    ("Japanese Yen / 6J", (9, 1), (11, 12), "bearish"),
    ("Canadian Dollar / 6C", (9, 8), (11, 25), "bearish"),
    ("Swiss Franc / 6S", (8, 26), (11, 12), "bearish"),
    ("Euro FX / 6E", (8, 28), (10, 9), "bearish"),
    ("US Dollar Index / DX", (8, 31), (10, 6), "bullish"),
    ("Australian Dollar / 6A", (8, 12), (9, 29), "bearish"),
    ("Crude Oil / CL", (8, 17), (9, 2), "bullish"),
]


def directional_samples(result, direction):
    vals = [float(s["return_pct"]) for s in result["samples"]]
    return vals if direction == "bullish" else [-x for x in vals]


def fingerprint(market, start_md, end_md, direction, years):
    r = evaluate_window(market, start_md, end_md, years)
    if not r:
        return None
    vals = directional_samples(r, direction)
    wins = sum(x > 0 for x in vals)
    halves = [vals[: len(vals)//2], vals[len(vals)//2 :]]
    thirds = [vals[:5], vals[5:10], vals[10:15]] if len(vals) == 15 else []
    mean = sum(vals) / len(vals)
    med = median(vals)
    sd = pstdev(vals) if len(vals) > 1 else 0.0
    positive_halves = sum((sum(x)/len(x) > 0) for x in halves if x)
    positive_thirds = sum((sum(x)/len(x) > 0) for x in thirds if x)
    worst = min(vals)
    best = max(vals)
    # Robustness: remove the single best observation; good edges should survive it.
    trimmed = vals.copy()
    trimmed.remove(best)
    trimmed_mean = sum(trimmed) / len(trimmed) if trimmed else mean
    return {
        "wins": wins,
        "hit": wins / len(vals),
        "mean": mean,
        "median": med,
        "sd": sd,
        "worst": worst,
        "best": best,
        "trimmed_mean": trimmed_mean,
        "positive_halves": positive_halves,
        "positive_thirds": positive_thirds,
    }


def fmt(fp):
    return (
        f"wins={fp['wins']:2d}/15 hit={fp['hit']:.0%} mean={fp['mean']:+.2f}% "
        f"med={fp['median']:+.2f}% sd={fp['sd']:.2f} worst={fp['worst']:+.2f}% "
        f"trim={fp['trimmed_mean']:+.2f}% halves={fp['positive_halves']}/2 thirds={fp['positive_thirds']}/3"
    )

print("=" * 108)
print("INSTITUTIONAL EDGE — SEASONAL SELECTOR DISCRIMINATOR DIAGNOSTIC")
print("Known-good windows are labels for diagnosis only; no production score is being tuned here.")
print("=" * 108)

for instrument, start_md, end_md, direction in BENCHMARKS:
    market, error = prepare_market(instrument)
    print("\n" + "-" * 108)
    print(instrument)
    if not market:
        print("ERROR", error)
        continue
    anchor = market.dates[-1]
    years = list(range(anchor.year - LOOKBACK_YEARS, anchor.year))
    good = fingerprint(market, start_md, end_md, direction, years)
    if not good:
        print("benchmark unavailable")
        continue
    print(f"KNOWN GOOD {start_md[0]:02d}-{start_md[1]:02d}->{end_md[0]:02d}-{end_md[1]:02d} {direction.upper()}: {fmt(good)}")

    # Measure a ±7-calendar-day plateau around both boundaries. A real seasonal
    # regime should generally survive small date perturbations rather than exist
    # as a single lucky pixel on the calendar.
    neighbor = []
    base_s = date(2000, *start_md)
    base_e_year = 2001 if end_md <= start_md else 2000
    base_e = date(base_e_year, *end_md)
    for ds in (-7, -3, 0, 3, 7):
        for de in (-7, -3, 0, 3, 7):
            s = base_s + timedelta(days=ds)
            e = base_e + timedelta(days=de)
            smd, emd = (s.month, s.day), (e.month, e.day)
            fp = fingerprint(market, smd, emd, direction, years)
            if fp:
                neighbor.append(fp)
    if neighbor:
        plateau60 = sum(x["hit"] >= .60 and x["mean"] > 0 and x["median"] > 0 for x in neighbor) / len(neighbor)
        plateau67 = sum(x["hit"] >= 10/15 and x["trimmed_mean"] > 0 for x in neighbor) / len(neighbor)
        avg_hit = sum(x["hit"] for x in neighbor) / len(neighbor)
        avg_trim = sum(x["trimmed_mean"] for x in neighbor) / len(neighbor)
        print(f"LOCAL PLATEAU n={len(neighbor)} pass60={plateau60:.0%} robust67={plateau67:.0%} avg_hit={avg_hit:.0%} avg_trim={avg_trim:+.2f}%")

print("\n" + "=" * 108)
print("NEXT: compare these fingerprints/plateaus against the scanner's top-ranked false positives, then freeze a selector rule and rerun strict OOS.")
print("No production workstation code was changed.")
