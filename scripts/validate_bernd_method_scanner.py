"""Test whether the frozen near-perfect methodology rediscovers Bernd windows.

The scanner is run without benchmark dates.  Only after discovery do we compare
its candidates with the external video references.
"""

from __future__ import annotations

from datetime import date

from hptl.seasonality_workstation.bernd_method_scanner import (
    LOOKBACK_YEARS,
    discover_windows,
    evaluate_window,
    prepare_market,
)

BENCHMARKS = [
    ("Japanese Yen / 6J", (9, 1), (11, 12), "bearish", 15),
    ("Canadian Dollar / 6C", (9, 8), (11, 25), "bearish", 15),
    ("Swiss Franc / 6S", (8, 26), (11, 12), "bearish", 14),
    ("Euro FX / 6E", (8, 28), (10, 9), "bearish", 13),
    ("US Dollar Index / DX", (8, 31), (10, 6), "bullish", 12),
    ("Australian Dollar / 6A", (8, 12), (9, 29), "bearish", 12),
    ("Crude Oil / CL", (8, 17), (9, 2), "bullish", 13),
]


def md_offset(md: tuple[int, int]) -> int:
    return (date(2000, *md) - date(2000, 1, 1)).days


def circular_distance(a: int, b: int) -> int:
    raw = abs(a - b)
    return min(raw, 366 - raw)


def md_text(md: tuple[int, int]) -> str:
    return f"{md[0]:02d}-{md[1]:02d}"


print("=" * 96)
print("INSTITUTIONAL EDGE — NEAR-PERFECT METHODOLOGY AUTOMATIC REDISCOVERY TEST")
print("=" * 96)
print("Scanner receives market prices only. Bernd dates are comparison-only.\n")

summary = []

for instrument, target_start, target_end, target_direction, target_wins in BENCHMARKS:
    print("-" * 96)
    print(instrument)

    scan = discover_windows(instrument, max_results=30)
    if scan.get("status") != "ok":
        print("SCAN ERROR:", scan.get("error"))
        summary.append((instrument, "ERROR", None, None, None))
        continue

    print(f"candidates passing >=60%: {scan['candidate_count']:,}")
    print("Top 10 independent windows:")
    for rank, w in enumerate(scan["windows"][:10], 1):
        print(
            f"  {rank:2d}. {md_text(tuple(w['start_md']))} -> {md_text(tuple(w['end_md']))} "
            f"{w['direction'].upper():7} {w['wins']}/{w['n']} "
            f"avg-dir={w['directional_average_pct']:+.2f}%"
        )

    target_s = md_offset(target_start)
    target_e = md_offset(target_end)
    same_direction = [w for w in scan["windows"] if w["direction"] == target_direction]
    nearest = None
    nearest_distance = None
    nearest_rank = None
    for rank, w in enumerate(scan["windows"], 1):
        if w["direction"] != target_direction:
            continue
        ds = circular_distance(md_offset(tuple(w["start_md"])), target_s)
        de = circular_distance(md_offset(tuple(w["end_md"])), target_e)
        distance = ds + de
        if nearest is None or distance < nearest_distance:
            nearest = w
            nearest_distance = distance
            nearest_rank = rank

    market, error = prepare_market(instrument)
    reference = None
    if market is not None:
        anchor = market.dates[-1]
        years = list(range(anchor.year - LOOKBACK_YEARS, anchor.year))
        reference = evaluate_window(market, target_start, target_end, years)

    if reference:
        print(
            f"Reference under frozen method: {md_text(target_start)} -> {md_text(target_end)} "
            f"{reference['direction'].upper()} {reference['wins']}/15 "
            f"(Bernd {target_direction.upper()} {target_wins}/15)"
        )

    if nearest is None:
        print("Nearest same-direction discovery: NONE")
        summary.append((instrument, "NONE", None, None, None))
        continue

    ds = circular_distance(md_offset(tuple(nearest["start_md"])), target_s)
    de = circular_distance(md_offset(tuple(nearest["end_md"])), target_e)
    exact_dates = ds == 0 and de == 0
    near_dates = ds <= 7 and de <= 7
    status = "EXACT" if exact_dates else ("NEAR" if near_dates else "MISS")
    print(
        f"Nearest discovery: rank #{nearest_rank} | "
        f"{md_text(tuple(nearest['start_md']))} -> {md_text(tuple(nearest['end_md']))} "
        f"{nearest['direction'].upper()} {nearest['wins']}/15 | "
        f"start_delta={ds}d end_delta={de}d | {status}"
    )
    summary.append((instrument, status, nearest_rank, ds, de))

print("\n" + "=" * 96)
print("REDISCOVERY SUMMARY")
print("=" * 96)
for instrument, status, rank, ds, de in summary:
    if rank is None:
        print(f"{instrument:<27} {status}")
    else:
        print(f"{instrument:<27} {status:<5} rank={rank:>2} start_delta={ds}d end_delta={de}d")

exact = sum(row[1] == "EXACT" for row in summary)
near = sum(row[1] in {"EXACT", "NEAR"} for row in summary)
print()
print(f"Exact date rediscoveries: {exact}/{len(BENCHMARKS)}")
print(f"Within +/-7d at both ends: {near}/{len(BENCHMARKS)}")
print("No production workstation code was changed.")
