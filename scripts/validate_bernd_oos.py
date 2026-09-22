"""Strict out-of-sample validation for the frozen Bernd-style calendar scanner.

Purpose: separate persistent seasonal effects from in-sample calendar mining.
Bernd benchmark dates are deliberately NOT used by this test.

For each market and each holdout year:
1. use only the 15 years immediately BEFORE the holdout year;
2. scan the full calendar using the frozen window-resolution methodology;
3. retain only unusually strong training windows (>=12/15 wins, positive magnitude);
4. collapse near-duplicate windows;
5. test those frozen windows on the unseen holdout year;
6. report how many remain directionally correct out of sample.

No production workstation code is changed.
"""

from __future__ import annotations

from datetime import date, timedelta
from statistics import median

from hptl.seasonality_workstation.bernd_method_scanner import (
    CALENDAR_STEP_DAYS,
    MAX_WINDOW_DAYS,
    MIN_WINDOW_DAYS,
    evaluate_window,
    prepare_market,
)

INSTRUMENTS = [
    "Japanese Yen / 6J",
    "Canadian Dollar / 6C",
    "Swiss Franc / 6S",
    "Euro FX / 6E",
    "US Dollar Index / DX",
    "Australian Dollar / 6A",
    "Crude Oil / CL",
]

TRAIN_YEARS = 15
MIN_TRAIN_WINS = 12          # 80%+ in sample
MIN_TRAIN_AVG_PCT = 0.25     # reject tiny/noisy effects
TOP_WINDOWS = 20             # test a compact portfolio, not thousands of variants
DEDUP_DAYS = 7
HOLDOUT_YEARS = 8            # rolling unseen years, where history permits


def md_from_offset(offset: int) -> tuple[int, int]:
    d = date(2000, 1, 1) + timedelta(days=offset % 366)
    return d.month, d.day


def md_offset(md: tuple[int, int]) -> int:
    return (date(2000, *md) - date(2000, 1, 1)).days


def circular_distance(a: int, b: int) -> int:
    raw = abs(a - b)
    return min(raw, 366 - raw)


def discover_on_years(market, years: list[int]) -> tuple[list[dict], int]:
    candidates: list[dict] = []
    for start_offset in range(0, 366, CALENDAR_STEP_DAYS):
        start_md = md_from_offset(start_offset)
        for length in range(MIN_WINDOW_DAYS, MAX_WINDOW_DAYS + 1):
            end_md = md_from_offset(start_offset + length)
            result = evaluate_window(market, start_md, end_md, years)
            if result is None:
                continue
            if result["wins"] < MIN_TRAIN_WINS:
                continue
            if result["directional_average_pct"] < MIN_TRAIN_AVG_PCT:
                continue
            result["window_days"] = length
            candidates.append(result)

    raw_count = len(candidates)
    candidates.sort(
        key=lambda x: (
            -x["wins"],
            -x["directional_average_pct"],
            x["window_days"],
        )
    )

    selected: list[dict] = []
    for candidate in candidates:
        cs = md_offset(tuple(candidate["start_md"]))
        ce = md_offset(tuple(candidate["end_md"]))
        duplicate = False
        for existing in selected:
            if existing["direction"] != candidate["direction"]:
                continue
            es = md_offset(tuple(existing["start_md"]))
            ee = md_offset(tuple(existing["end_md"]))
            if circular_distance(cs, es) <= DEDUP_DAYS and circular_distance(ce, ee) <= DEDUP_DAYS:
                duplicate = True
                break
        if not duplicate:
            selected.append(candidate)
        if len(selected) >= TOP_WINDOWS:
            break
    return selected, raw_count


def test_one_year(market, candidate: dict, holdout_year: int) -> dict | None:
    result = evaluate_window(
        market,
        tuple(candidate["start_md"]),
        tuple(candidate["end_md"]),
        [holdout_year],
    )
    if result is None:
        return None
    ret = float(result["average_return_pct"])
    expected = candidate["direction"]
    directional_ret = ret if expected == "bullish" else -ret
    return {
        "win": directional_ret > 0,
        "directional_return_pct": directional_ret,
        "raw_return_pct": ret,
    }


print("=" * 100)
print("INSTITUTIONAL EDGE — STRICT ROLLING OUT-OF-SAMPLE SEASONALITY VALIDATION")
print("=" * 100)
print(
    f"Train={TRAIN_YEARS} prior years | gate={MIN_TRAIN_WINS}/{TRAIN_YEARS} "
    f"and avg-dir>={MIN_TRAIN_AVG_PCT:.2f}% | top deduped={TOP_WINDOWS} | holdouts={HOLDOUT_YEARS}"
)
print("Benchmark/Bernd dates are NOT supplied to discovery or scoring.\n")

market_summaries = []
all_tests = []

for instrument in INSTRUMENTS:
    print("-" * 100)
    print(instrument)
    market, error = prepare_market(instrument)
    if market is None:
        print("ERROR:", error)
        continue

    latest_completed_year = market.dates[-1].year - 1
    earliest_year = market.dates[0].year
    possible = [
        y for y in range(earliest_year + TRAIN_YEARS, latest_completed_year + 1)
        if y - TRAIN_YEARS >= earliest_year
    ]
    holdouts = possible[-HOLDOUT_YEARS:]
    if not holdouts:
        print("ERROR: insufficient history for rolling OOS")
        continue

    instrument_tests = []
    for holdout in holdouts:
        train_years = list(range(holdout - TRAIN_YEARS, holdout))
        selected, raw_count = discover_on_years(market, train_years)
        tested = []
        for candidate in selected:
            outcome = test_one_year(market, candidate, holdout)
            if outcome is not None:
                tested.append(outcome)
                instrument_tests.append(outcome)
                all_tests.append(outcome)

        wins = sum(x["win"] for x in tested)
        avg = sum(x["directional_return_pct"] for x in tested) / len(tested) if tested else 0.0
        print(
            f"  holdout {holdout}: train {train_years[0]}-{train_years[-1]} | "
            f"raw_pass={raw_count:>5} dedup={len(selected):>2} tested={len(tested):>2} | "
            f"OOS wins={wins:>2}/{len(tested):<2} ({(wins/len(tested)*100 if tested else 0):5.1f}%) "
            f"avg-dir={avg:+.2f}%"
        )

    n = len(instrument_tests)
    wins = sum(x["win"] for x in instrument_tests)
    avg = sum(x["directional_return_pct"] for x in instrument_tests) / n if n else 0.0
    med = median([x["directional_return_pct"] for x in instrument_tests]) if n else 0.0
    hit = wins / n if n else 0.0
    verdict = "PASS" if n and hit >= 0.60 and avg > 0 else ("WATCH" if n and hit >= 0.55 and avg > 0 else "FAIL")
    market_summaries.append((instrument, n, wins, hit, avg, med, verdict))
    print(f"  => {verdict}: {wins}/{n} ({hit*100:.1f}%) | avg-dir={avg:+.2f}% | median={med:+.2f}%")

print("\n" + "=" * 100)
print("STRICT OOS SUMMARY")
print("=" * 100)
for instrument, n, wins, hit, avg, med, verdict in market_summaries:
    print(
        f"{instrument:<27} {verdict:<5} OOS={wins:>3}/{n:<3} ({hit*100:5.1f}%) "
        f"avg-dir={avg:+.2f}% median={med:+.2f}%"
    )

n = len(all_tests)
wins = sum(x["win"] for x in all_tests)
avg = sum(x["directional_return_pct"] for x in all_tests) / n if n else 0.0
med = median([x["directional_return_pct"] for x in all_tests]) if n else 0.0
print("-" * 100)
print(
    f"PORTFOLIO OOS: {wins}/{n} ({(wins/n*100 if n else 0):.1f}%) | "
    f"avg-dir={avg:+.2f}% | median={med:+.2f}%"
)
print("\nInterpretation:")
print("  PASS  = >=60% unseen directional wins AND positive mean return.")
print("  WATCH = >=55% unseen directional wins AND positive mean return.")
print("  FAIL  = below those persistence thresholds.")
print("This is deliberately much harder than matching an in-sample reference window.")
print("No production workstation code was changed.")
