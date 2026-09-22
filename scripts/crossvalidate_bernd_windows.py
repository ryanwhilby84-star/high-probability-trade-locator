from __future__ import annotations

import math
import statistics
from bisect import bisect_left, bisect_right
from datetime import date, timedelta

from hptl.seasonality_workstation.returns import load_daily_closes


# Bernd benchmarks from the videos
BENCHMARKS = [
    ("Japanese Yen / 6J", (9, 1), (11, 12), "bearish", 15),
    ("Canadian Dollar / 6C", (9, 8), (11, 25), "bearish", 15),
    ("Swiss Franc / 6S", (8, 26), (11, 12), "bearish", 14),
    ("Euro FX / 6E", (8, 28), (10, 9), "bearish", 13),
    ("US Dollar Index / DX", (8, 31), (10, 6), "bullish", 12),
    ("Australian Dollar / 6A", (8, 12), (9, 29), "bearish", 12),

    # Older Bernd video:
    # Crude tends to rise from 17 Aug -> 2 Sep
    # 13 of the last 15 years = 87%
    ("Crude Oil / CL", (8, 17), (9, 2), "bullish", 13),
]


# Candidate methodology search space
START_SHIFTS = range(-10, 11)
END_SHIFTS = range(-10, 11)
ENTRY_OFFSETS = range(-3, 4)
EXIT_OFFSETS = range(-3, 4)
MODES = ("next", "previous")

YEARS = list(range(date.today().year - 15, date.today().year))


def prepare_market(instrument):
    daily, source, error = load_daily_closes(instrument)

    if error or not daily:
        return None, error or "no data"

    rows = []

    for d, close in daily:
        try:
            dt = date.fromisoformat(str(d)[:10])
            px = float(close)
        except Exception:
            continue

        if math.isfinite(px) and px > 0:
            rows.append((dt, px))

    rows = sorted(set(rows))

    if not rows:
        return None, "no usable rows"

    dates = [r[0] for r in rows]
    price = {d: p for d, p in rows}

    return {
        "dates": dates,
        "price": price,
        "source": source,
    }, None


def previous_available(dates, target):
    i = bisect_right(dates, target) - 1
    return dates[i] if i >= 0 else None


def next_available(dates, target):
    i = bisect_left(dates, target)
    return dates[i] if i < len(dates) else None


def nth_available_before(dates, target, n):
    i = bisect_right(dates, target) - 1 - n
    return dates[i] if i >= 0 else None


def nth_available_after(dates, target, n):
    i = bisect_left(dates, target) + n
    return dates[i] if i < len(dates) else None


def resolve(dates, target, mode, shift):
    shifted = target + timedelta(days=shift)

    if mode == "next":
        return next_available(dates, shifted)

    return previous_available(dates, shifted)


def evaluate(
    market,
    start_md,
    end_md,
    direction,
    start_shift,
    end_shift,
    start_mode,
    end_mode,
    entry_offset,
    exit_offset,
):
    dates = market["dates"]
    price = market["price"]

    samples = []

    for year in YEARS:
        target_start = date(year, *start_md)
        target_end = date(year, *end_md)

        s = resolve(dates, target_start, start_mode, start_shift)
        e = resolve(dates, target_end, end_mode, end_shift)

        if s is None or e is None:
            continue

        if entry_offset > 0:
            s = nth_available_after(dates, s, entry_offset)
        elif entry_offset < 0:
            s = nth_available_before(dates, s, -entry_offset)

        if exit_offset > 0:
            e = nth_available_after(dates, e, exit_offset)
        elif exit_offset < 0:
            e = nth_available_before(dates, e, -exit_offset)

        if s is None or e is None or e <= s:
            continue

        raw_ret = (price[e] / price[s] - 1.0) * 100.0

        if direction == "bullish":
            success = raw_ret > 0
        else:
            success = raw_ret < 0

        samples.append((year, s, e, raw_ret, success))

    if len(samples) != 15:
        return None

    wins = sum(x[4] for x in samples)

    return {
        "wins": wins,
        "mean": statistics.fmean(x[3] for x in samples),
        "samples": samples,
    }


print("Loading benchmark markets...")

markets = {}

for instrument, *_ in BENCHMARKS:
    market, error = prepare_market(instrument)

    if error:
        print(f"FAILED: {instrument}: {error}")
    else:
        markets[instrument] = market

if len(markets) != len(BENCHMARKS):
    raise SystemExit("One or more benchmark markets could not be loaded.")


print("Running methodology cross-validation...")
print()


results = []

for start_shift in START_SHIFTS:
    for end_shift in END_SHIFTS:
        for start_mode in MODES:
            for end_mode in MODES:
                for entry_offset in ENTRY_OFFSETS:
                    for exit_offset in EXIT_OFFSETS:

                        exact = 0
                        total_error = 0
                        market_results = []

                        for (
                            instrument,
                            start_md,
                            end_md,
                            direction,
                            target_wins,
                        ) in BENCHMARKS:

                            r = evaluate(
                                markets[instrument],
                                start_md,
                                end_md,
                                direction,
                                start_shift,
                                end_shift,
                                start_mode,
                                end_mode,
                                entry_offset,
                                exit_offset,
                            )

                            if r is None:
                                total_error += 99
                                market_results.append(
                                    (instrument, target_wins, None)
                                )
                                continue

                            actual = r["wins"]
                            error = abs(actual - target_wins)

                            total_error += error

                            if actual == target_wins:
                                exact += 1

                            market_results.append(
                                (instrument, target_wins, actual)
                            )

                        complexity = (
                            abs(start_shift)
                            + abs(end_shift)
                            + abs(entry_offset)
                            + abs(exit_offset)
                            + (0 if start_mode == "next" else 1)
                            + (0 if end_mode == "previous" else 1)
                        )

                        results.append(
                            (
                                -exact,
                                total_error,
                                complexity,
                                start_shift,
                                end_shift,
                                start_mode,
                                end_mode,
                                entry_offset,
                                exit_offset,
                                market_results,
                            )
                        )


results.sort()

TOTAL_BENCHMARKS = len(BENCHMARKS)

print("=" * 78)
print("TOP CROSS-VALIDATED METHODOLOGIES")
print("=" * 78)

for rank, row in enumerate(results[:10], 1):

    (
        neg_exact,
        total_error,
        complexity,
        ss,
        es,
        sm,
        em,
        eo,
        xo,
        market_results,
    ) = row

    exact = -neg_exact

    print()
    print(
        f"#{rank}  EXACT={exact}/{TOTAL_BENCHMARKS}  "
        f"ERROR={total_error}  COMPLEXITY={complexity}"
    )

    print(
        f"start_shift={ss:+d}d {sm} | "
        f"end_shift={es:+d}d {em} | "
        f"entry={eo:+d} sessions | "
        f"exit={xo:+d} sessions"
    )

    for instrument, target, actual in market_results:

        if actual is None:
            status = "NO DATA"
            actual_text = "-"
        else:
            status = "MATCH" if actual == target else "MISS"
            actual_text = str(actual)

        print(
            f"  {instrument:<27} "
            f"Bernd={target:>2}/15  "
            f"Ours={actual_text:>2}/15  "
            f"{status}"
        )


best = results[0]

print()
print("=" * 78)

if -best[0] == TOTAL_BENCHMARKS:
    print(f"RESULT: {TOTAL_BENCHMARKS}/{TOTAL_BENCHMARKS} EXACT MATCH FOUND")
else:
    print(
        f"RESULT: BEST METHODOLOGY MATCHES {-best[0]}/{TOTAL_BENCHMARKS} "
        f"BENCHMARKS WITH TOTAL ERROR {best[1]}"
    )

print("=" * 78)


# Print year-by-year detail for the best methodology
(
    neg_exact,
    total_error,
    complexity,
    best_ss,
    best_es,
    best_sm,
    best_em,
    best_eo,
    best_xo,
    _,
) = best

print()
print("BEST METHODOLOGY")
print("=" * 78)
print(
    f"start_shift={best_ss:+d}d {best_sm} | "
    f"end_shift={best_es:+d}d {best_em} | "
    f"entry={best_eo:+d} sessions | "
    f"exit={best_xo:+d} sessions"
)


# JPY diagnostic
print()
print("JPY YEAR-BY-YEAR")
print("=" * 78)

jpy_result = evaluate(
    markets["Japanese Yen / 6J"],
    (9, 1),
    (11, 12),
    "bearish",
    best_ss,
    best_es,
    best_sm,
    best_em,
    best_eo,
    best_xo,
)

if jpy_result:
    for year, s, e, ret, success in jpy_result["samples"]:
        status = "WIN" if success else "LOSS"
        print(
            f"{year}: {s} -> {e} | "
            f"{ret:+.3f}% | {status}"
        )


# Crude diagnostic
print()
print("CRUDE OIL YEAR-BY-YEAR")
print("=" * 78)

crude_result = evaluate(
    markets["Crude Oil / CL"],
    (8, 17),
    (9, 2),
    "bullish",
    best_ss,
    best_es,
    best_sm,
    best_em,
    best_eo,
    best_xo,
)

if crude_result:
    for year, s, e, ret, success in crude_result["samples"]:
        status = "WIN" if success else "LOSS"
        print(
            f"{year}: {s} -> {e} | "
            f"{ret:+.3f}% | {status}"
        )

    print()
    print(
        f"CRUDE RESULT: {crude_result['wins']}/15 | "
        f"Bernd target: 13/15"
    )
    print()
print("CRUDE OIL LOSS-YEAR DIAGNOSTIC")
print("=" * 70)

market = markets["Crude Oil / CL"]
dates = market["dates"]
price = market["price"]

for year in (2016, 2017, 2024):
    print()
    print(f"--- {year} ---")

    start_target = date(year, 8, 17)
    end_target = date(year, 9, 2)

    start_i = bisect_left(dates, start_target)
    end_i = bisect_right(dates, end_target)

    lo = max(0, start_i - 5)
    hi = min(len(dates), end_i + 5)

    for d in dates[lo:hi]:
        marker = ""

        if d == start_target:
            marker += "  <--- AUG 17"

        if d == end_target:
            marker += "  <--- SEP 2"

        print(f"{d} | {price[d]:.6f}{marker}")
        print()
print("CRUDE 2016 WINDOW SENSITIVITY")
print("=" * 70)

market = markets["Crude Oil / CL"]
dates = market["dates"]
price = market["price"]

for start_day in range(12, 22):
    for end_day in range(29, 8, -1):
        if end_day >= 29:
            end_date = date(2016, 8, end_day)
        else:
            end_date = date(2016, 9, end_day)

        start_date = date(2016, 8, start_day)

        s = next_available(dates, start_date)
        e = previous_available(dates, end_date)

        if s is None or e is None or e <= s:
            continue

        ret = (price[e] / price[s] - 1.0) * 100.0

        if ret > 0:
            print(
                f"WIN | target {start_date} -> {end_date} | "
                f"actual {s} -> {e} | {ret:+.3f}%"
            )