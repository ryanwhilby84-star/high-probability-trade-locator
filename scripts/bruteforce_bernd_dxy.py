from __future__ import annotations

import math
import statistics
from bisect import bisect_left, bisect_right
from datetime import date, timedelta

from hptl.seasonality_workstation.returns import load_daily_closes

INSTRUMENT = "US Dollar Index / DX"

# Known Bernd benchmark
TARGET_START = (9, 28)
TARGET_END = (10, 20)
TARGET_WINS = 15
TARGET_N = 15
TARGET_RECENT = 4

daily, source, error = load_daily_closes(INSTRUMENT)
if error or not daily:
    raise SystemExit(f"DXY unavailable: {error or 'no data'}")

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
dates = [r[0] for r in rows]
price = {d: p for d, p in rows}

YEARS = list(range(date.today().year - 15, date.today().year))


def previous_available(target):
    i = bisect_right(dates, target) - 1
    return dates[i] if i >= 0 else None


def next_available(target):
    i = bisect_left(dates, target)
    return dates[i] if i < len(dates) else None


def nth_available_before(target, n):
    i = bisect_right(dates, target) - 1 - n
    return dates[i] if i >= 0 else None


def nth_available_after(target, n):
    i = bisect_left(dates, target) + n
    return dates[i] if i < len(dates) else None


def resolve(target, mode, shift):
    shifted = target + timedelta(days=shift)

    if mode == "next":
        return next_available(shifted)
    if mode == "previous":
        return previous_available(shifted)

    raise ValueError(mode)


def evaluate(start_shift, end_shift, start_mode, end_mode, entry_offset, exit_offset):
    samples = []

    for year in YEARS:
        target_start = date(year, *TARGET_START)
        target_end = date(year, *TARGET_END)

        s = resolve(target_start, start_mode, start_shift)
        e = resolve(target_end, end_mode, end_shift)

        if s is None or e is None:
            continue

        if entry_offset > 0:
            s = nth_available_after(s, entry_offset)
        elif entry_offset < 0:
            s = nth_available_before(s, -entry_offset)

        if exit_offset > 0:
            e = nth_available_after(e, exit_offset)
        elif exit_offset < 0:
            e = nth_available_before(e, -exit_offset)

        if s is None or e is None or e <= s:
            continue

        ret = (price[e] / price[s] - 1.0) * 100.0
        samples.append((year, s, e, ret))

    if len(samples) != 15:
        return None

    wins = sum(ret > 0 for _, _, _, ret in samples)
    recent4 = sum(ret > 0 for _, _, _, ret in samples[-4:])
    returns = [x[3] for x in samples]

    return {
        "wins": wins,
        "recent4": recent4,
        "mean": statistics.fmean(returns),
        "median": statistics.median(returns),
        "samples": samples,
    }


print("=" * 80)
print("BERND DXY BRUTE-FORCE METHODOLOGY SEARCH")
print("=" * 80)
print(f"source: {source}")
print(f"history: {dates[0]} -> {dates[-1]}")
print(f"years: {YEARS[0]}-{YEARS[-1]}")
print("target: 28 Sep -> 20 Oct | BULLISH | 15/15 | recent 4/4")
print()

matches = []
tested = 0

# Sweep calendar shifts, date alignment and close-to-close offsets.
# This deliberately searches around Bernd's DISPLAYED dates rather than
# changing the displayed benchmark itself.
for start_shift in range(-10, 11):
    for end_shift in range(-10, 11):
        for start_mode in ("next", "previous"):
            for end_mode in ("next", "previous"):
                for entry_offset in range(-3, 4):
                    for exit_offset in range(-3, 4):

                        tested += 1

                        result = evaluate(
                            start_shift,
                            end_shift,
                            start_mode,
                            end_mode,
                            entry_offset,
                            exit_offset,
                        )

                        if not result:
                            continue

                        if (
                            result["wins"] == TARGET_WINS
                            and result["recent4"] == TARGET_RECENT
                        ):
                            complexity = (
                                abs(start_shift)
                                + abs(end_shift)
                                + abs(entry_offset)
                                + abs(exit_offset)
                                + (0 if start_mode == "next" else 1)
                                + (0 if end_mode == "previous" else 1)
                            )

                            matches.append(
                                (
                                    complexity,
                                    start_shift,
                                    end_shift,
                                    start_mode,
                                    end_mode,
                                    entry_offset,
                                    exit_offset,
                                    result,
                                )
                            )

print(f"definitions tested: {tested:,}")
print(f"15/15 + recent 4/4 matches: {len(matches):,}")
print()

matches.sort(key=lambda x: (x[0], -x[-1]["mean"]))

if not matches:
    print("NO EXACT MATCH.")
    print("That is useful evidence: simple date/calendar/close alignment")
    print("cannot reproduce Bernd's 15/15. We then move to seasonal-series")
    print("construction rather than blindly changing dates.")
    raise SystemExit(0)

print("TOP 20 LOWEST-COMPLEXITY MATCHES")
print("=" * 80)

for rank, m in enumerate(matches[:20], 1):
    (
        complexity,
        ss,
        es,
        sm,
        em,
        eo,
        xo,
        r,
    ) = m

    print(
        f"\n#{rank} complexity={complexity} | "
        f"start_shift={ss:+d}d {sm} | "
        f"end_shift={es:+d}d {em} | "
        f"entry_offset={eo:+d} sessions | "
        f"exit_offset={xo:+d} sessions | "
        f"15/15 | recent 4/4 | "
        f"mean={r['mean']:+.3f}% | median={r['median']:+.3f}%"
    )

    for year, s, e, ret in r["samples"]:
        print(f"   {year}: {s} -> {e}  {ret:+.3f}%")

print()
print("=" * 80)
print("IMPORTANT")
print("A 15/15 match is a candidate, NOT proof.")
print("Next step is cross-validation against Bernd's AUD and other known windows.")
print("=" * 80)
