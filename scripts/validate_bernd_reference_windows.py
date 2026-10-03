from __future__ import annotations

"""Validate Bernd-style seasonal claims without fitting dates to the answer.

This is deliberately separate from the production scanner.  The reference
videos give us useful external benchmarks, but they do not fully specify the
price series / contract construction used in the video.  This script therefore
checks the literal calendar claims against our canonical daily-close loader and
reports discrepancies instead of shifting dates until the win counts match.
"""

from bisect import bisect_left, bisect_right
from datetime import date

from hptl.seasonality_workstation.returns import load_daily_closes


REFERENCES = [
    ("Japanese Yen / 6J", (9, 1), (11, 12), "bearish", 15),
    ("Canadian Dollar / 6C", (9, 8), (11, 25), "bearish", 15),
    ("Swiss Franc / 6S", (8, 26), (11, 12), "bearish", 14),
    ("Euro FX / 6E", (8, 28), (10, 9), "bearish", 13),
    ("US Dollar Index / DX", (8, 31), (10, 6), "bullish", 12),
    ("Australian Dollar / 6A", (8, 12), (9, 29), "bearish", 12),
    # Transcript: "17th of August to the 2nd of September ... 13 of the last 15 years".
    ("Crude Oil / CL", (8, 17), (9, 2), "bullish", 13),
]

YEARS = list(range(date.today().year - 15, date.today().year))


def _prepare(instrument: str):
    daily, source, error = load_daily_closes(instrument)
    if error or not daily:
        raise RuntimeError(f"{instrument}: {error or 'no daily history'}")

    rows = []
    for raw_date, raw_close in daily:
        try:
            d = date.fromisoformat(str(raw_date)[:10])
            px = float(raw_close)
        except (TypeError, ValueError):
            continue
        if px > 0:
            rows.append((d, px))

    # Keep the last value for duplicate dates, then restore chronological order.
    price = dict(rows)
    dates = sorted(price)
    return dates, price, source


def _next_session(dates, target):
    i = bisect_left(dates, target)
    return dates[i] if i < len(dates) else None


def _previous_session(dates, target):
    i = bisect_right(dates, target) - 1
    return dates[i] if i >= 0 else None


def validate_reference(instrument, start_md, end_md, direction, target_wins):
    dates, price, source = _prepare(instrument)
    outcomes = []

    for year in YEARS:
        start = _next_session(dates, date(year, *start_md))
        end = _previous_session(dates, date(year, *end_md))
        if start is None or end is None or end <= start:
            continue

        ret = (price[end] / price[start] - 1.0) * 100.0
        win = ret > 0 if direction == "bullish" else ret < 0
        outcomes.append((year, start, end, ret, win))

    wins = sum(row[4] for row in outcomes)
    return {
        "instrument": instrument,
        "source": source,
        "direction": direction,
        "target": target_wins,
        "wins": wins,
        "n": len(outcomes),
        "delta": wins - target_wins,
        "outcomes": outcomes,
    }


def main():
    print("BERND REFERENCE-WINDOW VALIDATION")
    print("Literal calendar dates; no fitted shifts or session offsets.")
    print("=" * 78)

    exact = 0
    results = []
    for reference in REFERENCES:
        result = validate_reference(*reference)
        results.append(result)
        match = result["n"] == 15 and result["wins"] == result["target"]
        exact += int(match)
        status = "MATCH" if match else "DIFF"
        print(
            f"{result['instrument']:<27} "
            f"reference={result['target']:>2}/15  "
            f"ours={result['wins']:>2}/{result['n']:<2}  "
            f"delta={result['delta']:+d}  {status}"
        )

    print("=" * 78)
    print(f"Exact literal-window matches: {exact}/{len(REFERENCES)}")
    print()
    print("IMPORTANT: a difference is evidence of a data/contract-definition mismatch;")
    print("it is not permission to move the dates until the benchmark matches.")

    for result in results:
        if result["wins"] == result["target"] and result["n"] == 15:
            continue
        print()
        print(f"{result['instrument']} | source={result['source']}")
        for year, start, end, ret, win in result["outcomes"]:
            print(
                f"  {year}: {start} -> {end} | {ret:+.3f}% | "
                f"{'WIN' if win else 'LOSS'}"
            )


if __name__ == "__main__":
    main()
