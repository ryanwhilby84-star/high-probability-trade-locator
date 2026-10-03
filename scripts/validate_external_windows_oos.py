"""Validate the seven external Bernd seasonal windows out-of-sample.

This deliberately does NOT search, optimise, rank, or rediscover calendar dates.
The external dates/directions are frozen first, then evaluated year-by-year using
the already-frozen Institutional Edge date/session semantics.

Question answered:
    Do the externally specified seasonal windows themselves persist through time?

No production workstation code is changed by this script.
"""

from __future__ import annotations

from statistics import median

from hptl.seasonality_workstation.bernd_method_scanner import evaluate_window, prepare_market

BENCHMARKS = [
    ("Japanese Yen / 6J", (9, 1), (11, 12), "bearish", 15),
    ("Canadian Dollar / 6C", (9, 8), (11, 25), "bearish", 15),
    ("Swiss Franc / 6S", (8, 26), (11, 12), "bearish", 14),
    ("Euro FX / 6E", (8, 28), (10, 9), "bearish", 13),
    ("US Dollar Index / DX", (8, 31), (10, 6), "bullish", 12),
    ("Australian Dollar / 6A", (8, 12), (9, 29), "bearish", 12),
    ("Crude Oil / CL", (8, 17), (9, 2), "bullish", 13),
]


def md_text(md: tuple[int, int]) -> str:
    return f"{md[0]:02d}-{md[1]:02d}"


def directional_return(raw_return: float, direction: str) -> float:
    return raw_return if direction == "bullish" else -raw_return


print("=" * 100)
print("INSTITUTIONAL EDGE — EXTERNAL WINDOW OUT-OF-SAMPLE PERSISTENCE TEST")
print("=" * 100)
print("No calendar search. No ranking. No threshold tuning. External dates and directions are frozen.\n")

portfolio_directional: list[float] = []
summary = []

for instrument, start_md, end_md, direction, bernd_wins in BENCHMARKS:
    print("-" * 100)
    print(f"{instrument} | {md_text(start_md)} -> {md_text(end_md)} | frozen direction={direction.upper()}")

    market, error = prepare_market(instrument)
    if market is None:
        print("ERROR:", error)
        summary.append((instrument, "ERROR", 0, 0, 0.0, 0.0, 0.0))
        continue

    # Evaluate every complete year available under the frozen date/session semantics.
    first_year = market.dates[0].year
    last_year = market.dates[-1].year - 1
    rows = []
    for year in range(first_year, last_year + 1):
        result = evaluate_window(market, start_md, end_md, [year])
        if not result:
            continue
        sample = result["samples"][0]
        raw = float(sample["return_pct"])
        d_ret = directional_return(raw, direction)
        rows.append((year, sample["start"], sample["end"], raw, d_ret, d_ret > 0))

    if not rows:
        print("No complete annual observations.")
        summary.append((instrument, "NO DATA", 0, 0, 0.0, 0.0, 0.0))
        continue

    # Hard temporal split: oldest ~2/3 is historical context; newest ~1/3 is untouched holdout.
    split = max(1, int(len(rows) * 2 / 3))
    if split >= len(rows):
        split = len(rows) - 1
    train = rows[:split]
    holdout = rows[split:]

    print(f"available years: {rows[0][0]}-{rows[-1][0]} ({len(rows)})")
    print(f"temporal split: context {train[0][0]}-{train[-1][0]} | HOLDOUT {holdout[0][0]}-{holdout[-1][0]}")
    print(f"external reference wins: {bernd_wins}/15 (comparison only)")
    print("HOLDOUT observations:")
    for year, actual_start, actual_end, raw, d_ret, won in holdout:
        print(
            f"  {year}: {actual_start} -> {actual_end} | raw={raw:+7.3f}% | "
            f"dir={d_ret:+7.3f}% | {'WIN' if won else 'LOSS'}"
        )

    wins = sum(row[5] for row in holdout)
    n = len(holdout)
    hit = wins / n
    d_returns = [row[4] for row in holdout]
    avg = sum(d_returns) / n
    med = median(d_returns)
    portfolio_directional.extend(d_returns)

    if hit >= 0.60 and avg > 0 and med > 0:
        status = "PASS"
    elif hit >= 0.55 and avg > 0:
        status = "WATCH"
    else:
        status = "FAIL"

    print(f"=> {status}: {wins}/{n} ({hit:.1%}) | avg-dir={avg:+.2f}% | median={med:+.2f}%")
    summary.append((instrument, status, wins, n, hit, avg, med))

print("\n" + "=" * 100)
print("EXTERNAL WINDOW OOS SUMMARY")
print("=" * 100)
for instrument, status, wins, n, hit, avg, med in summary:
    if not n:
        print(f"{instrument:<27} {status}")
    else:
        print(
            f"{instrument:<27} {status:<5} OOS={wins:>2}/{n:<2} ({hit:5.1%}) "
            f"avg-dir={avg:+6.2f}% median={med:+6.2f}%"
        )

if portfolio_directional:
    p_wins = sum(x > 0 for x in portfolio_directional)
    p_n = len(portfolio_directional)
    p_avg = sum(portfolio_directional) / p_n
    p_med = median(portfolio_directional)
    print("-" * 100)
    print(
        f"PORTFOLIO HOLDOUT: {p_wins}/{p_n} ({p_wins/p_n:.1%}) | "
        f"avg-dir={p_avg:+.2f}% | median={p_med:+.2f}%"
    )

print("\nInterpretation:")
print("  PASS  = frozen external seasonal window persisted in the newest third of available history.")
print("  WATCH = some positive persistence, but below the stronger threshold.")
print("  FAIL  = external window itself did not persist strongly enough in holdout history.")
print("  This test contains NO automatic date selection, so it cannot win by choosing a prettier window.")
print("  No production workstation code was changed.")
