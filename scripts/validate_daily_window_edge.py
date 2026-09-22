"""Validate the parallel daily-window edge against external reference windows.

This script never feeds benchmark dates into discovery. It first discovers each
market's strongest windows independently, then separately evaluates the known
reference windows for auditability.
"""

from __future__ import annotations

from datetime import date

from hptl.seasonality_workstation.daily_window_edge import (
    discover_daily_windows,
    evaluate_daily_window,
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


def md_text(md: tuple[int, int]) -> str:
    return f"{md[0]:02d}-{md[1]:02d}"


print("=" * 88)
print("INSTITUTIONAL EDGE — PARALLEL DAILY WINDOW VALIDATION")
print("=" * 88)
print("Discovery is independent. Benchmarks are comparison-only.\n")

for instrument, start_md, end_md, expected_direction, expected_wins in BENCHMARKS:
    print("-" * 88)
    print(instrument)

    discovered = discover_daily_windows(instrument, max_results=10)
    if discovered.get("status") != "ok":
        print("DISCOVERY ERROR:", discovered.get("error"))
        continue

    print("Top independent windows:")
    for i, w in enumerate(discovered["windows"][:5], 1):
        print(
            f"  {i}. {md_text(tuple(w['start_md']))} -> {md_text(tuple(w['end_md']))} "
            f"{w['direction'].upper():7} {w['wins']}/{w['n']} "
            f"({w['hit_rate'] * 100:.0f}%) avg-dir={w['directional_average_pct']:+.2f}%"
        )

    reference = evaluate_daily_window(instrument, start_md, end_md)
    if reference.get("status") != "ok":
        print("REFERENCE ERROR:", reference)
        continue

    direction_ok = reference["direction"] == expected_direction
    wins_delta = int(reference["wins"]) - expected_wins
    print(
        "Reference check: "
        f"{md_text(start_md)} -> {md_text(end_md)} | "
        f"Bernd={expected_direction} {expected_wins}/15 | "
        f"IE={reference['direction']} {reference['wins']}/15 | "
        f"direction={'MATCH' if direction_ok else 'MISS'} | wins_delta={wins_delta:+d}"
    )

print("\n" + "=" * 88)
print("DONE — no production workstation code was changed.")
print("=" * 88)
