#!/usr/bin/env python3
"""Profile Stage-4 style FX valuation calls for RBA workbook parse counts.

Proves the regression fix: repeated market/week valuation must not re-parse RBA Excel.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def _simulate_inner_loop(*, enable_skip: bool, n_weeks: int = 4, n_markets: int = 8) -> dict:
    from hptl.fx import fx_macro_history as hist
    from hptl.valuation.engine import compute_valuation

    hist.clear_fx_macro_history_caches()
    if enable_skip:
        os.environ["HPTL_SKIP_VALUATION"] = "1"
    else:
        os.environ.pop("HPTL_SKIP_VALUATION", None)

    markets = [
        "Australian Dollar / 6A",
        "Euro FX / 6E",
        "British Pound / 6B",
        "Japanese Yen / 6J",
        "Swiss Franc / 6S",
        "Canadian Dollar / 6C",
        "NZ Dollar / 6N",
        "Gold",
    ][:n_markets]
    weeks = [f"2026-0{7}-{d:02d}" for d in (7, 14, 21, 28)][:n_weeks]

    t0 = time.perf_counter()
    for week in weeks:
        for market in markets:
            compute_valuation(market=market, as_of_week=week)
    elapsed = time.perf_counter() - t0
    return {
        "skip_valuation": enable_skip,
        "calls": len(weeks) * len(markets),
        "rba_parse_count": hist.rba_workbook_parse_count(),
        "elapsed_s": round(elapsed, 3),
        "sec_per_call": round(elapsed / max(1, len(weeks) * len(markets)), 4),
    }


def main() -> int:
    # Baseline with valuation ON (cache should keep parse count ≤ 3).
    with_val = _simulate_inner_loop(enable_skip=False)
    # Production COT path with skip gate restored.
    with_skip = _simulate_inner_loop(enable_skip=True)

    print("=== Stage-4 valuation inner-loop profile ===")
    print("valuation ON (histories cached):", with_val)
    print("HPTL_SKIP_VALUATION=1:", with_skip)
    print()
    print("Invariant checks:")
    print(f"  RBA parses with valuation ON: {with_val['rba_parse_count']} (must be <= 2)")
    print(f"  RBA parses with skip gate:    {with_skip['rba_parse_count']} (must be 0)")
    ok = with_val["rba_parse_count"] <= 2 and with_skip["rba_parse_count"] == 0
    # Extrapolate old vs new Stage-4 cost for 140 markets x 500 weeks.
    old_sec_per_week = 372.5  # measured before fix
    new_sec_per_call = with_skip["sec_per_call"]
    new_sec_per_week = new_sec_per_call * 140
    print()
    print("Runtime estimate (500 weeks x 140 markets):")
    print(
        f"  BEFORE (measured): ~{old_sec_per_week:.1f}s/week -> "
        f"~{old_sec_per_week * 500 / 3600:.1f}h Stage 4"
    )
    print(
        f"  AFTER  (skip+cache profile): ~{new_sec_per_week:.3f}s/week -> "
        f"~{new_sec_per_week * 500 / 3600:.2f}h Stage 4"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
