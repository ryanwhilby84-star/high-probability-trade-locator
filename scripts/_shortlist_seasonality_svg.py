#!/usr/bin/env python3
"""Render a tight shortlist SVG of reference-family constructions."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
spec = importlib.util.spec_from_file_location(
    "cmp", ROOT / "scripts" / "compare_seasonality_constructions.py"
)
cmp = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules["cmp"] = cmp
spec.loader.exec_module(cmp)

from hptl.seasonality_workstation.indexed_seasonality import (  # noqa: E402
    build_normalised_seasonal_curve,
)


def main() -> None:
    daily, _ = cmp.load_daily_closes_for_seasonality(cmp.ICE_DXY_ID)
    asof = daily[-1][0]
    asof_d = cmp._parse(asof)
    asof_year = asof_d.year
    asof_doy = cmp.calendar_doy(asof_d)
    by_year: dict = {}
    for d_s, c in daily:
        d = cmp._parse(d_s)
        if d.year >= asof_year:
            continue
        by_year.setdefault(d.year, []).append((d, c))

    pack = build_normalised_seasonal_curve(daily, lookback_years=10, smooth=14)
    rejected = [
        pack["curve"][str(d)]
        for d in range(1, 366)
        if pack["curve"].get(str(d)) is not None
    ]

    jobs = [
        ("1_REJECTED_over_smoothed_levels", rejected, 10, "rejected"),
        (
            "2_cal_retCum_mean_sma9_15Y",
            cmp.path_calendar_return_cumsum(by_year, asof_year, 15, agg="mean", smooth=9),
            15,
            "cal_ret",
        ),
        (
            "3_cal_retCum_mean_sma15_15Y",
            cmp.path_calendar_return_cumsum(by_year, asof_year, 15, agg="mean", smooth=15),
            15,
            "cal_ret",
        ),
        (
            "4_tdoy_retCum_mean_sma9_15Y",
            cmp.path_trading_day_return_cumsum(by_year, asof_year, 15, agg="mean", smooth=9),
            15,
            "tdoy_ret",
        ),
        (
            "5_tdoy_retCum_mean_sma15_15Y",
            cmp.path_trading_day_return_cumsum(by_year, asof_year, 15, agg="mean", smooth=15),
            15,
            "tdoy_ret",
        ),
        (
            "6_tdoy_retCum_mean_sma21_15Y",
            cmp.path_trading_day_return_cumsum(by_year, asof_year, 15, agg="mean", smooth=21),
            15,
            "tdoy_ret",
        ),
        (
            "7_tdoy_idx_mean_sma9_15Y",
            cmp.path_trading_day_indexed(by_year, asof_year, 15, agg="mean", smooth=9),
            15,
            "tdoy_idx",
        ),
        (
            "8_tdoy_idx_mean_sma15_15Y",
            cmp.path_trading_day_indexed(by_year, asof_year, 15, agg="mean", smooth=15),
            15,
            "tdoy_idx",
        ),
    ]

    panels = []
    for name, path, lb, fam in jobs:
        sc = cmp.score(name, path, lb, fam)
        panels.append((name, cmp.path_to_doy_curve(path), sc))
        print(
            ("PASS" if sc.passes_structure else "fail"),
            name,
            "zz",
            sc.zigzag_turns,
            "prom",
            sc.prominent_extrema,
            "range",
            sc.range_pts,
        )

    out = cmp.OUT_DIR / "dxy_shortlist_reference_family.svg"
    cmp.write_svg(panels, asof_doy, out)
    print("wrote", out)


if __name__ == "__main__":
    main()
