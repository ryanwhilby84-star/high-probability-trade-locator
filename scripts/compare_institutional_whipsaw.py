#!/usr/bin/env python3
"""Before/after: legacy single-score logic vs five-layer institutional narrative."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd

from hptl.confluence.build_decision_table import (
    TARGET_MARKETS,
    _compute_positioning_state,
    _load_cot_history,
    _load_macro_history,
    _zone_decision_layer_fields,
)
from hptl.context.institutional_context import precompute_institutional_context_index
from hptl.context.regime_store import RegimeStore

COMPARE_MARKETS = [
    "NASDAQ / NQ",
    "S&P 500 / ES",
    "Gold",
    "Silver",
    "Wheat",
    "Natural Gas / NG",
    "Crude Oil / CL",
]


def _legacy_action(positioning_state: str, setup_type: str, cot_score) -> str:
    st = positioning_state.lower()
    setup = setup_type.lower()
    score = f" CONF={cot_score}" if cot_score not in (None, "N/A") else ""
    if "stalk short" in setup or "supply reaction" in setup or "bearish strengthening" in st:
        return f"Stalk Short{score}"
    if "stalk long" in setup or "demand reaction" in setup or "bullish strengthening" in st:
        return f"Stalk Long{score}"
    if "avoid" in setup or "overextended" in setup:
        return f"Avoid{score}"
    return f"Watch{score}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--weeks", type=int, default=10)
    args = parser.parse_args()

    os.environ.setdefault("HPTL_SKIP_LIVE_FEEDS", "1")
    print("Loading COT + macro…")
    cot = _load_cot_history()
    macro = _load_macro_history()
    if cot.empty:
        print("ERROR: empty COT", file=sys.stderr)
        return 1

    cot["cot_report_date"] = pd.to_datetime(cot["cot_report_date"], errors="coerce")
    state_path = ROOT / "data" / "processed" / "institutional_regime_state_compare.json"
    if state_path.exists():
        state_path.unlink()
    index, _ = precompute_institutional_context_index(
        cot,
        markets=list(TARGET_MARKETS),
        macro=macro,
        store=RegimeStore(path=state_path),
        save_store=False,
    )

    print(f"\n=== Legacy (state + CONF) vs institutional narrative — last {args.weeks} weeks ===\n")

    for market in COMPARE_MARKETS:
        sub = cot.loc[cot["market"] == market].sort_values("cot_report_date")
        if sub.empty:
            print(f"## {market}\n(no rows)\n")
            continue
        weeks = sub["cot_report_date"].dropna().unique()[-args.weeks :]
        print(f"## {market}\n")
        for week in weeks:
            row = sub.loc[sub["cot_report_date"] == week].iloc[-1]
            week_str = pd.Timestamp(week).strftime("%Y-%m-%d")
            netf = float(row["net_value"]) if pd.notna(row.get("net_value")) else None
            w1f = float(row["weekly_change"]) if pd.notna(row.get("weekly_change")) else None
            w4f = float(row["four_week_change"]) if pd.notna(row.get("four_week_change")) else None
            lw = float(row["long_weekly_change"]) if pd.notna(row.get("long_weekly_change")) else None
            sw = float(row["short_weekly_change"]) if pd.notna(row.get("short_weekly_change")) else None
            legacy_state = _compute_positioning_state(netf, w1f, w4f, lw, sw)
            zone = _zone_decision_layer_fields(legacy_state, netf is not None)
            cot_score = row.get("cot_score", "N/A")
            legacy = _legacy_action(legacy_state, zone.get("setup_type", ""), cot_score)

            ctx = index.get((market, week_str), {})
            sd = ctx.get("scanner_display") or {}
            print(f"**{week_str}**")
            print(f"- BEFORE (compressed): {legacy_state} -> {legacy}")
            if sd:
                print("```")
                for line in sd.get("lines", []):
                    print(f"{line.get('layer', '?')}: {line.get('value', '—')}")
                print("```")
            else:
                print("- AFTER: (no institutional_context — rebuild export)")
            print()
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
