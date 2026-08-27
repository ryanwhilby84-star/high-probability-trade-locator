"""Phase 1: Trace Gold market-clearing data path and latest-date identities."""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.gold_market_clearing_valuation import (  # noqa: E402
    CACHE_PATH,
    DELTA_LOG_BOUND,
    HISTORY_CSV,
    JSON_OUT,
    build_panel,
    load_gdt_sectors,
    solve_market_clearing,
    _stage_specs,
    _fit_sector,
    _enrich_row_features,
    MIN_TRAIN_Q,
)


def main() -> int:
    print("=== PATH ===")
    print("1. WGC cache:", CACHE_PATH)
    print("2. Engine: gold_market_clearing_valuation.run_gold_market_clearing_valuation")
    print("3. History:", HISTORY_CSV)
    print("4. Ranking JSON:", JSON_OUT)
    print("5. Dashboard export: gold_market_clearing_export → gold_valuation_latest.json")
    print("6. UI: GoldValuationPage.jsx ← /data/gold_valuation_latest.json")

    gdt = load_gdt_sectors()
    print("\n=== CACHE ===")
    print("n_quarters", gdt.get("n_quarters"), gdt.get("earliest"), "→", gdt.get("latest"))

    panel = build_panel()
    rows = panel["rows"]
    print("\n=== PANEL ===")
    print("ok", panel.get("ok"), "n_rows", len(rows), "meta_n", panel["meta"].get("n_quarters"))
    if not rows:
        print("ERROR panel empty", panel.get("error"))
        return 1

    # Reproduce latest OOS solve (stage 2 = published best)
    stage = 2
    specs = _stage_specs(stage)
    _enrich_row_features(rows)
    t = len(rows) - 1
    train = rows[:t]
    test = rows[t]
    d_fits = [_fit_sector(train, sp) for sp in specs["demand"]]
    s_fits = [_fit_sector(train, sp) for sp in specs["supply"]]
    sol = solve_market_clearing(
        demand_fits=d_fits,
        supply_fits=s_fits,
        demand_specs=specs["demand"],
        supply_specs=specs["supply"],
        row=test,
    )

    print("\n=== LATEST QUARTER SOLVE (stage 2, walk-forward) ===")
    print("obs_date", test["obs_date"], "usable", test["usable_date"])
    print("gold_price", sol["gold_price"])
    print("D0 total_demand", sol["D0"])
    print("S0 total_supply", sol["S0"])
    print("imbalance", sol["imbalance"], "check", round(sol["D0"] - sol["S0"], 6))
    print("demand_elas", sol["demand_elasticity"])
    print("supply_elas", sol["supply_elasticity"])
    print("net_elas", sol["net_elasticity"])
    print("raw_delta", sol.get("raw_delta_log_price"))
    print("bounded_delta", sol.get("delta_log_price"), "DELTA_LOG_BOUND", DELTA_LOG_BOUND)
    print("fair_value", sol["fair_value"])
    print("deviation_pct", sol["deviation_pct"])
    print("bucket", sol["bucket"])
    print("bound_hit", sol["bound_hit"], "solve_ok", sol["solve_ok"])
    print("demand_parts", sol["demand_parts"])
    print("supply_parts", sol["supply_parts"])
    print("sector elas:")
    for f in d_fits + s_fits:
        print(" ", f.get("id"), "price_elas", f.get("price_elasticity"), "exog", f.get("exogenous"))

    # Identities
    imb = sol["D0"] - sol["S0"]
    print("\n=== IDENTITY CHECKS ===")
    print("imbalance == D0-S0", abs(imb - sol["imbalance"]) < 1e-6)
    if sol.get("raw_delta_log_price") is not None and sol["net_elasticity"] not in (0, None):
        # may be None if invalid
        pass
    ui = json.loads((ROOT / "data/gold_valuation_latest.json").read_text(encoding="utf-8"))
    live_spot = 4045  # approximate user-reported live
    fv = sol["fair_value"]
    if fv and fv > 0:
        print("If live spot=4045 vs model FV:", round(100 * (live_spot - fv) / fv, 3))

    print("\n=== UI INSTRUMENT (published) ===")
    inst = ui.get("instrument") or {}
    for k in [
        "spot_price",
        "fair_value",
        "deviation_pct",
        "net_imbalance_tonnes",
        "implied_dlog_price",
        "valuation_bucket",
    ]:
        print(f"  {k}={inst.get(k)}")

    print("\nMIN_TRAIN_Q", MIN_TRAIN_Q, "→ max OOS", max(0, len(rows) - MIN_TRAIN_Q))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
