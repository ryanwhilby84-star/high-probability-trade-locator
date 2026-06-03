#!/usr/bin/env python3
"""Corn institution score breakdown — read-only debug (no rebuild)."""
from __future__ import annotations

import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd

from hptl.confluence.build_decision_table import _compute_positioning_state, _load_cot_history
from hptl.cot.scoring_engine import (
    _FLOW_SCALE,
    _NET_SCALE,
    _persistence_features,
    score_cot_row,
)

MARKET = "Corn"


def _load_corn_cot() -> pd.DataFrame:
    cot = _load_cot_history()
    if cot.empty:
        raise SystemExit("ERROR: empty COT history")
    corn = cot.loc[cot["market"] == MARKET].copy()
    if corn.empty:
        raise SystemExit(f"ERROR: no rows for {MARKET}")
    corn["cot_report_date"] = pd.to_datetime(corn["cot_report_date"], errors="coerce").dt.normalize()
    return corn.dropna(subset=["cot_report_date"]).sort_values("cot_report_date")


def main() -> int:
    corn = _load_corn_cot()
    latest = corn.iloc[-1]
    week = latest["cot_report_date"]
    hist = corn.loc[corn["cot_report_date"] < week]
    persist = _persistence_features(hist)

    net = float(latest["net_value"])
    w1 = float(latest["weekly_change"]) if pd.notna(latest.get("weekly_change")) else None
    w4 = float(latest["four_week_change"]) if pd.notna(latest.get("four_week_change")) else None
    lw = float(latest["long_weekly_change"]) if pd.notna(latest.get("long_weekly_change")) else None
    sw = float(latest["short_weekly_change"]) if pd.notna(latest.get("short_weekly_change")) else None

    sign = 1.0
    net_mag = math.tanh(net / _NET_SCALE)
    m1_raw = sign * math.tanh(w1 / _FLOW_SCALE) if w1 is not None else 0.0
    m4_raw = sign * math.tanh(w4 / (_FLOW_SCALE * 1.6)) if w4 is not None else 0.0
    m1 = max(0.0, min(1.0, m1_raw))
    m4 = max(0.0, min(1.0, m4_raw))
    aligned = persist["aligned_weeks"]
    opposed = persist["opposed_weeks"]
    persist_score = max(0.0, min(1.0, (aligned - 0.35 * opposed) / 4.0))
    accel_score = max(0.0, min(1.0, 0.5 + 0.25 * sign * persist["accel_ratio"]))
    part_score = max(0.0, min(1.0, 0.5 + 0.12 * persist["participation_expansion"]))
    conviction = max(
        0.0,
        min(
            1.0,
            0.22 * net_mag
            + 0.28 * m1
            + 0.18 * m4
            + 0.17 * persist_score
            + 0.08 * accel_score
            + 0.07 * part_score,
        ),
    )
    signal = round(1.0 + 9.0 * conviction, 1)

    res = score_cot_row(
        net=net,
        w1=w1,
        w4=w4,
        long_w1=lw,
        short_w1=sw,
        persist=persist,
        price_week_pct=None,
    )
    positioning_state = _compute_positioning_state(net, w1, w4, lw, sw)

    ic = latest.get("institutional_context")
    if isinstance(ic, str):
        import json

        try:
            ic = json.loads(ic)
        except json.JSONDecodeError:
            ic = {}
    ic = ic or {}

    last13 = corn.tail(13)
    avg13 = last13["net_value"].mean()

    print(f"=== Latest 13 {MARKET} rows ===")
    for _, row in last13.iterrows():
        d = pd.Timestamp(row["cot_report_date"]).strftime("%Y-%m-%d")
        print(
            f"{d} | net={int(row['net_value']):+,} | w1={int(row['weekly_change']):+,}"
            if pd.notna(row.get("weekly_change"))
            else f"{d} | net={int(row['net_value']):+,} | w1=N/A"
        )

    print(f"\n=== Latest week: {pd.Timestamp(week).strftime('%Y-%m-%d')} ===")
    print(f"current net position: {int(net):+,}")
    print(f"13 week average net position: {avg13:,.1f}")
    print(f"weekly net change: {int(w1):+,}" if w1 is not None else "weekly net change: N/A")
    print(f"institutional structural bias: {ic.get('structural_regime_label', 'N/A')}")
    print(f"institutional flow direction: {ic.get('flow_momentum_label', 'N/A')}")
    print(f"positioning_state: {positioning_state}")
    print(f"cot_bias: {res.cot_bias}")
    print(f"final institution label: BULLISH {signal}/10" if "bull" in res.cot_bias.lower() else f"final institution label: {res.cot_bias.upper()} {signal}/10")

    print("\n=== Calculation chain ===")
    print(f"net_mag = tanh({net}/{_NET_SCALE}) = {net_mag:.4f}  -> 0.22 * = {0.22 * net_mag:.4f}")
    print(f"weekly momentum m1 = clamp({m1_raw:.4f}) = {m1:.4f}  -> 0.28 * = {0.28 * m1:.4f}")
    print(f"4w momentum m4 = clamp({m4_raw:.4f}) = {m4:.4f}  -> 0.18 * = {0.18 * m4:.4f}")
    print(f"trend persist_score = ({aligned} - 0.35*{opposed})/4 = {persist_score:.4f}  -> 0.17 * = {0.17 * persist_score:.4f}")
    print(f"accel_score = {accel_score:.4f}  -> 0.08 * = {0.08 * accel_score:.4f}")
    print(f"participation part_score = {part_score:.4f}  -> 0.07 * = {0.07 * part_score:.4f}")
    print(f"conviction = {conviction:.4f}")
    print(f"Institutions = 1 + 9 * {conviction:.4f} = {1 + 9 * conviction:.4f} -> {signal}/10")

    if net > 0:
        reason = (
            f"net long (+{int(net):,}) => directional bias Bullish; "
            f"1w net change {int(w1):+,} => Bullish / Weakening; "
            f"leg flow (longs {int(lw):+,}, shorts {int(sw):+,}) => {positioning_state}"
        )
    else:
        reason = f"net short ({int(net):+,}) => Bearish directional bias"
    print(f"\nreason: {reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
