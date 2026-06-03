"""Evaluate static intermarket drivers vs spot COT + rates snapshot.

Context only — not predictive and not an entry system.
"""
from __future__ import annotations

from typing import Any, Literal

import pandas as pd

from hptl.intermarket.correlation_map import INTERMARKET_DRIVERS, Relationship
from hptl.macro.macro_scoring import _yield_direction

Verdict = Literal["support", "conflict", "neutral", "unknown"]


def build_cot_snapshot_from_week(by_market: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """``by_market``: market -> pandas Series row from COT pipeline."""
    out: dict[str, dict[str, Any]] = {}
    for m, row in by_market.items():
        if row is None:
            continue
        out[m] = {
            "cot_bias": str(row.get("cot_bias") or "N/A"),
            "positioning_state": str(row.get("positioning_state") or "N/A"),
        }
    return out


def _bias_bucket(cot_bias: str | None) -> str:
    if not cot_bias or not str(cot_bias).strip() or str(cot_bias).upper() == "N/A":
        return "neutral"
    s = str(cot_bias).lower()
    if "bull" in s:
        return "bull"
    if "bear" in s:
        return "bear"
    return "neutral"


def _aggregate_yield_stance(row: pd.Series | None) -> str:
    if row is None:
        return "unknown"
    dirs = [
        _yield_direction(row.get("dgs2_1w_change")),
        _yield_direction(row.get("dgs10_1w_change")),
        _yield_direction(row.get("dgs30_1w_change")),
    ]
    if all(d == "rising" for d in dirs):
        return "rising"
    if all(d == "falling" for d in dirs):
        return "falling"
    if sum(1 for d in dirs if d == "rising") >= 2:
        return "rising"
    if sum(1 for d in dirs if d == "falling") >= 2:
        return "falling"
    return "mixed"


def _dgs2_stance(row: pd.Series | None) -> str:
    if row is None:
        return "unknown"
    return _yield_direction(row.get("dgs2_1w_change"))


def evaluate_driver(
    target_market: str,
    target_bias_bucket: str,
    driver_key: str,
    relation: Relationship,
    label: str,
    cot_by_market: dict[str, dict[str, Any]],
    rates_row: pd.Series | None,
    macro_signal: str,
) -> tuple[Verdict, str]:
    """Return (verdict, one-line note for UI)."""

    if driver_key.startswith("proxy:"):
        kind = driver_key.split(":", 1)[1]
        if kind in ("dxy", "vix", "geopolitical"):
            return "unknown", f"{label}: no integrated series in V1 — treat as unscored."
        if kind == "real_yields":
            ys = _aggregate_yield_stance(rates_row)
            return _verdict_inverse_vs_yields(target_bias_bucket, ys, label)

    if driver_key == "macro:yields_1w":
        ys = _aggregate_yield_stance(rates_row)
        if relation == "inverse":
            return _verdict_inverse_vs_yields(target_bias_bucket, ys, label)
        if relation == "mixed_growth":
            return "neutral", f"{label}: crude vs growth/yield mix — leave unforced on V1 static rules."

    if driver_key == "macro:dgs2_1w":
        st = _dgs2_stance(rates_row)
        if st == "unknown" or st == "neutral":
            return "neutral", f"{label}: 1w 2Y change neutral/missing."
        mapped = "rising" if st == "rising" else "falling" if st == "falling" else "mixed"
        return _verdict_inverse_vs_yields(target_bias_bucket, mapped, label)

    if driver_key == "macro:risk_signal":
        ms = (macro_signal or "").lower()
        if target_market in {"Copper / HG", "NASDAQ / NQ", "S&P 500 / ES", "Dow / YM", "Crude Oil / CL"}:
            if ms == "risk_on" and target_bias_bucket == "bull":
                return "support", f"{label}: risk-on macro label vs bullish cohort read."
            if ms == "risk_off" and target_bias_bucket == "bull":
                return "conflict", f"{label}: risk-off macro label vs bullish cohort read."
            if ms == "risk_on" and target_bias_bucket == "bear":
                return "conflict", f"{label}: risk-on macro vs bearish cohort read."
            if ms == "risk_off" and target_bias_bucket == "bear":
                return "support", f"{label}: risk-off macro vs bearish cohort read."
        return "neutral", f"{label}: macro signal not mapped strongly for this asset."

    if driver_key.startswith("cot:"):
        other = driver_key.split(":", 1)[1]
        other_info = cot_by_market.get(other) or {}
        ob = other_info.get("cot_bias")
        other_bucket = _bias_bucket(ob if isinstance(ob, str) else "")
        if other_bucket == "neutral" or target_bias_bucket == "neutral":
            return "neutral", f"{label}: missing bias on target or driver leg."

        if relation in ("positive", "energy_complex_positive", "positive_demand"):
            if target_bias_bucket == other_bucket:
                return "support", f"{label}: aligned with {other} on side."
            return "conflict", f"{label}: divergent lean vs {other}."

        if relation == "mixed_growth":
            if target_bias_bucket == other_bucket:
                return "support", f"{label}: aligned with {other} on direction."
            return "neutral", f"{label}: classic mixed growth / haven split — inconclusive."

        if relation == "inflation_positive":
            if target_bias_bucket == "bull" and other_bucket == "bull":
                return "support", f"{label}: inflation / impulse narrative aligned."
            if target_bias_bucket == "bull" and other_bucket == "bear":
                return "conflict", f"{label}: impulse leg soft vs driver."
            return "neutral", f"{label}: non-bullish stance — weak linkage."

    return "unknown", f"{label}: unscored driver."


def _verdict_inverse_vs_yields(target_bucket: str, yield_stance: str, label: str) -> tuple[Verdict, str]:
    if yield_stance == "unknown":
        return "unknown", f"{label}: yields stance unknown."
    if yield_stance == "mixed":
        return "neutral", f"{label}: yields mixed week — no clean impulse."
    # inverse: high yields hurt gold/NQ bull; help bear
    if target_bucket == "bull":
        if yield_stance == "rising":
            return "conflict", f"{label}: rising yields pressure historically inverse cohorts."
        if yield_stance == "falling":
            return "support", f"{label}: easier yields historically supportive."
    elif target_bucket == "bear":
        if yield_stance == "rising":
            return "support", f"{label}: rising yields often align with bearish risk/length."
        if yield_stance == "falling":
            return "conflict", f"{label}: falling yields can relieve pressure on bearish thesis."
    return "neutral", f"{label}: neutral vs yields read."
