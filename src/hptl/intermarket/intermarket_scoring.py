"""Intermarket confirmation label + impulse score (0–10) — intelligence only."""
from __future__ import annotations

from typing import Any, Literal

import pandas as pd

from hptl.intermarket.correlation_map import INTERMARKET_DRIVERS
from hptl.intermarket.impulse_engine import build_cot_snapshot_from_week, evaluate_driver, _bias_bucket

Confirmation = Literal["CONFIRMING", "MIXED", "DIVERGING", "WARNING"]


def _score_to_confirmation(
    support: int,
    conflict: int,
    neutral: int,
    unknown: int,
    target_state: str,
) -> Confirmation:
    """Map counts to qualitative label."""
    if conflict >= 4 and support <= 1:
        return "DIVERGING"
    if conflict >= 3 and support == 0:
        return "WARNING"
    if support >= 4 and conflict <= 1:
        return "CONFIRMING"
    if support >= 3 and conflict == 0:
        return "CONFIRMING"
    if conflict >= 3 and support <= 2:
        if "Strengthening" in (target_state or "") and "Bear" in (target_state or ""):
            return "WARNING"
        return "DIVERGING"
    return "MIXED"


def _impulse_score(support: int, conflict: int, neutral: int, unknown: int) -> int:
    base = 5 + support - conflict
    if unknown >= 5:
        base -= 1
    return int(max(0, min(10, base)))


def build_intermarket_impulse_context(
    market: str,
    *,
    target_cot_bias: str,
    target_positioning_state: str,
    by_market_rows: dict[str, Any],
    rates_row: pd.Series | None,
    macro_signal: str,
) -> dict[str, Any]:
    """Build dashboard-ready intermarket panel for one instrument / week."""
    drivers_def = INTERMARKET_DRIVERS.get(market)
    cot_snap = build_cot_snapshot_from_week(by_market_rows)
    tb = _bias_bucket(target_cot_bias)

    if not drivers_def:
        return {
            "intermarket_confirmation": "MIXED",
            "impulse_score": 5,
            "impulse_summary": "No intermarket driver map for this market key yet.",
            "supporting_drivers": [],
            "conflicting_drivers": [],
            "driver_notes": [],
        }

    supporting: list[str] = []
    conflicting: list[str] = []
    notes: list[str] = []
    sc = sh = nu = un = 0

    for driver_key, relation, label in drivers_def:
        v, note = evaluate_driver(
            market,
            tb,
            driver_key,
            relation,
            label,
            cot_snap,
            rates_row,
            macro_signal,
        )
        notes.append(note)
        if v == "support":
            supporting.append(label)
            sc += 1
        elif v == "conflict":
            conflicting.append(label)
            sh += 1
        elif v == "neutral":
            nu += 1
        else:
            un += 1

    label_out = _score_to_confirmation(sc, sh, nu, un, target_positioning_state)
    impulse = _impulse_score(sc, sh, nu, un)

    if label_out == "WARNING" and impulse > 4:
        impulse = min(impulse, 4)

    summary = _compose_summary(
        market,
        tb,
        target_positioning_state,
        label_out,
        impulse,
        supporting,
        conflicting,
    )

    return {
        "intermarket_confirmation": label_out,
        "impulse_score": impulse,
        "impulse_summary": summary,
        "supporting_drivers": supporting[:12],
        "conflicting_drivers": conflicting[:12],
        "driver_detail_notes": notes[:20],
    }


def _compose_summary(
    market: str,
    target_bucket: str,
    pos_state: str,
    conf: Confirmation,
    impulse: int,
    supp: list[str],
    confd: list[str],
) -> str:
    """Short readable paragraph — not a trading call."""
    supp_txt = ", ".join(supp[:5]) if supp else "no strong supporting drivers scored"
    confd_txt = ", ".join(confd[:5]) if confd else "no strong conflicting drivers scored"
    head = (
        f"{market} COT read is {target_bucket}-leaning with positioning «{pos_state or 'N/A'}». "
        f"Intermarket impulse is {conf} (score {impulse}/10). "
    )
    mid = (
        f"Supporting drivers scored: {supp_txt}. Conflicting: {confd_txt}. "
    )
    tail = (
        "This is intermarket context only — it does not imply entries or forecasts. "
        "Use it to see whether related markets are aligned with the same week’s cohort story."
    )
    if conf == "CONFIRMING":
        tail = (
            "Related cohort and macro impulses mostly align with this week’s lean — backdrop is relatively clean on static rules. "
            + tail
        )
    elif conf in ("DIVERGING", "WARNING"):
        tail = (
            "Several drivers disagree — the tape may be more idiosyncratic or conflicted versus cohort norms. "
            + tail
        )
    else:
        tail = "Drivers disagree in places; treat as a mixed backdrop until other layers (calendar, narratives) add colour. " + tail
    return head + mid + tail
