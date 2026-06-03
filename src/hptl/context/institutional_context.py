"""Orchestrate L1–L5 and attach to COT / confluence rows."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd

from hptl.context.flow_momentum import compute_flow_layer
from hptl.context.macro_alignment import compute_macro_layer
from hptl.context.positioning_exhaustion import compute_exhaustion_layer
from hptl.context.regime_store import RegimeStore
from hptl.context.structural_regime import compute_structural_layer
from hptl.context.attention_engine import build_attention_layer
from hptl.context.scanner_narrative import build_scanner_display
from hptl.context.tactical_readiness import compute_tactical_layer
from hptl.validation import safe_float as _finite


def build_institutional_context_for_row(
    *,
    market: str,
    net: float | None,
    w1: float | None,
    w4: float | None,
    long_w1: float | None,
    short_w1: float | None,
    hist: pd.DataFrame,
    store: RegimeStore,
    cot_week: str,
    macro_signal: str | None = None,
    macro_score: float | None = None,
    full_loaded_ctx: dict[str, Any] | None = None,
    expanding_ctx: dict[str, Any] | None = None,
    legacy_positioning_state: str | None = None,
    legacy_cot_bias: str | None = None,
) -> dict[str, Any]:
    """Build full institutional_context bundle for one market-week."""
    prev = store.get(market)
    l1_raw = compute_structural_layer(
        net=net,
        w1=w1,
        w4=w4,
        hist=hist,
        prev_score_ema=prev.structural_score_ema,
        prev_regime=prev.structural_regime,
    )
    committed = store.commit_regime(
        market,
        proposed_regime=l1_raw["structural_regime"],
        cot_week=cot_week,
        score_ema=l1_raw["structural_score_ema"],
    )
    structural_regime = committed.structural_regime
    weeks_in_regime = committed.weeks_in_regime

    from hptl.context.structural_regime import REGIME_LABELS

    l1 = {
        **l1_raw,
        "structural_regime": structural_regime,
        "structural_regime_label": REGIME_LABELS.get(
            structural_regime, structural_regime.replace("_", " ").title()
        ),
        "weeks_in_regime": weeks_in_regime,
        "regime_since_cot_week": committed.regime_since_cot_week,
        "pending_flip_target": (committed.pending_flip.target if committed.pending_flip else None),
    }

    l2 = compute_flow_layer(
        net=net,
        w1=w1,
        w4=w4,
        long_w1=long_w1,
        short_w1=short_w1,
        hist=hist,
        structural_regime=structural_regime,
    )
    committed.last_flow_momentum = l2["flow_momentum"]

    l3 = compute_macro_layer(
        structural_regime=structural_regime,
        macro_signal=macro_signal,
        macro_score=macro_score,
    )
    l4 = compute_exhaustion_layer(
        structural_regime=structural_regime,
        full_loaded_ctx=full_loaded_ctx,
        expanding_ctx=expanding_ctx,
        w1=w1,
    )
    l5 = compute_tactical_layer(
        structural_regime=structural_regime,
        structural_label=l1["structural_regime_label"],
        weeks_in_regime=weeks_in_regime,
        flow=l2,
        macro=l3,
        exhaust=l4,
        structural=l1,
    )
    committed.last_tactical_posture = l5["tactical_posture"]

    scanner = build_scanner_display(
        structural_regime=structural_regime,
        structural_regime_label=l1["structural_regime_label"],
        flow_momentum_label=l2["flow_momentum_label"],
        flow_l1_l2_conflict=bool(l2.get("flow_l1_l2_conflict")),
        flow_conflict_narrative=l2.get("flow_conflict_narrative"),
        macro_alignment=l3["macro_alignment"],
        macro_signal=str(l3.get("macro_signal") or ""),
        positioning_extreme=l4["positioning_extreme"],
        positioning_extreme_label=l4["positioning_extreme_label"],
        net_percentile=l4.get("net_percentile"),
        tactical_posture=l5["tactical_posture"],
        tactical_posture_label=l5["tactical_posture_label"],
    )

    ctx_base = {
        "structural_regime": l1["structural_regime"],
        "structural_regime_label": l1["structural_regime_label"],
        "structural_score": l1["structural_score"],
        "structural_conviction": l1["structural_conviction"],
        "weeks_in_regime": weeks_in_regime,
        "flow_momentum": l2["flow_momentum"],
        "flow_momentum_label": l2["flow_momentum_label"],
        "flow_intensity": l2["flow_intensity"],
        "flow_l1_l2_conflict": l2["flow_l1_l2_conflict"],
        "flow_conflict_narrative": l2.get("flow_conflict_narrative"),
        "macro_alignment": l3["macro_alignment"],
        "macro_alignment_label": l3["macro_alignment_label"],
        "macro_alignment_score": l3["macro_alignment_score"],
        "positioning_extreme": l4["positioning_extreme"],
        "positioning_extreme_label": l4["positioning_extreme_label"],
        "net_percentile": l4.get("net_percentile"),
        "exhaustion_risk_score": l4["exhaustion_risk_score"],
        "tactical_posture": l5["tactical_posture"],
        "tactical_posture_label": l5["tactical_posture_label"],
        "setup_type": l5["setup_type"],
        "internal_scores": {
            "structural_score": l1["structural_score"],
            "macro_alignment_score": l3["macro_alignment_score"],
            "exhaustion_risk_score": l4["exhaustion_risk_score"],
            "tactical_confidence": l5["tactical_confidence"],
            "flow_intensity": l2["flow_intensity"],
        },
        "confidence_label": l5["confidence_label"],
        "zone_focus": l5["zone_focus"],
        "summary_lines": l5["summary_lines"],
        "scanner_display": scanner,
        "legacy_positioning_state": legacy_positioning_state,
        "legacy_cot_bias": legacy_cot_bias,
    }
    attention = build_attention_layer(
        market=market,
        ctx=ctx_base,
        hist=hist,
        net=net,
        w1=w1,
        long_w1=long_w1,
        short_w1=short_w1,
    )
    ctx_base["attention"] = attention
    if attention.get("tactical_readable"):
        ctx_base["scanner_display"] = {
            **scanner,
            "tactical": attention["tactical_readable"],
        }
    return ctx_base


def institutional_context_to_legacy_fields(ctx: dict[str, Any]) -> dict[str, str]:
    """Map L5 + L1 for backward-compatible columns (display uses new labels)."""
    return {
        "setup_type": str(ctx.get("setup_type") or "No clean institutional edge"),
        "confidence_label": str(ctx.get("confidence_label") or "Low"),
        "zone_focus": str(ctx.get("zone_focus") or "Wait"),
        "positioning_state_display": str(ctx.get("structural_regime_label") or "N/A"),
        "flow_state_display": str(ctx.get("flow_momentum_label") or "N/A"),
    }


def precompute_institutional_context_index(
    cot: pd.DataFrame,
    *,
    markets: list[str],
    macro: pd.DataFrame,
    store: RegimeStore | None = None,
    save_store: bool = True,
) -> tuple[dict[tuple[str, str], dict[str, Any]], RegimeStore]:
    """Causal per-market pass; key = (market, cot_report_date YYYY-MM-DD)."""
    regime_store = store or RegimeStore()
    index: dict[tuple[str, str], dict[str, Any]] = {}

    if cot.empty or "cot_report_date" not in cot.columns:
        return index, regime_store

    work = cot.copy()
    work["cot_report_date"] = pd.to_datetime(work["cot_report_date"], errors="coerce")
    macro_work = macro.copy() if not macro.empty else macro
    if not macro_work.empty and "macro_snapshot_date" in macro_work.columns:
        macro_work["macro_snapshot_date"] = pd.to_datetime(macro_work["macro_snapshot_date"], errors="coerce")

    for market in markets:
        sub = work.loc[work["market"] == market].sort_values("cot_report_date")
        if sub.empty:
            continue
        for _idx, row in sub.iterrows():
            week = row["cot_report_date"]
            if pd.isna(week):
                continue
            week_str = pd.Timestamp(week).strftime("%Y-%m-%d")
            hist = sub.loc[sub["cot_report_date"] < week]

            macro_signal = None
            macro_score = None
            if not macro_work.empty:
                avail = macro_work[macro_work["macro_snapshot_date"] <= week]
                if not avail.empty:
                    mrow = avail.iloc[-1]
                    macro_signal = str(mrow.get("macro_signal") or "") or None
                    try:
                        macro_score = float(mrow.get("macro_score"))
                    except (TypeError, ValueError):
                        macro_score = None

            fl_ctx: dict[str, Any] = {}
            ex_ctx: dict[str, Any] = {}
            pn = row.get("full_loaded_current_net_percentile")
            if pn is not None and not (isinstance(pn, float) and pd.isna(pn)):
                try:
                    fl_ctx["current_net_percentile"] = float(pn)
                except (TypeError, ValueError):
                    pass
            pen = row.get("expanding_current_net_percentile")
            if pen is not None and not (isinstance(pen, float) and pd.isna(pen)):
                try:
                    ex_ctx["current_net_percentile"] = float(pen)
                except (TypeError, ValueError):
                    pass

            ctx = build_institutional_context_for_row(
                market=str(market),
                net=_finite(row.get("net_value")),
                w1=_finite(row.get("weekly_change")),
                w4=_finite(row.get("four_week_change")),
                long_w1=_finite(row.get("long_weekly_change")),
                short_w1=_finite(row.get("short_weekly_change")),
                hist=hist,
                store=regime_store,
                cot_week=week_str,
                macro_signal=macro_signal,
                macro_score=macro_score,
                full_loaded_ctx=fl_ctx,
                expanding_ctx=ex_ctx,
            )
            index[(str(market), week_str)] = ctx

    if save_store:
        regime_store.save()
    return index, regime_store


def apply_institutional_context_to_cot(
    cot: pd.DataFrame,
    *,
    store: RegimeStore | None = None,
    save_store: bool = True,
) -> pd.DataFrame:
    """Process each market in causal order; add institutional_context JSON column."""
    if cot.empty:
        return cot

    out = cot.copy()
    if "cot_report_date" not in out.columns:
        return out

    out["cot_report_date"] = pd.to_datetime(out["cot_report_date"], errors="coerce")
    regime_store = store or RegimeStore()
    ctx_col: list[str | None] = [None] * len(out)

    for market, grp in out.groupby("market", sort=False):
        g = grp.sort_values("cot_report_date")
        for idx, row in g.iterrows():
            week = row["cot_report_date"]
            hist = g.loc[g["cot_report_date"] < week]
            week_str = pd.Timestamp(week).strftime("%Y-%m-%d") if pd.notna(week) else ""

            ctx = build_institutional_context_for_row(
                market=str(market),
                net=_finite(row.get("net_value")),
                w1=_finite(row.get("weekly_change")),
                w4=_finite(row.get("four_week_change")),
                long_w1=_finite(row.get("long_weekly_change")),
                short_w1=_finite(row.get("short_weekly_change")),
                hist=hist,
                store=regime_store,
                cot_week=week_str,
                macro_signal=None,
                macro_score=None,
            )
            ctx_col[out.index.get_loc(idx)] = json.dumps(ctx)

    out["institutional_context"] = ctx_col
    if save_store:
        regime_store.save()
    return out
