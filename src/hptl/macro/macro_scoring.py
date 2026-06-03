from __future__ import annotations

from typing import Any

import pandas as pd

from hptl.validation import safe_float, safe_gt, safe_gte, safe_int, safe_lt, safe_lte

REQUIRED_SCORING_INPUTS = [
    "dgs2",
    "dgs10",
    "dgs30",
    "dgs2_1w_change",
    "dgs10_1w_change",
    "dgs30_1w_change",
    "dgs2_4w_change",
    "dgs10_4w_change",
    "dgs30_4w_change",
    "yield_curve_10y2y_1w_change",
]

CORE_REQUIRED = ["dgs2", "dgs10", "dgs30"]

YIELD_THRESHOLD = 0.10  # 10 bps; FRED yields are percentage points
CURVE_THRESHOLD = 0.05  # 5 bps
SCORE_MAP = {0: 0, 1: 2, 2: 4, 3: 6, 4: 10}


def _yield_direction(value) -> str:
    v = safe_float(value)
    if v is None:
        return "neutral"
    if safe_gt(v, YIELD_THRESHOLD):
        return "rising"
    if safe_lt(v, -YIELD_THRESHOLD):
        return "falling"
    return "neutral"


def _curve_direction(value) -> str:
    v = safe_float(value)
    if v is None:
        return "Neutral"
    if safe_gt(v, CURVE_THRESHOLD):
        return "Steepening"
    if safe_lt(v, -CURVE_THRESHOLD):
        return "Flattening"
    return "Neutral"


def _strength(score) -> str | None:
    s = safe_int(score)
    if s is None:
        return None
    if safe_lte(s, 2):
        return "Weak"
    if s == 4:
        return "Moderate"
    if s == 6:
        return "Strong"
    return "Very Strong"


def _context_for_trades(signal: str, score) -> str:
    s = safe_int(score)
    if s is None:
        return "Neutral/Unclear"
    if signal == "risk_on" and safe_gte(s, 8):
        return "Strongly Supportive"
    if signal == "risk_on" and s in (4, 6):
        return "Supportive"
    if signal == "risk_off" and s in (4, 6):
        return "Hostile"
    if signal == "risk_off" and safe_gte(s, 8):
        return "Strongly Hostile"
    return "Neutral/Unclear"


def _technical_filter(context: str) -> str:
    if context == "Strongly Supportive":
        return "Support high-quality long setups; allow normal confidence if technicals confirm."
    if context == "Supportive":
        return "Supports long setups, but still require technical confirmation."
    if context == "Hostile":
        return "Be selective with long setups; filter marginal trades."
    if context == "Strongly Hostile":
        return "Avoid marginal long setups; reduce confidence/size unless technicals are exceptional."
    return "Macro unclear; do not adjust trade confidence materially."


def _empty_context() -> dict:
    return {
        "rates_bias": "Neutral",
        "curve_context": "Neutral",
        "policy_pressure": "Neutral",
        "macro_signal": "insufficient_data",
        "macro_score": pd.NA,
        "macro_strength": pd.NA,
        "technical_trade_filter": "Do not use macro layer; required yield data is incomplete.",
        "macro_context_for_trades": "Neutral/Unclear",
        "macro_summary": "Missing required yield data",
    }


def _row_has_core_yields(row: pd.Series) -> bool:
    return all(pd.notna(row.get(col)) for col in CORE_REQUIRED)


def _row_has_required_scoring_inputs(row: pd.Series) -> bool:
    return all(pd.notna(row.get(col)) for col in REQUIRED_SCORING_INPUTS)


def _rates_alignment_breakdown(row: pd.Series) -> dict:
    """Intermediate state shared by scoring and audit (single source of truth)."""
    dgs10_1w_dir = _yield_direction(row.get("dgs10_1w_change"))
    dgs2_1w_dir = _yield_direction(row.get("dgs2_1w_change"))
    dgs30_1w_dir = _yield_direction(row.get("dgs30_1w_change"))
    dgs10_4w_dir = _yield_direction(row.get("dgs10_4w_change"))
    dgs2_4w_dir = _yield_direction(row.get("dgs2_4w_change"))
    dgs30_4w_dir = _yield_direction(row.get("dgs30_4w_change"))
    curve_context = _curve_direction(row.get("yield_curve_10y2y_1w_change"))

    fed_1w_dir = _yield_direction(row.get("fed_funds_1w_change"))
    fed_4w_dir = _yield_direction(row.get("fed_funds_4w_change"))

    one_week_easing = all(d == "falling" for d in [dgs2_1w_dir, dgs10_1w_dir, dgs30_1w_dir])
    one_week_restrictive = all(d == "rising" for d in [dgs2_1w_dir, dgs10_1w_dir, dgs30_1w_dir])

    four_week_easing = all(d == "falling" for d in [dgs2_4w_dir, dgs10_4w_dir, dgs30_4w_dir])
    four_week_restrictive = all(d == "rising" for d in [dgs2_4w_dir, dgs10_4w_dir, dgs30_4w_dir])

    curve_risk_on = curve_context == "Steepening" and not one_week_restrictive
    curve_risk_off = curve_context == "Flattening" and not one_week_easing

    policy_easing = fed_1w_dir == "falling" or fed_4w_dir == "falling"
    policy_restrictive = fed_1w_dir == "rising" or fed_4w_dir == "rising"
    fed_both_neutral = fed_1w_dir == "neutral" and fed_4w_dir == "neutral"
    policy_used_dgs2_proxy = False
    if fed_both_neutral:
        policy_easing = dgs2_4w_dir == "falling"
        policy_restrictive = dgs2_4w_dir == "rising"
        policy_used_dgs2_proxy = True

    risk_on_aligned = int(sum([one_week_easing, four_week_easing, curve_risk_on, policy_easing]))
    risk_off_aligned = int(sum([one_week_restrictive, four_week_restrictive, curve_risk_off, policy_restrictive]))

    return {
        "dirs_1w": {"dgs2": dgs2_1w_dir, "dgs10": dgs10_1w_dir, "dgs30": dgs30_1w_dir},
        "dirs_4w": {"dgs2": dgs2_4w_dir, "dgs10": dgs10_4w_dir, "dgs30": dgs30_4w_dir},
        "fed_1w_dir": fed_1w_dir,
        "fed_4w_dir": fed_4w_dir,
        "fed_both_neutral": fed_both_neutral,
        "policy_used_dgs2_proxy": policy_used_dgs2_proxy,
        "curve_context": curve_context,
        "one_week_easing": one_week_easing,
        "one_week_restrictive": one_week_restrictive,
        "four_week_easing": four_week_easing,
        "four_week_restrictive": four_week_restrictive,
        "curve_risk_on": curve_risk_on,
        "curve_risk_off": curve_risk_off,
        "policy_easing": policy_easing,
        "policy_restrictive": policy_restrictive,
        "risk_on_aligned": risk_on_aligned,
        "risk_off_aligned": risk_off_aligned,
    }


def _score_complete_row(row: pd.Series) -> dict:
    b = _rates_alignment_breakdown(row)
    curve_context = b["curve_context"]
    one_week_easing = b["one_week_easing"]
    one_week_restrictive = b["one_week_restrictive"]
    four_week_easing = b["four_week_easing"]
    four_week_restrictive = b["four_week_restrictive"]
    curve_risk_on = b["curve_risk_on"]
    curve_risk_off = b["curve_risk_off"]
    policy_easing = b["policy_easing"]
    policy_restrictive = b["policy_restrictive"]
    risk_on_aligned = b["risk_on_aligned"]
    risk_off_aligned = b["risk_off_aligned"]

    if risk_on_aligned > risk_off_aligned:
        macro_signal = "risk_on"
        rates_bias = "Bullish"
        aligned_count = risk_on_aligned
    elif risk_off_aligned > risk_on_aligned:
        macro_signal = "risk_off"
        rates_bias = "Bearish"
        aligned_count = risk_off_aligned
    else:
        macro_signal = "neutral"
        rates_bias = "Neutral"
        aligned_count = max(risk_on_aligned, risk_off_aligned)

    macro_score = SCORE_MAP[int(aligned_count)]
    macro_strength = _strength(macro_score)

    if policy_easing and not policy_restrictive:
        policy_pressure = "Easing"
    elif policy_restrictive and not policy_easing:
        policy_pressure = "Restrictive"
    else:
        policy_pressure = "Neutral"

    macro_context = _context_for_trades(macro_signal, macro_score)
    if macro_signal == "risk_on":
        macro_summary = (
            f"Rates context is risk-on: {aligned_count}/4 components align. "
            f"Falling/easing yields support risk assets; curve is {curve_context.lower()} "
            f"and policy pressure is {policy_pressure.lower()}. Confluence only; technicals locate trades."
        )
    elif macro_signal == "risk_off":
        macro_summary = (
            f"Rates context is risk-off: {aligned_count}/4 components align. "
            f"Rising/restrictive yields pressure risk assets; curve is {curve_context.lower()} "
            f"and policy pressure is {policy_pressure.lower()}. Confluence only; technicals locate trades."
        )
    else:
        macro_summary = "Mixed rates context. No clear macro edge; use technicals as the primary locator."

    return {
        "rates_bias": rates_bias,
        "curve_context": curve_context,
        "policy_pressure": policy_pressure,
        "macro_signal": macro_signal,
        "macro_score": macro_score,
        "macro_strength": macro_strength,
        "macro_context_for_trades": macro_context,
        "technical_trade_filter": _technical_filter(macro_context),
        "macro_summary": macro_summary,
    }


def _fmt_pp(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)) or pd.isna(value):
        return "N/A"
    try:
        return f"{float(value):+.4f} pp"
    except (TypeError, ValueError):
        return "N/A"


def _fmt_level(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)) or pd.isna(value):
        return "N/A"
    try:
        return f"{float(value):.4f}"
    except (TypeError, ValueError):
        return "N/A"


def build_macro_audit_payload(row: pd.Series | None) -> dict[str, Any]:
    """Transparent audit of existing macro_scoring rules for one fully-populated rates row.

    Does not change scoring; mirrors ``_score_complete_row`` / ``_rates_alignment_breakdown``.
    """
    _unavailable_regime = {
        "macro_signal": "source unavailable",
        "macro_score": None,
        "rates_bias": "source unavailable",
        "curve_context": "source unavailable",
        "curve_state": "source unavailable",
        "policy_pressure": "source unavailable",
        "macro_rationale": "source unavailable",
        "liquidity_regime": "source unavailable",
    }

    if row is None:
        return {
            "available": False,
            "reason": "No macro/rates snapshot row was available.",
            "resolved_regime": dict(_unavailable_regime),
        }

    if not _row_has_required_scoring_inputs(row):
        missing = [c for c in REQUIRED_SCORING_INPUTS if c not in row.index or pd.isna(row.get(c))]
        return {
            "available": False,
            "reason": "Required FRED inputs missing for the macro snapshot row.",
            "missing_inputs": missing,
            "resolved_regime": dict(_unavailable_regime),
        }

    b = _rates_alignment_breakdown(row)
    scored = _score_complete_row(row)

    risk_off_lines: list[str] = []
    if b["one_week_restrictive"]:
        risk_off_lines.append(
            "+1 toward risk_off aligned count: one_week_restrictive — DGS2, DGS10, and DGS30 "
            f'1w changes are all "rising" (each Δ > +{YIELD_THRESHOLD} pp vs ~5 business days prior). '
            f"Values: DGS2 Δ={_fmt_pp(row.get('dgs2_1w_change'))}, DGS10 Δ={_fmt_pp(row.get('dgs10_1w_change'))}, "
            f"DGS30 Δ={_fmt_pp(row.get('dgs30_1w_change'))}."
        )
    if b["four_week_restrictive"]:
        risk_off_lines.append(
            "+1 toward risk_off aligned count: four_week_restrictive — DGS2, DGS10, and DGS30 "
            f'4w changes are all "rising" (each Δ > +{YIELD_THRESHOLD} pp vs ~20 business days prior). '
            f"Values: DGS2 Δ={_fmt_pp(row.get('dgs2_4w_change'))}, DGS10 Δ={_fmt_pp(row.get('dgs10_4w_change'))}, "
            f"DGS30 Δ={_fmt_pp(row.get('dgs30_4w_change'))}."
        )
    if b["curve_risk_off"]:
        risk_off_lines.append(
            "+1 toward risk_off aligned count: curve_risk_off — T10Y2Y 1w change (yield_curve_10y2y_1w_change) "
            f'is classified as Flattening (Δ < -{CURVE_THRESHOLD} pp), and one_week_easing is false. '
            f"Raw curve 1w Δ={_fmt_pp(row.get('yield_curve_10y2y_1w_change'))}; curve label={b['curve_context']}."
        )
    if b["policy_restrictive"]:
        proxy_note = (
            " Fed 1w/4w both neutral → policy_restrictive uses DGS2 4w direction as proxy (see macro_scoring.py)."
            if b["policy_used_dgs2_proxy"]
            else ""
        )
        risk_off_lines.append(
            "+1 toward risk_off aligned count: policy_restrictive — DFF (fed_funds) 1w or 4w change is "
            f'"rising" (Δ > +{YIELD_THRESHOLD} pp), unless both are neutral then DGS2 4w sets policy side.{proxy_note} '
            f"fed_funds 1w Δ={_fmt_pp(row.get('fed_funds_1w_change'))}, fed_funds 4w Δ={_fmt_pp(row.get('fed_funds_4w_change'))}, "
            f"DGS2 4w Δ={_fmt_pp(row.get('dgs2_4w_change'))}."
        )

    risk_on_lines: list[str] = []
    if b["one_week_easing"]:
        risk_on_lines.append(
            "+1 toward risk_on aligned count: one_week_easing — DGS2, DGS10, and DGS30 1w changes are all "
            f'"falling" (each Δ < -{YIELD_THRESHOLD} pp vs ~5 business days prior). '
            f"Values: DGS2 Δ={_fmt_pp(row.get('dgs2_1w_change'))}, DGS10 Δ={_fmt_pp(row.get('dgs10_1w_change'))}, "
            f"DGS30 Δ={_fmt_pp(row.get('dgs30_1w_change'))}."
        )
    if b["four_week_easing"]:
        risk_on_lines.append(
            "+1 toward risk_on aligned count: four_week_easing — DGS2, DGS10, and DGS30 4w changes are all "
            f'"falling" (each Δ < -{YIELD_THRESHOLD} pp vs ~20 business days prior). '
            f"Values: DGS2 Δ={_fmt_pp(row.get('dgs2_4w_change'))}, DGS10 Δ={_fmt_pp(row.get('dgs10_4w_change'))}, "
            f"DGS30 Δ={_fmt_pp(row.get('dgs30_4w_change'))}."
        )
    if b["curve_risk_on"]:
        risk_on_lines.append(
            "+1 toward risk_on aligned count: curve_risk_on — T10Y2Y 1w change is classified as Steepening "
            f"(Δ > +{CURVE_THRESHOLD} pp), and one_week_restrictive is false. "
            f"Raw curve 1w Δ={_fmt_pp(row.get('yield_curve_10y2y_1w_change'))}; curve label={b['curve_context']}."
        )
    if b["policy_easing"]:
        proxy_note = (
            " Fed 1w/4w both neutral → policy_easing uses DGS2 4w direction as proxy (see macro_scoring.py)."
            if b["policy_used_dgs2_proxy"]
            else ""
        )
        risk_on_lines.append(
            "+1 toward risk_on aligned count: policy_easing — DFF (fed_funds) 1w or 4w change is "
            f'"falling" (Δ < -{YIELD_THRESHOLD} pp), unless both are neutral then DGS2 4w sets policy side.{proxy_note} '
            f"fed_funds 1w Δ={_fmt_pp(row.get('fed_funds_1w_change'))}, fed_funds 4w Δ={_fmt_pp(row.get('fed_funds_4w_change'))}, "
            f"DGS2 4w Δ={_fmt_pp(row.get('dgs2_4w_change'))}."
        )

    risk_on_aligned = b["risk_on_aligned"]
    risk_off_aligned = b["risk_off_aligned"]
    if risk_on_aligned > risk_off_aligned:
        winner = "risk_on"
        aligned_count = risk_on_aligned
    elif risk_off_aligned > risk_on_aligned:
        winner = "risk_off"
        aligned_count = risk_off_aligned
    else:
        winner = "neutral"
        aligned_count = max(risk_on_aligned, risk_off_aligned)

    score_map_str = ", ".join(f"{k}: {v}" for k, v in sorted(SCORE_MAP.items()))
    tie_note = None
    if winner == "neutral":
        tie_note = (
            f"Tie: risk_on_aligned={risk_on_aligned}, risk_off_aligned={risk_off_aligned}; "
            f"macro_signal=neutral; macro_score uses SCORE_MAP[max({risk_on_aligned}, {risk_off_aligned})] "
            f"= SCORE_MAP[{aligned_count}] = {SCORE_MAP[int(aligned_count)]}."
        )

    snap_date = row.get("date")
    snap_iso = pd.Timestamp(snap_date).strftime("%Y-%m-%d") if snap_date is not None and pd.notna(snap_date) else None

    return {
        "available": True,
        "rates_snapshot_date": snap_iso,
        "fred_series_note": "Treasury: FRED DGS2, DGS10, DGS30 (%). Curve: T10Y2Y or DGS10−DGS2. Policy rate: DFF as fed_funds.",
        "levels": {
            "dgs2": _fmt_level(row.get("dgs2")),
            "dgs10": _fmt_level(row.get("dgs10")),
            "dgs30": _fmt_level(row.get("dgs30")),
            "t10y2y_or_synthetic": _fmt_level(row.get("yield_curve_10y2y")),
            "fed_funds_dff": _fmt_level(row.get("fed_funds")),
        },
        "one_week_deltas_pp": {
            "dgs2": _fmt_pp(row.get("dgs2_1w_change")),
            "dgs10": _fmt_pp(row.get("dgs10_1w_change")),
            "dgs30": _fmt_pp(row.get("dgs30_1w_change")),
            "t10y2y": _fmt_pp(row.get("yield_curve_10y2y_1w_change")),
        },
        "fed_funds_changes_pp": {
            "1w": _fmt_pp(row.get("fed_funds_1w_change")),
            "4w": _fmt_pp(row.get("fed_funds_4w_change")),
        },
        "thresholds": {
            "yield_direction_pp": YIELD_THRESHOLD,
            "curve_direction_pp": CURVE_THRESHOLD,
            "yield_rule": f'|Δ| > {YIELD_THRESHOLD} pp => "rising" or "falling"; else "neutral".',
            "curve_rule": f'Δ > +{CURVE_THRESHOLD} pp => Steepening; Δ < -{CURVE_THRESHOLD} pp => Flattening; else Neutral label.',
        },
        "risk_off_contribution_lines": risk_off_lines,
        "risk_on_contribution_lines": risk_on_lines,
        "counts": {
            "risk_off_aligned": b["risk_off_aligned"],
            "risk_on_aligned": b["risk_on_aligned"],
        },
        "winner": winner,
        "score_mapping": {
            "formula": "macro_score = SCORE_MAP[aligned_count]",
            "score_map": SCORE_MAP,
            "score_map_explained": score_map_str,
            "aligned_count_used_for_score": int(aligned_count),
            "macro_score_from_audit": SCORE_MAP[int(aligned_count)],
        },
        "resolved_regime": {
            "macro_signal": scored["macro_signal"],
            "macro_score": float(scored["macro_score"]) if pd.notna(scored.get("macro_score")) else None,
            "rates_bias": scored["rates_bias"],
            "curve_context": scored["curve_context"],
            "curve_state": scored["curve_context"],
            "policy_pressure": scored["policy_pressure"],
            "macro_rationale": scored["macro_summary"],
            "liquidity_regime": "source unavailable",
        },
        "tie_break_note": tie_note,
        "reconcile_note": (
            "Audit recomputes from the same row via _rates_alignment_breakdown + _score_complete_row; "
            "macro_signal/macro_score above should match the decision table row when the same snapshot is used."
        ),
    }


def score_macro(df: pd.DataFrame) -> pd.DataFrame:
    """Score macro/rates regime context as an as-of time series.

    This layer is a regime/context filter only. It must not generate standalone
    trade entries. Technicals locate the trade; macro context filters or weights
    setup quality.

    Rates_History is intentionally as-of: every row carries the latest prior or
    same-day valid scoring snapshot. Invalid current rows can show snapshot lag,
    but they never receive a macro_score.
    """
    df = df.copy().sort_values("date").reset_index(drop=True)

    for col in REQUIRED_SCORING_INPUTS:
        if col not in df.columns:
            df[col] = pd.NA

    valid_scoring_mask = df.apply(_row_has_required_scoring_inputs, axis=1)

    last_snapshot_date = pd.NaT
    output_rows = []

    for idx, row in df.iterrows():
        row_date = pd.Timestamp(row["date"])
        row_is_valid = bool(valid_scoring_mask.iloc[idx])

        if row_is_valid:
            last_snapshot_date = row_date

        if pd.notna(last_snapshot_date):
            macro_snapshot_date = last_snapshot_date
            data_lag_days = (row_date.normalize() - macro_snapshot_date.normalize()).days
        else:
            macro_snapshot_date = pd.NaT
            data_lag_days = pd.NA

        if row_is_valid:
            context = _score_complete_row(row)
            valid_for_trading = pd.notna(context["macro_score"])
        else:
            context = _empty_context()
            valid_for_trading = False

        context.update(
            {
                "macro_snapshot_date": macro_snapshot_date,
                "data_lag_days": data_lag_days,
                "macro_valid_for_trading": bool(valid_for_trading),
            }
        )
        output_rows.append(context)

    scored_cols = pd.DataFrame(output_rows)
    for col in scored_cols.columns:
        df[col] = scored_cols[col]

    # Explicit invariant: no valid-for-trading flag without a score, no score without required inputs.
    df.loc[df["macro_score"].isna(), "macro_valid_for_trading"] = False
    invalid_input_mask = ~valid_scoring_mask
    df.loc[invalid_input_mask, "macro_score"] = pd.NA
    df.loc[invalid_input_mask, "macro_signal"] = "insufficient_data"
    df.loc[invalid_input_mask, "macro_strength"] = pd.NA
    df.loc[invalid_input_mask, "macro_valid_for_trading"] = False

    return df
