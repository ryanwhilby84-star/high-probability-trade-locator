from __future__ import annotations

import pandas as pd

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
    if pd.isna(value):
        return "neutral"
    if value > YIELD_THRESHOLD:
        return "rising"
    if value < -YIELD_THRESHOLD:
        return "falling"
    return "neutral"


def _curve_direction(value) -> str:
    if pd.isna(value):
        return "Neutral"
    if value > CURVE_THRESHOLD:
        return "Steepening"
    if value < -CURVE_THRESHOLD:
        return "Flattening"
    return "Neutral"


def _strength(score) -> str | None:
    if pd.isna(score):
        return None
    score = int(score)
    if score <= 2:
        return "Weak"
    if score == 4:
        return "Moderate"
    if score == 6:
        return "Strong"
    return "Very Strong"


def _context_for_trades(signal: str, score) -> str:
    if pd.isna(score):
        return "Neutral/Unclear"
    score = int(score)
    if signal == "risk_on" and score >= 8:
        return "Strongly Supportive"
    if signal == "risk_on" and score in (4, 6):
        return "Supportive"
    if signal == "risk_off" and score in (4, 6):
        return "Hostile"
    if signal == "risk_off" and score >= 8:
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


def _score_complete_row(row: pd.Series) -> dict:
    dgs10_1w_dir = _yield_direction(row.get("dgs10_1w_change"))
    dgs2_1w_dir = _yield_direction(row.get("dgs2_1w_change"))
    dgs30_1w_dir = _yield_direction(row.get("dgs30_1w_change"))
    dgs10_4w_dir = _yield_direction(row.get("dgs10_4w_change"))
    dgs2_4w_dir = _yield_direction(row.get("dgs2_4w_change"))
    dgs30_4w_dir = _yield_direction(row.get("dgs30_4w_change"))
    curve_context = _curve_direction(row.get("yield_curve_10y2y_1w_change"))

    # DFF/fed_funds is historical/effective rate data, not a forward-looking
    # policy expectations series. If unchanged/blank, use 2Y trend as a market
    # proxy for policy/rates pressure rather than a policy expectation forecast.
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
    if fed_1w_dir == "neutral" and fed_4w_dir == "neutral":
        policy_easing = dgs2_4w_dir == "falling"
        policy_restrictive = dgs2_4w_dir == "rising"

    risk_on_aligned = sum([one_week_easing, four_week_easing, curve_risk_on, policy_easing])
    risk_off_aligned = sum([one_week_restrictive, four_week_restrictive, curve_risk_off, policy_restrictive])

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
