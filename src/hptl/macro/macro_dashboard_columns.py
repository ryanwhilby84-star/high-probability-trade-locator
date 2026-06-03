"""Post-scoring dashboard columns (does not alter macro_score / macro_signal math)."""
from __future__ import annotations

import pandas as pd


def augment_scored_macro_dashboard_columns(scored: pd.DataFrame) -> pd.DataFrame:
    """Attach ``macro_rationale``, ``curve_state``, and explicit ``liquidity_regime`` labels."""
    df = scored.copy()
    valid = df["macro_valid_for_trading"].astype(bool) & df["macro_score"].notna()
    df["macro_rationale"] = "source unavailable"
    df.loc[valid, "macro_rationale"] = df.loc[valid, "macro_summary"].astype(str)
    df["curve_state"] = "source unavailable"
    df.loc[valid, "curve_state"] = df.loc[valid, "curve_context"].astype(str)
    df["liquidity_regime"] = "source unavailable"
    return df
