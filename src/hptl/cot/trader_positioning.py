"""COT trader-group enrichment — Legacy Futures Only via ``legacy_cot_latest.json``.

Read-only enrichment for dashboard charts — does not alter scoring columns (``long_value`` / ``short_value``).
"""
from __future__ import annotations

from typing import Any

import pandas as pd

from hptl.cot.legacy_cot_loader import (
    legacy_trader_groups_payload,
    load_legacy_trader_positioning_by_market_date,
)


def cot_profile_for_market(market: str) -> str:
    """All HTPL instruments use Legacy COT (no TFF / financial profile)."""
    return "legacy"


def trader_groups_payload(row: pd.Series | dict[str, Any]) -> dict[str, Any]:
    """JSON-safe positioning groups — Legacy NC / Commercial / Non-reportable only."""
    return legacy_trader_groups_payload(row)


def load_trader_positioning_by_market_date(
    *,
    map_market_fn: Any = None,
    parse_dates_fn: Any = None,
) -> pd.DataFrame:
    """Build market × report_date trader-group columns from ``data/legacy_cot_latest.json``."""
    _ = map_market_fn, parse_dates_fn
    return load_legacy_trader_positioning_by_market_date()


def _dedupe_extra_for_merge(extra: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if extra.empty:
        return extra
    sort_col = "open_interest" if "open_interest" in extra.columns else keys[0]
    out = extra.sort_values(sort_col, ascending=False, na_position="last")
    return out.drop_duplicates(keys, keep="first").reset_index(drop=True)


def merge_trader_positioning_into_cot(cot: pd.DataFrame, map_market_fn: Any, parse_dates_fn: Any) -> pd.DataFrame:
    """Left-join Legacy trader-group columns onto the scoring COT frame."""
    if cot.empty:
        return cot
    extra = load_trader_positioning_by_market_date(
        map_market_fn=map_market_fn,
        parse_dates_fn=parse_dates_fn,
    )
    if extra.empty:
        return cot
    value_cols = [c for c in extra.columns if c not in ("market", "cot_report_date", "raw_cftc_market_name")]
    if "raw_cftc_market_name" in cot.columns:
        extra_named = _dedupe_extra_for_merge(extra, ["market", "cot_report_date", "raw_cftc_market_name"])
        merged = cot.merge(
            extra_named,
            on=["market", "cot_report_date", "raw_cftc_market_name"],
            how="left",
            suffixes=("", "_tg"),
        )
        probe_col = "mm_long" if "mm_long" in merged.columns else "nc_long"
        miss = merged[probe_col].isna() if probe_col in merged.columns else pd.Series(False, index=merged.index)
        if miss.any():
            fb_extra = _dedupe_extra_for_merge(
                extra.drop(columns=["raw_cftc_market_name"], errors="ignore"),
                ["market", "cot_report_date"],
            )
            mis_idx = merged.index[miss]
            fill = merged.loc[miss, ["market", "cot_report_date"]].merge(
                fb_extra,
                on=["market", "cot_report_date"],
                how="left",
                suffixes=("", "_fb"),
            )
            if len(fill) == len(mis_idx):
                fill.index = mis_idx
                for col in value_cols:
                    if col in fill.columns:
                        merged.loc[mis_idx, col] = fill[col]
        return merged
    extra_dated = _dedupe_extra_for_merge(extra, ["market", "cot_report_date"])
    return cot.merge(extra_dated, on=["market", "cot_report_date"], how="left", suffixes=("", "_tg"))
