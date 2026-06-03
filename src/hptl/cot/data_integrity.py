"""Hard COT row integrity validation — quarantine invalid / placeholder positioning rows.

These checks run BEFORE scoring. Any row that fails is excluded from scoring and flagged
in the coverage audit so it never appears as a legitimate positioning state.

Note on open interest: the normalized COT master in this pipeline stores managed-money
``long_value`` / ``short_value`` (and ``net_value``), not a true total_open_interest column.
We therefore validate against side magnitudes and a derived ``reported_positions = long + short``
proxy, and we say so explicitly rather than inventing an OI figure.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

# --- Thresholds (institutional futures markets are deeply liquid) ---------------
# Any managed-money side below this in a normally-liquid market is implausible.
MIN_SIDE_POSITIONS = 100
# long + short must clear this; placeholder rows seen as 25 / 40 / 59 fall far below.
MIN_REPORTED_POSITIONS = 1000
# Week-over-week collapse fraction that is suspicious without a documented source reason.
WOW_COLLAPSE_FRACTION = 0.85
# Below this absolute size, a WoW collapse is treated as a data artifact, not a real flow.
WOW_COLLAPSE_FLOOR = MIN_REPORTED_POSITIONS


@dataclass
class RowIntegrity:
    valid: bool
    reasons: list[str] = field(default_factory=list)
    reported_positions: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "cot_valid": self.valid,
            "cot_invalid_reasons": list(self.reasons),
            "reported_positions": self.reported_positions,
        }


def _num(v: Any) -> float | None:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if x == x else None  # reject NaN


def validate_row(
    *,
    long_value: Any,
    short_value: Any,
    net_value: Any = None,
    prev_long: Any = None,
    prev_short: Any = None,
) -> RowIntegrity:
    """Validate a single COT row. Returns RowIntegrity(valid, reasons, reported_positions)."""
    reasons: list[str] = []
    lv = _num(long_value)
    sv = _num(short_value)

    if lv is None or sv is None:
        reasons.append("non_numeric_positions")
        return RowIntegrity(valid=False, reasons=reasons, reported_positions=None)

    reported = lv + sv

    if lv <= 0 or sv <= 0:
        reasons.append("zero_side_in_liquid_market")

    if lv < MIN_SIDE_POSITIONS or sv < MIN_SIDE_POSITIONS:
        reasons.append("tiny_side_below_floor")

    if reported < MIN_REPORTED_POSITIONS:
        reasons.append("reported_positions_below_market_threshold")

    nv = _num(net_value)
    if nv is not None and abs(nv - (lv - sv)) > max(50.0, 0.02 * reported):
        # net should equal long - short within tolerance; large gap = bad join / column mismatch
        reasons.append("net_inconsistent_with_long_minus_short")

    pl = _num(prev_long)
    ps = _num(prev_short)
    if pl is not None and pl >= WOW_COLLAPSE_FLOOR and lv < pl * (1 - WOW_COLLAPSE_FRACTION):
        reasons.append("long_collapse_wow_unexplained")
    if ps is not None and ps >= WOW_COLLAPSE_FLOOR and sv < ps * (1 - WOW_COLLAPSE_FRACTION):
        reasons.append("short_collapse_wow_unexplained")

    return RowIntegrity(valid=not reasons, reasons=reasons, reported_positions=reported)


def validate_cot_frame(cot: pd.DataFrame) -> pd.DataFrame:
    """Annotate a COT DataFrame with cot_valid + cot_invalid_reasons per row.

    Expects columns: market, cot_report_date, long_value, short_value, net_value (optional).
    Adds: cot_valid (bool), cot_invalid_reasons (list[str]), reported_positions (float).
    Does not drop rows — quarantine is applied by callers using the cot_valid flag.
    """
    if cot.empty:
        out = cot.copy()
        out["cot_valid"] = pd.Series(dtype=bool)
        out["cot_invalid_reasons"] = pd.Series(dtype=object)
        out["reported_positions"] = pd.Series(dtype=float)
        return out

    df = cot.copy()
    has_market = "market" in df.columns
    has_date = "cot_report_date" in df.columns
    if has_market and has_date:
        df = df.sort_values(["market", "cot_report_date"]).reset_index(drop=True)

    valids: list[bool] = []
    reasons_col: list[list[str]] = []
    reported_col: list[float | None] = []

    prev_by_market: dict[str, tuple[Any, Any]] = {}
    for _, r in df.iterrows():
        mkt = str(r.get("market")) if has_market else ""
        prev_long, prev_short = prev_by_market.get(mkt, (None, None))
        res = validate_row(
            long_value=r.get("long_value"),
            short_value=r.get("short_value"),
            net_value=r.get("net_value"),
            prev_long=prev_long,
            prev_short=prev_short,
        )
        valids.append(res.valid)
        reasons_col.append(res.reasons)
        reported_col.append(res.reported_positions)
        # Only advance the WoW baseline from rows that were themselves plausible in size,
        # so a single bad week doesn't poison the next comparison.
        lv = _num(r.get("long_value"))
        sv = _num(r.get("short_value"))
        if lv is not None and sv is not None and (lv + sv) >= MIN_REPORTED_POSITIONS:
            prev_by_market[mkt] = (lv, sv)

    df["cot_valid"] = valids
    df["cot_invalid_reasons"] = reasons_col
    df["reported_positions"] = reported_col
    return df


def frame_integrity_summary(cot: pd.DataFrame) -> dict[str, Any]:
    """Per-market valid/invalid counts + reason tally from a validated frame."""
    if cot.empty or "cot_valid" not in cot.columns:
        return {"total_rows": 0, "valid_rows": 0, "invalid_rows": 0, "by_market": {}, "reason_tally": {}}

    by_market: dict[str, dict[str, Any]] = {}
    reason_tally: dict[str, int] = {}
    for mkt, sub in cot.groupby("market"):
        invalid = sub[~sub["cot_valid"].astype(bool)]
        valid = sub[sub["cot_valid"].astype(bool)]
        latest_valid = None
        if not valid.empty and "cot_report_date" in valid.columns:
            dates = pd.to_datetime(valid["cot_report_date"], errors="coerce").dropna()
            if not dates.empty:
                latest_valid = dates.max().strftime("%Y-%m-%d")
        by_market[str(mkt)] = {
            "valid_rows": int(len(valid)),
            "invalid_rows": int(len(invalid)),
            "latest_valid_cot_week": latest_valid,
        }
        for rl in invalid["cot_invalid_reasons"]:
            for reason in rl or []:
                reason_tally[reason] = reason_tally.get(reason, 0) + 1

    return {
        "total_rows": int(len(cot)),
        "valid_rows": int(cot["cot_valid"].astype(bool).sum()),
        "invalid_rows": int((~cot["cot_valid"].astype(bool)).sum()),
        "by_market": by_market,
        "reason_tally": reason_tally,
    }
