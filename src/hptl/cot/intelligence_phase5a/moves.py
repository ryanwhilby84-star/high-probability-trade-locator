"""Market-specific significant price-move detection."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from hptl.cot.intelligence_phase5a.config import (
    COOLDOWN_BY_HORIZON,
    HORIZONS_WEEKS,
    MIN_FORWARD_COMPLETE,
    MIN_HISTORY_WEEKS,
    RALLY_PERCENTILE,
    SELLOFF_PERCENTILE,
)
from hptl.cot.positioning_research_engine import _finite, _forward_path_stats


def detect_market_moves(
    market: str,
    asset_class: str,
    dates: list[str],
    prices: list[float | None],
) -> list[dict[str, Any]]:
    """Detect independent major rallies / sell-offs for one market."""
    n = len(prices)
    if n < MIN_HISTORY_WEEKS + MIN_FORWARD_COMPLETE:
        return []

    # Precompute forward returns per horizon for threshold estimation
    fwd: dict[int, list[float | None]] = {h: [None] * n for h in HORIZONS_WEEKS}
    path: dict[int, list[dict[str, Any] | None]] = {h: [None] * n for h in HORIZONS_WEEKS}
    for i in range(n):
        for h in HORIZONS_WEEKS:
            st = _forward_path_stats(prices, i, h)
            if st is None:
                continue
            fwd[h][i] = float(st["return_pct"])
            path[h][i] = st

    moves: list[dict[str, Any]] = []
    for h in HORIZONS_WEEKS:
        vals = [v for v in fwd[h] if v is not None]
        if len(vals) < max(40, MIN_HISTORY_WEEKS):
            continue
        thr_hi = float(np.nanpercentile(vals, RALLY_PERCENTILE))
        thr_lo = float(np.nanpercentile(vals, SELLOFF_PERCENTILE))
        cooldown = COOLDOWN_BY_HORIZON[h]

        candidates: list[dict[str, Any]] = []
        for i, ret in enumerate(fwd[h]):
            if ret is None or i < MIN_HISTORY_WEEKS:
                continue
            if ret >= thr_hi:
                direction = "rally"
            elif ret <= thr_lo:
                direction = "selloff"
            else:
                continue
            st = path[h][i] or {}
            end_idx = i + h
            candidates.append(
                {
                    "market": market,
                    "asset_class": asset_class,
                    "onset_index": i,
                    "onset_date": dates[i],
                    "horizon_weeks": h,
                    "direction": direction,
                    "forward_return_pct": round(float(ret), 4),
                    "mfe_pct": st.get("favourable_excursion_pct"),
                    "mae_pct": st.get("adverse_excursion_pct"),
                    "move_end_date": dates[end_idx] if end_idx < n else None,
                    "price_threshold_pct": round(thr_hi if direction == "rally" else thr_lo, 4),
                    "threshold_rule": (
                        f"market_horizon_p{int(RALLY_PERCENTILE)}"
                        if direction == "rally"
                        else f"market_horizon_p{int(SELLOFF_PERCENTILE)}"
                    ),
                    "cooldown_weeks": cooldown,
                    "independent": False,
                }
            )

        # Independence per (direction)
        for direction in ("rally", "selloff"):
            subset = [c for c in candidates if c["direction"] == direction]
            subset.sort(key=lambda x: x["onset_index"])
            last_kept = -10_000
            for c in subset:
                if c["onset_index"] - last_kept < cooldown:
                    c["independent"] = False
                    moves.append(c)
                    continue
                c["independent"] = True
                last_kept = c["onset_index"]
                moves.append(c)

    return moves


def moves_to_frame(moves: list[dict[str, Any]]) -> pd.DataFrame:
    if not moves:
        return pd.DataFrame(
            columns=[
                "market",
                "asset_class",
                "onset_date",
                "onset_index",
                "horizon_weeks",
                "direction",
                "forward_return_pct",
                "mfe_pct",
                "mae_pct",
                "move_end_date",
                "price_threshold_pct",
                "threshold_rule",
                "cooldown_weeks",
                "independent",
            ]
        )
    return pd.DataFrame(moves)
