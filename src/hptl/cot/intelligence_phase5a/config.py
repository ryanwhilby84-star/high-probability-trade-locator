"""Frozen Phase 5A definitions — declared before discovery outcomes are used."""

from __future__ import annotations

from typing import Any

# --- Price move detection (frozen) ---
HORIZONS_WEEKS: tuple[int, ...] = (4, 8, 12)
RALLY_PERCENTILE = 90.0  # top 10% of market-horizon forward returns
SELLOFF_PERCENTILE = 10.0  # bottom 10%
# Independence: within (market, horizon, direction), suppress onsets closer than
# the horizon length in weeks (episode / cooldown).
COOLDOWN_BY_HORIZON: dict[int, int] = {4: 4, 8: 8, 12: 12}

# --- Lookbacks (frozen) ---
LOOKBACK_WEEKS: tuple[int, ...] = (4, 8, 12, 26)
CHANGE_LAGS: tuple[int, ...] = (1, 4, 8, 12)
SLOPE_WINDOWS: tuple[int, ...] = (4, 8, 12)
EXTREME_HIGH = 90.0
EXTREME_LOW = 10.0
INTENSITY_TAIL_PCT = 5.0  # top/bottom 5% of historical weekly Δnet
MIN_HISTORY_WEEKS = 52
MIN_FORWARD_COMPLETE = 12  # need enough future weeks for longest horizon

# --- Controls ---
RANDOM_CONTROL_MULTIPLIER = 3  # random non-event weeks per independent event (capped)
VOL_REGIME_WEEKS = 12  # realized abs-return window for regime matching
SEASONAL_MONTH_MATCH = True

# --- Clustering (structure chosen on feature-space stability, NOT returns) ---
CLUSTER_LINKAGE = "average"
CLUSTER_METRIC = "euclidean"
CLUSTER_K_CANDIDATES: tuple[int, ...] = (3, 4, 5, 6)
CLUSTER_MIN_CASES = 8
CLUSTER_STABILITY_BOOTSTRAPS = 5
CLUSTER_STABILITY_FRACTION = 0.8
FEATURE_COLS_FOR_CLUSTER: tuple[str, ...] = (
    "c_pct",
    "nc_pct",
    "nr_pct",
    "c_pct_chg_4w",
    "nc_pct_chg_4w",
    "nr_pct_chg_4w",
    "c_pct_chg_12w",
    "nc_pct_chg_12w",
    "nr_pct_chg_12w",
    "c_slope_8w",
    "nc_slope_8w",
    "nr_slope_8w",
    "c_weeks_above_90_12w",
    "nc_weeks_above_90_12w",
    "c_weeks_below_10_12w",
    "nc_weeks_below_10_12w",
    "c_exit_extreme_12w",
    "nc_exit_extreme_12w",
    "c_nc_opp_score",
    "c_nc_spread_chg_8w",
    "c_nr_spread_chg_8w",
    "c_nc_agree_4w",
)

GROUPS = ("commercial", "noncommercial", "nonreportable")


def frozen_definitions_payload() -> dict[str, Any]:
    return {
        "version": "cot_intelligence_phase5a_v1",
        "scope": "price_anchored_behaviour_discovery",
        "not_for_live_alerts": True,
        "not_validated": True,
        "copper_excluded": "via Phase-1 trustworthy_markets gate",
        "price_move_detection": {
            "horizons_weeks": list(HORIZONS_WEEKS),
            "rally_rule": f"forward return >= market-horizon {RALLY_PERCENTILE}th percentile",
            "selloff_rule": f"forward return <= market-horizon {SELLOFF_PERCENTILE}th percentile",
            "thresholds": "market-specific empirical percentiles (not universal %)",
            "independence": {
                "unit": "(market, horizon, direction)",
                "cooldown_weeks_by_horizon": dict(COOLDOWN_BY_HORIZON),
                "rule": (
                    "Sort candidate onsets by date; keep an onset only if its index "
                    "is at least cooldown weeks after the previously kept onset "
                    "for the same (market, horizon, direction)."
                ),
            },
        },
        "lookbacks_weeks": list(LOOKBACK_WEEKS),
        "extreme_bands": {"high": EXTREME_HIGH, "low": EXTREME_LOW},
        "intensity_tail_pct": INTENSITY_TAIL_PCT,
        "change_lags_weeks": list(CHANGE_LAGS),
        "slope_windows_weeks": list(SLOPE_WINDOWS),
        "controls": {
            "random_multiplier": RANDOM_CONTROL_MULTIPLIER,
            "vol_regime_weeks": VOL_REGIME_WEEKS,
            "seasonal_month_match": SEASONAL_MONTH_MATCH,
        },
        "clustering": {
            "method": "scipy hierarchical average-linkage + maxclust cut",
            "metric": CLUSTER_METRIC,
            "k_candidates": list(CLUSTER_K_CANDIDATES),
            "k_selection": (
                "argmax mean silhouette on feature distance only "
                "(never optimized against forward returns)"
            ),
            "feature_columns": list(FEATURE_COLS_FOR_CLUSTER),
            "min_cases": CLUSTER_MIN_CASES,
            "stability": {
                "bootstraps": CLUSTER_STABILITY_BOOTSTRAPS,
                "subsample_fraction": CLUSTER_STABILITY_FRACTION,
                "metric": "adjusted Rand agreement of labels on overlapping cases",
            },
        },
        "sequence_stages": [
            "BUILD_UP",
            "BUILD_DOWN",
            "EXTREME_HIGH",
            "EXTREME_LOW",
            "FLATTEN",
            "FLIP_UP",
            "FLIP_DOWN",
            "SATURATION",
            "OPPOSITION_WIDENING",
            "OPPOSITION_PEAK",
            "ALIGNMENT_BEGINNING",
        ],
        "pit_safety": (
            "All onset features use only series history through onset index; "
            "forward returns / MFE / MAE are outcome labels attached after features."
        ),
    }
