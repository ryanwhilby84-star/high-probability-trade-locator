"""Control comparisons for discovered pre-move behaviour."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from hptl.cot.intelligence_phase5a.config import (
    FEATURE_COLS_FOR_CLUSTER,
    RANDOM_CONTROL_MULTIPLIER,
    VOL_REGIME_WEEKS,
)
from hptl.cot.intelligence_phase5a.features import extract_case_features


def _realized_vol(prices: list[float | None], idx: int, weeks: int = VOL_REGIME_WEEKS) -> float | None:
    rets = []
    for i in range(max(1, idx - weeks + 1), idx + 1):
        a, b = prices[i - 1], prices[i]
        if a is None or b is None or a == 0:
            continue
        rets.append(abs((b - a) / a))
    if len(rets) < 4:
        return None
    return float(np.mean(rets))


def sample_control_onsets(
    panel: dict[str, Any],
    event_moves: list[dict[str, Any]],
    *,
    rng: np.random.Generator,
) -> list[dict[str, Any]]:
    """Sample random / vol-matched / seasonal control onsets for a market."""
    dates = panel["dates"]
    prices = panel["prices"]
    n = len(dates)
    event_idx = {int(m["onset_index"]) for m in event_moves if m.get("independent")}
    # eligible non-event weeks
    eligible = [
        i
        for i in range(52, n - 12)
        if i not in event_idx and prices[i] is not None
    ]
    if not eligible:
        return []

    n_events = max(1, sum(1 for m in event_moves if m.get("independent")))
    n_rand = min(len(eligible), n_events * RANDOM_CONTROL_MULTIPLIER)

    controls: list[dict[str, Any]] = []
    # Random
    for i in rng.choice(eligible, size=n_rand, replace=False):
        controls.append(
            {
                "market": event_moves[0]["market"] if event_moves else "",
                "asset_class": event_moves[0].get("asset_class") if event_moves else None,
                "onset_index": int(i),
                "onset_date": dates[i],
                "horizon_weeks": None,
                "direction": "control_random",
                "forward_return_pct": None,
                "mfe_pct": None,
                "mae_pct": None,
                "independent": True,
                "control_type": "random_non_event",
            }
        )

    # Vol-regime matched: for each independent event, pick control with closest vol
    event_vols = []
    for m in event_moves:
        if not m.get("independent"):
            continue
        v = _realized_vol(prices, int(m["onset_index"]))
        if v is not None:
            event_vols.append((m, v))
    eligible_vols = [(i, _realized_vol(prices, i)) for i in eligible]
    eligible_vols = [(i, v) for i, v in eligible_vols if v is not None]
    used = set()
    for m, ev in event_vols:
        if not eligible_vols:
            break
        best = min(eligible_vols, key=lambda t: abs(t[1] - ev) + (1000 if t[0] in used else 0))
        i = best[0]
        used.add(i)
        controls.append(
            {
                "market": m["market"],
                "asset_class": m.get("asset_class"),
                "onset_index": int(i),
                "onset_date": dates[i],
                "horizon_weeks": m.get("horizon_weeks"),
                "direction": "control_vol_matched",
                "forward_return_pct": None,
                "mfe_pct": None,
                "mae_pct": None,
                "independent": True,
                "control_type": "vol_regime_matched",
                "matched_event_date": m["onset_date"],
            }
        )

    # Seasonal month match
    for m in event_moves:
        if not m.get("independent"):
            continue
        month = str(m["onset_date"])[5:7]
        cands = [i for i in eligible if dates[i][5:7] == month and i not in used]
        if not cands:
            continue
        i = int(rng.choice(cands))
        used.add(i)
        controls.append(
            {
                "market": m["market"],
                "asset_class": m.get("asset_class"),
                "onset_index": i,
                "onset_date": dates[i],
                "horizon_weeks": m.get("horizon_weeks"),
                "direction": "control_seasonal",
                "forward_return_pct": None,
                "mfe_pct": None,
                "mae_pct": None,
                "independent": True,
                "control_type": "seasonal_month_matched",
                "matched_event_date": m["onset_date"],
            }
        )

    return controls


def build_control_comparison(
    event_features: pd.DataFrame,
    control_features: pd.DataFrame,
) -> pd.DataFrame:
    """Compare key feature distributions: events vs controls."""
    cols = [c for c in FEATURE_COLS_FOR_CLUSTER if c in event_features.columns]
    rows = []
    for direction in ("rally", "selloff"):
        ev = event_features[
            (event_features["case_role"] == "event")
            & (event_features["direction"] == direction)
            & (event_features["independent"] == True)  # noqa: E712
        ]
        if ev.empty:
            continue
        for ctype in sorted(control_features["direction"].dropna().unique()):
            ctrl = control_features[control_features["direction"] == ctype]
            if ctrl.empty:
                continue
            for col in cols:
                a = pd.to_numeric(ev[col], errors="coerce").dropna()
                b = pd.to_numeric(ctrl[col], errors="coerce").dropna()
                if len(a) < 5 or len(b) < 5:
                    continue
                # standardized mean difference
                pooled = float(np.sqrt(0.5 * (a.var(ddof=1) + b.var(ddof=1)))) or 1.0
                smd = float((a.mean() - b.mean()) / pooled)
                # Mann-Whitney-ish via rank-biserial approximation
                # Distinctive if |SMD| >= 0.35
                rows.append(
                    {
                        "direction": direction,
                        "control_type": ctype,
                        "feature": col,
                        "event_n": int(len(a)),
                        "control_n": int(len(b)),
                        "event_median": round(float(a.median()), 4),
                        "control_median": round(float(b.median()), 4),
                        "event_mean": round(float(a.mean()), 4),
                        "control_mean": round(float(b.mean()), 4),
                        "standardized_mean_diff": round(smd, 4),
                        "distinctive": abs(smd) >= 0.35,
                    }
                )
    return pd.DataFrame(rows)


def extract_control_features(
    panel: dict[str, Any],
    controls: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    feats, seqs = [], []
    for c in controls:
        f, s = extract_case_features(panel, c, case_role="control")
        f["control_type"] = c.get("control_type")
        s["control_type"] = c.get("control_type")
        feats.append(f)
        seqs.append(s)
    return feats, seqs
