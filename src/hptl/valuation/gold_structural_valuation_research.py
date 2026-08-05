"""Gold Structural / Keynesian Valuation Research (research only).

Reconstructs the Natural Gas *research architecture* (hypothesis → fair-value
series → expanding walk-forward → deviation vs forward returns → regimes)
for Gold structural equilibrium models.

This track is separate from the Tier-1 standalone ranking gate. Combinations
are allowed only when they form an economically defensible joint state /
equilibrium — not as a bypass of the Reject ranking.

Does NOT modify:
  - published Natural Gas valuation (ng_storage_production_v2)
  - metals_real_yield_v1
  - COT / Scanner / Seasonality / Stage 4
  - any production valuation endpoint
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from hptl.config import PROJECT_ROOT
from hptl.fx.fx_macro_history import load_fred_daily_map
from hptl.prices.canonical_timeline import load_canonical_timeline
from hptl.valuation.energy_natural_gas_valuation_v1 import (
    _multivariate_ols,
    _predict_log_price,
)
from hptl.valuation.gold_macro_tier1_discovery import _align, _asof_series, _load_dx_daily
from hptl.valuation.metals_valuation_v1 import (
    DXY_SERIES,
    MODEL_ID as PUBLISHED_GOLD_MODEL_ID,
    REAL_YIELD_SERIES,
    _build_weekly_panel,
    _load_dxy_series,
)
from hptl.valuation.ng_driver_validation_phase2_production import (
    MIN_TRAIN,
    STEP,
    _eval_model,
    _walk_forward_predictions,
)

AUDIT_DIR = PROJECT_ROOT / "data" / "audits" / "gold_structural_valuation"
CHART_DIR = AUDIT_DIR / "charts"
REPORT_MD = AUDIT_DIR / "gold_structural_research_report.md"
RANKING_JSON = AUDIT_DIR / "gold_structural_candidate_ranking.json"
METRICS_CSV = AUDIT_DIR / "gold_structural_candidate_metrics.csv"
REGIME_CSV = AUDIT_DIR / "gold_structural_regime_results.csv"
EXTREME_CSV = AUDIT_DIR / "gold_structural_extreme_returns.csv"

# Monthly FRED series are dated at month-start but released later.
MONTHLY_PUBLICATION_LAG_DAYS = 42
HORIZONS = (4, 8, 13, 26, 52)
BUCKETS = (
    ("materially_undervalued", None, -15.0),
    ("undervalued", -15.0, -5.0),
    ("near_fair_value", -5.0, 5.0),
    ("overvalued", 5.0, 15.0),
    ("materially_overvalued", 15.0, None),
)

# Promotion philosophy (structural valuation, not price-forecast R²).
SCORE_PROMOTE = 70.0
SCORE_CONTINUE = 45.0


def ng_methodology_transfer_notes() -> dict[str, Any]:
    """Exact NG methodology discovered + Gold transfer map."""
    return {
        "accepted_ng_engine": "ng_storage_production_v2",
        "fair_value_form": (
            "log(P_t) = α + β_s·storage_surplus_bcf_t + β_y·production_yoy_pct_t; "
            "fair_t = exp(log(P_t))"
        ),
        "keynesian_or_structural_method": (
            "Physical equilibrium: inventory surplus vs seasonal norm + supply growth. "
            "Not a single-variable forecast of price; joint structural state of the "
            "physical market defines fair value, then market price is measured as "
            "deviation from that estimate."
        ),
        "input_transforms": {
            "storage_surplus_bcf": (
                "level − trailing same-ISO-week 5y average (prior years only, ≥3 peers)"
            ),
            "production_yoy_pct": (
                "monthly YoY % on native monthly dates, as-of onto weekly price dates"
            ),
            "rejected": [
                "raw_level",
                "seasonal_deviation",
                "trailing_zscore_156",
                "chg_4w",
                "chg_12w",
                "v1_fullsample_zscore (leaky)",
            ],
        },
        "estimation_rules": {
            "estimator": "OLS with intercept on log(spot)",
            "tip_coefficients": "full-sample OLS on aligned panel (published tip)",
            "walk_forward": "expanding OLS, min_train=156 weeks, step=13 weeks",
            "workstation_pit": "refit each week using only prior weeks (train_end < model_week)",
        },
        "deviation": {
            "formula": "deviation_pct = 100 * (spot - fair) / fair",
            "buckets": {
                "materially_undervalued": "<= -15%",
                "undervalued": "(-15%, -5%]",
                "near_fair": "|d| < 5%",
                "overvalued": "[5%, 15%)",
                "materially_overvalued": ">= 15%",
            },
        },
        "anti_leakage": [
            "as-of joins only (obs_date <= week)",
            "storage 5y peers from prior years only",
            "reject full-sample z-scores",
            "production staleness gate (100 days) → storage-only fallback",
            "walk-forward train_end < predict week",
        ],
        "acceptance_metrics": {
            "nested_promote_bar": ">= 2% OOS RMSE improvement vs nested baseline",
            "dm_alpha_one_sided": 0.10,
            "sign_stable": True,
            "no_pit_leakage": True,
            "live_tip_oos_r2_approx": 0.256,
        },
        "transferable_to_gold": [
            "log-price OLS fair value + exp back-transform",
            "expanding walk-forward (156 / 13) with OOS R²/RMSE/MAE and sign-flip paths",
            "deviation % of fair with ±5 / ±15 buckets",
            "forward-return usefulness of valuation buckets (not price-tracking alone)",
            "as-of alignment + publication lags",
            "reject full-sample scalers / future-calibrated bands",
            "prefer simplest economically coherent model that survives OOS",
            "tip full-sample fit separate from walk-forward diagnostics",
        ],
        "market_specific_do_not_reuse": [
            "EIA storage surplus / dry-gas YoY features and signs",
            "NG phase ladder A–H experimental set (LNG, HDD/CDD)",
            "100-day monthly production staleness rule",
            "storage-only fallback path",
            "NG coefficients / equation coefficients",
        ],
        "gold_adaptation_principle": (
            "Reuse the workflow and leakage philosophy. Rebuild fair value from "
            "Gold-specific structural hypotheses (monetary purchasing power, "
            "liquidity/opportunity-cost state, gold–silver monetary equilibrium, "
            "carry/rate structure). Do not transplant NG variables."
        ),
    }


def _parse_iso(d: str) -> date:
    return date.fromisoformat(str(d)[:10])


def _add_days(iso: str, days: int) -> str:
    return (_parse_iso(iso) + timedelta(days=days)).isoformat()


def _asof_with_lag(
    daily: dict[str, float], dates: list[str], *, lag_days: int
) -> list[float | None]:
    """As-of series where values become usable only after a publication lag."""
    if not daily:
        return [None] * len(dates)
    if lag_days <= 0:
        return _asof_series(daily, dates)
    # Shift availability forward: observation dated D usable at D+lag.
    shifted: dict[str, float] = {}
    for d, v in daily.items():
        try:
            shifted[_add_days(d, lag_days)] = float(v)
        except Exception:
            continue
    return _asof_series(shifted, dates)


def _finite_ffill(series: list[float | None]) -> list[float | None]:
    """Causal forward-fill; leading gaps remain None until first observation."""
    out: list[float | None] = []
    last: float | None = None
    for v in series:
        if v is not None and math.isfinite(float(v)):
            last = float(v)
        out.append(last)
    return out


def _first_complete_index(cols: list[list[float | None]]) -> int | None:
    n = min(len(c) for c in cols) if cols else 0
    for i in range(n):
        if all(c[i] is not None and math.isfinite(float(c[i])) for c in cols):  # type: ignore[arg-type]
            return i
    return None


def _weekly_prices(market: str) -> list[tuple[str, float]]:
    tl = load_canonical_timeline(market)
    if not tl:
        return []
    pairs, _ = tl.derive_weekly_iso()
    out: list[tuple[str, float]] = []
    for d, px in pairs:
        try:
            v = float(px)
        except (TypeError, ValueError):
            continue
        if math.isfinite(v) and v > 0:
            out.append((str(d)[:10], v))
    return out


def _naive_oos_metrics(
    y: list[float], indices: list[int], preds: list[float], actuals: list[float]
) -> dict[str, float | None]:
    if not indices or len(preds) != len(actuals):
        return {"naive_oos_rmse": None, "naive_oos_mae": None, "rmse_vs_naive_impr_pct": None}
    naive_err2: list[float] = []
    naive_abs: list[float] = []
    for i in indices:
        if i < 1:
            continue
        mu = sum(y[:i]) / i
        a = y[i]
        naive_err2.append((mu - a) ** 2)
        naive_abs.append(abs(mu - a))
    if not naive_err2:
        return {"naive_oos_rmse": None, "naive_oos_mae": None, "rmse_vs_naive_impr_pct": None}
    naive_rmse = math.sqrt(sum(naive_err2) / len(naive_err2))
    naive_mae = sum(naive_abs) / len(naive_abs)
    model_rmse = math.sqrt(sum((p - a) ** 2 for p, a in zip(preds, actuals)) / len(preds))
    impr = None
    if naive_rmse > 1e-12:
        impr = 100.0 * (naive_rmse - model_rmse) / naive_rmse
    return {
        "naive_oos_rmse": round(naive_rmse, 6),
        "naive_oos_mae": round(naive_mae, 6),
        "rmse_vs_naive_impr_pct": round(impr, 3) if impr is not None else None,
    }


def _classify_deviation(dev_pct: float) -> str:
    if dev_pct <= -15.0:
        return "materially_undervalued"
    if dev_pct < -5.0:
        return "undervalued"
    if dev_pct < 5.0:
        return "near_fair_value"
    if dev_pct < 15.0:
        return "overvalued"
    return "materially_overvalued"


def _walk_forward_fair_logs_multi(
    y: list[float],
    cols: list[list[float]],
    *,
    min_train: int = MIN_TRAIN,
    step: int = STEP,
    rolling_window: int | None = None,
) -> tuple[list[float | None], dict[str, Any]]:
    n = len(y)
    fair: list[float | None] = [None] * n
    names = [f"x{i}" for i in range(len(cols))]
    # Diagnostics still use expanding walk-forward for a stable OOS contract.
    wf = _walk_forward_predictions(y, cols, feature_names=names, min_train=min_train, step=step)
    t = min_train
    while t < n:
        if rolling_window is not None and t >= rolling_window:
            sl = slice(t - rolling_window, t)
        else:
            sl = slice(0, t)
        beta, r2 = _multivariate_ols(y[sl], [c[sl] for c in cols])
        if not beta or r2 is None:
            t += step
            continue
        end = min(t + step, n)
        for i in range(t, end):
            fair[i] = _predict_log_price(beta, [c[i] for c in cols])
        t += step
    return fair, wf


def _expanding_ratio_fair(
    prices: list[float],
    scale: list[float],
    *,
    min_train: int = MIN_TRAIN,
) -> list[float | None]:
    """fair_t = mean(price/scale for i<t) * scale_t — past-only expanding mean ratio."""
    n = len(prices)
    fair: list[float | None] = [None] * n
    ratios: list[float] = []
    for i in range(n):
        if scale[i] <= 0 or prices[i] <= 0:
            continue
        if len(ratios) >= min_train:
            mu = sum(ratios) / len(ratios)
            fair[i] = mu * scale[i]
        ratios.append(prices[i] / scale[i])
    return fair


def _fair_logs_from_levels(fair_levels: list[float | None]) -> list[float | None]:
    out: list[float | None] = []
    for f in fair_levels:
        if f is None or not math.isfinite(f) or f <= 0:
            out.append(None)
        else:
            out.append(math.log(f))
    return out


def _deviation_series(
    prices: list[float], fair_logs: list[float | None]
) -> list[float | None]:
    out: list[float | None] = []
    for px, fl in zip(prices, fair_logs):
        if fl is None or not math.isfinite(fl):
            out.append(None)
            continue
        fair = math.exp(fl)
        if fair <= 0 or px <= 0:
            out.append(None)
        else:
            out.append(100.0 * (px / fair - 1.0))
    return out


def _bucket_forward_returns(
    dates: list[str],
    prices: list[float],
    deviations: list[float | None],
    *,
    horizons: tuple[int, ...] = HORIZONS,
) -> list[dict[str, Any]]:
    n = len(prices)
    rows: list[dict[str, Any]] = []
    for bucket, lo, hi in BUCKETS:
        for h in horizons:
            rets: list[float] = []
            mae_paths: list[float] = []
            toward: list[bool] = []
            times_to_fair: list[int] = []
            for i in range(n - h):
                d = deviations[i]
                if d is None or not math.isfinite(d):
                    continue
                if lo is not None and not (d > lo):
                    continue
                if hi is not None and not (d <= hi):
                    continue
                if prices[i] <= 0 or prices[i + h] <= 0:
                    continue
                fwd = 100.0 * (prices[i + h] / prices[i] - 1.0)
                if not math.isfinite(fwd):
                    continue
                rets.append(fwd)
                # Max adverse excursion over the horizon (weekly marks).
                path = [100.0 * (prices[j] / prices[i] - 1.0) for j in range(i, i + h + 1)]
                if d < 0:
                    # undervalued: adverse = further down
                    mae_paths.append(min(path))
                else:
                    mae_paths.append(max(path) if d > 0 else 0.0)
                # Toward equilibrium: |dev| shrinks at horizon when fair exists.
                d_h = deviations[i + h]
                if d_h is not None and math.isfinite(d_h):
                    toward.append(abs(d_h) < abs(d))
                # Time toward fair: first week |dev| crosses below 5%
                ttf: int | None = None
                for k in range(1, h + 1):
                    dk = deviations[i + k]
                    if dk is not None and abs(dk) < 5.0:
                        ttf = k
                        break
                if ttf is not None:
                    times_to_fair.append(ttf)
            if not rets:
                rows.append(
                    {
                        "bucket": bucket,
                        "horizon_weeks": h,
                        "n": 0,
                        "mean_return_pct": None,
                        "median_return_pct": None,
                        "positive_return_rate": None,
                        "max_adverse_excursion_mean": None,
                        "avg_weeks_toward_fair": None,
                        "pct_extremes_toward_equilibrium": None,
                    }
                )
                continue
            rets_sorted = sorted(rets)
            mid = len(rets_sorted) // 2
            median = (
                rets_sorted[mid]
                if len(rets_sorted) % 2 == 1
                else 0.5 * (rets_sorted[mid - 1] + rets_sorted[mid])
            )
            rows.append(
                {
                    "bucket": bucket,
                    "horizon_weeks": h,
                    "n": len(rets),
                    "mean_return_pct": round(sum(rets) / len(rets), 3),
                    "median_return_pct": round(median, 3),
                    "positive_return_rate": round(
                        sum(1 for r in rets if r > 0) / len(rets), 3
                    ),
                    "max_adverse_excursion_mean": round(sum(mae_paths) / len(mae_paths), 3)
                    if mae_paths
                    else None,
                    "avg_weeks_toward_fair": round(
                        sum(times_to_fair) / len(times_to_fair), 2
                    )
                    if times_to_fair
                    else None,
                    "pct_extremes_toward_equilibrium": round(
                        100.0 * sum(1 for t in toward if t) / len(toward), 1
                    )
                    if toward
                    else None,
                }
            )
    return rows


def _valuation_usefulness(extreme_rows: list[dict[str, Any]], *, horizon: int = 13) -> dict[str, Any]:
    """Score whether undervalued buckets beat overvalued on forward returns.

    Uses sample-weighted pooled under (<= -5%) vs over (>= +5%) means so a tiny
    'materially undervalued' cell cannot dominate in a one-way bull sample.
    """
    rows = [
        r
        for r in extreme_rows
        if r.get("horizon_weeks") == horizon and (r.get("n") or 0) >= 1
    ]
    under_buckets = {"materially_undervalued", "undervalued"}
    over_buckets = {"materially_overvalued", "overvalued"}
    under_n = 0
    under_sum = 0.0
    over_n = 0
    over_sum = 0.0
    for r in rows:
        mean = r.get("mean_return_pct")
        n = int(r.get("n") or 0)
        if mean is None or n <= 0:
            continue
        if r["bucket"] in under_buckets:
            under_n += n
            under_sum += float(mean) * n
        elif r["bucket"] in over_buckets:
            over_n += n
            over_sum += float(mean) * n
    if under_n < 12 or over_n < 12:
        return {"ok": False, "spread_pp": None, "score": 0.0, "reason": "sparse_buckets"}
    u = under_sum / under_n
    o = over_sum / over_n
    spread = u - o
    # Require mild monotonicity vs near-fair when available.
    near = next((r for r in rows if r["bucket"] == "near_fair_value" and (r.get("n") or 0) >= 8), None)
    mono_bonus = 0.0
    if near and near.get("mean_return_pct") is not None:
        nf = float(near["mean_return_pct"])
        if u > nf > o:
            mono_bonus = 5.0
        elif u > o and u > nf:
            mono_bonus = 2.0
    # +10pp pooled spread → 25 points (+ optional mono bonus to 30)
    base = max(0.0, min(25.0, (spread / 10.0) * 25.0)) if spread > 0 else 0.0
    score = min(30.0, base + mono_bonus) if spread > 0 else 0.0
    return {
        "ok": True,
        "horizon_weeks": horizon,
        "under_bucket": "pooled_undervalued",
        "over_bucket": "pooled_overvalued",
        "under_mean_pct": round(u, 3),
        "over_mean_pct": round(o, 3),
        "under_n": under_n,
        "over_n": over_n,
        "spread_pp": round(spread, 3),
        "score": round(score, 2),
        "near_fair_mean_pct": near.get("mean_return_pct") if near else None,
    }


def _price_duplication_stats(
    prices: list[float], fair_logs: list[float | None]
) -> dict[str, Any]:
    pairs: list[tuple[float, float]] = []
    abs_devs: list[float] = []
    for px, fl in zip(prices, fair_logs):
        if fl is None or not math.isfinite(fl):
            continue
        fair = math.exp(fl)
        if fair <= 0 or px <= 0:
            continue
        pairs.append((px, fair))
        abs_devs.append(abs(100.0 * (px / fair - 1.0)))
    if len(pairs) < 40:
        return {"ok": False, "corr_price_fair": None, "median_abs_dev_pct": None, "is_price_mirror": False}
    px = [p for p, _ in pairs]
    fv = [f for _, f in pairs]
    mx = sum(px) / len(px)
    my = sum(fv) / len(fv)
    num = sum((a - mx) * (b - my) for a, b in zip(px, fv))
    den = math.sqrt(sum((a - mx) ** 2 for a in px) * sum((b - my) ** 2 for b in fv))
    corr = num / den if den > 1e-12 else None
    med = sorted(abs_devs)[len(abs_devs) // 2]
    # Mirror if fair tracks price extremely tightly (no independent valuation info).
    is_mirror = corr is not None and corr >= 0.995 and med < 2.0
    return {
        "ok": True,
        "n": len(pairs),
        "corr_price_fair": round(corr, 4) if corr is not None else None,
        "median_abs_dev_pct": round(med, 3),
        "mean_abs_dev_pct": round(sum(abs_devs) / len(abs_devs), 3),
        "is_price_mirror": is_mirror,
    }


def _coef_flip(stability: dict[str, Any]) -> bool:
    for st in stability.values():
        if bool(st.get("sign_flip")):
            return True
    return False


def _structural_score(
    *,
    signs_ok: bool,
    flip: bool,
    oos_r2: float | None,
    vs_naive_impr: float | None,
    usefulness: dict[str, Any],
    duplication: dict[str, Any],
    fair_vol_ok: bool,
) -> dict[str, Any]:
    parts: dict[str, float] = {}
    parts["economic_sign"] = 20.0 if signs_ok else 0.0
    parts["stability"] = 0.0 if flip else 15.0
    if oos_r2 is None or not math.isfinite(float(oos_r2)):
        parts["oos_r2"] = 0.0
    else:
        # Cap — structural models need not forecast weekly price tightly.
        # Very high OOS R² alone is not a promote signal for valuation.
        parts["oos_r2"] = max(0.0, min(15.0, float(oos_r2) / 0.30 * 15.0))
    if vs_naive_impr is None:
        parts["vs_naive"] = 0.0
    else:
        parts["vs_naive"] = max(0.0, min(15.0, float(vs_naive_impr) / 20.0 * 15.0))
    # Usefulness: only reward correctly signed under>over spreads.
    spread = usefulness.get("spread_pp")
    if spread is None or not math.isfinite(float(spread)):
        parts["valuation_usefulness"] = 0.0
    elif float(spread) <= 0:
        # Wrong-way valuation signal — explicit penalty.
        parts["valuation_usefulness"] = max(-20.0, float(spread) / 10.0 * 10.0)
    else:
        parts["valuation_usefulness"] = float(usefulness.get("score") or 0.0)
    # Independence of fair value from raw price (0..20)
    if duplication.get("is_price_mirror"):
        parts["independence"] = 0.0
    else:
        med = duplication.get("median_abs_dev_pct")
        corr = duplication.get("corr_price_fair")
        if med is None:
            parts["independence"] = 0.0
        elif med < 2.0 or (corr is not None and float(corr) >= 0.99):
            # Near-price tracking / co-moving twin (e.g. silver ratio) — weak independence.
            parts["independence"] = 3.0
        elif med < 8.0:
            parts["independence"] = 12.0
        else:
            parts["independence"] = 20.0
    parts["fair_vol"] = 5.0 if fair_vol_ok else 0.0
    total = sum(parts.values())
    return {
        "structural_score": round(total, 1),
        "score_parts": {k: round(v, 2) for k, v in parts.items()},
        "max_possible": 100.0,
    }


def _regime_labels(
    dates: list[str],
    dff: list[float],
    dxy: list[float],
    breakeven: list[float],
) -> list[dict[str, str]]:
    """Point-in-time regime tags using only information available at each date."""
    out: list[dict[str, str]] = []
    for i, d in enumerate(dates):
        tags: dict[str, str] = {
            "calendar": "pre_2020" if d < "2020-01-01" else "post_2020",
        }
        if i >= 52:
            dff_chg = dff[i] - dff[i - 52]
            if dff_chg > 0.5:
                tags["rate"] = "rising_rate"
            elif dff_chg < -0.5:
                tags["rate"] = "falling_rate"
            else:
                tags["rate"] = "stable_rate"
            # Past-only z of DXY over prior 156 weeks
            window = dxy[max(0, i - 156) : i]
            if len(window) >= 40:
                mu = sum(window) / len(window)
                sd = math.sqrt(sum((x - mu) ** 2 for x in window) / len(window))
                z = (dxy[i] - mu) / sd if sd > 1e-12 else 0.0
                if z > 0.5:
                    tags["dollar"] = "strong_dollar"
                elif z < -0.5:
                    tags["dollar"] = "weak_dollar"
                else:
                    tags["dollar"] = "neutral_dollar"
            be_win = breakeven[max(0, i - 156) : i]
            if be_win:
                be_mu = sum(be_win) / len(be_win)
                tags["inflation"] = (
                    "inflationary" if breakeven[i] > be_mu + 0.25 else (
                        "disinflationary" if breakeven[i] < be_mu - 0.25 else "neutral_inflation"
                    )
                )
            # Zero-rate regime: DFF < 0.25
            tags["zlb"] = "near_zlb" if dff[i] < 0.25 else "away_from_zlb"
        else:
            tags["rate"] = "warmup"
            tags["dollar"] = "warmup"
            tags["inflation"] = "warmup"
            tags["zlb"] = "warmup"
        out.append(tags)
    return out


def _regime_results(
    dates: list[str],
    prices: list[float],
    deviations: list[float | None],
    regime_tags: list[dict[str, str]],
    *,
    horizon: int = 13,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    dimensions = ("calendar", "rate", "dollar", "inflation", "zlb")
    for dim in dimensions:
        levels = sorted({tags.get(dim, "na") for tags in regime_tags})
        for level in levels:
            if level in {"warmup", "na"}:
                continue
            under_rets: list[float] = []
            over_rets: list[float] = []
            n = len(prices)
            for i in range(n - horizon):
                if regime_tags[i].get(dim) != level:
                    continue
                d = deviations[i]
                if d is None:
                    continue
                fwd = 100.0 * (prices[i + horizon] / prices[i] - 1.0)
                if not math.isfinite(fwd):
                    continue
                if d <= -5.0:
                    under_rets.append(fwd)
                elif d >= 5.0:
                    over_rets.append(fwd)
            spread = None
            if under_rets and over_rets:
                spread = (sum(under_rets) / len(under_rets)) - (
                    sum(over_rets) / len(over_rets)
                )
            rows.append(
                {
                    "dimension": dim,
                    "regime": level,
                    "horizon_weeks": horizon,
                    "n_under": len(under_rets),
                    "n_over": len(over_rets),
                    "under_mean_fwd_pct": round(sum(under_rets) / len(under_rets), 3)
                    if under_rets
                    else None,
                    "over_mean_fwd_pct": round(sum(over_rets) / len(over_rets), 3)
                    if over_rets
                    else None,
                    "spread_under_minus_over_pp": round(spread, 3) if spread is not None else None,
                }
            )
    return rows


def _write_sync_chart_svg(
    path: Path,
    *,
    title: str,
    dates: list[str],
    prices: list[float],
    fair_logs: list[float | None],
    deviations: list[float | None],
) -> None:
    """Synchronized price / fair value / deviation SVG (no matplotlib dependency)."""
    w, h = 1100, 640
    pad_l, pad_r, pad_t, pad_b = 60, 20, 40, 40
    mid = 360
    plot_w = w - pad_l - pad_r
    top_h = mid - pad_t - 20
    bot_h = h - mid - pad_b

    pairs: list[tuple[str, float, float, float]] = []
    for d, px, fl, dv in zip(dates, prices, fair_logs, deviations):
        if fl is None or dv is None:
            continue
        fair = math.exp(fl)
        if fair > 0 and px > 0:
            pairs.append((d, px, fair, dv))
    if len(pairs) < 10:
        path.write_text(
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}">'
            f'<text x="20" y="40">Insufficient series for {title}</text></svg>',
            encoding="utf-8",
        )
        return

    xs = list(range(len(pairs)))
    pxs = [p[1] for p in pairs]
    fvs = [p[2] for p in pairs]
    dvs = [p[3] for p in pairs]
    ymin = min(min(pxs), min(fvs))
    ymax = max(max(pxs), max(fvs))
    if ymax <= ymin:
        ymax = ymin + 1.0
    dmin = min(dvs)
    dmax = max(dvs)
    if abs(dmax - dmin) < 1e-9:
        dmax = dmin + 1.0

    def x_of(i: int) -> float:
        return pad_l + (i / max(1, len(xs) - 1)) * plot_w

    def y_price(v: float) -> float:
        return pad_t + (1.0 - (v - ymin) / (ymax - ymin)) * top_h

    def y_dev(v: float) -> float:
        return mid + (1.0 - (v - dmin) / (dmax - dmin)) * bot_h

    def poly(vals: list[float], yfun: Callable[[float], float], color: str, width: float = 1.5) -> str:
        pts = " ".join(f"{x_of(i):.1f},{yfun(v):.1f}" for i, v in enumerate(vals))
        return f'<polyline fill="none" stroke="{color}" stroke-width="{width}" points="{pts}" />'

    # Regime shading for key calendar events (informational, not used in fit).
    event_bands = [
        ("2020-03-01", "2020-06-30", "#3b82f622", "COVID"),
        ("2022-01-01", "2022-12-31", "#f59e0b22", "2022 rates"),
        ("2024-01-01", "2026-12-31", "#10b98122", "2024-26 advance"),
    ]
    band_rects: list[str] = []
    date_list = [p[0] for p in pairs]
    for start, end, color, _label in event_bands:
        i0 = next((i for i, d in enumerate(date_list) if d >= start), None)
        i1 = next((i for i, d in enumerate(reversed(date_list)) if d <= end), None)
        if i0 is None:
            continue
        if i1 is None:
            continue
        i1 = len(date_list) - 1 - i1
        if i1 < i0:
            continue
        x0, x1 = x_of(i0), x_of(i1)
        band_rects.append(
            f'<rect x="{x0:.1f}" y="{pad_t}" width="{max(1.0, x1 - x0):.1f}" '
            f'height="{top_h:.1f}" fill="{color}" />'
        )
        band_rects.append(
            f'<rect x="{x0:.1f}" y="{mid}" width="{max(1.0, x1 - x0):.1f}" '
            f'height="{bot_h:.1f}" fill="{color}" />'
        )

    zero_y = y_dev(0.0)
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" '
        f'style="background:#0b1220;font-family:Segoe UI,Arial,sans-serif">',
        f'<text x="{pad_l}" y="24" fill="#e2e8f0" font-size="16">{title}</text>',
        *band_rects,
        f'<line x1="{pad_l}" y1="{mid - 10}" x2="{w - pad_r}" y2="{mid - 10}" '
        f'stroke="#334155" stroke-width="1" />',
        poly(pxs, y_price, "#38bdf8", 1.8),
        poly(fvs, y_price, "#f472b6", 1.8),
        poly(dvs, y_dev, "#a3e635", 1.4),
        f'<line x1="{pad_l}" y1="{zero_y:.1f}" x2="{w - pad_r}" y2="{zero_y:.1f}" '
        f'stroke="#64748b" stroke-dasharray="4 3" stroke-width="1" />',
        f'<text x="{pad_l}" y="{pad_t + 14}" fill="#38bdf8" font-size="11">Gold price</text>',
        f'<text x="{pad_l + 90}" y="{pad_t + 14}" fill="#f472b6" font-size="11">Fair value</text>',
        f'<text x="{pad_l}" y="{mid + 16}" fill="#a3e635" font-size="11">Deviation %</text>',
        f'<text x="{pad_l}" y="{h - 12}" fill="#94a3b8" font-size="10">'
        f'{date_list[0]} → {date_list[-1]} · n={len(pairs)}</text>',
        "</svg>",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


@dataclass
class FeatureBundle:
    dates: list[str]
    prices: list[float]
    features: dict[str, list[float]]
    meta: dict[str, Any]


def build_gold_structural_feature_bundle(
    *, as_of_week: str | None = None
) -> FeatureBundle | None:
    """Weekly Gold panel with structural inputs (publication lags applied)."""
    gold = _weekly_prices("Gold")
    silver = dict(_weekly_prices("Silver"))
    if as_of_week:
        gold = [(d, p) for d, p in gold if d <= str(as_of_week)[:10]]
    if len(gold) < MIN_TRAIN + 40:
        return None

    dates = [d for d, _ in gold]
    prices = [p for _, p in gold]

    m2 = load_fred_daily_map("M2SL", observation_start="1990-01-01")
    cpi = load_fred_daily_map("CPIAUCSL", observation_start="1990-01-01")
    dfii = load_fred_daily_map(REAL_YIELD_SERIES, observation_start="2000-01-01")
    dgs10 = load_fred_daily_map("DGS10", observation_start="1990-01-01")
    dgs2 = load_fred_daily_map("DGS2", observation_start="1990-01-01")
    t10yie = load_fred_daily_map("T10YIE", observation_start="2000-01-01")
    dff = load_fred_daily_map("DFF", observation_start="1990-01-01")
    broad = load_fred_daily_map(DXY_SERIES, observation_start="2000-01-01")
    if len(broad) < 52:
        broad = _load_dxy_series()
    dx = _load_dx_daily()

    m2_w = _finite_ffill(_asof_with_lag(m2, dates, lag_days=MONTHLY_PUBLICATION_LAG_DAYS))
    cpi_w = _finite_ffill(_asof_with_lag(cpi, dates, lag_days=MONTHLY_PUBLICATION_LAG_DAYS))
    ry_w = _finite_ffill(_asof_series(dfii, dates))
    n10_w = _finite_ffill(_asof_series(dgs10, dates))
    n2_w = _finite_ffill(_asof_series(dgs2, dates))
    be_w = _finite_ffill(_asof_series(t10yie, dates))
    ff_w = _finite_ffill(_asof_series(dff, dates))
    broad_w = _finite_ffill(_asof_series(broad, dates))
    dx_raw = _finite_ffill(_asof_series(dx, dates)) if dx else None
    sil_w = _finite_ffill([silver.get(d) for d in dates])

    core_cols = [m2_w, cpi_w, ry_w, n10_w, n2_w, be_w, ff_w, broad_w, sil_w]
    start_i = _first_complete_index(core_cols)
    if start_i is None:
        return None

    dates = dates[start_i:]
    prices = prices[start_i:]
    m2_w = m2_w[start_i:]
    cpi_w = cpi_w[start_i:]
    ry_w = ry_w[start_i:]
    n10_w = n10_w[start_i:]
    n2_w = n2_w[start_i:]
    be_w = be_w[start_i:]
    ff_w = ff_w[start_i:]
    broad_w = broad_w[start_i:]
    sil_w = sil_w[start_i:]
    dx_w = dx_raw[start_i:] if dx_raw is not None else None

    if len(dates) < MIN_TRAIN + 40:
        return None
    if any(v is None for v in m2_w + cpi_w + ry_w + n10_w + n2_w + be_w + ff_w + broad_w + sil_w):
        return None

    # Dense floats after trim.
    m2_f = [float(v) for v in m2_w]  # type: ignore[arg-type]
    cpi_f = [float(v) for v in cpi_w]  # type: ignore[arg-type]
    ry_f = [float(v) for v in ry_w]  # type: ignore[arg-type]
    n10_f = [float(v) for v in n10_w]  # type: ignore[arg-type]
    n2_f = [float(v) for v in n2_w]  # type: ignore[arg-type]
    be_f = [float(v) for v in be_w]  # type: ignore[arg-type]
    ff_f = [float(v) for v in ff_w]  # type: ignore[arg-type]
    broad_f = [float(v) for v in broad_w]  # type: ignore[arg-type]
    sil_f = [float(v) for v in sil_w]  # type: ignore[arg-type]

    real_m2 = [m2_f[i] / cpi_f[i] for i in range(len(dates))]
    log_m2 = [math.log(v) for v in m2_f]
    log_cpi = [math.log(v) for v in cpi_f]
    log_broad = [math.log(v) for v in broad_f]
    log_silver = [math.log(v) for v in sil_f]
    curve = [n10_f[i] - n2_f[i] for i in range(len(dates))]
    opp_cost = [ry_f[i] + 0.01 * (broad_f[i] - broad_f[0]) for i in range(len(dates))]

    features = {
        "m2": m2_f,
        "cpi": cpi_f,
        "real_m2": real_m2,
        "log_m2": log_m2,
        "log_cpi": log_cpi,
        "real_yield": ry_f,
        "nominal_10y": n10_f,
        "nominal_2y": n2_f,
        "breakeven": be_f,
        "fed_funds": ff_f,
        "broad_usd": broad_f,
        "log_broad": log_broad,
        "curve_10y_2y": curve,
        "silver": sil_f,
        "log_silver": log_silver,
        "opp_cost_proxy": opp_cost,
    }
    if dx_w is not None and all(v is not None for v in dx_w):
        dx_f = [float(v) for v in dx_w]  # type: ignore[arg-type]
        features["dxy_ice"] = dx_f
        features["log_dxy_ice"] = [math.log(v) for v in dx_f]

    return FeatureBundle(
        dates=dates,
        prices=prices,
        features=features,
        meta={
            "n_weeks": len(dates),
            "start": dates[0],
            "end": dates[-1],
            "trimmed_leading_weeks": start_i,
            "monthly_publication_lag_days": MONTHLY_PUBLICATION_LAG_DAYS,
            "note": (
                "Canonical Gold weekly history currently begins mid-2016 in this store; "
                "leading weeks trimmed until M2/CPI (lagged) and all core inputs are available. "
                "Regime splits are applied within that available sample."
            ),
        },
    )


def _candidate_specs() -> list[dict[str, Any]]:
    """Economically motivated structural candidates (Track A/B)."""
    return [
        {
            "id": "mpp_m2_cpi_ols",
            "track": "B1_monetary_purchasing_power",
            "label": "Monetary PP OLS: log(G) ~ log(M2) + log(CPI)",
            "hypothesis": (
                "Gold's monetary purchasing-power equilibrium is jointly defined by "
                "nominal money supply and the general price level."
            ),
            "kind": "ols",
            "features": ["log_m2", "log_cpi"],
            "expected_signs": {"log_m2": "positive", "log_cpi": "positive"},
        },
        {
            "id": "mpp_real_m2_ratio",
            "track": "B1_monetary_purchasing_power",
            "label": "Real-M2 ratio expanding equilibrium",
            "hypothesis": (
                "A historically stable Gold / (M2/CPI) relationship defines fair value; "
                "deviation is the valuation gap. Expanding mean only (no future ratios)."
            ),
            "kind": "expanding_ratio",
            "scale_feature": "real_m2",
            "expected_signs": {},
        },
        {
            "id": "mpp_m2_cpi_rolling260",
            "track": "B1_monetary_purchasing_power",
            "label": "Monetary PP rolling 260w OLS: log(G) ~ log(M2) + log(CPI)",
            "hypothesis": (
                "Same purchasing-power equilibrium, but rolling coefficients allow "
                "slow monetary-regime shifts that break expanding fits."
            ),
            "kind": "ols",
            "features": ["log_m2", "log_cpi"],
            "expected_signs": {"log_m2": "positive", "log_cpi": "positive"},
            "rolling_window": 260,
        },
        {
            "id": "keynes_core3",
            "track": "B2_keynesian_liquidity",
            "label": "Keynesian core: real yield + log(broad USD) + breakeven",
            "hypothesis": (
                "Liquidity and opportunity cost jointly define Gold's state even when "
                "no single variable forecasts weekly price standalone."
            ),
            "kind": "ols",
            "features": ["real_yield", "log_broad", "breakeven"],
            "expected_signs": {
                "real_yield": "negative",
                "log_broad": "negative",
                "breakeven": "positive",
            },
        },
        {
            "id": "keynes_liq_opp",
            "track": "B2_keynesian_liquidity",
            "label": "Keynesian liquidity+policy: + fed funds",
            "hypothesis": (
                "Adding the policy rate captures ZLB vs tightening regimes that "
                "reprice the demand for liquid/safe assets."
            ),
            "kind": "ols",
            "features": ["real_yield", "log_broad", "breakeven", "fed_funds"],
            "expected_signs": {
                "real_yield": "negative",
                "log_broad": "negative",
                "breakeven": "positive",
                "fed_funds": "negative",
            },
        },
        {
            "id": "gs_ratio_expanding",
            "track": "B3_gold_silver",
            "label": "Gold–Silver expanding ratio equilibrium",
            "hypothesis": (
                "Silver provides a monetary precious-metal equilibrium anchor; "
                "fair Gold = Silver × expanding mean(Gold/Silver)."
            ),
            "kind": "expanding_ratio",
            "scale_feature": "silver",
            "expected_signs": {},
        },
        {
            "id": "gs_equilibrium_ols",
            "track": "B3_gold_silver",
            "label": "Gold ~ Silver + USD + real yield (shared-factor control)",
            "hypothesis": (
                "After controlling shared USD/rates exposure, Silver still carries "
                "precious-metal equilibrium information for Gold."
            ),
            "kind": "ols",
            "features": ["log_silver", "log_broad", "real_yield"],
            "expected_signs": {
                "log_silver": "positive",
                "log_broad": "negative",
                "real_yield": "negative",
            },
        },
        {
            "id": "carry_state",
            "track": "B4_cost_of_carry",
            "label": "Carry/rate structure: real yield + curve + breakeven",
            "hypothesis": (
                "Opportunity cost is a state: real yields, curve shape, and inflation "
                "expectations jointly — not a single yield-level regression."
            ),
            "kind": "ols",
            "features": ["real_yield", "curve_10y_2y", "breakeven"],
            "expected_signs": {
                "real_yield": "negative",
                "curve_10y_2y": "negative",
                "breakeven": "positive",
            },
        },
        {
            "id": "rel_monetary_baseline",
            "track": "B5_relative_monetary",
            "label": "Relative monetary (published-form reference): real yield + log(USD)",
            "hypothesis": (
                "Research mirror of metals_real_yield_v1 form for comparison only; "
                "does not mutate the published engine."
            ),
            "kind": "ols",
            "features": ["real_yield", "log_broad"],
            "expected_signs": {"real_yield": "negative", "log_broad": "negative"},
            "is_published_form_reference": True,
        },
        {
            "id": "rel_monetary_plus_m2",
            "track": "B5_relative_monetary",
            "label": "Relative monetary + real M2: rates/USD + log(real M2)",
            "hypothesis": (
                "Safe-haven / monetary-asset competition plus purchasing-power "
                "liquidity from real money balances."
            ),
            "kind": "ols",
            "features": ["real_yield", "log_broad", "log_m2"],
            "expected_signs": {
                "real_yield": "negative",
                "log_broad": "negative",
                "log_m2": "positive",
            },
        },
    ]


def _evaluate_candidate(bundle: FeatureBundle, spec: dict[str, Any]) -> dict[str, Any]:
    dates = bundle.dates
    prices = bundle.prices
    y_all = [math.log(p) for p in prices]

    if spec["kind"] == "expanding_ratio":
        scale = bundle.features[spec["scale_feature"]]
        fair_levels = _expanding_ratio_fair(prices, scale)
        fair_logs = _fair_logs_from_levels(fair_levels)
        # Pseudo OOS: compare log-fair vs log-price where fair exists
        idxs = [i for i, fl in enumerate(fair_logs) if fl is not None]
        preds = [fair_logs[i] for i in idxs]  # type: ignore[index]
        actuals = [y_all[i] for i in idxs]
        if len(preds) < 20:
            return {"id": spec["id"], "ok": False, "reason": "insufficient_ratio_oos"}
        err2 = [(p - a) ** 2 for p, a in zip(preds, actuals)]
        mae = sum(abs(p - a) for p, a in zip(preds, actuals)) / len(preds)
        rmse = math.sqrt(sum(err2) / len(err2))
        mean_a = sum(actuals) / len(actuals)
        ss_tot = sum((a - mean_a) ** 2 for a in actuals)
        oos_r2 = 1.0 - sum(err2) / ss_tot if ss_tot > 0 else None
        naive = _naive_oos_metrics(y_all, idxs, preds, actuals)
        eval_row = {
            "ok": True,
            "r_squared": None,
            "oos_r2": round(oos_r2, 4) if oos_r2 is not None else None,
            "oos_rmse": round(rmse, 6),
            "oos_mae": round(mae, 6),
            "n_oos": len(preds),
            "coefficients": {},
            "signs_ok": True,  # non-parametric equilibrium
            "coef_sign_flip": False,
            "coefficient_stability": {},
            **naive,
        }
        feature_names: list[str] = [spec["scale_feature"]]
    else:
        feature_names = list(spec["features"])
        fmap = {f: bundle.features[f] for f in feature_names}
        # Align to finite feature rows (should already be dense).
        d_al, y_al, x_al = _align(
            dates, y_all, {k: list(v) for k, v in fmap.items()}, feature_names
        )
        if len(y_al) < MIN_TRAIN + 40:
            return {"id": spec["id"], "ok": False, "reason": "insufficient_aligned"}
        price_map = {d: p for d, p in zip(dates, prices)}
        prices_al = [price_map[d] for d in d_al]
        cols = [x_al[f] for f in feature_names]
        eval_row = _eval_model(
            name=spec["id"],
            dates=d_al,
            y=y_al,
            feature_names=feature_names,
            cols=cols,
            expected_signs=spec.get("expected_signs") or {},
        )
        if not eval_row.get("ok"):
            return {"id": spec["id"], "ok": False, "reason": "fit_failed"}
        rolling_window = spec.get("rolling_window")
        fair_logs, _ = _walk_forward_fair_logs_multi(
            y_al, cols, rolling_window=int(rolling_window) if rolling_window else None
        )
        dates = d_al
        prices = prices_al
        y_all = y_al
        flip = _coef_flip(eval_row.get("coefficient_stability") or {})
        eval_row["coef_sign_flip"] = flip
        if rolling_window:
            # Recompute OOS forecast metrics from the rolling fair path itself.
            idxs = [i for i, fl in enumerate(fair_logs) if fl is not None]
            preds = [float(fair_logs[i]) for i in idxs]  # type: ignore[arg-type]
            actuals = [y_al[i] for i in idxs]
            if len(preds) >= 20:
                err2 = [(p - a) ** 2 for p, a in zip(preds, actuals)]
                mae = sum(abs(p - a) for p, a in zip(preds, actuals)) / len(preds)
                rmse = math.sqrt(sum(err2) / len(err2))
                mean_a = sum(actuals) / len(actuals)
                ss_tot = sum((a - mean_a) ** 2 for a in actuals)
                oos_r2 = 1.0 - sum(err2) / ss_tot if ss_tot > 0 else None
                eval_row["oos_r2"] = round(oos_r2, 4) if oos_r2 is not None else None
                eval_row["oos_rmse"] = round(rmse, 6)
                eval_row["oos_mae"] = round(mae, 6)
                eval_row["n_oos"] = len(preds)
                eval_row["_indices"] = idxs
                eval_row["_preds"] = preds
                eval_row["_actuals"] = actuals
        naive = _naive_oos_metrics(
            y_al,
            list(eval_row.get("_indices") or []),
            list(eval_row.get("_preds") or []),
            list(eval_row.get("_actuals") or []),
        )
        eval_row.update(naive)

    deviations = _deviation_series(prices, fair_logs)
    extreme_rows = _bucket_forward_returns(dates, prices, deviations)
    usefulness = _valuation_usefulness(extreme_rows, horizon=13)
    duplication = _price_duplication_stats(prices, fair_logs)

    # Fair-value volatility vs price volatility (should not be near-zero noise).
    fair_rets: list[float] = []
    px_rets: list[float] = []
    prev_f: float | None = None
    prev_p: float | None = None
    for px, fl in zip(prices, fair_logs):
        if fl is None:
            continue
        f = math.exp(fl)
        if prev_f and prev_p and f > 0 and px > 0:
            fair_rets.append(abs(math.log(f / prev_f)))
            px_rets.append(abs(math.log(px / prev_p)))
        prev_f, prev_p = f, px
    fair_vol = (sum(fair_rets) / len(fair_rets)) if fair_rets else None
    px_vol = (sum(px_rets) / len(px_rets)) if px_rets else None
    fair_vol_ok = (
        fair_vol is not None
        and px_vol is not None
        and fair_vol > 0.15 * px_vol
        and fair_vol < 3.0 * px_vol
    )

    scored = _structural_score(
        signs_ok=bool(eval_row.get("signs_ok")),
        flip=bool(eval_row.get("coef_sign_flip")),
        oos_r2=eval_row.get("oos_r2"),
        vs_naive_impr=eval_row.get("rmse_vs_naive_impr_pct"),
        usefulness=usefulness,
        duplication=duplication,
        fair_vol_ok=bool(fair_vol_ok),
    )

    # Regime tags need rate/dollar/be series aligned to evaluation dates.
    idx_map = {d: i for i, d in enumerate(bundle.dates)}
    dff_al = [bundle.features["fed_funds"][idx_map[d]] for d in dates]
    dxy_al = [bundle.features["broad_usd"][idx_map[d]] for d in dates]
    be_al = [bundle.features["breakeven"][idx_map[d]] for d in dates]
    tags = _regime_labels(dates, dff_al, dxy_al, be_al)
    regimes = _regime_results(dates, prices, deviations, tags, horizon=13)

    decision = "Reject"
    score = scored["structural_score"]
    spread = usefulness.get("spread_pp")
    useful_ok = spread is not None and math.isfinite(float(spread)) and float(spread) > 0.0
    high_corr = (
        duplication.get("corr_price_fair") is not None
        and float(duplication["corr_price_fair"]) >= 0.99
    )
    if duplication.get("is_price_mirror") or high_corr:
        decision = "Reject_price_mirror"
    elif (
        score >= SCORE_PROMOTE
        and eval_row.get("signs_ok")
        and not eval_row.get("coef_sign_flip")
        and useful_ok
        and not duplication.get("is_price_mirror")
    ):
        decision = "Promote_candidate"
    elif score >= SCORE_CONTINUE and useful_ok:
        decision = "Continue"
    elif score >= SCORE_CONTINUE:
        # Attractive fit metrics but wrong-way or missing valuation usefulness.
        decision = "Continue_weak_valuation"
    else:
        decision = "Reject"

    # Sensitivity: drop first 52 weeks and re-score usefulness quickly.
    sens = None
    if len(dates) > MIN_TRAIN + 92:
        d2, p2, fl2 = dates[52:], prices[52:], fair_logs[52:]
        dev2 = _deviation_series(p2, fl2)
        ext2 = _bucket_forward_returns(d2, p2, dev2)
        u2 = _valuation_usefulness(ext2, horizon=13)
        sens = {
            "drop_first_52w_spread_pp": u2.get("spread_pp"),
            "spread_delta_pp": (
                None
                if u2.get("spread_pp") is None or usefulness.get("spread_pp") is None
                else round(float(u2["spread_pp"]) - float(usefulness["spread_pp"]), 3)
            ),
        }

    return {
        "id": spec["id"],
        "ok": True,
        "track": spec["track"],
        "label": spec["label"],
        "hypothesis": spec["hypothesis"],
        "kind": spec["kind"],
        "features": feature_names,
        "is_published_form_reference": bool(spec.get("is_published_form_reference")),
        "sample_start": dates[0],
        "sample_end": dates[-1],
        "n_weeks": len(dates),
        "in_sample_r2": eval_row.get("r_squared"),
        "oos_r2": eval_row.get("oos_r2"),
        "oos_rmse": eval_row.get("oos_rmse"),
        "oos_mae": eval_row.get("oos_mae"),
        "naive_oos_rmse": eval_row.get("naive_oos_rmse"),
        "rmse_vs_naive_impr_pct": eval_row.get("rmse_vs_naive_impr_pct"),
        "n_oos": eval_row.get("n_oos"),
        "coefficients": eval_row.get("coefficients"),
        "expected_signs": spec.get("expected_signs") or {},
        "signs_ok": eval_row.get("signs_ok"),
        "coef_sign_flip": eval_row.get("coef_sign_flip"),
        "coefficient_stability": eval_row.get("coefficient_stability"),
        "valuation_usefulness": usefulness,
        "price_duplication": duplication,
        "fair_vol_vs_price": {
            "fair_mean_abs_logret": round(fair_vol, 6) if fair_vol is not None else None,
            "price_mean_abs_logret": round(px_vol, 6) if px_vol is not None else None,
            "fair_vol_ok": fair_vol_ok,
        },
        "start_date_sensitivity": sens,
        "structural_score": scored["structural_score"],
        "score_parts": scored["score_parts"],
        "decision": decision,
        "extreme_returns": extreme_rows,
        "regime_results": regimes,
        "_dates": dates,
        "_prices": prices,
        "_fair_logs": fair_logs,
        "_deviations": deviations,
    }


def _verdict(ranking: list[dict[str, Any]]) -> dict[str, Any]:
    promote = [r for r in ranking if r.get("decision") == "Promote_candidate"]
    cont = [
        r
        for r in ranking
        if r.get("decision") in {"Continue", "Continue_weak_valuation"}
        and not r.get("is_published_form_reference")
    ]
    # Prefer candidates with correctly signed valuation usefulness.
    cont_useful = [
        r
        for r in cont
        if ((r.get("valuation_usefulness") or {}).get("spread_pp") or -1e9) > 0
    ]
    primary = [r for r in promote if not r.get("is_published_form_reference")]
    if primary:
        best = primary[0]
        return {
            "verdict": "PROMOTE",
            "strongest_candidate": best["id"],
            "narrative": (
                f"{best['id']} cleared structural gates with score "
                f"{best['structural_score']} and useful valuation bucket behaviour."
            ),
        }
    if cont_useful:
        best = cont_useful[0]
    elif cont:
        # Among weak leads, prefer the least-wrong valuation spread, then score.
        # Do not crown a high-R² price-tracking twin with a large negative spread.
        def _weak_key(r: dict[str, Any]) -> tuple[float, float]:
            sp = (r.get("valuation_usefulness") or {}).get("spread_pp")
            sp_v = float(sp) if sp is not None and math.isfinite(float(sp)) else -1e9
            return (sp_v, float(r.get("structural_score") or 0.0))

        best = sorted(cont, key=_weak_key, reverse=True)[0]
    else:
        best = None
    if best is not None:
        return {
            "verdict": "CONTINUE_RESEARCH",
            "strongest_candidate": best["id"],
            "narrative": (
                f"No candidate cleared the promote bar. Strongest research lead is "
                f"{best['id']} (score={best['structural_score']}, "
                f"decision={best['decision']}, "
                f"useful_spread_13w={(best.get('valuation_usefulness') or {}).get('spread_pp')}). "
                f"Refine transforms / sample / state-dependence; do not deploy. "
                f"Note: high price-tracking alone (e.g. Gold/Silver ratio) is not sufficient "
                f"if undervalued buckets do not earn higher subsequent returns."
            ),
        }
    return {
        "verdict": "REJECT_APPROACH",
        "strongest_candidate": ranking[0]["id"] if ranking else None,
        "narrative": (
            "No structural candidate produced stable, economically useful valuation "
            "behaviour under walk-forward and regime checks within the available sample."
        ),
    }


def run_gold_structural_valuation_research(
    *, as_of_week: str | None = None
) -> dict[str, Any]:
    t0 = datetime.now(timezone.utc)
    generated_at = t0.replace(microsecond=0).isoformat()

    # Safety: published models remain untouched (identity check only).
    published_panel = _build_weekly_panel("Gold")
    published_model_id = PUBLISHED_GOLD_MODEL_ID

    bundle = build_gold_structural_feature_bundle(as_of_week=as_of_week)
    if bundle is None:
        return {
            "generated_at": generated_at,
            "ok": False,
            "error": "Could not build Gold structural feature bundle",
            "research_only": True,
        }

    results: list[dict[str, Any]] = []
    for spec in _candidate_specs():
        row = _evaluate_candidate(bundle, spec)
        results.append(row)

    ok_rows = [r for r in results if r.get("ok")]
    ok_rows.sort(key=lambda r: float(r.get("structural_score") or 0.0), reverse=True)
    for i, r in enumerate(ok_rows, start=1):
        r["rank"] = i

    verdict = _verdict(ok_rows)
    transfer = ng_methodology_transfer_notes()

    # Charts for top candidates + strongest
    chart_paths: list[str] = []
    chart_candidates = ok_rows[:4]
    strongest_id = verdict.get("strongest_candidate")
    if strongest_id and all(c["id"] != strongest_id for c in chart_candidates):
        for r in ok_rows:
            if r["id"] == strongest_id:
                chart_candidates.append(r)
                break

    CHART_DIR.mkdir(parents=True, exist_ok=True)
    for r in chart_candidates:
        path = CHART_DIR / f"{r['id']}_sync.svg"
        _write_sync_chart_svg(
            path,
            title=f"Gold structural: {r['id']}",
            dates=list(r.get("_dates") or []),
            prices=list(r.get("_prices") or []),
            fair_logs=list(r.get("_fair_logs") or []),
            deviations=list(r.get("_deviations") or []),
        )
        chart_paths.append(str(path.relative_to(PROJECT_ROOT)).replace("\\", "/"))
        r["chart"] = chart_paths[-1]

    # Strip heavy series from ranking JSON (keep paths).
    ranking_public = []
    for r in ok_rows:
        pub = {k: v for k, v in r.items() if not str(k).startswith("_")}
        ranking_public.append(pub)

    failed = [r for r in results if not r.get("ok")]

    payload = {
        "generated_at": generated_at,
        "ok": True,
        "research_only": True,
        "published_models_untouched": {
            "gold_model_id": published_model_id,
            "gold_panel_n": len(published_panel),
            "ng_engine": "ng_storage_production_v2",
            "note": "This research writes only under data/audits/gold_structural_valuation/.",
        },
        "ng_methodology_transfer": transfer,
        "panel": bundle.meta,
        "walk_forward": {"min_train": MIN_TRAIN, "step": STEP},
        "leakage_controls": {
            "monthly_publication_lag_days": MONTHLY_PUBLICATION_LAG_DAYS,
            "as_of_joins": True,
            "expanding_only_scalers": True,
            "no_full_sample_thresholds": True,
            "standalone_tier1_gate_untouched": True,
        },
        "candidates_tested": len(results),
        "ranking": ranking_public,
        "failed_candidates": failed,
        "charts": chart_paths,
        "verdict": verdict,
        "runtime_sec": round((datetime.now(timezone.utc) - t0).total_seconds(), 2),
        # Keep private series for CSV/report writers in-process.
        "_ok_rows_private": ok_rows,
    }
    return payload


def _write_metrics_csv(rows: list[dict[str, Any]], path: Path) -> None:
    fields = [
        "rank",
        "id",
        "track",
        "decision",
        "structural_score",
        "signs_ok",
        "coef_sign_flip",
        "oos_r2",
        "oos_rmse",
        "oos_mae",
        "naive_oos_rmse",
        "rmse_vs_naive_impr_pct",
        "in_sample_r2",
        "n_oos",
        "sample_start",
        "sample_end",
        "usefulness_spread_pp",
        "median_abs_dev_pct",
        "corr_price_fair",
        "is_price_mirror",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in rows:
            u = r.get("valuation_usefulness") or {}
            d = r.get("price_duplication") or {}
            w.writerow(
                {
                    "rank": r.get("rank"),
                    "id": r.get("id"),
                    "track": r.get("track"),
                    "decision": r.get("decision"),
                    "structural_score": r.get("structural_score"),
                    "signs_ok": r.get("signs_ok"),
                    "coef_sign_flip": r.get("coef_sign_flip"),
                    "oos_r2": r.get("oos_r2"),
                    "oos_rmse": r.get("oos_rmse"),
                    "oos_mae": r.get("oos_mae"),
                    "naive_oos_rmse": r.get("naive_oos_rmse"),
                    "rmse_vs_naive_impr_pct": r.get("rmse_vs_naive_impr_pct"),
                    "in_sample_r2": r.get("in_sample_r2"),
                    "n_oos": r.get("n_oos"),
                    "sample_start": r.get("sample_start"),
                    "sample_end": r.get("sample_end"),
                    "usefulness_spread_pp": u.get("spread_pp"),
                    "median_abs_dev_pct": d.get("median_abs_dev_pct"),
                    "corr_price_fair": d.get("corr_price_fair"),
                    "is_price_mirror": d.get("is_price_mirror"),
                }
            )


def _write_regime_csv(rows: list[dict[str, Any]], path: Path) -> None:
    fields = [
        "candidate_id",
        "dimension",
        "regime",
        "horizon_weeks",
        "n_under",
        "n_over",
        "under_mean_fwd_pct",
        "over_mean_fwd_pct",
        "spread_under_minus_over_pp",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in rows:
            cid = r.get("id")
            for rr in r.get("regime_results") or []:
                w.writerow({"candidate_id": cid, **rr})


def _write_extreme_csv(rows: list[dict[str, Any]], path: Path) -> None:
    fields = [
        "candidate_id",
        "bucket",
        "horizon_weeks",
        "n",
        "mean_return_pct",
        "median_return_pct",
        "positive_return_rate",
        "max_adverse_excursion_mean",
        "avg_weeks_toward_fair",
        "pct_extremes_toward_equilibrium",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in rows:
            cid = r.get("id")
            for er in r.get("extreme_returns") or []:
                w.writerow({"candidate_id": cid, **er})


def render_markdown(payload: dict[str, Any]) -> str:
    transfer = payload.get("ng_methodology_transfer") or {}
    verdict = payload.get("verdict") or {}
    panel = payload.get("panel") or {}
    lines: list[str] = [
        "# Gold Structural / Keynesian Valuation Research",
        "",
        f"Generated: `{payload.get('generated_at')}`",
        "",
        "**Research only — not deployed. Published models untouched.**",
        "",
        f"**Verdict: {verdict.get('verdict')}**",
        "",
        verdict.get("narrative") or "",
        "",
        "## 1. Natural Gas methodology (accepted)",
        "",
        f"- Engine: `{transfer.get('accepted_ng_engine')}`",
        f"- Fair value: `{transfer.get('fair_value_form')}`",
        f"- Structural method: {transfer.get('keynesian_or_structural_method')}",
        f"- Walk-forward: `{transfer.get('estimation_rules')}`",
        f"- Deviation: `{transfer.get('deviation')}`",
        "",
        "### Transferable",
        "",
    ]
    for item in transfer.get("transferable_to_gold") or []:
        lines.append(f"- {item}")
    lines.extend(["", "### Market-specific (do not reuse)", ""])
    for item in transfer.get("market_specific_do_not_reuse") or []:
        lines.append(f"- {item}")
    lines.extend(
        [
            "",
            f"Adaptation principle: {transfer.get('gold_adaptation_principle')}",
            "",
            "## 2. Gold sample",
            "",
            f"- Weeks: **{panel.get('n_weeks')}** ({panel.get('start')} → {panel.get('end')})",
            f"- Monthly publication lag: **{panel.get('monthly_publication_lag_days')}** days",
            f"- Note: {panel.get('note')}",
            "",
            "## 3. Candidate ranking",
            "",
            "| Rank | ID | Track | Score | Decision | OOS R2 | vs Naive RMSE% | Signs | Flip | Useful spread 13w |",
            "| ---: | --- | --- | ---: | --- | ---: | ---: | --- | --- | ---: |",
        ]
    )
    for r in payload.get("ranking") or []:
        u = r.get("valuation_usefulness") or {}
        lines.append(
            f"| {r.get('rank')} | `{r.get('id')}` | {r.get('track')} | "
            f"{r.get('structural_score')} | {r.get('decision')} | {r.get('oos_r2')} | "
            f"{r.get('rmse_vs_naive_impr_pct')} | {r.get('signs_ok')} | "
            f"{r.get('coef_sign_flip')} | {u.get('spread_pp')} |"
        )
    lines.extend(["", "## 4. Strongest candidate detail", ""])
    strongest = None
    sid = verdict.get("strongest_candidate")
    for r in payload.get("ranking") or []:
        if r.get("id") == sid:
            strongest = r
            break
    if strongest:
        lines.extend(
            [
                f"- **ID:** `{strongest.get('id')}`",
                f"- **Hypothesis:** {strongest.get('hypothesis')}",
                f"- **Features:** {strongest.get('features')}",
                f"- **Coefficients:** `{strongest.get('coefficients')}`",
                f"- **OOS R² / RMSE / MAE:** {strongest.get('oos_r2')} / "
                f"{strongest.get('oos_rmse')} / {strongest.get('oos_mae')}",
                f"- **Price duplication:** `{strongest.get('price_duplication')}`",
                f"- **Chart:** `{strongest.get('chart')}`",
                "",
                "### 13-week bucket returns (strongest)",
                "",
                "| Bucket | n | Mean % | Median % | Hit rate | Toward eq % |",
                "| --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for er in strongest.get("extreme_returns") or []:
            if er.get("horizon_weeks") != 13:
                continue
            lines.append(
                f"| {er.get('bucket')} | {er.get('n')} | {er.get('mean_return_pct')} | "
                f"{er.get('median_return_pct')} | {er.get('positive_return_rate')} | "
                f"{er.get('pct_extremes_toward_equilibrium')} |"
            )
    lines.extend(
        [
            "",
            "## 5. Interpretation (this round)",
            "",
            "- Standalone Tier-1 Reject result stands: rates/USD variables do **not** "
            "earn Keep as independent weekly forecasters.",
            "- Structural combinations were tested with economic rationale only.",
            "- In the 2016–2026 Gold bull sample, most 'overvalued' states still earned "
            "positive forward returns — valuation usefulness is weak / wrong-way for "
            "pooled under vs over buckets.",
            "- Monetary purchasing-power (`mpp_m2_cpi_ols`) is the least-bad lead: "
            "correct coefficient signs in tip fit, independent fair value "
            "(median |dev| ~12%), but walk-forward coefficient flips block promotion.",
            "- Gold/Silver expanding ratio tracks price well but produces wrong-way "
            "valuation buckets — rejected as a promote candidate.",
            "- Keynesian liquidity / carry / published-form reference specs fail "
            "sign stability and do not clear useful-valuation gates.",
            "",
            "### Suggested next rounds (research only)",
            "",
            "1. Extend Gold weekly history pre-2016 if a consistent continuous series exists.",
            "2. Regime-conditioned M2/CPI (state-dependent β around QE / hiking cycles).",
            "3. Error-correction around a cointegrating monetary relation (levels FV + "
            "lagged residual dynamics) without forcing short-horizon R².",
            "4. Keep Bitcoin / Copper excluded until a clear monetary role is evidenced.",
            "",
            "## 6. Safety",
            "",
            f"- Published Gold model id unchanged: `{PUBLISHED_GOLD_MODEL_ID}`",
            "- Standalone Tier-1 ranking gate not weakened",
            "- No production valuation endpoint modified",
            "- Outputs confined to `data/audits/gold_structural_valuation/`",
            "",
            "## 7. Charts",
            "",
        ]
    )
    for c in payload.get("charts") or []:
        lines.append(f"- `{c}`")
    lines.extend(["", f"Runtime: {payload.get('runtime_sec')}s", ""])
    return "\n".join(lines)


def write_outputs(payload: dict[str, Any]) -> dict[str, str]:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    private = list(payload.get("_ok_rows_private") or [])
    public = {k: v for k, v in payload.items() if not str(k).startswith("_")}

    RANKING_JSON.write_text(json.dumps(public, indent=2, ensure_ascii=False), encoding="utf-8")
    REPORT_MD.write_text(render_markdown(public), encoding="utf-8")
    _write_metrics_csv(private or public.get("ranking") or [], METRICS_CSV)
    _write_regime_csv(private or public.get("ranking") or [], REGIME_CSV)
    _write_extreme_csv(private or public.get("ranking") or [], EXTREME_CSV)

    return {
        "report": str(REPORT_MD.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "ranking_json": str(RANKING_JSON.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "metrics_csv": str(METRICS_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "regime_csv": str(REGIME_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "extreme_csv": str(EXTREME_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "charts_dir": str(CHART_DIR.relative_to(PROJECT_ROOT)).replace("\\", "/"),
    }
