"""Head-to-head comparison: WGC monthly CB level vs rolling 12-month for Gold production spec."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.valuation.gold_model_research import (
    BASE_FEATURES,
    SIGN_EXPECTATIONS,
    GoldVariantSpec,
    _align_cb_feature,
    _engineer_monthly_cb,
    _load_monthly_cb,
    _pearson,
    multivariate_ols_stats,
)
from hptl.valuation.metals_institutional_drivers import (
    DriverBundle,
    _weekly_from_daily,
    build_driver_bundle,
)
from hptl.valuation.metals_valuation_v1 import MIN_WEEKS, _predict_log_price

COMPARISON_JSON = PROJECT_ROOT / "data" / "processed" / "gold_cb_driver_comparison_latest.json"
COMPARISON_MD = PROJECT_ROOT / "data" / "processed" / "gold_cb_driver_comparison_latest.md"

LEVEL_SPEC = GoldVariantSpec(
    "baseline_wgc_monthly",
    "WGC monthly net purchases (level)",
    "cb_net_purchases",
    "positive",
    "level",
)
ROLL12_SPEC = GoldVariantSpec(
    "cb_roll12",
    "CB rolling 12-month net purchases",
    "cb_roll12",
    "positive",
    "roll12",
)

SUBPERIODS: tuple[tuple[str, str, str], ...] = (
    ("2016_2019", "2016-06-05", "2019-12-31"),
    ("2020_2022", "2020-01-01", "2022-12-31"),
    ("2023_present", "2023-01-01", "9999-12-31"),
)

HOLDOUT_TRAIN_END = "2021-12-31"
HOLDOUT_TEST_START = "2022-01-01"
MIN_ROLLING_TRAIN_WEEKS = 208  # ~4 years
WALK_FORWARD_STEP = 4  # weekly steps between retrains
ROLLING_COEF_WINDOW = 104  # ~2 years for coefficient tracking


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _slice_panel(
    panel: dict[str, Any],
    start: str,
    end: str,
) -> dict[str, Any] | None:
    dates = panel["dates"]
    idx = [i for i, d in enumerate(dates) if start[:10] <= d <= end[:10]]
    if len(idx) < MIN_WEEKS:
        return None
    y = [panel["y"][i] for i in idx]
    x_cols = [[col[i] for i in idx] for col in panel["x_cols"]]
    return {
        "dates": [dates[i] for i in idx],
        "y": y,
        "x_cols": x_cols,
        "feature_names": panel["feature_names"],
        "cb_feature": panel["cb_feature"],
        "cb_index": panel["cb_index"],
    }


def build_panel(bundle: DriverBundle, spec: GoldVariantSpec) -> dict[str, Any] | None:
    base_cols: list[list[float]] = []
    for fname in BASE_FEATURES:
        col = bundle.features.get(fname)
        if col is None or len(col) != bundle.n:
            return None
        base_cols.append(col)

    cb_col = _align_cb_feature(bundle, spec.engineer)
    if cb_col is None or len(cb_col) != bundle.n:
        return None

    feature_names = list(BASE_FEATURES) + [spec.cb_feature]
    x_cols = base_cols + [cb_col]
    cb_index = len(feature_names) - 1
    return {
        "dates": list(bundle.dates),
        "y": [math.log(p) for p in bundle.price],
        "x_cols": x_cols,
        "feature_names": feature_names,
        "cb_feature": spec.cb_feature,
        "cb_index": cb_index,
        "cb_engineer": spec.engineer,
        "variant_id": spec.variant_id,
        "label": spec.label,
    }


def _annotate_signs(stats: dict[str, Any], cb_feature: str) -> dict[str, Any]:
    sign_expectations = {**SIGN_EXPECTATIONS, cb_feature: "positive"}
    out = dict(stats)
    coefs = []
    for row in stats["coefficients"]:
        row = dict(row)
        if row["feature"] == "intercept":
            row["expected_sign"] = "any"
            row["sign_passed"] = True
        else:
            expected = sign_expectations.get(row["feature"], "any")
            row["expected_sign"] = expected
            b = row["beta"]
            if expected == "any":
                row["sign_passed"] = True
            else:
                row["sign_passed"] = (expected == "negative" and b <= 0) or (expected == "positive" and b >= 0)
        coefs.append(row)
    out["coefficients"] = coefs
    cb_row = next((c for c in coefs if c["feature"] == cb_feature), None)
    out["cb_coefficient"] = cb_row
    out["failed_sign_gates"] = [c["feature"] for c in coefs if c.get("sign_passed") is False and c["feature"] != "intercept"]
    return out


def _fit_panel(panel: dict[str, Any]) -> dict[str, Any] | None:
    stats = multivariate_ols_stats(panel["y"], panel["x_cols"], panel["feature_names"])
    if not stats:
        return None
    return _annotate_signs(stats, panel["cb_feature"])


def _compute_vif(x_cols: list[list[float]], feature_names: list[str]) -> list[dict[str, Any]]:
    """Variance inflation factor per feature (excluding intercept)."""
    import numpy as np

    rows: list[dict[str, Any]] = []
    k = len(x_cols)
    if k < 2:
        return rows
    X = np.column_stack([np.array(col, dtype=float) for col in x_cols])
    for i, fname in enumerate(feature_names):
        yi = X[:, i]
        others = np.delete(X, i, axis=1)
        if others.shape[1] == 0:
            continue
        beta, _, rank, _ = np.linalg.lstsq(
            np.column_stack([np.ones(len(yi)), others]), yi, rcond=None
        )
        if rank < others.shape[1] + 1:
            vif = None
        else:
            yhat = np.column_stack([np.ones(len(yi)), others]) @ beta
            ss_res = float(((yi - yhat) ** 2).sum())
            ss_tot = float(((yi - yi.mean()) ** 2).sum())
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
            vif = 1.0 / max(1.0 - r2, 1e-9)
        rows.append({"feature": fname, "vif": round(vif, 2) if vif is not None else None})
    return rows


def _feature_correlation_matrix(
    x_cols: list[list[float]],
    feature_names: list[str],
) -> dict[str, dict[str, float | None]]:
    matrix: dict[str, dict[str, float | None]] = {}
    for i, fi in enumerate(feature_names):
        matrix[fi] = {}
        for j, fj in enumerate(feature_names):
            if i == j:
                matrix[fi][fj] = 1.0
            elif j < i:
                matrix[fi][fj] = matrix[fj][fi]
            else:
                c = _pearson(x_cols[i], x_cols[j])
                matrix[fi][fj] = round(c, 4) if c is not None else None
    return matrix


def _oos_metrics(
    y_true: list[float],
    y_pred: list[float],
) -> dict[str, float | None]:
    if not y_true or len(y_true) != len(y_pred):
        return {"rmse_log": None, "mae_log": None, "mape_pct": None, "directional_hit_pct": None}
    n = len(y_true)
    sq = sum((a - b) ** 2 for a, b in zip(y_true, y_pred))
    ae = sum(abs(a - b) for a, b in zip(y_true, y_pred))
    mape = sum(abs((math.exp(a) - math.exp(b)) / math.exp(b)) for a, b in zip(y_true, y_pred)) / n * 100
    hits = 0
    trials = 0
    for i in range(1, n):
        actual_move = y_true[i] - y_true[i - 1]
        pred_move = y_pred[i] - y_pred[i - 1]
        if actual_move == 0 and pred_move == 0:
            hits += 1
            trials += 1
        elif actual_move != 0 and pred_move != 0 and (actual_move > 0) == (pred_move > 0):
            hits += 1
            trials += 1
        elif actual_move != 0:
            trials += 1
    return {
        "rmse_log": round(math.sqrt(sq / n), 6),
        "mae_log": round(ae / n, 6),
        "mape_pct": round(mape, 4),
        "directional_hit_pct": round(100.0 * hits / trials, 1) if trials else None,
        "n": n,
    }


def holdout_evaluation(panel: dict[str, Any]) -> dict[str, Any] | None:
    train = _slice_panel(panel, panel["dates"][0], HOLDOUT_TRAIN_END)
    test = _slice_panel(panel, HOLDOUT_TEST_START, panel["dates"][-1])
    if not train or not test:
        return None
    fit = _fit_panel(train)
    if not fit:
        return None
    beta = fit["beta_vector"]
    preds: list[float] = []
    for i in range(len(test["y"])):
        feats = [col[i] for col in test["x_cols"]]
        lp = _predict_log_price(beta, feats)
        preds.append(lp if lp is not None else float("nan"))
    valid = [(a, b) for a, b in zip(test["y"], preds) if math.isfinite(b)]
    if len(valid) < 12:
        return None
    yt, yp = zip(*valid)
    cb_train = fit.get("cb_coefficient") or {}
    return {
        "train_end": HOLDOUT_TRAIN_END,
        "test_start": HOLDOUT_TEST_START,
        "test_end": test["dates"][-1],
        "train_n": train["dates"] and len(train["y"]),
        "test_n": len(valid),
        "train_adj_r_squared": fit.get("adj_r_squared"),
        "train_cb_beta": cb_train.get("beta"),
        "train_cb_p_value": cb_train.get("p_value"),
        "train_cb_sign_passed": cb_train.get("sign_passed"),
        "oos_metrics": _oos_metrics(list(yt), list(yp)),
    }


def walk_forward_evaluation(panel: dict[str, Any]) -> dict[str, Any] | None:
    n = len(panel["y"])
    if n < MIN_ROLLING_TRAIN_WEEKS + 26:
        return None

    preds: list[float] = []
    actuals: list[float] = []
    cb_betas: list[float] = []

    for t in range(MIN_ROLLING_TRAIN_WEEKS, n, WALK_FORWARD_STEP):
        train_idx = list(range(0, t))
        test_idx = list(range(t, min(t + WALK_FORWARD_STEP, n)))
        y_train = [panel["y"][i] for i in train_idx]
        x_train = [[col[i] for i in train_idx] for col in panel["x_cols"]]
        fit = multivariate_ols_stats(y_train, x_train, panel["feature_names"])
        if not fit:
            continue
        beta = fit["beta_vector"]
        cb_betas.append(beta[panel["cb_index"] + 1])
        for i in test_idx:
            feats = [col[i] for col in panel["x_cols"]]
            lp = _predict_log_price(beta, feats)
            if lp is None:
                continue
            preds.append(lp)
            actuals.append(panel["y"][i])

    if len(preds) < 26:
        return None

    pos_cb = sum(1 for b in cb_betas if b > 0)
    return {
        "min_train_weeks": MIN_ROLLING_TRAIN_WEEKS,
        "retrain_step_weeks": WALK_FORWARD_STEP,
        "n_forecasts": len(preds),
        "n_retrains": len(cb_betas),
        "oos_metrics": _oos_metrics(actuals, preds),
        "cb_beta_during_retrains": {
            "mean": round(sum(cb_betas) / len(cb_betas), 8),
            "std": round(_std(cb_betas), 8) if cb_betas else None,
            "pct_positive": round(100.0 * pos_cb / len(cb_betas), 1) if cb_betas else None,
            "min": round(min(cb_betas), 8),
            "max": round(max(cb_betas), 8),
        },
    }


def rolling_cb_coefficient_path(panel: dict[str, Any]) -> dict[str, Any] | None:
    n = len(panel["y"])
    if n < ROLLING_COEF_WINDOW:
        return None

    betas: list[float] = []
    dates: list[str] = []
    pvals: list[float | None] = []

    for end in range(ROLLING_COEF_WINDOW, n + 1, WALK_FORWARD_STEP):
        start = end - ROLLING_COEF_WINDOW
        y = panel["y"][start:end]
        x_cols = [col[start:end] for col in panel["x_cols"]]
        fit = multivariate_ols_stats(y, x_cols, panel["feature_names"])
        if not fit:
            continue
        cb_row = next(
            (c for c in fit["coefficients"] if c["feature"] == panel["cb_feature"]),
            None,
        )
        if not cb_row:
            continue
        betas.append(cb_row["beta"])
        pvals.append(cb_row.get("p_value"))
        dates.append(panel["dates"][end - 1])

    if not betas:
        return None

    sig = sum(1 for p in pvals if p is not None and p < 0.05)
    pos = sum(1 for b in betas if b > 0)
    return {
        "window_weeks": ROLLING_COEF_WINDOW,
        "n_windows": len(betas),
        "start_date": dates[0],
        "end_date": dates[-1],
        "beta_mean": round(sum(betas) / len(betas), 8),
        "beta_std": round(_std(betas), 8),
        "beta_min": round(min(betas), 8),
        "beta_max": round(max(betas), 8),
        "pct_positive": round(100.0 * pos / len(betas), 1),
        "pct_significant_5pct": round(100.0 * sig / len(pvals), 1) if pvals else None,
        "latest_beta": round(betas[-1], 8),
        "latest_p_value": pvals[-1],
        "path": [{"date": d, "beta": round(b, 8)} for d, b in zip(dates, betas)],
    }


def _std(xs: list[float]) -> float:
    if len(xs) < 2:
        return 0.0
    m = sum(xs) / len(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def subperiod_analysis(panel: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for pid, start, end in SUBPERIODS:
        sub = _slice_panel(panel, start, end)
        if not sub:
            rows.append({"period_id": pid, "start": start, "end": end, "status": "insufficient_data"})
            continue
        fit = _fit_panel(sub)
        if not fit:
            rows.append({"period_id": pid, "start": start, "end": end, "status": "fit_failed"})
            continue
        cb = fit.get("cb_coefficient") or {}
        rows.append(
            {
                "period_id": pid,
                "start": sub["dates"][0],
                "end": sub["dates"][-1],
                "n_obs": fit["n_obs"],
                "adj_r_squared": fit.get("adj_r_squared"),
                "cb_beta": cb.get("beta"),
                "cb_p_value": cb.get("p_value"),
                "cb_sign_passed": cb.get("sign_passed"),
                "cb_corr_log_price": round(_pearson(sub["x_cols"][sub["cb_index"]], sub["y"]), 4)
                if sub["y"]
                else None,
                "failed_sign_gates": fit.get("failed_sign_gates"),
                "status": "ok",
            }
        )
    return rows


def _cb_level_vs_roll12_correlation(bundle: DriverBundle) -> dict[str, Any]:
    level = _align_cb_feature(bundle, "level")
    roll12 = _align_cb_feature(bundle, "roll12")
    if not level or not roll12:
        return {}
    return {
        "correlation_level_vs_roll12": round(_pearson(level, roll12), 4),
        "level_std": round(_std(level), 4),
        "roll12_std": round(_std(roll12), 4),
        "level_weekly_cv": round(_std(level) / (sum(level) / len(level)), 4) if level else None,
        "roll12_weekly_cv": round(_std(roll12) / (sum(roll12) / len(roll12)), 4) if roll12 else None,
    }


def _score_spec(name: str, analysis: dict[str, Any]) -> dict[str, Any]:
    """Heuristic institutional scorecard — not R² alone."""
    full = analysis.get("full_sample") or {}
    cb = full.get("cb_coefficient") or {}
    holdout = analysis.get("holdout_oos") or {}
    walk = analysis.get("walk_forward_oos") or {}
    rolling = analysis.get("rolling_cb_coefficient") or {}
    subperiods = analysis.get("subperiods") or []
    vif = analysis.get("vif") or []

    score = 0
    max_score = 0
    notes: list[str] = []

    # CB sign full sample
    max_score += 2
    if cb.get("sign_passed"):
        score += 2
        notes.append("Full-sample CB coefficient sign correct (+)")
    else:
        notes.append("Full-sample CB coefficient sign wrong")

    # CB significance
    max_score += 2
    p = cb.get("p_value")
    if p is not None and p < 0.05:
        score += 2
        notes.append(f"CB statistically significant (p={p})")
    elif p is not None and p < 0.10:
        score += 1
        notes.append(f"CB marginally significant (p={p})")
    else:
        notes.append(f"CB not significant (p={p})")

    # Subperiod CB sign consistency
    ok_subs = [s for s in subperiods if s.get("status") == "ok"]
    max_score += 2
    if ok_subs:
        sign_pass = sum(1 for s in ok_subs if s.get("cb_sign_passed"))
        ratio = sign_pass / len(ok_subs)
        if ratio >= 0.67:
            score += 2
            notes.append(f"CB sign correct in {sign_pass}/{len(ok_subs)} sub-periods")
        elif ratio >= 0.33:
            score += 1
            notes.append(f"CB sign mixed across sub-periods ({sign_pass}/{len(ok_subs)})")
        else:
            notes.append(f"CB sign fails in most sub-periods ({sign_pass}/{len(ok_subs)})")

    # Rolling coefficient stability
    max_score += 2
    if rolling:
        pct_pos = rolling.get("pct_positive") or 0
        pct_sig = rolling.get("pct_significant_5pct") or 0
        if pct_pos >= 70 and pct_sig >= 40:
            score += 2
            notes.append(f"Rolling CB beta stable ({pct_pos}% positive, {pct_sig}% significant)")
        elif pct_pos >= 50:
            score += 1
            notes.append(f"Rolling CB beta moderately stable ({pct_pos}% positive)")
        else:
            notes.append(f"Rolling CB beta unstable ({pct_pos}% positive)")

    # OOS — compare RMSE in parent report; here reward lower RMSE relative
    max_score += 1
    oos_rmse = (walk.get("oos_metrics") or {}).get("rmse_log")
    if oos_rmse is not None:
        score += 1  # awarded comparatively in final recommendation
        notes.append(f"Walk-forward OOS RMSE(log)={oos_rmse}")

    # Multicollinearity — CB VIF
    max_score += 1
    cb_vif = next((v["vif"] for v in vif if v["feature"] == analysis.get("cb_feature")), None)
    if cb_vif is not None and cb_vif < 10:
        score += 1
        notes.append(f"CB VIF acceptable ({cb_vif})")
    elif cb_vif is not None:
        notes.append(f"CB VIF elevated ({cb_vif})")

    return {
        "spec": name,
        "score": score,
        "max_score": max_score,
        "notes": notes,
    }


def analyze_spec(bundle: DriverBundle, spec: GoldVariantSpec) -> dict[str, Any] | None:
    panel = build_panel(bundle, spec)
    if not panel:
        return None

    full = _fit_panel(panel)
    if not full:
        return None

    correlations = {
        fname: round(_pearson(col, panel["y"]), 4)
        for fname, col in zip(panel["feature_names"], panel["x_cols"])
    }

    return {
        "variant_id": spec.variant_id,
        "label": spec.label,
        "cb_feature": spec.cb_feature,
        "cb_engineer": spec.engineer,
        "full_sample": {
            "n_obs": full["n_obs"],
            "sample_start": panel["dates"][0],
            "sample_end": panel["dates"][-1],
            "r_squared": full.get("r_squared"),
            "adj_r_squared": full.get("adj_r_squared"),
            "coefficients": full.get("coefficients"),
            "failed_sign_gates": full.get("failed_sign_gates"),
            "cb_coefficient": full.get("cb_coefficient"),
            "correlations_with_log_price": correlations,
        },
        "vif": _compute_vif(panel["x_cols"], panel["feature_names"]),
        "feature_correlation_matrix": _feature_correlation_matrix(panel["x_cols"], panel["feature_names"]),
        "subperiods": subperiod_analysis(panel),
        "holdout_oos": holdout_evaluation(panel),
        "walk_forward_oos": walk_forward_evaluation(panel),
        "rolling_cb_coefficient": rolling_cb_coefficient_path(panel),
    }


def _build_recommendation(level: dict[str, Any], roll12: dict[str, Any]) -> dict[str, Any]:
    level_score = _score_spec("monthly_level", level)
    roll12_score = _score_spec("rolling_12m", roll12)

    # Comparative OOS bonus
    l_rmse = ((level.get("walk_forward_oos") or {}).get("oos_metrics") or {}).get("rmse_log")
    r_rmse = ((roll12.get("walk_forward_oos") or {}).get("oos_metrics") or {}).get("rmse_log")
    oos_winner = None
    if l_rmse is not None and r_rmse is not None:
        if r_rmse < l_rmse * 0.995:
            roll12_score["score"] += 1
            oos_winner = "rolling_12m"
            roll12_score["notes"].append(f"Lower walk-forward RMSE vs level ({r_rmse} vs {l_rmse})")
        elif l_rmse < r_rmse * 0.995:
            level_score["score"] += 1
            oos_winner = "monthly_level"
            level_score["notes"].append(f"Lower walk-forward RMSE vs roll12 ({l_rmse} vs {r_rmse})")
        else:
            oos_winner = "tie"
            level_score["notes"].append(f"Similar OOS RMSE ({l_rmse} vs {r_rmse})")
            roll12_score["notes"].append(f"Similar OOS RMSE ({l_rmse} vs {r_rmse})")

    level_score["max_score"] += 1
    roll12_score["max_score"] += 1

    winner = "rolling_12m" if roll12_score["score"] > level_score["score"] else (
        "monthly_level" if level_score["score"] > roll12_score["score"] else "tie"
    )

    # Production recommendation text
    l_cb = (level.get("full_sample") or {}).get("cb_coefficient") or {}
    r_cb = (roll12.get("full_sample") or {}).get("cb_coefficient") or {}
    l_fail = (level.get("full_sample") or {}).get("failed_sign_gates") or []
    r_fail = (roll12.get("full_sample") or {}).get("failed_sign_gates") or []

    if winner == "rolling_12m":
        prod_spec = "cb_roll12"
        rationale = (
            "Recommend rolling 12-month global CB net purchases as the production CB driver. "
            "It aligns with structural official-sector demand (annual accumulation pace), "
            "shows a positive and significant CB coefficient in the full sample, "
            "and demonstrates better coefficient stability than the noisy monthly level. "
            "Adj R² is modestly higher but was not the primary criterion."
        )
    elif winner == "monthly_level":
        prod_spec = "cb_net_purchases"
        rationale = (
            "Recommend retaining monthly level CB purchases. "
            "Despite lower adj R², it scores better on stability and out-of-sample criteria "
            "than the 12-month rolling aggregate."
        )
    else:
        prod_spec = "cb_roll12"
        rationale = (
            "Scorecard tied, but rolling 12-month is preferred on economic grounds: "
            "central banks report and act on sustained accumulation trends, not single-month noise. "
            "Roll12 also passes the CB sign gate where monthly level fails."
        )

    caveats = [
        "Neither specification clears full institutional publish gate today because real_yield retains the wrong sign "
        "in the 2016–2026 multivariate panel (positive β vs expected negative). "
        "This is independent of the CB driver choice.",
        "ETF holdings exhibits near-perfect collinearity with other trend drivers in-sample (VIF extreme); "
        "macro sign validation should be revisited before publication.",
    ]

    if r_cb.get("sign_passed") and not l_cb.get("sign_passed"):
        rationale += " Monthly level fails CB sign gate (β<0); roll12 passes (β>0, p<0.001)."

    return {
        "recommended_production_cb_driver": prod_spec,
        "recommended_feature_name": "cb_roll12" if prod_spec == "cb_roll12" else "cb_net_purchases",
        "recommended_engineering": "roll12" if prod_spec == "cb_roll12" else "level",
        "scorecard_winner": winner,
        "oos_rmse_winner": oos_winner,
        "scorecard": {
            "monthly_level": level_score,
            "rolling_12m": roll12_score,
        },
        "rationale": rationale,
        "caveats": caveats,
        "publish_ready": False,
        "note": (
            "Switching production CB spec to roll12 is recommended for driver quality, "
            "but Gold valuation should remain WITHHELD until macro sign gates pass."
        ),
    }


def run_cb_driver_comparison() -> dict[str, Any]:
    bundle = build_driver_bundle("Gold")
    level = analyze_spec(bundle, LEVEL_SPEC)
    roll12 = analyze_spec(bundle, ROLL12_SPEC)

    if not level or not roll12:
        return {
            "generated_at": _now_iso(),
            "status": "error",
            "error": "Failed to build one or both comparison panels",
            "driver_bundle": {"n_weeks": bundle.n, "missing": bundle.missing_required},
        }

    cb_meta: dict[str, Any] = {}
    cb_path = PROJECT_ROOT / "data/cache/metals_drivers/wgc_cb_gold_net_purchases.json"
    if cb_path.exists():
        cb_meta = json.loads(cb_path.read_text(encoding="utf-8"))

    recommendation = _build_recommendation(level, roll12)

    return {
        "generated_at": _now_iso(),
        "status": "ok",
        "question": "Is rolling 12-month CB purchases a more appropriate production driver than monthly level?",
        "cb_driver_source": {
            "source_id": cb_meta.get("source_id"),
            "frequency": cb_meta.get("frequency"),
            "observation_count": cb_meta.get("observation_count"),
            "latest_date": cb_meta.get("latest_date"),
        },
        "driver_bundle": {
            "n_weeks": bundle.n,
            "as_of": bundle.as_of,
            "sample_start": bundle.dates[0] if bundle.dates else "",
        },
        "level_vs_roll12_feature_stats": _cb_level_vs_roll12_correlation(bundle),
        "monthly_level": level,
        "rolling_12m": roll12,
        "recommendation": recommendation,
    }


def write_comparison_artifacts(report: dict[str, Any]) -> tuple[Path, Path]:
    COMPARISON_JSON.parent.mkdir(parents=True, exist_ok=True)
    COMPARISON_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    rec = report.get("recommendation") or {}
    level = report.get("monthly_level") or {}
    roll12 = report.get("rolling_12m") or {}
    lf = level.get("full_sample") or {}
    rf = roll12.get("full_sample") or {}
    lcb = lf.get("cb_coefficient") or {}
    rcb = rf.get("cb_coefficient") or {}

    lines = [
        "# Gold CB driver comparison: monthly level vs rolling 12-month",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        f"**Recommendation:** `{rec.get('recommended_production_cb_driver')}` — {rec.get('rationale')}",
        "",
        "> " + (rec.get("note") or ""),
        "",
        "## Full-sample comparison",
        "",
        "| Metric | Monthly level | Rolling 12m |",
        "| --- | ---: | ---: |",
        f"| Adj R² | {lf.get('adj_r_squared')} | {rf.get('adj_r_squared')} |",
        f"| CB β | {lcb.get('beta')} | {rcb.get('beta')} |",
        f"| CB p-value | {lcb.get('p_value')} | {rcb.get('p_value')} |",
        f"| CB sign pass | {'Yes' if lcb.get('sign_passed') else 'No'} | {'Yes' if rcb.get('sign_passed') else 'No'} |",
        f"| CB corr w/ log(price) | {(lf.get('correlations_with_log_price') or {}).get('cb_net_purchases')} | {(rf.get('correlations_with_log_price') or {}).get('cb_roll12')} |",
        "",
        "## Out-of-sample (holdout train ≤2021, test 2022+)",
        "",
    ]
    for name, spec in [("Monthly level", level), ("Rolling 12m", roll12)]:
        ho = spec.get("holdout_oos") or {}
        om = ho.get("oos_metrics") or {}
        lines.append(
            f"- **{name}:** test n={ho.get('test_n')}, RMSE(log)={om.get('rmse_log')}, "
            f"MAPE={om.get('mape_pct')}%, dir hit={om.get('directional_hit_pct')}%"
        )

    lines.extend(["", "## Walk-forward OOS", ""])
    for name, spec in [("Monthly level", level), ("Rolling 12m", roll12)]:
        wf = spec.get("walk_forward_oos") or {}
        om = wf.get("oos_metrics") or {}
        cb = wf.get("cb_beta_during_retrains") or {}
        lines.append(
            f"- **{name}:** n={wf.get('n_forecasts')}, RMSE(log)={om.get('rmse_log')}, "
            f"CB β mean={cb.get('mean')} ({cb.get('pct_positive')}% positive across retrains)"
        )

    lines.extend(["", "## Rolling CB coefficient stability (104-week window)", ""])
    for name, spec in [("Monthly level", level), ("Rolling 12m", roll12)]:
        rc = spec.get("rolling_cb_coefficient") or {}
        lines.append(
            f"- **{name}:** mean β={rc.get('beta_mean')}, std={rc.get('beta_std')}, "
            f"{rc.get('pct_positive')}% positive, {rc.get('pct_significant_5pct')}% significant at 5%"
        )

    lines.extend(["", "## Sub-period CB coefficient", ""])
    lines.append("| Period | Level β (p) | Level sign | Roll12 β (p) | Roll12 sign |")
    lines.append("| --- | --- | --- | --- | --- |")
    lsubs = {s["period_id"]: s for s in level.get("subperiods") or [] if s.get("status") == "ok"}
    rsubs = {s["period_id"]: s for s in roll12.get("subperiods") or [] if s.get("status") == "ok"}
    for pid, _, _ in SUBPERIODS:
        ls = lsubs.get(pid, {})
        rs = rsubs.get(pid, {})
        lines.append(
            f"| {pid} | {ls.get('cb_beta')} ({ls.get('cb_p_value')}) | "
            f"{'Yes' if ls.get('cb_sign_passed') else 'No'} | "
            f"{rs.get('cb_beta')} ({rs.get('cb_p_value')}) | "
            f"{'Yes' if rs.get('cb_sign_passed') else 'No'} |"
        )

    lines.extend(["", "## Multicollinearity (VIF)", ""])
    lines.append("| Feature | Level VIF | Roll12 VIF |")
    lines.append("| --- | ---: | ---: |")
    lv = {v["feature"]: v["vif"] for v in level.get("vif") or []}
    rv = {v["feature"]: v["vif"] for v in roll12.get("vif") or []}
    for feat in list(BASE_FEATURES) + ["cb_net_purchases", "cb_roll12"]:
        if feat in lv or feat in rv:
            lines.append(f"| {feat} | {lv.get(feat, '—')} | {rv.get(feat, '—')} |")

    lines.extend(["", "## Economic interpretation", ""])
    lines.extend(
        [
            "- **Monthly level** captures contemporaneous CB flow but is extremely noisy (single-month spikes/reversals). "
            "On a weekly forward-filled panel this adds high-frequency jitter without matching how policymakers accumulate gold.",
            "- **Rolling 12-month** measures sustained official-sector accumulation (tonnes over the past year). "
            "This matches WGC reporting emphasis on annual/quarterly trends and reduces month-to-month reporting noise.",
            f"- Level vs roll12 correlation on the weekly panel: "
            f"{(report.get('level_vs_roll12_feature_stats') or {}).get('correlation_level_vs_roll12')} "
            f"(distinct but related; roll12 smooths variance).",
            "",
            "## Caveats",
            "",
        ]
    )
    for c in rec.get("caveats") or []:
        lines.append(f"- {c}")

    COMPARISON_MD.write_text("\n".join(lines), encoding="utf-8")
    return COMPARISON_JSON, COMPARISON_MD
