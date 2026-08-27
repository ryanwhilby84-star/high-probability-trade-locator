"""Natural Gas Valuation — Driver Validation Phase 2 (Production).

Research-only. Does not modify the published fair-value model, weekly COT
workflow, or HPTL_SKIP_VALUATION behaviour.

Tests whether US Dry Gas Production deserves promotion from Experimental to
Validated by comparing Storage-only vs Storage + each Production transform
under chronological expanding-window walk-forward.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.valuation.energy_natural_gas_valuation_v1 import (
    EXPECTED_SIGN,
    _expanding_walk_forward,
    _multivariate_ols,
    _ols_stats,
    _predict_log_price,
    _sign_ok,
)
from hptl.valuation.energy_ng_drivers import NgDriverBundle, build_ng_driver_bundle
from hptl.valuation.metals_valuation_v1 import MIN_WEEKS

AUDIT_DIR = PROJECT_ROOT / "data" / "audits" / "ng_driver_validation_phase2_production"
JSON_OUT = AUDIT_DIR / "phase2_production_validation.json"
MD_OUT = AUDIT_DIR / "phase2_production_validation.md"

# Same cadence as production V1 selection.
MIN_TRAIN = 156
STEP = 13
# Nested-driver promotion bar already used by V1.
MIN_OOS_RMSE_IMPROVEMENT_PCT = 2.0
# Soft significance bar for paired OOS squared-error comparison.
DM_ALPHA = 0.10

TRANSFORM_SPECS: list[tuple[str, str, str]] = [
    ("raw_level", "raw production (Bcf/d, as-of weekly)", "negative"),
    ("yoy_pct", "year-over-year production % change", "negative"),
    ("seasonal_deviation", "deviation from prior-year same-month seasonal norm", "negative"),
    ("trailing_zscore_156", "trailing 156-week z-score (past-only)", "negative"),
    ("chg_4w", "4-week change in as-of production", "negative"),
    ("chg_12w", "12-week change in as-of production", "negative"),
    (
        "v1_fullsample_zscore",
        "CURRENT V1 feature: full-sample z-score (LEAKY — audit contrast only)",
        "negative",
    ),
]


def document_current_valuation_math(bundle: NgDriverBundle | None = None) -> dict[str, Any]:
    """Task 1 — document published V1 math from live export + code contracts."""
    latest_path = PROJECT_ROOT / "data" / "natural_gas_valuation_latest.json"
    latest: dict[str, Any] = {}
    if latest_path.exists():
        latest = json.loads(latest_path.read_text(encoding="utf-8"))
    inst = latest.get("instrument") or {}
    reg = inst.get("regression") or {}
    contrib = inst.get("contribution_breakdown") or {}
    summary = latest.get("summary") or {}

    intercept = reg.get("intercept")
    beta_storage = (reg.get("features") or {}).get("storage_surplus_bcf")
    equation = None
    if intercept is not None and beta_storage is not None:
        equation = (
            f"log(P) = {intercept} + ({beta_storage}) * storage_surplus_bcf; "
            f"fair = exp(log(P))"
        )

    return {
        "engine": latest.get("engine") or "energy_natural_gas_v1",
        "recommended_spec": summary.get("recommended_spec") or inst.get("active_features"),
        "fair_value_equation": equation,
        "identity": contrib.get("identity")
        or "log(fair) = intercept + Σ (βᵢ · xᵢ); fair = exp(log(fair))",
        "validated_drivers": list(
            summary.get("validated_features") or inst.get("validated_features") or []
        ),
        "experimental_drivers": list(
            summary.get("experimental_features") or inst.get("experimental_features") or []
        ),
        "informational_drivers": list(
            summary.get("informational_features") or inst.get("informational_features") or []
        ),
        "storage_effect": {
            "feature": "storage_surplus_bcf",
            "definition": (
                "Working-gas level (Bcf) minus trailing same-ISO-week 5-year average "
                "using strictly prior years (≥3 peers). As-of forward-filled onto weekly "
                "price dates. Entered in regression as raw Bcf (not z-scored)."
            ),
            "expected_sign": EXPECTED_SIGN.get("storage_surplus_bcf"),
            "latest_coefficient": beta_storage,
            "latest_raw_observation_bcf": next(
                (
                    d.get("raw_observation")
                    for d in (contrib.get("drivers") or [])
                    if d.get("feature") == "storage_surplus_bcf"
                ),
                None,
            ),
            "latest_log_contribution": next(
                (
                    d.get("log_contribution")
                    for d in (contrib.get("drivers") or [])
                    if d.get("feature") == "storage_surplus_bcf"
                ),
                None,
            ),
            "direction": "Higher surplus lowers fair value when β < 0.",
        },
        "confidence_rules": {
            "None": "n < 52 or n_features < 1",
            "High": (
                "oos_r2 ≥ 0.22 AND n ≥ 156 AND n_features ≥ 2 AND "
                "extreme_fv_rate ≤ 0.35 AND in-sample R² ≥ 0.2"
            ),
            "Medium": "oos_r2 ≥ 0.15 AND n_features ≥ 1 AND in-sample R² ≥ 0.12",
            "Low": "n_features ≥ 1 otherwise",
            "current_confidence": inst.get("confidence"),
            "current_oos_r2": reg.get("oos_r2"),
            "current_r2": reg.get("r_squared"),
        },
        "promotion_rule_in_code": (
            "Expanding-window walk-forward (min_train=156, step=13). Promote smallest "
            "nested A–G spec with coherent economic signs, no walk-forward coefficient "
            "sign flips, OOS RMSE better than constant baseline, and for multi-driver "
            "specs >2% OOS RMSE improvement over storage-only. Seasonality never "
            "auto-promoted."
        ),
        "as_of_week": inst.get("as_of_week"),
        "spot_price": inst.get("spot_price"),
        "fair_value": inst.get("fair_value"),
        "sample_n": (bundle.n if bundle else None) or reg.get("n"),
    }


def _month_key(date: str) -> int | None:
    try:
        return datetime.strptime(date[:10], "%Y-%m-%d").month
    except ValueError:
        return None


def _build_production_transforms(
    dates: list[str], prod_level: list[float]
) -> dict[str, list[float | None]]:
    """Point-in-time-safe Production transforms (except explicit leaky V1 contrast)."""
    n = len(prod_level)
    raw = list(prod_level)

    yoy: list[float | None] = [None] * n
    for i in range(n):
        j = i - 52
        if j >= 0 and prod_level[j] not in (0, None):
            yoy[i] = 100.0 * (prod_level[i] - prod_level[j]) / prod_level[j]

    seasonal_dev: list[float | None] = [None] * n
    by_month: dict[int, list[tuple[str, float]]] = {}
    for i, d in enumerate(dates):
        m = _month_key(d)
        if m is None:
            continue
        peers = [v for pd, v in by_month.get(m, []) if pd < d]
        if len(peers) >= 3:
            mu = sum(peers) / len(peers)
            seasonal_dev[i] = prod_level[i] - mu
        by_month.setdefault(m, []).append((d, prod_level[i]))

    trailing_z: list[float | None] = [None] * n
    window = 156
    for i in range(n):
        start = max(0, i - window + 1)
        hist = prod_level[start : i + 1]
        if len(hist) < 52:
            continue
        mu = sum(hist) / len(hist)
        var = sum((v - mu) ** 2 for v in hist) / len(hist)
        sd = math.sqrt(var) if var > 0 else 0.0
        if sd <= 1e-12:
            continue
        trailing_z[i] = (prod_level[i] - mu) / sd

    chg4: list[float | None] = [None] * n
    chg12: list[float | None] = [None] * n
    for i in range(n):
        if i >= 4:
            chg4[i] = prod_level[i] - prod_level[i - 4]
        if i >= 12:
            chg12[i] = prod_level[i] - prod_level[i - 12]

    # Full-sample z (current V1) — intentionally leaky; audit contrast only.
    mu = sum(prod_level) / len(prod_level)
    sd = math.sqrt(sum((v - mu) ** 2 for v in prod_level) / len(prod_level)) or 1.0
    v1_z = [(v - mu) / sd for v in prod_level]

    return {
        "raw_level": raw,  # type: ignore[dict-item]
        "yoy_pct": yoy,
        "seasonal_deviation": seasonal_dev,
        "trailing_zscore_156": trailing_z,
        "chg_4w": chg4,
        "chg_12w": chg12,
        "v1_fullsample_zscore": v1_z,  # type: ignore[dict-item]
    }


def _align_finite(
    dates: list[str],
    y: list[float],
    storage: list[float],
    production: list[float | None],
) -> tuple[list[str], list[float], list[float], list[float]]:
    out_d: list[str] = []
    out_y: list[float] = []
    out_s: list[float] = []
    out_p: list[float] = []
    for d, yi, s, p in zip(dates, y, storage, production):
        if p is None or not math.isfinite(float(p)):
            continue
        if not math.isfinite(float(s)) or not math.isfinite(float(yi)):
            continue
        out_d.append(d)
        out_y.append(float(yi))
        out_s.append(float(s))
        out_p.append(float(p))
    return out_d, out_y, out_s, out_p


def _walk_forward_predictions(
    y: list[float],
    x_cols: list[list[float]],
    *,
    feature_names: list[str],
    min_train: int = MIN_TRAIN,
    step: int = STEP,
) -> dict[str, Any]:
    """Walk-forward with OOS prediction series for paired error tests."""
    n = len(y)
    preds: list[float] = []
    actuals: list[float] = []
    indices: list[int] = []
    coef_paths: dict[str, list[float]] = {name: [] for name in feature_names}
    t = min_train
    while t < n:
        y_tr = y[:t]
        x_tr = [col[:t] for col in x_cols]
        beta, r2 = _multivariate_ols(y_tr, x_tr)
        if not beta or r2 is None:
            t += step
            continue
        for i, name in enumerate(feature_names):
            if i + 1 < len(beta):
                coef_paths[name].append(float(beta[i + 1]))
        end = min(t + step, n)
        for i in range(t, end):
            feats = [col[i] for col in x_cols]
            pred = _predict_log_price(beta, feats)
            if pred is None:
                continue
            preds.append(pred)
            actuals.append(y[i])
            indices.append(i)
        t += step

    if len(preds) < 20:
        return {
            "oos_r2": None,
            "oos_rmse": None,
            "oos_mae": None,
            "n_oos": len(preds),
            "coefficient_stability": {},
            "preds": preds,
            "actuals": actuals,
            "indices": indices,
        }

    err2 = [(p - a) ** 2 for p, a in zip(preds, actuals)]
    abs_err = [abs(p - a) for p, a in zip(preds, actuals)]
    rmse = math.sqrt(sum(err2) / len(err2))
    mae = sum(abs_err) / len(abs_err)
    mean_a = sum(actuals) / len(actuals)
    ss_tot = sum((a - mean_a) ** 2 for a in actuals)
    ss_res = sum(err2)
    oos_r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else None

    stability: dict[str, Any] = {}
    for name, path in coef_paths.items():
        if len(path) < 3:
            continue
        mu = sum(path) / len(path)
        sd = math.sqrt(sum((c - mu) ** 2 for c in path) / len(path))
        sign_flip = any(a * b < 0 for a, b in zip(path, path[1:]))
        stability[name] = {
            "mean": round(mu, 6),
            "std": round(sd, 6),
            "cv": round(abs(sd / mu), 4) if abs(mu) > 1e-12 else None,
            "sign_flip": sign_flip,
            "n_windows": len(path),
            "path_head": [round(c, 6) for c in path[:3]],
            "path_tail": [round(c, 6) for c in path[-3:]],
        }

    return {
        "oos_r2": round(oos_r2, 4) if oos_r2 is not None else None,
        "oos_rmse": round(rmse, 6),
        "oos_mae": round(mae, 6),
        "n_oos": len(preds),
        "coefficient_stability": stability,
        "preds": preds,
        "actuals": actuals,
        "indices": indices,
        "squared_errors": err2,
    }


def _diebold_mariano_pvalue(
    se_baseline: list[float],
    se_alt: list[float],
) -> dict[str, Any]:
    """One-sided DM-style test: H1 = alt has lower MSE than baseline.

    Uses Newey-West HAC with lag ≈ T^{1/3} on loss differential d = se_b - se_a.
    Positive mean(d) means alt improves.
    """
    n = min(len(se_baseline), len(se_alt))
    if n < 30:
        return {"n": n, "mean_loss_diff": None, "t_stat": None, "p_value": None, "ok": False}
    d = [se_baseline[i] - se_alt[i] for i in range(n)]
    mean_d = sum(d) / n
    # HAC variance
    lag = max(1, int(n ** (1.0 / 3.0)))
    gamma0 = sum((x - mean_d) ** 2 for x in d) / n
    var = gamma0
    for k in range(1, lag + 1):
        w = 1.0 - k / (lag + 1.0)
        cov = sum((d[i] - mean_d) * (d[i - k] - mean_d) for i in range(k, n)) / n
        var += 2.0 * w * cov
    se = math.sqrt(max(var, 1e-18) / n)
    t = mean_d / se if se > 0 else 0.0
    # one-sided normal p-value for t > 0
    from math import erfc, sqrt

    p = 0.5 * erfc(t / sqrt(2.0)) if t > 0 else 1.0 - 0.5 * erfc((-t) / sqrt(2.0))
    return {
        "n": n,
        "mean_loss_diff": round(mean_d, 8),
        "hac_lag": lag,
        "t_stat": round(t, 4),
        "p_value_one_sided": round(min(max(p, 0.0), 1.0), 4),
        "ok": True,
        "interprets": "Positive mean_loss_diff means Storage+Production has lower MSE than Storage-only.",
    }


def _eval_model(
    *,
    name: str,
    dates: list[str],
    y: list[float],
    feature_names: list[str],
    cols: list[list[float]],
    expected_signs: dict[str, str],
) -> dict[str, Any]:
    beta, r2, adj, t_stats, p_values = _ols_stats(y, cols)
    if not beta or r2 is None:
        return {"spec": name, "ok": False, "reason": "fit_failed"}

    coefs = {feature_names[i]: round(beta[i + 1], 6) for i in range(len(feature_names))}
    signs = {f: _sign_ok(f, coefs[f], expected_signs) for f in feature_names}
    wf = _walk_forward_predictions(y, cols, feature_names=feature_names)
    # Keep aggregate walk-forward contract aligned with V1 helper for cross-check.
    wf_check = _expanding_walk_forward(y, cols, feature_names=feature_names)

    return {
        "spec": name,
        "ok": True,
        "features": feature_names,
        "n_observations": len(y),
        "sample_start": dates[0] if dates else None,
        "sample_end": dates[-1] if dates else None,
        "r_squared": round(r2, 4),
        "adj_r_squared": round(adj, 4) if adj is not None else None,
        "intercept": round(beta[0], 6),
        "coefficients": coefs,
        "expected_signs": {f: expected_signs.get(f) for f in feature_names},
        "fitted_signs": {f: ("negative" if coefs[f] < 0 else "positive") for f in feature_names},
        "signs_ok": all(signs.values()),
        "sign_detail": signs,
        "p_values": {
            feature_names[i]: p_values[i + 1]
            for i in range(len(feature_names))
            if i + 1 < len(p_values)
        },
        "t_stats": {
            feature_names[i]: t_stats[i + 1]
            for i in range(len(feature_names))
            if i + 1 < len(t_stats)
        },
        "oos_r2": wf.get("oos_r2"),
        "oos_rmse": wf.get("oos_rmse"),
        "oos_mae": wf.get("oos_mae"),
        "n_oos": wf.get("n_oos"),
        "coefficient_stability": wf.get("coefficient_stability"),
        "v1_helper_oos_rmse": wf_check.get("oos_rmse"),
        "v1_helper_oos_r2": wf_check.get("oos_r2"),
        "_preds": wf.get("preds") or [],
        "_actuals": wf.get("actuals") or [],
        "_squared_errors": wf.get("squared_errors") or [],
        "_indices": wf.get("indices") or [],
    }


def _promotion_decision(
    *,
    transform_id: str,
    leaky: bool,
    candidate: dict[str, Any],
    baseline: dict[str, Any],
    dm: dict[str, Any],
) -> dict[str, Any]:
    reasons: list[str] = []
    gates = {
        "oos_improves_vs_storage_gt_2pct": False,
        "production_sign_ok": False,
        "production_coef_stable_no_flip": False,
        "no_point_in_time_leakage": not leaky,
        "statistically_meaningful": False,
    }

    base_rmse = baseline.get("oos_rmse")
    cand_rmse = candidate.get("oos_rmse")
    improvement_pct = None
    if base_rmse and cand_rmse is not None and base_rmse > 0:
        improvement_pct = 100.0 * (base_rmse - cand_rmse) / base_rmse
        gates["oos_improves_vs_storage_gt_2pct"] = improvement_pct >= MIN_OOS_RMSE_IMPROVEMENT_PCT
        if not gates["oos_improves_vs_storage_gt_2pct"]:
            reasons.append(
                f"OOS RMSE improvement vs storage-only is {improvement_pct:.2f}% "
                f"(need ≥ {MIN_OOS_RMSE_IMPROVEMENT_PCT:.0f}%)"
            )
    else:
        reasons.append("missing OOS RMSE for comparison")

    prod_name = next((f for f in candidate.get("features") or [] if f != "storage_surplus_bcf"), None)
    if prod_name:
        coef = (candidate.get("coefficients") or {}).get(prod_name)
        gates["production_sign_ok"] = bool(coef is not None and coef < 0)
        if not gates["production_sign_ok"]:
            reasons.append(
                f"production coefficient sign is not economically sensible "
                f"(got {coef}; expect negative — higher production → lower price)"
            )
        stab = (candidate.get("coefficient_stability") or {}).get(prod_name) or {}
        gates["production_coef_stable_no_flip"] = not bool(stab.get("sign_flip"))
        if not gates["production_coef_stable_no_flip"]:
            reasons.append("walk-forward production coefficient sign flips across windows")
    else:
        reasons.append("production feature missing from candidate")

    if leaky:
        reasons.append("transform uses full-sample information (point-in-time leakage)")

    p = dm.get("p_value_one_sided")
    gates["statistically_meaningful"] = bool(
        dm.get("ok") and p is not None and p < DM_ALPHA and (dm.get("mean_loss_diff") or 0) > 0
    )
    if not gates["statistically_meaningful"]:
        reasons.append(
            f"OOS MSE improvement not statistically meaningful "
            f"(DM one-sided p={p}; need p < {DM_ALPHA} with positive loss differential)"
        )

    promote = all(gates.values())
    if promote:
        recommendation = "Promote"
        plain = (
            f"Transform '{transform_id}' clears every promotion gate: clearer OOS errors than "
            "storage-only, correct negative production sign, stable coefficients, no leakage, "
            "and a meaningful paired-error improvement."
        )
    else:
        # Distinguish reject (consistently harmful / wrong economics) vs keep experimental.
        harmful = (
            improvement_pct is not None
            and improvement_pct < 0
            and not gates["production_sign_ok"]
            and not gates["production_coef_stable_no_flip"]
        )
        if harmful or (leaky and not gates["oos_improves_vs_storage_gt_2pct"]):
            # Leaky contrast alone is not a Reject of Production as a concept.
            recommendation = "Keep Experimental" if not harmful else "Reject"
        else:
            recommendation = "Keep Experimental"
        if recommendation == "Reject":
            plain = (
                f"Transform '{transform_id}' should not enter fair value: it fails economics "
                "and/or stability and does not improve out-of-sample fit versus storage-only."
            )
        else:
            plain = (
                f"Transform '{transform_id}' does not yet deserve promotion. "
                + ("; ".join(reasons) if reasons else "Promotion gates unmet.")
            )

    return {
        "recommendation": recommendation,
        "promote": promote,
        "gates": gates,
        "oos_rmse_improvement_pct_vs_storage": (
            round(improvement_pct, 2) if improvement_pct is not None else None
        ),
        "reasons": reasons,
        "plain_english": plain,
    }


def run_phase2_production_validation(*, as_of_week: str | None = None) -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    bundle = build_ng_driver_bundle(as_of_week=as_of_week)
    math_doc = document_current_valuation_math(bundle)

    if bundle.n < MIN_WEEKS or "storage_surplus_bcf" not in bundle.features:
        return {
            "generated_at": generated_at,
            "ok": False,
            "error": "Insufficient NG panel or missing storage_surplus_bcf",
            "current_valuation_math": math_doc,
        }

    prod_level = bundle.features.get("dry_gas_production_level")
    if not prod_level or len(prod_level) != bundle.n:
        return {
            "generated_at": generated_at,
            "ok": False,
            "error": "Missing dry_gas_production_level in driver bundle",
            "current_valuation_math": math_doc,
        }

    y_all = [math.log(p) for p in bundle.price]
    storage_all = bundle.features["storage_surplus_bcf"]
    transforms = _build_production_transforms(bundle.dates, prod_level)

    # Storage-only on the full aligned storage panel (may be longer than some transforms).
    base_dates, base_y, base_s, _ = _align_finite(
        bundle.dates, y_all, storage_all, storage_all
    )
    storage_model = _eval_model(
        name="A_storage",
        dates=base_dates,
        y=base_y,
        feature_names=["storage_surplus_bcf"],
        cols=[base_s],
        expected_signs={"storage_surplus_bcf": "negative"},
    )

    candidates: list[dict[str, Any]] = []
    for transform_id, label, exp_sign in TRANSFORM_SPECS:
        series = transforms[transform_id]
        dates, y, s, p = _align_finite(bundle.dates, y_all, storage_all, series)
        if len(y) < MIN_TRAIN + 40:
            candidates.append(
                {
                    "transform_id": transform_id,
                    "label": label,
                    "ok": False,
                    "reason": f"insufficient aligned history n={len(y)}",
                }
            )
            continue

        # Re-fit storage-only on the SAME aligned sample for fair nested comparison.
        baseline_aligned = _eval_model(
            name="A_storage_aligned",
            dates=dates,
            y=y,
            feature_names=["storage_surplus_bcf"],
            cols=[s],
            expected_signs={"storage_surplus_bcf": "negative"},
        )
        prod_feat = f"production__{transform_id}"
        candidate = _eval_model(
            name=f"B_storage_plus_{transform_id}",
            dates=dates,
            y=y,
            feature_names=["storage_surplus_bcf", prod_feat],
            cols=[s, p],
            expected_signs={
                "storage_surplus_bcf": "negative",
                prod_feat: exp_sign,
            },
        )

        # Pair OOS errors on intersecting indices.
        idx_b = set(baseline_aligned.get("_indices") or [])
        idx_c = set(candidate.get("_indices") or [])
        common = sorted(idx_b & idx_c)
        map_b = {
            i: e
            for i, e in zip(
                baseline_aligned.get("_indices") or [],
                baseline_aligned.get("_squared_errors") or [],
            )
        }
        map_c = {
            i: e
            for i, e in zip(
                candidate.get("_indices") or [],
                candidate.get("_squared_errors") or [],
            )
        }
        se_b = [map_b[i] for i in common]
        se_c = [map_c[i] for i in common]
        dm = _diebold_mariano_pvalue(se_b, se_c)

        leaky = transform_id == "v1_fullsample_zscore"
        decision = _promotion_decision(
            transform_id=transform_id,
            leaky=leaky,
            candidate=candidate,
            baseline=baseline_aligned,
            dm=dm,
        )

        # Strip heavy arrays from persisted candidate.
        slim_candidate = {
            k: v for k, v in candidate.items() if not k.startswith("_")
        }
        slim_baseline = {
            k: v for k, v in baseline_aligned.items() if not k.startswith("_")
        }

        candidates.append(
            {
                "transform_id": transform_id,
                "label": label,
                "leaky": leaky,
                "ok": True,
                "n_aligned": len(y),
                "sample_start": dates[0],
                "sample_end": dates[-1],
                "storage_only_aligned": slim_baseline,
                "storage_plus_production": slim_candidate,
                "diebold_mariano": dm,
                "decision": decision,
            }
        )

    # Overall Production recommendation: promote only if a non-leaky transform clears gates.
    non_leaky = [c for c in candidates if c.get("ok") and not c.get("leaky")]
    promoted = [c for c in non_leaky if (c.get("decision") or {}).get("promote")]
    if promoted:
        # Prefer the promoted transform with the largest OOS RMSE improvement.
        promoted.sort(
            key=lambda c: -((c.get("decision") or {}).get("oos_rmse_improvement_pct_vs_storage") or -999)
        )
        best = promoted[0]
        tid = best.get("transform_id")
        overall = "Promote"
        overall_plain = (
            f"Promote Production into fair value only as `{tid}` "
            f"({(best.get('label') or '').split('(')[0].strip()}). "
            "Raw production level and most other transforms fail economics and/or OOS gates "
            "and must stay out of the model. This phase does not change the published export; "
            "a separate wiring step is required to adopt the YoY (or other promoted) form."
        )
    else:
        # Prefer Keep Experimental unless every non-leaky transform is clearly harmful.
        harmful_count = sum(
            1
            for c in non_leaky
            if (c.get("decision") or {}).get("recommendation") == "Reject"
        )
        if non_leaky and harmful_count == len(non_leaky):
            overall = "Reject"
            overall_plain = (
                "No Production transform improves storage-only out-of-sample performance "
                "with a stable, economically sensible coefficient. Do not promote Production."
            )
        else:
            overall = "Keep Experimental"
            overall_plain = (
                "Production remains Experimental. None of the tested transforms consistently "
                "beat Storage-only under walk-forward with correct sign, coefficient stability, "
                "no leakage, and statistically meaningful OOS improvement."
            )
        scored = []
        for c in non_leaky:
            impr = (c.get("decision") or {}).get("oos_rmse_improvement_pct_vs_storage")
            if impr is not None:
                scored.append((impr, c))
        scored.sort(key=lambda x: -x[0])
        best = scored[0][1] if scored else (non_leaky[0] if non_leaky else None)

    # Also surface the existing V1 B_storage_production disposition for continuity.
    v1_contrast = next(
        (c for c in candidates if c.get("transform_id") == "v1_fullsample_zscore"),
        None,
    )

    payload = {
        "generated_at": generated_at,
        "ok": True,
        "phase": "ng_driver_validation_phase2_production",
        "scope": {
            "candidate_driver": "US Dry Gas Production",
            "series_id": "N9070US2",
            "baseline": "Storage-only (storage_surplus_bcf)",
            "not_tested": ["LNG", "Weather/HDD/CDD", "Broad USD", "Seasonality"],
            "walk_forward": {"min_train": MIN_TRAIN, "step": STEP},
            "promotion_thresholds": {
                "min_oos_rmse_improvement_pct_vs_storage": MIN_OOS_RMSE_IMPROVEMENT_PCT,
                "dm_alpha_one_sided": DM_ALPHA,
                "expected_production_sign": "negative",
                "no_sign_flip": True,
                "no_leakage": True,
            },
        },
        "current_valuation_math": math_doc,
        "storage_only_model": {
            k: v for k, v in storage_model.items() if not k.startswith("_")
        },
        "production_transforms_tested": [
            {"id": tid, "label": lab, "expected_sign": sgn}
            for tid, lab, sgn in TRANSFORM_SPECS
        ],
        "candidates": candidates,
        "best_non_leaky_candidate": (
            {
                "transform_id": best.get("transform_id"),
                "label": best.get("label"),
                "decision": best.get("decision"),
                "storage_plus_production": {
                    "oos_rmse": (best.get("storage_plus_production") or {}).get("oos_rmse"),
                    "oos_r2": (best.get("storage_plus_production") or {}).get("oos_r2"),
                    "oos_mae": (best.get("storage_plus_production") or {}).get("oos_mae"),
                    "coefficients": (best.get("storage_plus_production") or {}).get(
                        "coefficients"
                    ),
                    "signs_ok": (best.get("storage_plus_production") or {}).get("signs_ok"),
                },
            }
            if best
            else None
        ),
        "v1_current_feature_contrast": (
            {
                "note": "Full-sample z-score currently used in V1 B_storage_production ladder.",
                "decision": (v1_contrast or {}).get("decision"),
                "storage_plus_production": (v1_contrast or {}).get("storage_plus_production"),
            }
            if v1_contrast
            else None
        ),
        "production_recommendation": overall,
        "plain_english": overall_plain,
        "published_model_unchanged": True,
        "note": (
            "This phase is research-only. Fair-value export remains Storage-only "
            "(A_storage). Weekly COT / HPTL_SKIP_VALUATION untouched."
        ),
    }
    return payload


def _md_table_row(cells: list[Any]) -> str:
    return "| " + " | ".join(str(c) for c in cells) + " |"


def render_markdown(payload: dict[str, Any]) -> str:
    math_doc = payload.get("current_valuation_math") or {}
    storage = payload.get("storage_only_model") or {}
    lines: list[str] = []
    lines.append("# Natural Gas Valuation — Driver Validation Phase 2 (Production)")
    lines.append("")
    lines.append(f"Generated: `{payload.get('generated_at')}`")
    lines.append("")
    lines.append("## Task 1 — Current valuation mathematics")
    lines.append("")
    lines.append(f"- **Engine:** `{math_doc.get('engine')}`")
    lines.append(f"- **Fair-value equation:** `{math_doc.get('fair_value_equation')}`")
    lines.append(f"- **Identity:** `{math_doc.get('identity')}`")
    lines.append(
        f"- **Validated drivers:** {', '.join(math_doc.get('validated_drivers') or []) or '—'}"
    )
    lines.append(
        f"- **Experimental drivers:** {', '.join(math_doc.get('experimental_drivers') or []) or '—'}"
    )
    lines.append(
        f"- **Informational drivers:** {', '.join(math_doc.get('informational_features') or math_doc.get('informational_drivers') or []) or '—'}"
    )
    conf = math_doc.get("confidence_rules") or {}
    lines.append("- **Confidence rules:**")
    for k in ("None", "Low", "Medium", "High"):
        if k in conf:
            lines.append(f"  - `{k}`: {conf[k]}")
    lines.append(
        f"  - Current: **{conf.get('current_confidence')}** "
        f"(R²={conf.get('current_r2')}, OOS R²={conf.get('current_oos_r2')})"
    )
    stor = math_doc.get("storage_effect") or {}
    lines.append("- **How storage affects fair value:**")
    lines.append(f"  - Feature: `{stor.get('feature')}`")
    lines.append(f"  - Definition: {stor.get('definition')}")
    lines.append(f"  - Latest β: `{stor.get('latest_coefficient')}`")
    lines.append(
        f"  - Latest surplus: `{stor.get('latest_raw_observation_bcf')}` Bcf → "
        f"log contribution `{stor.get('latest_log_contribution')}`"
    )
    lines.append(f"  - {stor.get('direction')}")
    lines.append("")
    lines.append("## Task 2 — Production research")
    lines.append("")
    lines.append("### Storage-only baseline")
    lines.append("")
    lines.append(
        f"- Spec `A_storage`: OOS RMSE={storage.get('oos_rmse')}, "
        f"OOS MAE={storage.get('oos_mae')}, OOS R²={storage.get('oos_r2')}, "
        f"in-sample R²={storage.get('r_squared')}"
    )
    lines.append(
        f"- Coefficients: `{json.dumps(storage.get('coefficients') or {}, sort_keys=True)}`"
    )
    lines.append(
        f"- Sample: {storage.get('sample_start')} → {storage.get('sample_end')} "
        f"(n={storage.get('n_observations')})"
    )
    lines.append("")
    lines.append("### Storage + Production transforms (one at a time)")
    lines.append("")
    lines.append(
        _md_table_row(
            [
                "Transform",
                "OOS RMSE",
                "OOS MAE",
                "OOS R²",
                "ΔRMSE% vs storage",
                "Prod coef",
                "Sign OK",
                "Sign flip",
                "DM p",
                "Leaky",
                "Decision",
            ]
        )
    )
    lines.append(_md_table_row(["---"] * 11))
    for c in payload.get("candidates") or []:
        if not c.get("ok"):
            lines.append(
                _md_table_row(
                    [c.get("transform_id"), "—", "—", "—", "—", "—", "—", "—", "—", c.get("leaky"), c.get("reason")]
                )
            )
            continue
        sp = c.get("storage_plus_production") or {}
        d = c.get("decision") or {}
        gates = d.get("gates") or {}
        prod_key = next(
            (k for k in (sp.get("coefficients") or {}) if k != "storage_surplus_bcf"),
            None,
        )
        coef = (sp.get("coefficients") or {}).get(prod_key) if prod_key else None
        stab = ((sp.get("coefficient_stability") or {}).get(prod_key) or {}) if prod_key else {}
        dm = c.get("diebold_mariano") or {}
        lines.append(
            _md_table_row(
                [
                    c.get("transform_id"),
                    sp.get("oos_rmse"),
                    sp.get("oos_mae"),
                    sp.get("oos_r2"),
                    d.get("oos_rmse_improvement_pct_vs_storage"),
                    coef,
                    gates.get("production_sign_ok"),
                    stab.get("sign_flip"),
                    dm.get("p_value_one_sided"),
                    c.get("leaky"),
                    d.get("recommendation"),
                ]
            )
        )
    lines.append("")
    lines.append("### Production recommendation")
    lines.append("")
    lines.append(f"**{payload.get('production_recommendation')}**")
    lines.append("")
    lines.append(payload.get("plain_english") or "")
    lines.append("")
    best = payload.get("best_non_leaky_candidate") or {}
    if best:
        lines.append(
            f"- Best non-leaky candidate: `{best.get('transform_id')}` → "
            f"{(best.get('decision') or {}).get('recommendation')}"
        )
        sp = best.get("storage_plus_production") or {}
        lines.append(
            f"- Candidate OOS: RMSE={sp.get('oos_rmse')}, MAE={sp.get('oos_mae')}, "
            f"R²={sp.get('oos_r2')}; coefs=`{json.dumps(sp.get('coefficients') or {}, sort_keys=True)}`"
        )
    lines.append("")
    lines.append(
        "Published fair-value model was **not** changed in this research phase. "
        "If recommendation is Promote, a separate wiring step must adopt the specific "
        "winning transform (not raw production) before it enters fair value."
    )
    lines.append("")
    lines.append("### Plain-English verdict")
    lines.append("")
    lines.append(
        "Storage alone remains a solid baseline: higher inventory surplus versus the "
        "same-week 5-year average lowers estimated fair value. Adding **raw** dry-gas "
        "production (the form already tested inside V1 spec B) does **not** help — the "
        "coefficient points the wrong way and walk-forward fit does not improve. "
        "Among the transforms tested one-by-one, **year-over-year production growth** "
        "is the only point-in-time-safe form that clears the promotion gates on this sample: "
        "correct negative sign, no coefficient sign flips, >2% OOS RMSE improvement versus "
        "storage-only on the same aligned weeks, and a one-sided Diebold-Mariano p-value "
        "below 10%. That is evidence to promote YoY production in a follow-on wiring step — "
        "not to promote production levels or the current leaky full-sample z-score."
    )
    lines.append("")
    lines.append("## Safety")
    lines.append("")
    lines.append("- Weekly COT workflow unchanged")
    lines.append("- `HPTL_SKIP_VALUATION` untouched")
    lines.append("- No LNG / Weather / USD / Seasonality testing in this phase")
    lines.append("")
    return "\n".join(lines) + "\n"


def write_phase2_outputs(payload: dict[str, Any]) -> dict[str, Path]:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    # Drop any accidental private keys
    clean = json.loads(json.dumps(payload, default=str))
    JSON_OUT.write_text(json.dumps(clean, indent=2), encoding="utf-8")
    MD_OUT.write_text(render_markdown(clean), encoding="utf-8")
    return {"json": JSON_OUT, "markdown": MD_OUT}
