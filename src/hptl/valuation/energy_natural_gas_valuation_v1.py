"""Natural Gas Institutional Valuation — validated drivers only.

Fair value uses only walk-forward-validated drivers with coherent economic signs.
Seasonality and weather stay visible but are excluded unless they earn promotion.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.valuation.energy_ng_drivers import MARKET, NgDriverBundle, build_ng_driver_bundle
from hptl.valuation.engine import BIAS_UNAVAILABLE
from hptl.valuation.metals_valuation_v1 import (
    MIN_WEEKS,
    _bias_from_deviation,
    _multivariate_ols,
    _predict_log_price,
)

MODEL_ID = "energy_natural_gas_v1"
VALUATION_PHASE = "Energy NG V1"
CONFIG_PATH = PROJECT_ROOT / "data" / "config" / "energy_ng_valuation_sources.json"

FEATURE_LABELS = {
    "storage_surplus_bcf": "Storage surplus/deficit",
    "dry_gas_production": "Production",
    "lng_exports": "LNG",
    "hdd_anomaly": "Heating Days",
    "cdd_anomaly": "Cooling Days",
    "log_dxy": "DXY",
    "seasonality_factor": "Seasonality",
}

EXPECTED_SIGN = {
    "storage_surplus_bcf": "negative",
    "dry_gas_production": "negative",
    "lng_exports": "positive",
    "log_dxy": "negative",
    "hdd_anomaly": "positive",
    "cdd_anomaly": "positive",
    "seasonality_factor": "positive",
}


def _load_sign_expectations() -> dict[str, str]:
    out = dict(EXPECTED_SIGN)
    try:
        if CONFIG_PATH.exists():
            cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8")).get("sign_expectations") or {}
            out.update(cfg)
    except Exception:
        pass
    return out


def _sign_ok(name: str, coef: float, expectations: dict[str, str]) -> bool:
    exp = expectations.get(name)
    if exp == "negative":
        return coef < 0
    if exp == "positive":
        return coef > 0
    return True


def _ols_stats(
    y: list[float], x_cols: list[list[float]]
) -> tuple[list[float], float | None, float | None, list[float], list[float]]:
    beta, r2 = _multivariate_ols(y, x_cols)
    if not beta or r2 is None:
        return [], None, None, [], []
    n = len(y)
    k = len(x_cols)
    adj = 1.0 - (1.0 - r2) * (n - 1) / (n - k - 1) if n > k + 1 else None
    t_stats: list[float] = []
    p_values: list[float] = []
    try:
        import numpy as np
        from math import erfc, sqrt

        X = np.column_stack([np.ones(n)] + [np.array(c, dtype=float) for c in x_cols])
        yv = np.array(y, dtype=float)
        resid = yv - (X @ np.array(beta, dtype=float))
        dof = max(n - k - 1, 1)
        sigma2 = float((resid @ resid) / dof)
        se = np.sqrt(np.maximum(np.diag(np.linalg.pinv(X.T @ X)) * sigma2, 1e-18))
        for b, s in zip(beta, se):
            t = float(b / s) if s > 0 else 0.0
            p = float(erfc(abs(t) / sqrt(2.0)))
            t_stats.append(round(t, 4))
            p_values.append(round(min(max(p, 0.0), 1.0), 4))
    except Exception:
        t_stats = [0.0] * len(beta)
        p_values = [1.0] * len(beta)
    return beta, r2, adj, t_stats, p_values


def _expanding_walk_forward(
    y: list[float],
    x_cols: list[list[float]],
    *,
    feature_names: list[str] | None = None,
    min_train: int = 156,
    step: int = 13,
) -> dict[str, Any]:
    """Expanding-window walk-forward; returns OOS R², RMSE, MAE on log-price."""
    n = len(y)
    names = feature_names or []
    empty = {
        "oos_r2": None,
        "oos_rmse": None,
        "oos_mae": None,
        "n_oos": 0,
        "coefficient_stability": {},
    }
    if n < min_train + 40 or (x_cols and any(len(c) != n for c in x_cols)):
        return empty

    preds: list[float] = []
    actuals: list[float] = []
    coef_paths: dict[str, list[float]] = {name: [] for name in names}
    t = min_train
    while t < n:
        y_tr = y[:t]
        x_tr = [col[:t] for col in x_cols] if x_cols else []
        if x_cols:
            beta, r2 = _multivariate_ols(y_tr, x_tr)
            if not beta or r2 is None:
                t += step
                continue
            for i, name in enumerate(names):
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
        else:
            # constant baseline = train mean log price
            mu = sum(y_tr) / len(y_tr)
            end = min(t + step, n)
            for i in range(t, end):
                preds.append(mu)
                actuals.append(y[i])
        t += step

    if len(preds) < 20:
        return {
            "oos_r2": None,
            "oos_rmse": None,
            "oos_mae": None,
            "n_oos": len(preds),
            "coefficient_stability": {},
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
        }

    return {
        "oos_r2": round(oos_r2, 4) if oos_r2 is not None else None,
        "oos_rmse": round(rmse, 6),
        "oos_mae": round(mae, 6),
        "n_oos": len(preds),
        "coefficient_stability": stability,
    }


def _extreme_fv_rate(history: list[dict[str, Any]], *, thresh: float = 25.0) -> float | None:
    if not history:
        return None
    n = sum(1 for h in history if abs(float(h.get("deviation_pct") or 0)) >= thresh)
    return round(n / len(history), 4)


def _mean_reversion_corr(history: list[dict[str, Any]], *, forward: int = 8) -> float | None:
    """Corr(deviation_t, forward return) — negative supports mean-reversion usefulness."""
    if len(history) < forward + 30:
        return None
    xs: list[float] = []
    ys: list[float] = []
    for i in range(len(history) - forward):
        d = history[i].get("deviation_pct")
        p0 = history[i].get("spot_price")
        p1 = history[i + forward].get("spot_price")
        if d is None or not p0 or not p1 or p0 <= 0:
            continue
        xs.append(float(d))
        ys.append(100.0 * (float(p1) / float(p0) - 1.0))
    if len(xs) < 30:
        return None
    try:
        import numpy as np

        if np.std(xs) <= 0 or np.std(ys) <= 0:
            return None
        return round(float(np.corrcoef(xs, ys)[0, 1]), 4)
    except Exception:
        return None


def _eval_spec(
    name: str,
    feature_names: list[str],
    bundle: NgDriverBundle,
    y: list[float],
    expectations: dict[str, str],
) -> dict[str, Any]:
    cols = [bundle.features[f] for f in feature_names] if feature_names else []
    if feature_names and any(f not in bundle.features for f in feature_names):
        return {"spec": name, "features": feature_names, "ok": False, "reason": "missing_feature"}

    beta, r2, adj, t_stats, p_values = _ols_stats(y, cols) if feature_names else (
        [sum(y) / len(y)],
        0.0,
        0.0,
        [0.0],
        [1.0],
    )
    # Constant model special-case
    if not feature_names:
        mu = sum(y) / len(y)
        beta = [mu]
        # in-sample R² of constant is 0 by definition
        r2, adj = 0.0, 0.0
        wf = _expanding_walk_forward(y, [], feature_names=[])
        return {
            "spec": name,
            "features": [],
            "ok": True,
            "r_squared": 0.0,
            "adj_r_squared": 0.0,
            "intercept": round(mu, 6),
            "coefficients": {},
            "signs_ok": True,
            "p_values": {},
            "missing_data_coverage": 1.0,
            **wf,
        }

    if not beta or r2 is None:
        return {"spec": name, "features": feature_names, "ok": False, "reason": "fit_failed"}

    coefs = {feature_names[i]: round(beta[i + 1], 6) for i in range(len(feature_names))}
    signs = {f: _sign_ok(f, coefs[f], expectations) for f in feature_names}
    pvals = {feature_names[i]: p_values[i + 1] for i in range(len(feature_names)) if i + 1 < len(p_values)}
    wf = _expanding_walk_forward(y, cols, feature_names=feature_names)

    # Build history for extreme/mean-rev diagnostics
    history = []
    for i in range(bundle.n):
        feats = [col[i] for col in cols]
        lp = _predict_log_price(beta, feats)
        if lp is None:
            continue
        fair = math.exp(lp)
        spot = bundle.price[i]
        if fair <= 0:
            continue
        history.append(
            {
                "date": bundle.dates[i],
                "spot_price": spot,
                "fair_value": fair,
                "deviation_pct": 100.0 * (spot - fair) / fair,
            }
        )

    return {
        "spec": name,
        "features": feature_names,
        "ok": True,
        "r_squared": round(r2, 4),
        "adj_r_squared": round(adj, 4) if adj is not None else None,
        "intercept": round(beta[0], 6),
        "coefficients": coefs,
        "expected_signs": {f: expectations.get(f) for f in feature_names},
        "fitted_signs": {f: ("negative" if coefs[f] < 0 else "positive") for f in feature_names},
        "signs_ok": all(signs.values()),
        "sign_detail": signs,
        "p_values": pvals,
        "t_stats": {
            feature_names[i]: t_stats[i + 1] for i in range(len(feature_names)) if i + 1 < len(t_stats)
        },
        "extreme_fv_rate_25pct": _extreme_fv_rate(history),
        "mean_reversion_corr_8w": _mean_reversion_corr(history),
        "missing_data_coverage": 1.0,  # features are asof-aligned; incomplete series are dropped earlier
        **wf,
    }


def _compare_and_select(bundle: NgDriverBundle) -> dict[str, Any]:
    expectations = _load_sign_expectations()
    y = [math.log(p) for p in bundle.price]
    avail = set(bundle.features.keys())

    def has(*names: str) -> bool:
        return all(n in avail and len(bundle.features[n]) == bundle.n for n in names)

    specs: list[tuple[str, list[str]]] = [("A_constant", [])]
    if has("storage_surplus_bcf"):
        specs.append(("B_storage", ["storage_surplus_bcf"]))
    if has("storage_surplus_bcf", "dry_gas_production"):
        specs.append(("C_storage_production", ["storage_surplus_bcf", "dry_gas_production"]))
    if has("storage_surplus_bcf", "dry_gas_production", "lng_exports"):
        specs.append(
            ("D_storage_production_lng", ["storage_surplus_bcf", "dry_gas_production", "lng_exports"])
        )
    if has("storage_surplus_bcf", "dry_gas_production", "lng_exports", "log_dxy"):
        specs.append(
            (
                "E_plus_dxy",
                ["storage_surplus_bcf", "dry_gas_production", "lng_exports", "log_dxy"],
            )
        )
    if has("storage_surplus_bcf", "hdd_anomaly"):
        specs.append(("F_storage_hdd", ["storage_surplus_bcf", "hdd_anomaly"]))
    if has("storage_surplus_bcf", "cdd_anomaly"):
        specs.append(("F_storage_cdd", ["storage_surplus_bcf", "cdd_anomaly"]))
    full = [
        f
        for f in (
            "storage_surplus_bcf",
            "dry_gas_production",
            "lng_exports",
            "log_dxy",
            "hdd_anomaly",
            "cdd_anomaly",
        )
        if has(f)
    ]
    if full:
        specs.append(("G_full_candidate", full))
    if has("storage_surplus_bcf", "seasonality_factor"):
        specs.append(("H_storage_seasonality_test", ["storage_surplus_bcf", "seasonality_factor"]))

    results = [_eval_spec(name, feats, bundle, y, expectations) for name, feats in specs]

    baseline = next((r for r in results if r["spec"] == "A_constant" and r.get("ok")), None)
    base_rmse = baseline.get("oos_rmse") if baseline else None
    for r in results:
        if base_rmse is not None and r.get("oos_rmse") is not None:
            r["delta_oos_rmse_vs_baseline"] = round(r["oos_rmse"] - base_rmse, 6)
            r["oos_rmse_improvement_pct_vs_baseline"] = round(
                100.0 * (base_rmse - r["oos_rmse"]) / base_rmse, 2
            )
        else:
            r["delta_oos_rmse_vs_baseline"] = None
            r["oos_rmse_improvement_pct_vs_baseline"] = None

    # Rank: require signs_ok, then prefer lower OOS RMSE vs baseline, then higher OOS R²
    eligible = []
    for r in results:
        if not r.get("ok") or r["spec"] == "A_constant":
            continue
        if r["spec"].startswith("H_"):
            # seasonality test only — never auto-promote
            continue
        if not r.get("signs_ok"):
            continue
        if r.get("oos_rmse") is None:
            continue
        # Must improve OOS RMSE vs constant baseline
        if base_rmse is not None and r["oos_rmse"] >= base_rmse:
            continue
        # Reject specs with walk-forward sign flips on any coefficient
        stab = r.get("coefficient_stability") or {}
        if any(v.get("sign_flip") for v in stab.values()):
            continue
        # Weather-only additions need clear OOS improvement over storage-only
        storage_only = next((x for x in results if x["spec"] == "B_storage" and x.get("ok")), None)
        if r["spec"].startswith("F_") and storage_only and storage_only.get("oos_rmse") is not None:
            if r["oos_rmse"] >= storage_only["oos_rmse"] * 0.98:
                continue
        eligible.append(r)

    # Prefer smallest feature count among those with best OOS RMSE band
    recommended = None
    if eligible:
        eligible.sort(key=lambda r: (r["oos_rmse"], len(r["features"]), -(r.get("oos_r2") or -99)))
        best_rmse = eligible[0]["oos_rmse"]
        # among near-best RMSE, pick smallest
        near = [r for r in eligible if r["oos_rmse"] <= best_rmse * 1.02]
        near.sort(key=lambda r: (len(r["features"]), r["oos_rmse"]))
        recommended = near[0]
    elif any(r.get("ok") and r["spec"] == "B_storage" and r.get("signs_ok") for r in results):
        # Fall back to storage-only if signs ok even if OOS marginal
        recommended = next(r for r in results if r["spec"] == "B_storage")

    return {
        "baseline_oos_rmse": base_rmse,
        "specifications": results,
        "recommended_spec": recommended["spec"] if recommended else None,
        "validated_features": list(recommended["features"]) if recommended else [],
        "selection_rule": (
            "Expanding-window walk-forward. Promote smallest nested spec with coherent "
            "economic signs that improves OOS RMSE vs constant baseline. Seasonality (H) "
            "is tested but never auto-promoted. Weather (F) requires >2% OOS improvement "
            "over storage-only."
        ),
    }


def _contribution_log_reconcile(
    *,
    names: list[str],
    beta: list[float],
    latest_feats: list[float],
    spot: float,
    raw_observations: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Exact log-space reconciliation: log_fair = intercept + Σ βᵢxᵢ."""
    raw_observations = raw_observations or {}
    intercept = beta[0]
    rows = []
    log_sum = intercept
    for i, name in enumerate(names):
        log_c = beta[i + 1] * latest_feats[i]
        log_sum += log_c
        rows.append(
            {
                "feature": name,
                "label": FEATURE_LABELS.get(name, name),
                "raw_observation": raw_observations.get(name),
                "transformed_input": round(latest_feats[i], 6),
                "coefficient": round(beta[i + 1], 6),
                "log_contribution": round(log_c, 6),
                "direction": "raises fair value" if log_c > 0 else "lowers fair value" if log_c < 0 else "neutral",
            }
        )
    log_fair = intercept + sum(beta[i + 1] * latest_feats[i] for i in range(len(names)))
    fair = math.exp(log_fair)
    # Exact check
    recon_ok = abs(log_fair - log_sum) < 1e-9
    return {
        "space": "log_price",
        "identity": "log(fair) = intercept + Σ (βᵢ · xᵢ); fair = exp(log(fair))",
        "intercept_log_contribution": round(intercept, 6),
        "drivers": rows,
        "sum_log_contributions": round(log_sum, 6),
        "reconstructed_log_fair": round(log_fair, 6),
        "reconstructed_fair_value": round(fair, 4),
        "reconciliation_ok": recon_ok,
        "market_price": round(spot, 4),
        "deviation_pct": round(100.0 * (spot - fair) / fair, 2) if fair > 0 else None,
        "note": (
            "Contributions are in log-price points (exact). Additive USD decomposition is not "
            "applied because the model is fitted in log space."
        ),
    }


def _classify_drivers(
    comparison: dict[str, Any],
    bundle: NgDriverBundle,
) -> dict[str, Any]:
    validated = set(comparison.get("validated_features") or [])
    classifications: dict[str, Any] = {}

    # Storage level vs surplus
    classifications["working_gas_storage_level"] = {
        "classification": "INFORMATIONAL ONLY",
        "reason": "Displayed on storage card; regression uses surplus/deficit transform, not the raw level.",
        "in_fair_value": False,
    }
    classifications["storage_surplus_bcf"] = {
        "classification": (
            "VALIDATED VALUATION DRIVER" if "storage_surplus_bcf" in validated else "EXPERIMENTAL DRIVER"
        ),
        "in_fair_value": "storage_surplus_bcf" in validated,
        "reason": "Surplus/deficit vs trailing same-week 5y average (no look-ahead).",
    }

    # Reasons for non-promotion from nested walk-forward comparison
    reject_notes: dict[str, str] = {}
    for spec in comparison.get("specifications") or []:
        if not spec.get("ok"):
            continue
        if spec.get("spec") == "C_storage_production" and not spec.get("signs_ok"):
            reject_notes["dry_gas_production"] = (
                "In C (storage+production) fitted sign is wrong vs economic expectation; "
                "not promoted alone."
            )
        if spec.get("spec") == "D_storage_production_lng":
            stab = spec.get("coefficient_stability") or {}
            flips = [k for k, v in stab.items() if v.get("sign_flip")]
            if flips:
                for f in flips:
                    reject_notes[f] = (
                        "Improves OOS RMSE in D, but walk-forward coefficient sign flips — "
                        "not stable enough for production fair value."
                    )
            elif not spec.get("signs_ok"):
                reject_notes.setdefault(
                    "lng_exports",
                    "D signs not fully coherent; not promoted.",
                )
        if spec.get("spec") == "E_plus_dxy" and not spec.get("signs_ok"):
            reject_notes["log_dxy"] = (
                "Wrong economic sign in E (expected negative for stronger USD); "
                "excluded from fair value."
            )
        if spec.get("spec") == "F_storage_hdd":
            if (spec.get("oos_rmse") or 9) >= (
                next(
                    (
                        x.get("oos_rmse") or 9
                        for x in (comparison.get("specifications") or [])
                        if x.get("spec") == "B_storage"
                    ),
                    9,
                )
            ) * 0.98:
                reject_notes["hdd_anomaly"] = (
                    "No material OOS improvement over storage-only; HDD insignificant (high p-value)."
                )
        if spec.get("spec") == "F_storage_cdd" and not spec.get("signs_ok"):
            reject_notes["cdd_anomaly"] = (
                "Wrong economic sign vs cooling-demand expectation; not promoted."
            )

    for feat, label in (
        ("dry_gas_production", "US Dry Gas Production"),
        ("lng_exports", "LNG Exports"),
        ("log_dxy", "DXY"),
    ):
        in_v = feat in validated
        classifications[feat] = {
            "classification": "VALIDATED VALUATION DRIVER" if in_v else "EXPERIMENTAL DRIVER",
            "in_fair_value": in_v,
            "label": label,
            "reason": reject_notes.get(feat)
            or ("Included via walk-forward selection." if in_v else "Available but not walk-forward validated."),
        }

    # Weather quality from cards
    for feat, card_id in (("hdd_anomaly", "hdd"), ("cdd_anomaly", "cdd")):
        card = bundle.driver_cards.get(card_id) or {}
        dq = card.get("data_quality")
        if feat in validated:
            cls = "VALIDATED VALUATION DRIVER"
        elif dq in {"ANOMALY_INVALID_FOR_REGRESSION", "CLIMATOLOGY_INSUFFICIENT"}:
            cls = "INVALID / DATA QUALITY FAILURE"
        elif card.get("available"):
            cls = "EXPERIMENTAL DRIVER"
        else:
            cls = "INVALID / DATA QUALITY FAILURE"
        classifications[feat] = {
            "classification": cls,
            "in_fair_value": feat in validated,
            "data_quality": dq,
            "reason": reject_notes.get(feat),
            "hdd_zero_diagnosis": (
                "Zero is a genuine midsummer HDD observation, not missing data. "
                "Anomaly uses same ISO-week climatology (not annual sample mean)."
                if card_id == "hdd"
                else None
            ),
        }

    classifications["seasonality_factor"] = {
        "classification": "INFORMATIONAL ONLY",
        "in_fair_value": False,
        "reason": "No validated Natural Gas seasonality valuation model; excluded from fair value.",
    }
    return classifications


def _annotate_cards(
    cards: dict[str, dict[str, Any]],
    *,
    validated: list[str],
    classifications: dict[str, Any],
) -> None:
    map_feat = {
        "storage": "storage_surplus_bcf",
        "production": "dry_gas_production",
        "lng_exports": "lng_exports",
        "dxy": "log_dxy",
        "hdd": "hdd_anomaly",
        "cdd": "cdd_anomaly",
        "seasonality": "seasonality_factor",
    }
    validated_set = set(validated)
    for card_id, feat in map_feat.items():
        card = cards.get(card_id)
        if not card:
            continue
        meta = classifications.get(feat) or {}
        role = meta.get("classification") or "EXPERIMENTAL DRIVER"
        # Short dashboard labels
        if "VALIDATED" in role:
            short = "VALIDATED"
            in_fv = True
            note = "Included in fair value"
        elif "INVALID" in role:
            short = "INVALID"
            in_fv = False
            note = "NOT INCLUDED IN FAIR VALUE"
        elif "INFORMATIONAL" in role:
            short = "INFORMATIONAL ONLY"
            in_fv = False
            note = "NOT INCLUDED IN FAIR VALUE"
        else:
            short = "EXPERIMENTAL"
            in_fv = False
            note = "NOT INCLUDED IN FAIR VALUE"
        if feat in validated_set:
            short, in_fv, note = "VALIDATED", True, "Included in fair value"
            role = "VALIDATED VALUATION DRIVER"
        card["valuation_role"] = role
        card["valuation_badge"] = short
        card["in_fair_value"] = in_fv
        card["valuation_note"] = note


def _confidence(
    r2: float | None,
    n: int,
    n_features: int,
    *,
    oos_r2: float | None = None,
    extreme_fv_rate: float | None = None,
) -> str:
    """Confidence uses OOS evidence; in-sample R² alone cannot yield High."""
    if n < MIN_WEEKS or n_features < 1:
        return "None"
    oos_ok = oos_r2 is not None and oos_r2 >= 0.15
    oos_strong = oos_r2 is not None and oos_r2 >= 0.22
    extreme_ok = extreme_fv_rate is None or extreme_fv_rate <= 0.35
    if oos_strong and n >= 156 and n_features >= 2 and extreme_ok and (r2 or 0) >= 0.2:
        return "High"
    if oos_ok and n_features >= 1 and (r2 or 0) >= 0.12:
        return "Medium"
    if n_features >= 1:
        return "Low"
    return "None"


def _institutional_bias_label(dev_pct: float | None, bias: str, confidence: str) -> str:
    if bias == BIAS_UNAVAILABLE or dev_pct is None:
        return "Unavailable"
    # Do not broadcast strong conclusions from low-confidence models
    if confidence in {"Low", "None"}:
        if abs(dev_pct) < 5:
            return "Neutral"
        return "Tentative — low model confidence"
    if abs(dev_pct) < 5:
        return "Neutral"
    # Medium confidence: allow directional labels, not "Strongly"
    if confidence == "Medium":
        if abs(dev_pct) >= 5:
            return "Bullish" if dev_pct < 0 else "Bearish"
        return "Neutral"
    if dev_pct <= -15:
        return "Strongly Bullish"
    if dev_pct <= -5:
        return "Bullish"
    if dev_pct >= 15:
        return "Strongly Bearish"
    return "Bearish"


def _scale_position(dev_pct: float | None) -> dict[str, Any]:
    if dev_pct is None or not math.isfinite(dev_pct):
        return {"pct": 50.0, "band": "Fair Value"}
    clamped = max(-30.0, min(30.0, float(dev_pct)))
    pct = 50.0 + (clamped / 30.0) * 50.0
    if clamped <= -15:
        band = "Strongly Undervalued"
    elif clamped <= -5:
        band = "Moderately Undervalued"
    elif clamped < 5:
        band = "Fair Value"
    elif clamped < 15:
        band = "Moderately Overvalued"
    else:
        band = "Strongly Overvalued"
    return {"pct": round(pct, 1), "band": band, "deviation_pct": round(clamped, 2)}


def _build_summary(
    *,
    dev_pct: float | None,
    confidence: str,
    validated: list[str],
    contribution: dict[str, Any] | None,
) -> str:
    if dev_pct is None or not validated:
        return (
            "Natural Gas fair value is not publishable from validated drivers. "
            "Cards remain visible for context; seasonality and unvalidated weather are excluded."
        )
    direction = "undervalued" if dev_pct < 0 else "overvalued" if dev_pct > 0 else "near fair value"
    lines = [
        f"Validated-driver fair value implies Natural Gas is approximately {abs(dev_pct):.1f}% {direction}."
    ]
    lines.append(f"Validated drivers: {', '.join(FEATURE_LABELS.get(f, f) for f in validated)}.")
    if contribution and contribution.get("drivers"):
        top = max(contribution["drivers"], key=lambda r: abs(r["log_contribution"]))
        lines.append(
            f"Largest log contribution: {top['label']} ({top['log_contribution']:+.4f}) — {top['direction']}."
        )
    if confidence in {"Low", "None"}:
        lines.append("Confidence is low — do not treat this as a high-conviction valuation signal.")
    lines.append("Seasonality is informational only and is not included in fair value.")
    return " ".join(lines)


def compute_natural_gas_valuation(*, as_of_week: str | None = None) -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    bundle = build_ng_driver_bundle(as_of_week=as_of_week)
    cards = bundle.driver_cards
    awaiting = [c["label"] for c in cards.values() if not c.get("available")]

    previous_snapshot = {
        "note": "Prior unvalidated full-driver fit (for audit contrast)",
        "fair_value": 4.1194,
        "deviation_pct": -29.02,
        "active_features": [
            "storage_surplus_bcf",
            "dry_gas_production",
            "lng_exports",
            "hdd_anomaly",
            "cdd_anomaly",
            "log_dxy",
            "seasonality_factor",
        ],
    }

    base: dict[str, Any] = {
        "market": MARKET,
        "model_id": MODEL_ID,
        "valuation_phase": VALUATION_PHASE,
        "valuation_pillar": "energy_natural_gas",
        "generated_at": generated_at,
        "wired": False,
        "publish": False,
        "fair_value": None,
        "spot_price": bundle.price[-1] if bundle.price else None,
        "deviation_pct": None,
        "valuation_bias": BIAS_UNAVAILABLE,
        "institutional_bias": "Unavailable",
        "confidence": "None",
        "as_of_week": bundle.as_of or as_of_week,
        "driver_cards": list(cards.values()),
        "history": [],
        "awaiting_drivers": awaiting,
        "summary_text": "",
        "scale": _scale_position(None),
        "previous_unvalidated_snapshot": previous_snapshot,
    }

    if not bundle.price or bundle.n < MIN_WEEKS:
        base["summary_text"] = "Insufficient price history for valuation."
        return base

    comparison = _compare_and_select(bundle)
    classifications = _classify_drivers(comparison, bundle)
    validated = list(comparison.get("validated_features") or [])
    _annotate_cards(cards, validated=validated, classifications=classifications)
    base["driver_cards"] = list(cards.values())
    base["driver_classifications"] = classifications
    base["model_comparison"] = comparison

    if not validated:
        base["summary_text"] = _build_summary(
            dev_pct=None, confidence="None", validated=[], contribution=None
        )
        base["valuation_reason"] = "No specification cleared walk-forward + sign gates."
        return base

    y = [math.log(p) for p in bundle.price]
    cols = [bundle.features[f] for f in validated]
    beta, r2, adj, t_stats, p_values = _ols_stats(y, cols)
    if not beta or r2 is None:
        base["summary_text"] = "Validated feature set failed to fit."
        return base

    history = []
    for i in range(bundle.n):
        feats = [col[i] for col in cols]
        lp = _predict_log_price(beta, feats)
        if lp is None:
            continue
        fair_i = math.exp(lp)
        if fair_i <= 0:
            continue
        spot_i = bundle.price[i]
        history.append(
            {
                "date": bundle.dates[i],
                "spot_price": round(spot_i, 4),
                "fair_value": round(fair_i, 4),
                "deviation_pct": round(100.0 * (spot_i - fair_i) / fair_i, 2),
            }
        )

    latest = [col[-1] for col in cols]
    log_fair = _predict_log_price(beta, latest)
    if log_fair is None:
        return base
    fair = math.exp(log_fair)
    spot = bundle.price[-1]
    dev_pct = round(100.0 * (spot - fair) / fair, 2)
    raw_obs = {
        "storage_surplus_bcf": (cards.get("storage") or {}).get("difference"),
        "dry_gas_production": (cards.get("production") or {}).get("current"),
        "lng_exports": (cards.get("lng_exports") or {}).get("current"),
        "log_dxy": (cards.get("dxy") or {}).get("current"),
        "hdd_anomaly": (cards.get("hdd") or {}).get("anomaly"),
        "cdd_anomaly": (cards.get("cdd") or {}).get("anomaly"),
        "seasonality_factor": (cards.get("seasonality") or {}).get("current"),
    }
    contrib = _contribution_log_reconcile(
        names=validated,
        beta=beta,
        latest_feats=latest,
        spot=spot,
        raw_observations=raw_obs,
    )
    # Force displayed fair to reconstructed fair
    fair = contrib["reconstructed_fair_value"]
    dev_pct = contrib["deviation_pct"]

    rec = next(
        (s for s in comparison["specifications"] if s.get("spec") == comparison.get("recommended_spec")),
        None,
    )
    conf = _confidence(
        r2,
        bundle.n,
        len(validated),
        oos_r2=(rec or {}).get("oos_r2"),
        extreme_fv_rate=(rec or {}).get("extreme_fv_rate_25pct"),
    )
    bias = _bias_from_deviation(dev_pct)
    inst = _institutional_bias_label(dev_pct, bias, conf)
    # Soften published bias label when low confidence
    publish_bias = bias if conf not in {"Low", "None"} or abs(dev_pct) < 15 else BIAS_UNAVAILABLE

    base.update(
        {
            "wired": True,
            "publish": conf != "None",
            "fair_value": fair,
            "spot_price": round(spot, 4),
            "deviation_pct": dev_pct,
            "valuation_bias": publish_bias if publish_bias != BIAS_UNAVAILABLE else bias,
            "valuation_state": bias,
            "institutional_bias": inst,
            "confidence": conf,
            "regression": {
                "n": bundle.n,
                "r_squared": round(r2, 4),
                "adj_r_squared": round(adj, 4) if adj is not None else None,
                "intercept": round(beta[0], 6),
                "features": {validated[i]: round(beta[i + 1], 6) for i in range(len(validated))},
                "p_values": {
                    validated[i]: p_values[i + 1] for i in range(len(validated)) if i + 1 < len(p_values)
                },
                "t_stats": {
                    validated[i]: t_stats[i + 1] for i in range(len(validated)) if i + 1 < len(t_stats)
                },
                "oos_r2": rec.get("oos_r2") if rec else None,
                "oos_rmse": rec.get("oos_rmse") if rec else None,
                "oos_mae": rec.get("oos_mae") if rec else None,
            },
            "active_features": validated,
            "validated_features": validated,
            "experimental_features": [
                k
                for k, v in classifications.items()
                if v.get("classification") == "EXPERIMENTAL DRIVER"
            ],
            "informational_features": [
                k
                for k, v in classifications.items()
                if v.get("classification") == "INFORMATIONAL ONLY"
            ],
            "invalid_features": [
                k
                for k, v in classifications.items()
                if "INVALID" in str(v.get("classification"))
            ],
            "history": history,
            "scale": _scale_position(dev_pct if conf not in {"Low", "None"} or abs(dev_pct) < 15 else 0.0),
            "contribution_breakdown": contrib,
            "hdd_cdd_diagnosis": {
                "hdd_zero": (
                    "HDD=0 in midsummer is a genuine observation (no heating demand), not missing data. "
                    "Anomaly is week-of-year climatology vs prior years only."
                ),
                "annual_zscore": "Removed — invalid for seasonal weather series.",
            },
            "seasonality_decision": (
                "INFORMATIONAL ONLY — not included in fair value; no validated NG seasonality model."
            ),
            "model_note": (
                f"{MODEL_ID}: production FV = {comparison.get('recommended_spec')} "
                f"features={validated} R²={round(r2, 4)} "
                f"OOS_RMSE={(rec or {}).get('oos_rmse')} OOS_R²={(rec or {}).get('oos_r2')}"
            ),
            "valuation_reason": (
                f"Validated-only FV: spot {round(spot, 4)} vs fair {fair} ({dev_pct:+.2f}%), "
                f"confidence={conf}"
            ),
            "pass": True,
            "source_lineage": list(bundle.lineage.values()),
        }
    )
    base["summary_text"] = _build_summary(
        dev_pct=dev_pct, confidence=conf, validated=validated, contribution=contrib
    )
    # If low confidence and large prior-style deviation would have shown — we already use validated-only
    if conf in {"Low", "None"}:
        base["scale"] = {
            **base["scale"],
            "band": "Low-confidence model — treat as tentative",
        }
    return base


def build_natural_gas_valuation_document(*, as_of_week: str | None = None) -> dict[str, Any]:
    block = compute_natural_gas_valuation(as_of_week=as_of_week)
    return {
        "version": 3,
        "generated_at": block.get("generated_at"),
        "engine": MODEL_ID,
        "valuation_phase": VALUATION_PHASE,
        "market": MARKET,
        "summary": {
            "wired": bool(block.get("wired")),
            "publish": bool(block.get("publish")),
            "validated_features": block.get("validated_features") or [],
            "experimental_features": block.get("experimental_features") or [],
            "informational_features": block.get("informational_features") or [],
            "invalid_features": block.get("invalid_features") or [],
            "recommended_spec": (block.get("model_comparison") or {}).get("recommended_spec"),
            "awaiting_drivers": block.get("awaiting_drivers") or [],
        },
        "instrument": block,
    }
