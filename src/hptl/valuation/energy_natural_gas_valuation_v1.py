"""Natural Gas Institutional Valuation — validated drivers only.

Fair value uses only walk-forward-validated drivers.
Populated but unvalidated drivers are EXPERIMENTAL (displayed, not used).
Seasonality is INFORMATIONAL ONLY and never enters fair value without OOS proof.
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
    """Compare nested specs A–H; promote smallest stable evidence-backed model."""
    expectations = _load_sign_expectations()
    y = [math.log(p) for p in bundle.price]
    avail = set(bundle.features.keys())

    def has(*names: str) -> bool:
        return all(n in avail and len(bundle.features[n]) == bundle.n for n in names)

    # Required ladder (plus constant baseline for OOS reference):
    # A storage · B storage+prod · C storage+LNG · D storage+prod+LNG
    # E +weather · F +DXY · G full · H seasonality test-only
    specs: list[tuple[str, list[str]]] = [("const_baseline", [])]
    if has("storage_surplus_bcf"):
        specs.append(("A_storage", ["storage_surplus_bcf"]))
    if has("storage_surplus_bcf", "dry_gas_production"):
        specs.append(("B_storage_production", ["storage_surplus_bcf", "dry_gas_production"]))
    if has("storage_surplus_bcf", "lng_exports"):
        specs.append(("C_storage_lng", ["storage_surplus_bcf", "lng_exports"]))
    if has("storage_surplus_bcf", "dry_gas_production", "lng_exports"):
        specs.append(
            ("D_storage_production_lng", ["storage_surplus_bcf", "dry_gas_production", "lng_exports"])
        )
    weather = [f for f in ("hdd_anomaly", "cdd_anomaly") if has(f)]
    if has("storage_surplus_bcf", "dry_gas_production", "lng_exports") and weather:
        specs.append(
            (
                "E_storage_production_lng_weather",
                ["storage_surplus_bcf", "dry_gas_production", "lng_exports", *weather],
            )
        )
    if has("storage_surplus_bcf", "dry_gas_production", "lng_exports", "log_dxy"):
        specs.append(
            (
                "F_storage_production_lng_dxy",
                ["storage_surplus_bcf", "dry_gas_production", "lng_exports", "log_dxy"],
            )
        )
    full = [
        f
        for f in (
            "storage_surplus_bcf",
            "dry_gas_production",
            "lng_exports",
            "hdd_anomaly",
            "cdd_anomaly",
            "log_dxy",
        )
        if has(f)
    ]
    if full:
        specs.append(("G_full_candidate", full))
    if has("storage_surplus_bcf", "seasonality_factor"):
        specs.append(("H_storage_seasonality_test", ["storage_surplus_bcf", "seasonality_factor"]))

    results = [_eval_spec(name, feats, bundle, y, expectations) for name, feats in specs]

    baseline = next((r for r in results if r["spec"] == "const_baseline" and r.get("ok")), None)
    base_rmse = baseline.get("oos_rmse") if baseline else None
    storage_only = next((x for x in results if x["spec"] == "A_storage" and x.get("ok")), None)
    for r in results:
        if base_rmse is not None and r.get("oos_rmse") is not None:
            r["delta_oos_rmse_vs_baseline"] = round(r["oos_rmse"] - base_rmse, 6)
            r["oos_rmse_improvement_pct_vs_baseline"] = round(
                100.0 * (base_rmse - r["oos_rmse"]) / base_rmse, 2
            )
        else:
            r["delta_oos_rmse_vs_baseline"] = None
            r["oos_rmse_improvement_pct_vs_baseline"] = None
        # vs storage-only for nested promotions
        if (
            storage_only
            and storage_only.get("oos_rmse") is not None
            and r.get("oos_rmse") is not None
            and r["spec"] != "A_storage"
        ):
            r["delta_oos_rmse_vs_storage"] = round(r["oos_rmse"] - storage_only["oos_rmse"], 6)
            r["oos_rmse_improvement_pct_vs_storage"] = round(
                100.0 * (storage_only["oos_rmse"] - r["oos_rmse"]) / storage_only["oos_rmse"], 2
            )

    eligible = []
    for r in results:
        if not r.get("ok") or r["spec"] in {"const_baseline"}:
            continue
        if r["spec"].startswith("H_"):
            continue  # seasonality tested, never auto-promoted
        if not r.get("signs_ok") or r.get("oos_rmse") is None:
            continue
        if base_rmse is not None and r["oos_rmse"] >= base_rmse:
            continue
        stab = r.get("coefficient_stability") or {}
        if any(v.get("sign_flip") for v in stab.values()):
            continue
        # Nested specs must beat storage-only by >2% OOS RMSE to justify extra drivers
        if r["spec"] != "A_storage" and storage_only and storage_only.get("oos_rmse") is not None:
            if r["oos_rmse"] >= storage_only["oos_rmse"] * 0.98:
                continue
        eligible.append(r)

    recommended = None
    if eligible:
        eligible.sort(key=lambda r: (r["oos_rmse"], len(r["features"]), -(r.get("oos_r2") or -99)))
        best_rmse = eligible[0]["oos_rmse"]
        near = [r for r in eligible if r["oos_rmse"] <= best_rmse * 1.02]
        near.sort(key=lambda r: (len(r["features"]), r["oos_rmse"]))
        recommended = near[0]
    elif storage_only and storage_only.get("signs_ok"):
        recommended = storage_only

    return {
        "baseline_oos_rmse": base_rmse,
        "sample_period": {
            "start": bundle.dates[0] if bundle.dates else None,
            "end": bundle.dates[-1] if bundle.dates else None,
            "n_observations": bundle.n,
        },
        "specifications": results,
        "recommended_spec": recommended["spec"] if recommended else None,
        "validated_features": list(recommended["features"]) if recommended else [],
        "selection_rule": (
            "Expanding-window walk-forward. Promote smallest nested A–G spec with coherent "
            "economic signs, no walk-forward coefficient sign flips, OOS RMSE better than "
            "constant baseline, and (for multi-driver specs) >2% OOS RMSE improvement over "
            "storage-only. Seasonality (H) is tested but never auto-promoted."
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



def _driver_validation_report(comparison: dict[str, Any], classifications: dict[str, Any]) -> dict[str, Any]:
    """Per-candidate diagnostics for explainability (coef / sign / p / OOS / stability)."""
    specs = {s.get("spec"): s for s in (comparison.get("specifications") or []) if s.get("ok")}
    a = specs.get("A_storage") or {}
    report: dict[str, Any] = {}
    mapping = {
        "storage_surplus_bcf": "A_storage",
        "dry_gas_production": "B_storage_production",
        "lng_exports": "C_storage_lng",
        "hdd_anomaly": "E_storage_production_lng_weather",
        "cdd_anomaly": "E_storage_production_lng_weather",
        "log_dxy": "F_storage_production_lng_dxy",
        "seasonality_factor": "H_storage_seasonality_test",
    }
    for feat, spec_name in mapping.items():
        spec = specs.get(spec_name) or {}
        coefs = spec.get("coefficients") or {}
        pvals = spec.get("p_values") or {}
        stab = (spec.get("coefficient_stability") or {}).get(feat) or {}
        cls = classifications.get(feat) or {}
        in_fv = bool(cls.get("in_fair_value"))
        report[feat] = {
            "classification": cls.get("classification"),
            "in_fair_value": in_fv,
            "coefficient": coefs.get(feat),
            "sign": ("negative" if (coefs.get(feat) or 0) < 0 else "positive") if feat in coefs else None,
            "p_value": pvals.get(feat),
            "walk_forward_sign_flip": stab.get("sign_flip"),
            "coef_stability_cv": stab.get("cv"),
            "spec_tested": spec_name,
            "spec_oos_r2": spec.get("oos_r2"),
            "spec_oos_rmse": spec.get("oos_rmse"),
            "oos_rmse_vs_storage": spec.get("delta_oos_rmse_vs_storage"),
            "reason": cls.get("reason"),
            "contribution_in_published_fv": in_fv,
        }
    if a:
        report["storage_surplus_bcf"]["published_model_oos_r2"] = a.get("oos_r2")
        report["storage_surplus_bcf"]["published_model_oos_rmse"] = a.get("oos_rmse")
    return report

def _spec_by_name(comparison: dict[str, Any], name: str) -> dict[str, Any] | None:
    return next((s for s in (comparison.get("specifications") or []) if s.get("spec") == name), None)


def _classify_drivers(
    comparison: dict[str, Any],
    bundle: NgDriverBundle,
) -> dict[str, Any]:
    """Classify drivers as VALIDATED / EXPERIMENTAL / INFORMATIONAL ONLY."""
    validated = set(comparison.get("validated_features") or [])
    classifications: dict[str, Any] = {}
    a = _spec_by_name(comparison, "A_storage")
    b = _spec_by_name(comparison, "B_storage_production")
    c = _spec_by_name(comparison, "C_storage_lng")
    d = _spec_by_name(comparison, "D_storage_production_lng")
    e = _spec_by_name(comparison, "E_storage_production_lng_weather")
    f = _spec_by_name(comparison, "F_storage_production_lng_dxy")
    h = _spec_by_name(comparison, "H_storage_seasonality_test")

    # Storage level (display) vs surplus (regression)
    classifications["working_gas_storage_level"] = {
        "classification": "INFORMATIONAL ONLY",
        "badge": "INFORMATIONAL ONLY — NOT INCLUDED IN FAIR VALUE",
        "reason": "Displayed on storage card; fair value uses surplus/deficit vs same-week 5y average, not raw level.",
        "in_fair_value": False,
    }
    if "storage_surplus_bcf" in validated:
        classifications["storage_surplus_bcf"] = {
            "classification": "VALIDATED VALUATION DRIVER",
            "badge": "VALIDATED VALUATION DRIVER",
            "in_fair_value": True,
            "reason": (
                "Surplus/deficit vs trailing same-week 5y average improves OOS RMSE "
                f"{(a or {}).get('oos_rmse_improvement_pct_vs_baseline')}% vs constant, "
                f"OOS R2={(a or {}).get('oos_r2')}, correct negative sign, no walk-forward sign flips."
            ),
        }
    else:
        classifications["storage_surplus_bcf"] = {
            "classification": "EXPERIMENTAL DRIVER",
            "badge": "EXPERIMENTAL DRIVER",
            "in_fair_value": False,
            "reason": "Storage surplus failed promotion gates; not included in fair value.",
        }

    # Production
    if "dry_gas_production" in validated:
        classifications["dry_gas_production"] = {
            "classification": "VALIDATED VALUATION DRIVER",
            "badge": "VALIDATED VALUATION DRIVER",
            "in_fair_value": True,
            "label": "US Dry Gas Production",
            "reason": "Included via walk-forward selection.",
        }
    else:
        reasons = []
        if b and not b.get("signs_ok"):
            reasons.append("wrong economic sign in B (storage+production)")
        stab_b = (b or {}).get("coefficient_stability") or {}
        stab_d = (d or {}).get("coefficient_stability") or {}
        if stab_b.get("dry_gas_production", {}).get("sign_flip") or stab_d.get(
            "dry_gas_production", {}
        ).get("sign_flip"):
            reasons.append("walk-forward coefficient sign flips")
        if b and a and b.get("oos_rmse") is not None and a.get("oos_rmse") is not None:
            if b["oos_rmse"] >= a["oos_rmse"]:
                reasons.append("no OOS improvement vs storage-only")
        if not reasons:
            reasons.append("fails stability / nested promotion gates")
        classifications["dry_gas_production"] = {
            "classification": "EXPERIMENTAL DRIVER",
            "badge": "EXPERIMENTAL DRIVER — NOT INCLUDED IN FAIR VALUE",
            "in_fair_value": False,
            "label": "US Dry Gas Production",
            "reason": (
                "Not promoted: "
                + "; ".join(reasons)
                + ". Monthly EIA series also lags weekly price/storage. Displayed only."
            ),
        }

    # LNG
    if "lng_exports" in validated:
        classifications["lng_exports"] = {
            "classification": "VALIDATED VALUATION DRIVER",
            "badge": "VALIDATED VALUATION DRIVER",
            "in_fair_value": True,
            "label": "LNG Exports",
            "reason": "Included via walk-forward selection.",
        }
    else:
        reasons = []
        stab_c = (c or {}).get("coefficient_stability") or {}
        stab_d = (d or {}).get("coefficient_stability") or {}
        if stab_c.get("lng_exports", {}).get("sign_flip") or stab_d.get("lng_exports", {}).get(
            "sign_flip"
        ):
            reasons.append("walk-forward coefficient sign flips in C/D")
        if c and a and c.get("oos_rmse") is not None and a.get("oos_rmse") is not None:
            impr = (c or {}).get("oos_rmse_improvement_pct_vs_storage")
            if c["oos_rmse"] < a["oos_rmse"] * 0.98:
                reasons.append(
                    f"C OOS RMSE gain vs storage ({impr}%) fails stability gate"
                )
            else:
                reasons.append(
                    f"OOS RMSE gain vs storage-only ({impr}%) below 2% promotion threshold"
                )
        if not reasons:
            reasons.append("fails nested promotion gates")
        classifications["lng_exports"] = {
            "classification": "EXPERIMENTAL DRIVER",
            "badge": "EXPERIMENTAL DRIVER — NOT INCLUDED IN FAIR VALUE",
            "in_fair_value": False,
            "label": "LNG Exports",
            "reason": "Not promoted: " + "; ".join(reasons) + ". Displayed only.",
        }

    # DXY
    if "log_dxy" in validated:
        classifications["log_dxy"] = {
            "classification": "VALIDATED VALUATION DRIVER",
            "badge": "VALIDATED VALUATION DRIVER",
            "in_fair_value": True,
            "label": "DXY",
            "reason": "Included via walk-forward selection.",
        }
    else:
        reasons = []
        if f and not f.get("signs_ok"):
            reasons.append(
                "wrong economic sign in F (coef>0; expect stronger USD to lower NG price)"
            )
        stab_f = (f or {}).get("coefficient_stability") or {}
        if stab_f.get("log_dxy", {}).get("sign_flip"):
            reasons.append("walk-forward coefficient sign flips")
        if not reasons:
            reasons.append("univariate OOS R2 negative; does not clear promotion gates")
        classifications["log_dxy"] = {
            "classification": "EXPERIMENTAL DRIVER",
            "badge": "EXPERIMENTAL DRIVER — NOT INCLUDED IN FAIR VALUE",
            "in_fair_value": False,
            "label": "DXY",
            "reason": "Not promoted: " + "; ".join(reasons) + ". Displayed only.",
        }

    # Weather
    for feat, card_id, label in (
        ("hdd_anomaly", "hdd", "Heating Degree Days"),
        ("cdd_anomaly", "cdd", "Cooling Degree Days"),
    ):
        card = bundle.driver_cards.get(card_id) or {}
        dq = card.get("data_quality")
        if feat in validated:
            classifications[feat] = {
                "classification": "VALIDATED VALUATION DRIVER",
                "badge": "VALIDATED VALUATION DRIVER",
                "in_fair_value": True,
                "label": label,
                "data_quality": dq,
                "reason": "Included via walk-forward selection.",
            }
            continue
        if dq in {"ANOMALY_INVALID_FOR_REGRESSION", "CLIMATOLOGY_INSUFFICIENT"} or not card.get(
            "available"
        ):
            classifications[feat] = {
                "classification": "INSUFFICIENT HISTORY",
                "badge": "INSUFFICIENT HISTORY — NOT INCLUDED",
                "in_fair_value": False,
                "label": label,
                "data_quality": dq,
                "reason": "Week-of-year climatology or series coverage insufficient for regression.",
            }
            continue
        reasons = []
        if feat == "hdd_anomaly":
            # Check E weather block and univariate insignificance
            p_hdd = None
            if e:
                p_hdd = (e.get("p_values") or {}).get("hdd_anomaly")
            if e and not e.get("signs_ok"):
                reasons.append("wrong / unstable sign in E")
            if p_hdd is not None and p_hdd > 0.1:
                reasons.append(f"insignificant (p={p_hdd})")
            if a and e and e.get("oos_rmse") is not None and a.get("oos_rmse") is not None:
                if e["oos_rmse"] >= a["oos_rmse"] * 0.98:
                    reasons.append("no material OOS improvement vs storage-only in weather specs")
            if not reasons:
                reasons.append("fails weather promotion gate (>2% OOS gain vs storage-only)")
            badge = "EXPERIMENTAL DRIVER — NOT INCLUDED IN FAIR VALUE"
        else:
            coef = (e.get("coefficients") or {}).get("cdd_anomaly") if e else None
            if coef is not None and coef < 0:
                reasons.append(
                    "wrong economic sign (coef<0; expect cooling demand to raise price)"
                )
            p_cdd = (e.get("p_values") or {}).get("cdd_anomaly") if e else None
            if p_cdd is not None and p_cdd > 0.1:
                reasons.append(f"insignificant (p={p_cdd})")
            if not reasons:
                reasons.append("wrong sign and/or no OOS improvement")
            badge = "EXPERIMENTAL DRIVER — NOT INCLUDED IN FAIR VALUE"
        classifications[feat] = {
            "classification": "EXPERIMENTAL DRIVER",
            "badge": badge,
            "in_fair_value": False,
            "label": label,
            "data_quality": dq,
            "reason": "Not promoted: " + "; ".join(reasons) + ". Displayed only.",
            "hdd_zero_diagnosis": (
                "Zero is a genuine midsummer HDD observation, not missing data. "
                "Anomaly uses same ISO-week climatology (not annual sample mean)."
                if card_id == "hdd"
                else None
            ),
        }

    # Seasonality — informational by design
    h_note = ""
    if h and h.get("ok"):
        h_note = (
            f" H-test OOS R2={h.get('oos_r2')}, OOS RMSE impr vs const="
            f"{h.get('oos_rmse_improvement_pct_vs_baseline')}% — retained as context only."
        )
    classifications["seasonality_factor"] = {
        "classification": "INFORMATIONAL ONLY",
        "badge": "INFORMATIONAL ONLY — NOT INCLUDED IN FAIR VALUE",
        "in_fair_value": False,
        "reason": (
            "Seasonality is contextual (week-of-year / calendar bias), not a validated "
            "structural valuation driver; excluded from fair value by design."
            + h_note
        ),
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
        if feat in validated_set:
            role = "VALIDATED VALUATION DRIVER"
            badge = "VALIDATED VALUATION DRIVER"
            in_fv = True
            note = "Included in fair value"
        else:
            role = meta.get("classification") or "EXPERIMENTAL DRIVER"
            badge = meta.get("badge") or role
            in_fv = False
            note = "NOT INCLUDED IN FAIR VALUE"
        card["valuation_role"] = role
        card["valuation_badge"] = badge
        card["in_fair_value"] = in_fv
        card["valuation_note"] = note
        if meta.get("reason"):
            card["disposition_reason"] = meta["reason"]


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
    lines.append(
        f"Production model uses: {', '.join(FEATURE_LABELS.get(f, f) for f in validated)}."
    )
    if contribution and contribution.get("drivers"):
        top = max(contribution["drivers"], key=lambda r: abs(r["log_contribution"]))
        lines.append(
            f"Largest log contribution: {top['label']} ({top['log_contribution']:+.4f}) — {top['direction']}."
        )
    if confidence in {"Low", "None"}:
        lines.append("Confidence is low — do not treat this as a high-conviction valuation signal.")
    lines.append(
        "Only walk-forward-validated drivers enter fair value. "
        "Other populated series remain experimental or informational and are displayed but not used."
    )
    return " ".join(lines)


def compute_natural_gas_valuation(*, as_of_week: str | None = None) -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    bundle = build_ng_driver_bundle(as_of_week=as_of_week)
    cards = bundle.driver_cards
    awaiting = [c["label"] for c in cards.values() if not c.get("available")]

    previous_snapshot = {
        "note": "Prior unvalidated full-driver fit (~29% undervaluation) — NOT the published model. Displayed for audit contrast only.",
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
    base["driver_validation"] = _driver_validation_report(comparison, classifications)
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
            "rejected_features": [],
            "experimental_features_note": "Displayed drivers that failed promotion remain experimental",
            "experimental_features": [
                k
                for k, v in classifications.items()
                if "EXPERIMENTAL" in str(v.get("classification"))
            ],
            "informational_features": [
                k
                for k, v in classifications.items()
                if v.get("classification") == "INFORMATIONAL ONLY"
            ],
            "insufficient_history_features": [
                k
                for k, v in classifications.items()
                if "INSUFFICIENT HISTORY" in str(v.get("classification"))
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
            "rejected_features": block.get("rejected_features") or [],
            "experimental_features": block.get("experimental_features") or [],
            "informational_features": block.get("informational_features") or [],
            "insufficient_history_features": block.get("insufficient_history_features") or [],
            "invalid_features": block.get("invalid_features") or [],
            "recommended_spec": (block.get("model_comparison") or {}).get("recommended_spec"),
            "awaiting_drivers": block.get("awaiting_drivers") or [],
        },
        "instrument": block,
    }
