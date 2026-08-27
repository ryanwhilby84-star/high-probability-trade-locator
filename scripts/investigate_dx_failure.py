"""Phase 1E — investigate DX avg_g10_2y_vs_usd wrong-sign failure (diagnostic only)."""
from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
from scipy.optimize import lsq_linear

from hptl.config import DATA_DIR
from hptl.fx.fx_macro_history import currency_histories
from hptl.valuation.currency_futures_ive_v1 import (
    FUTURES_REGISTRY,
    _build_dx_panel,
    _load_futures_daily,
    _ols_log_futures,
)

AUDIT_JSON = DATA_DIR / "audits/dx_failure_investigation.json"
AUDIT_MD = DATA_DIR / "audits/dx_failure_investigation.md"

FEATURES = ("avg_g10_2y_vs_usd", "fed_funds", "real_yield_10y")


def _panel_df(panel: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for r in panel:
        rows.append(
            {
                "date": r["date"],
                "dx_close": float(r["close"]),
                "log_dx": math.log(float(r["close"])),
                "avg_g10_2y_vs_usd": float(r["avg_g10_2y_vs_usd"]),
                "fed_funds": float(r["fed_funds"]),
                "real_yield_10y": float(r["real_yield_10y"]),
            }
        )
    return pd.DataFrame(rows)


def _price_metrics(y_log: np.ndarray, pred_log: np.ndarray, dx: np.ndarray) -> dict[str, float]:
    pred_price = np.exp(pred_log)
    err = dx - pred_price
    ss_res = float(((y_log - pred_log) ** 2).sum())
    ss_tot = float(((y_log - y_log.mean()) ** 2).sum())
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return {
        "r_squared": round(r2, 4),
        "mae_price": round(float(np.mean(np.abs(err))), 4),
        "rmse_price": round(float(np.sqrt(np.mean(err**2))), 4),
    }


def _fit_ols(df: pd.DataFrame, feature_names: tuple[str, ...]) -> dict[str, Any]:
    reg = _ols_log_futures(
        [
            {
                "date": row.date,
                "close": row.dx_close,
                **{f: getattr(row, f) for f in feature_names},
            }
            for row in df.itertuples(index=False)
        ],
        feature_names,
    )
    if not reg.get("ok"):
        return reg
    coef = reg["coefficients"]
    y = df["log_dx"].values
    X = df[list(feature_names)].assign(intercept=1.0).values
    pred = X @ np.array([coef[f] for f in feature_names] + [coef["intercept"]])
    metrics = _price_metrics(y, pred, df["dx_close"].values)
    reg.update(metrics)
    return reg


def _correlation_matrix(df: pd.DataFrame) -> dict[str, Any]:
    cols = ["dx_close", "log_dx", *FEATURES]
    corr = df[cols].corr().round(4)
    matrix = {c: {r: float(corr.loc[r, c]) for r in cols} for c in cols}
    return {"variables": cols, "matrix": matrix}


def _vif(df: pd.DataFrame) -> dict[str, float]:
    out: dict[str, float] = {}
    for target in FEATURES:
        others = [f for f in FEATURES if f != target]
        y = df[target].values
        X = df[others].assign(intercept=1.0).values
        coef, *_ = np.linalg.lstsq(X, y, rcond=None)
        pred = X @ coef
        ss_res = float(((y - pred) ** 2).sum())
        ss_tot = float(((y - y.mean()) ** 2).sum())
        r2 = ss_res / ss_tot if ss_tot > 0 else 0.0
        out[target] = round(1.0 / (1.0 - r2), 4) if r2 < 1.0 else float("inf")
    return out


def _coefficient_stability(df: pd.DataFrame) -> dict[str, Any]:
    end = datetime.strptime(str(df["date"].iloc[-1])[:10], "%Y-%m-%d").date()
    windows = {
        "3_year": end - timedelta(days=int(3 * 365.25)),
        "5_year": end - timedelta(days=int(5 * 365.25)),
        "full_sample": date.min,
    }
    results: dict[str, Any] = {}
    full = _fit_ols(df, FEATURES)
    full_coef = full.get("coefficients") or {}

    for label, start in windows.items():
        sub = df[df["date"].apply(lambda d: datetime.strptime(str(d)[:10], "%Y-%m-%d").date() >= start)]
        fit = _fit_ols(sub, FEATURES)
        coef = fit.get("coefficients") or {}
        drift = {}
        if full_coef:
            for f in FEATURES:
                drift[f] = round(float(coef.get(f, 0)) - float(full_coef.get(f, 0)), 6)
        results[label] = {
            "n": fit.get("n"),
            "sample_start": sub["date"].iloc[0] if len(sub) else None,
            "sample_end": sub["date"].iloc[-1] if len(sub) else None,
            "coefficients": coef,
            "r_squared": fit.get("r_squared"),
            "coefficient_drift_vs_full": drift,
        }
    return {"windows": results, "full_sample_coefficients": full_coef}


def _sign_constrained_fit(df: pd.DataFrame) -> dict[str, Any]:
    """Force avg_g10_2y_vs_usd coefficient <= 0; others unconstrained."""
    y = df["log_dx"].values
    X = df[list(FEATURES)].values
    # lsq_linear: min ||Xb - y|| with bounds on b
    # columns: avg_g10, fed_funds, real_yield_10y — no intercept in X yet
    lb = [-np.inf, -np.inf, -np.inf]  # will add intercept separately
    # Add intercept column
    X_aug = np.column_stack([X, np.ones(len(y))])
    # bounds: [avg_g10 <= 0, fed >= -inf, real >= -inf, intercept free]
    lower = [-np.inf, -np.inf, -np.inf, -np.inf]
    upper = [0.0, np.inf, np.inf, np.inf]
    res = lsq_linear(X_aug, y, bounds=(lower, upper), lsmr_tol="auto", verbose=0)
    coefs = {
        "avg_g10_2y_vs_usd": float(res.x[0]),
        "fed_funds": float(res.x[1]),
        "real_yield_10y": float(res.x[2]),
        "intercept": float(res.x[3]),
    }
    pred = X_aug @ res.x
    metrics = _price_metrics(y, pred, df["dx_close"].values)
    return {"coefficients": coefs, "constraint": "avg_g10_2y_vs_usd <= 0", **metrics}


def _single_variable_tests(df: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for feat in FEATURES:
        fit = _fit_ols(df, (feat,))
        out[feat] = {
            "coefficient": (fit.get("coefficients") or {}).get(feat),
            "intercept": (fit.get("coefficients") or {}).get("intercept"),
            "r_squared": fit.get("r_squared"),
            "mae_price": fit.get("mae_price"),
            "rmse_price": fit.get("rmse_price"),
            "n": fit.get("n"),
            "economic_sign_expected": "negative" if feat == "avg_g10_2y_vs_usd" else "positive",
            "fitted_sign": (
                "negative"
                if (fit.get("coefficients") or {}).get(feat, 0) < 0
                else "positive"
            ),
            "sign_matches_economics": (
                (fit.get("coefficients") or {}).get(feat, 0) < 0
                if feat == "avg_g10_2y_vs_usd"
                else (fit.get("coefficients") or {}).get(feat, 0) > 0
            ),
        }
    return out


def _pairwise_correlations(df: pd.DataFrame) -> dict[str, float]:
    pairs: dict[str, float] = {}
    for i, a in enumerate(FEATURES):
        for b in FEATURES[i + 1 :]:
            pairs[f"{a} vs {b}"] = round(float(df[a].corr(df[b])), 4)
    for f in FEATURES:
        pairs[f"{f} vs log_dx"] = round(float(df[f].corr(df["log_dx"])), 4)
    return pairs


def _economic_attribution(
    baseline: dict[str, Any],
    single: dict[str, Any],
    vif: dict[str, float],
    pairwise: dict[str, float],
    stability: dict[str, Any],
) -> dict[str, Any]:
    uni_g10 = single["avg_g10_2y_vs_usd"]
    uni_ff = single["fed_funds"]
    uni_ry = single["real_yield_10y"]

    explanatory = []
    redundant = []
    proxy = []

    # Univariate R² ranking
    r2_rank = sorted(FEATURES, key=lambda f: single[f]["r_squared"], reverse=True)

    if uni_ff["r_squared"] >= max(uni_g10["r_squared"], uni_ry["r_squared"]):
        explanatory.append(
            {
                "variable": "fed_funds",
                "reason": f"Solo R²={uni_ff['r_squared']} — strongest single driver of log(DX).",
            }
        )
    if uni_ry["r_squared"] > 0.05:
        explanatory.append(
            {
                "variable": "real_yield_10y",
                "reason": f"Solo R²={uni_ry['r_squared']} — meaningful incremental USD real-rate channel.",
            }
        )
    if uni_g10["sign_matches_economics"]:
        explanatory.append(
            {
                "variable": "avg_g10_2y_vs_usd",
                "reason": f"Univariate sign is correct (β={uni_g10['coefficient']}) with R²={uni_g10['r_squared']}.",
            }
        )
    else:
        proxy.append(
            {
                "variable": "avg_g10_2y_vs_usd",
                "reason": (
                    f"Univariate β={uni_g10['coefficient']} ({uni_g10['fitted_sign']}) "
                    f"already conflicts with theory; multivariate sign flip is not only a collinearity artifact."
                ),
            }
        )

    # Collinearity
    if vif.get("avg_g10_2y_vs_usd", 0) >= 5 or abs(pairwise.get("avg_g10_2y_vs_usd vs fed_funds", 0)) >= 0.7:
        proxy.append(
            {
                "variable": "avg_g10_2y_vs_usd",
                "reason": (
                    f"VIF={vif.get('avg_g10_2y_vs_usd')}, "
                    f"corr with fed_funds={pairwise.get('avg_g10_2y_vs_usd vs fed_funds')} — "
                    "partially proxies US rate cycle when combined with fed_funds."
                ),
            }
        )
        redundant.append(
            {
                "variable": "avg_g10_2y_vs_usd",
                "reason": "Incremental R² in multivariate model may be absorbed by fed_funds/real_yield.",
            }
        )

    drift = stability["windows"]["3_year"]["coefficient_drift_vs_full"]
    if drift.get("avg_g10_2y_vs_usd") and abs(drift["avg_g10_2y_vs_usd"]) > 0.02:
        proxy.append(
            {
                "variable": "avg_g10_2y_vs_usd",
                "reason": f"Coefficient unstable across windows (3Y drift={drift['avg_g10_2y_vs_usd']}).",
            }
        )

    return {
        "primary_driver_ranking_by_univariate_r2": r2_rank,
        "explanatory_variables": explanatory,
        "redundant_variables": redundant,
        "proxy_variables": proxy,
        "interpretation": _interpretation(baseline, single, vif, pairwise),
    }


def _interpretation(
    baseline: dict[str, Any],
    single: dict[str, Any],
    vif: dict[str, float],
    pairwise: dict[str, float],
) -> str:
    g10_uni = single["avg_g10_2y_vs_usd"]
    g10_multi = (baseline.get("coefficients") or {}).get("avg_g10_2y_vs_usd")
    corr_ff = pairwise.get("avg_g10_2y_vs_usd vs fed_funds", 0)
    parts = []

    if g10_uni["sign_matches_economics"] and g10_multi > 0:
        parts.append(
            "Univariate avg_g10_2y_vs_usd has the economically correct NEGATIVE sign, "
            "but the multivariate OLS coefficient flips POSITIVE — classic multicollinearity / omitted-variable interaction."
        )
    elif not g10_uni["sign_matches_economics"]:
        parts.append(
            "avg_g10_2y_vs_usd has the wrong sign even alone — feature construction or the "
            "G10-minus-USD spread may not map cleanly to DX."
        )

    parts.append(
        f"avg_g10_2y_vs_usd vs fed_funds correlation = {corr_ff}: "
        + (
            "strong negative co-movement — when US hikes, G10-USD spread falls while fed_funds rises; "
            "OLS confounds the two effects."
            if corr_ff < -0.5
            else "moderate co-movement with fed_funds."
        )
    )
    parts.append(
        f"fed_funds alone explains R²={single['fed_funds']['r_squared']} vs "
        f"avg_g10 alone R²={g10_uni['r_squared']} — US policy rate dominates."
    )
    return " ".join(parts)


def _rebuild_recommendation(
    baseline: dict[str, Any],
    constrained: dict[str, Any],
    single: dict[str, Any],
    vif: dict[str, float],
    attribution: dict[str, Any],
) -> dict[str, Any]:
    g10_vif = vif.get("avg_g10_2y_vs_usd", 0)
    g10_uni_ok = single["avg_g10_2y_vs_usd"]["sign_matches_economics"]
    g10_multi_wrong = (baseline.get("coefficients") or {}).get("avg_g10_2y_vs_usd", 0) > 0
    r2_base = baseline.get("r_squared", 0)
    r2_con = constrained.get("r_squared", 0)
    r2_drop = r2_base - r2_con

    reasons: list[str] = []

    if g10_uni_ok and g10_multi_wrong and g10_vif >= 3:
        classification = "MODIFIED_MODEL"
        reasons.append(
            "avg_g10_2y_vs_usd is economically valid alone but sign-flips in multivariate fit due to "
            f"collinearity (VIF={g10_vif})."
        )
        reasons.append(
            f"Sign-constrained refit: R² {r2_base} → {r2_con} (Δ={round(r2_drop, 4)}), "
            f"MAE {baseline.get('mae_price')} → {constrained.get('mae_price')}."
        )
        reasons.append(
            "Recommendation: drop avg_g10_2y_vs_usd OR replace with orthogonalized spread "
            "(residual of avg_g10 regressed on fed_funds) OR enforce sign constraint — not full rebuild."
        )
    elif not g10_uni_ok:
        classification = "FULL_REBUILD"
        reasons.append("avg_g10_2y_vs_usd wrong sign even in univariate regression — feature spec failure.")
    elif r2_drop > 0.05:
        classification = "MODIFIED_MODEL"
        reasons.append(f"Sign constraint costs {round(r2_drop, 4)} R² — modify spec rather than keep as-is.")
    else:
        classification = "KEEP_MODEL"
        reasons.append("Sign constraint preserves fit — consider constrained OLS only.")

    if single["fed_funds"]["r_squared"] > 0.35:
        reasons.append(
            f"fed_funds is the genuine primary driver (solo R²={single['fed_funds']['r_squared']})."
        )

    return {"classification": classification, "reasons": reasons}


def build_investigation_report() -> dict[str, Any]:
    spec = FUTURES_REGISTRY["DX"]
    histories = currency_histories()
    futures_daily, _ = _load_futures_daily(spec.instrument_id)
    panel = _build_dx_panel(futures_daily, histories)
    df = _panel_df(panel)

    baseline = _fit_ols(df, FEATURES)
    corr = _correlation_matrix(df)
    vif = _vif(df)
    pairwise = _pairwise_correlations(df)
    stability = _coefficient_stability(df)
    constrained = _sign_constrained_fit(df)
    single = _single_variable_tests(df)
    attribution = _economic_attribution(baseline, single, vif, pairwise, stability)
    recommendation = _rebuild_recommendation(baseline, constrained, single, vif, attribution)

    return {
        "phase": "1E DX Failure Investigation",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sample": {
            "start": df["date"].iloc[0],
            "end": df["date"].iloc[-1],
            "n": len(df),
        },
        "baseline_multivariate": baseline,
        "correlation_matrix": corr,
        "multicollinearity": {
            "vif": vif,
            "pairwise_correlations": pairwise,
            "coefficient_stability": stability,
            "vif_interpretation": {
                "rule": "VIF > 5 suggests problematic collinearity; VIF > 10 severe",
                "avg_g10_2y_vs_usd": "high" if vif.get("avg_g10_2y_vs_usd", 0) >= 5 else "moderate" if vif.get("avg_g10_2y_vs_usd", 0) >= 3 else "low",
            },
        },
        "sign_constrained_test": {
            "constraint": "avg_g10_2y_vs_usd coefficient <= 0",
            "result": constrained,
            "baseline_comparison": {
                "r_squared_change": round(baseline.get("r_squared", 0) - constrained.get("r_squared", 0), 4),
                "mae_change": round(constrained.get("mae_price", 0) - baseline.get("mae_price", 0), 4),
                "rmse_change": round(constrained.get("rmse_price", 0) - baseline.get("rmse_price", 0), 4),
                "coefficient_changes": {
                    f: round(
                        constrained["coefficients"][f] - (baseline.get("coefficients") or {}).get(f, 0),
                        6,
                    )
                    for f in (*FEATURES, "intercept")
                },
            },
        },
        "single_variable_tests": single,
        "economic_attribution": attribution,
        "rebuild_recommendation": recommendation,
    }


def _render_md(doc: dict[str, Any]) -> str:
    base = doc["baseline_multivariate"]
    coef = base.get("coefficients") or {}
    vif = doc["multicollinearity"]["vif"]
    pairwise = doc["multicollinearity"]["pairwise_correlations"]
    con = doc["sign_constrained_test"]
    single = doc["single_variable_tests"]
    attr = doc["economic_attribution"]
    rec = doc["rebuild_recommendation"]

    lines = [
        "# Phase 1E — DX Failure Investigation",
        "",
        f"Generated: {doc['generated_at']}",
        f"Sample: {doc['sample']['start']} → {doc['sample']['end']} (n={doc['sample']['n']})",
        "",
        "## 1. Correlation Matrix",
        "",
    ]

    vars_ = doc["correlation_matrix"]["variables"]
    mat = doc["correlation_matrix"]["matrix"]
    lines.append("| | " + " | ".join(vars_) + " |")
    lines.append("|" + "---|" * (len(vars_) + 1))
    for r in vars_:
        lines.append("| **" + r + "** | " + " | ".join(str(mat[r][c]) for c in vars_) + " |")

    lines.extend(["", "## 2. Multicollinearity Analysis", "", "### VIF", ""])
    for k, v in vif.items():
        flag = " ⚠️" if v >= 5 else ""
        lines.append(f"- **{k}:** {v}{flag}")

    lines.extend(["", "### Pairwise correlations", ""])
    for k, v in sorted(pairwise.items()):
        lines.append(f"- {k}: **{v}**")

    lines.extend(["", "### Coefficient stability (multivariate OLS)", ""])
    for label, w in doc["multicollinearity"]["coefficient_stability"]["windows"].items():
        c = w.get("coefficients") or {}
        lines.append(f"**{label}** (n={w.get('n')}, {w.get('sample_start')} → {w.get('sample_end')})")
        lines.append(f"- avg_g10: {c.get('avg_g10_2y_vs_usd')} | fed: {c.get('fed_funds')} | real10: {c.get('real_yield_10y')} | R²={w.get('r_squared')}")
        lines.append("")

    lines.extend(["", "## 3. Sign-Constrained Test (avg_g10 ≤ 0)", ""])
    cc = con["result"]["coefficients"]
    lines.extend(
        [
            f"- R²: {con['result']['r_squared']} (baseline {base.get('r_squared')}, Δ={con['baseline_comparison']['r_squared_change']})",
            f"- MAE: {con['result']['mae_price']} (baseline {base.get('mae_price')}, Δ={con['baseline_comparison']['mae_change']})",
            f"- RMSE: {con['result']['rmse_price']} (baseline {base.get('rmse_price')}, Δ={con['baseline_comparison']['rmse_change']})",
            "",
            "**Coefficients under constraint:**",
            f"- avg_g10_2y_vs_usd: {cc['avg_g10_2y_vs_usd']}",
            f"- fed_funds: {cc['fed_funds']}",
            f"- real_yield_10y: {cc['real_yield_10y']}",
            f"- intercept: {cc['intercept']}",
            "",
        ]
    )

    lines.extend(["## 4. Single Variable Tests", ""])
    lines.append("| Variable | β | R² | MAE | RMSE | Expected sign | Actual sign | Match |")
    lines.append("|----------|---|-----|-----|------|---------------|-------------|-------|")
    for f in FEATURES:
        s = single[f]
        lines.append(
            f"| {f} | {s['coefficient']} | {s['r_squared']} | {s['mae_price']} | {s['rmse_price']} | "
            f"{s['economic_sign_expected']} | {s['fitted_sign']} | {'✓' if s['sign_matches_economics'] else '✗'} |"
        )

    lines.extend(["", "## 5. Economic Attribution", "", attr["interpretation"], ""])
    for item in attr.get("explanatory_variables") or []:
        lines.append(f"- **Explanatory — {item['variable']}:** {item['reason']}")
    for item in attr.get("redundant_variables") or []:
        lines.append(f"- **Redundant — {item['variable']}:** {item['reason']}")
    for item in attr.get("proxy_variables") or []:
        lines.append(f"- **Proxy — {item['variable']}:** {item['reason']}")

    lines.extend(["", "## 6. Rebuild Recommendation", "", f"### **{rec['classification']}**", ""])
    for r in rec["reasons"]:
        lines.append(f"- {r}")

    return "\n".join(lines) + "\n"


def main() -> int:
    doc = build_investigation_report()
    AUDIT_JSON.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_JSON.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    AUDIT_MD.write_text(_render_md(doc), encoding="utf-8")
    print(f"Wrote {AUDIT_JSON}")
    print(f"Wrote {AUDIT_MD}")
    print(f"RECOMMENDATION: {doc['rebuild_recommendation']['classification']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
