"""Gold institutional model research — WGC monthly CB feature variants with full OLS stats."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.valuation.institutional_publish_gate import (
    MIN_R2_PRODUCTION,
    apply_metals_institutional_publish_gate,
    build_metals_sign_diagnostic,
)
from hptl.valuation.metals_institutional_drivers import (
    DriverBundle,
    _asof_value,
    _load_cache_series,
    _weekly_from_daily,
    build_driver_bundle,
)
from hptl.valuation.metals_valuation_v1 import MIN_WEEKS, _predict_log_price

RESEARCH_JSON = PROJECT_ROOT / "data" / "processed" / "gold_valuation_model_research_latest.json"
RESEARCH_MD = PROJECT_ROOT / "data" / "processed" / "gold_valuation_model_research_latest.md"
CB_CACHE_REL = "data/cache/metals_drivers/wgc_cb_gold_net_purchases.json"

BASE_FEATURES = ("real_yield", "log_dxy", "etf_holdings")
SIGN_EXPECTATIONS: dict[str, str] = {
    "real_yield": "negative",
    "log_dxy": "negative",
    "etf_holdings": "positive",
}


@dataclass(frozen=True)
class GoldVariantSpec:
    variant_id: str
    label: str
    cb_feature: str
    cb_sign: str  # negative | positive | any
    engineer: str  # level | lag1 | roll12 | yoy | interaction_real_yield | interaction_dxy


GOLD_VARIANTS: tuple[GoldVariantSpec, ...] = (
    GoldVariantSpec("baseline_wgc_monthly", "WGC monthly net purchases (level)", "cb_net_purchases", "positive", "level"),
    GoldVariantSpec("cb_lag1", "CB purchases lagged 1 month", "cb_lag1", "positive", "lag1"),
    GoldVariantSpec("cb_roll12", "CB rolling 12-month net purchases", "cb_roll12", "positive", "roll12"),
    GoldVariantSpec("cb_yoy", "CB purchases year-over-year change", "cb_yoy", "positive", "yoy"),
    GoldVariantSpec(
        "cb_x_real_yield",
        "CB level × real yield interaction",
        "cb_x_real_yield",
        "any",
        "interaction_real_yield",
    ),
    GoldVariantSpec(
        "cb_x_dxy",
        "CB level × log(DXY) interaction",
        "cb_x_dxy",
        "any",
        "interaction_dxy",
    ),
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) != len(ys) or len(xs) < 3:
        return None
    mx = sum(xs) / len(xs)
    my = sum(ys) / len(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - mx) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - my) ** 2 for y in ys))
    if den_x <= 0 or den_y <= 0:
        return None
    return num / (den_x * den_y)


def _two_sided_pvalue(t_stat: float, df: int) -> float | None:
    if not math.isfinite(t_stat) or df <= 0:
        return None
    try:
        from scipy.stats import t as student_t

        return float(2 * student_t.sf(abs(t_stat), df))
    except ImportError:
        from math import erfc, sqrt

        return erfc(abs(t_stat) / sqrt(2))


def multivariate_ols_stats(
    y: list[float],
    x_cols: list[list[float]],
    feature_names: list[str],
) -> dict[str, Any] | None:
    """OLS with intercept; returns betas, R², adj R², standard errors, p-values."""
    n = len(y)
    k = len(x_cols)
    if n < MIN_WEEKS or k < 1 or any(len(col) != n for col in x_cols):
        return None
    try:
        import numpy as np

        X = np.column_stack([np.ones(n)] + [np.array(col, dtype=float) for col in x_cols])
        yv = np.array(y, dtype=float)
        beta, _, rank, _ = np.linalg.lstsq(X, yv, rcond=None)
        if rank < k + 1:
            return None
        yhat = X @ beta
        df_resid = n - k - 1
        if df_resid <= 0:
            return None
        ss_res = float(((yv - yhat) ** 2).sum())
        ss_tot = float(((yv - yv.mean()) ** 2).sum())
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else None
        adj_r2 = 1.0 - (1.0 - r2) * (n - 1) / df_resid if r2 is not None else None
        mse = ss_res / df_resid
        try:
            cov = mse * np.linalg.inv(X.T @ X)
            se = np.sqrt(np.maximum(np.diag(cov), 0.0))
        except np.linalg.LinAlgError:
            se = np.full(k + 1, float("nan"))

        coef_rows: list[dict[str, Any]] = [
            {
                "feature": "intercept",
                "beta": round(float(beta[0]), 6),
                "std_error": round(float(se[0]), 6) if math.isfinite(se[0]) else None,
                "t_stat": round(float(beta[0] / se[0]), 4) if se[0] > 0 else None,
                "p_value": round(_two_sided_pvalue(float(beta[0] / se[0]), df_resid), 6)
                if se[0] > 0
                else None,
                "expected_sign": "any",
                "sign_passed": True,
            }
        ]
        for i, fname in enumerate(feature_names):
            b = float(beta[i + 1])
            s = float(se[i + 1]) if math.isfinite(se[i + 1]) else float("nan")
            t = b / s if s > 0 else float("nan")
            coef_rows.append(
                {
                    "feature": fname,
                    "beta": round(b, 6),
                    "std_error": round(s, 6) if math.isfinite(s) else None,
                    "t_stat": round(t, 4) if math.isfinite(t) else None,
                    "p_value": round(_two_sided_pvalue(t, df_resid), 6) if math.isfinite(t) else None,
                }
            )

        return {
            "n_obs": n,
            "r_squared": round(r2, 4) if r2 is not None else None,
            "adj_r_squared": round(adj_r2, 4) if adj_r2 is not None else None,
            "df_residual": df_resid,
            "beta_vector": [float(b) for b in beta],
            "coefficients": coef_rows,
        }
    except Exception:
        return None


def _load_monthly_cb() -> list[tuple[str, float]]:
    daily = _load_cache_series(CB_CACHE_REL)
    return sorted((d, v) for d, v in daily.items())


def _engineer_monthly_cb(monthly: list[tuple[str, float]], engineer: str) -> dict[str, float]:
    dates = [d for d, _ in monthly]
    values = [v for _, v in monthly]
    out: dict[str, float] = {}
    for i, d in enumerate(dates):
        if engineer == "level":
            out[d] = values[i]
        elif engineer == "lag1" and i >= 1:
            out[d] = values[i - 1]
        elif engineer == "roll12" and i >= 11:
            out[d] = sum(values[i - 11 : i + 1])
        elif engineer == "yoy" and i >= 12:
            out[d] = values[i] - values[i - 12]
    return out


def _align_cb_feature(
    bundle: DriverBundle,
    engineer: str,
    *,
    base_cb_weekly: dict[str, float] | None = None,
) -> list[float] | None:
    dates = bundle.dates
    if engineer in {"interaction_real_yield", "interaction_dxy"}:
        if base_cb_weekly is None:
            monthly = _load_monthly_cb()
            base_cb_weekly = _weekly_from_daily(_engineer_monthly_cb(monthly, "level"), dates)
        ry = bundle.features.get("real_yield")
        dxy = bundle.features.get("log_dxy")
        if not ry or not dxy or len(ry) != len(dates):
            return None
        col: list[float] = []
        for i, d in enumerate(dates):
            cb = base_cb_weekly.get(d)
            if cb is None:
                return None
            if engineer == "interaction_real_yield":
                col.append(cb * ry[i])
            else:
                col.append(cb * dxy[i])
        return col

    monthly = _load_monthly_cb()
    daily = _engineer_monthly_cb(monthly, engineer)
    weekly = _weekly_from_daily(daily, dates)
    col = [weekly.get(d) for d in dates]
    if any(v is None for v in col):
        return None
    return [float(v) for v in col]


def _fit_variant(bundle: DriverBundle, spec: GoldVariantSpec) -> dict[str, Any] | None:
    base_cols: list[list[float]] = []
    feature_names: list[str] = list(BASE_FEATURES)
    for fname in BASE_FEATURES:
        col = bundle.features.get(fname)
        if col is None or len(col) != bundle.n:
            return None
        base_cols.append(col)

    base_cb_weekly = _weekly_from_daily(
        _engineer_monthly_cb(_load_monthly_cb(), "level"),
        bundle.dates,
    )
    cb_col = _align_cb_feature(bundle, spec.engineer, base_cb_weekly=base_cb_weekly)
    if cb_col is None or len(cb_col) != bundle.n:
        return None

    x_cols = base_cols + [cb_col]
    all_names = feature_names + [spec.cb_feature]
    y = [math.log(p) for p in bundle.price]

    stats = multivariate_ols_stats(y, x_cols, all_names)
    if not stats:
        return None

    sign_expectations = {**SIGN_EXPECTATIONS, spec.cb_feature: spec.cb_sign}
    beta_map = {row["feature"]: row["beta"] for row in stats["coefficients"] if row["feature"] != "intercept"}
    for row in stats["coefficients"]:
        if row["feature"] == "intercept":
            continue
        expected = sign_expectations.get(row["feature"], "any")
        if expected == "any":
            row["expected_sign"] = "any"
            row["sign_passed"] = True
        else:
            row["expected_sign"] = expected
            b = row["beta"]
            row["sign_passed"] = (expected == "negative" and b <= 0) or (expected == "positive" and b >= 0)

    correlations: dict[str, float | None] = {}
    feature_series = {fname: col for fname, col in zip(all_names, x_cols)}
    for fname, col in feature_series.items():
        correlations[fname] = round(_pearson(col, y), 4) if col else None

    beta_vec = stats["beta_vector"]
    latest_feats = [col[-1] for col in x_cols]
    log_fair = _predict_log_price(beta_vec, latest_feats)
    if log_fair is None:
        return None
    fair = math.exp(log_fair)
    spot = bundle.price[-1]
    dev_pct = round(100.0 * (spot - fair) / fair, 2) if fair > 0 else None

    reversion_series: list[dict[str, Any]] = []
    for i in range(bundle.n):
        feats_i = [col[i] for col in x_cols]
        lp = _predict_log_price(beta_vec, feats_i)
        if lp is None:
            continue
        f = math.exp(lp)
        if f <= 0:
            continue
        reversion_series.append(
            {"date": bundle.dates[i], "deviation_pct": round(100.0 * (bundle.price[i] - f) / f, 2)}
        )

    intercept = beta_vec[0]
    feat_contrib = sum(abs(b * f) for b, f in zip(beta_vec[1:], latest_feats))
    intercept_dominance = abs(intercept) / max(feat_contrib, 1e-9)

    gated = apply_metals_institutional_publish_gate(
        {
            "fair_value": round(fair, 4),
            "deviation_pct": dev_pct,
            "spot_price": round(spot, 4),
            "model_id": f"gold_research_{spec.variant_id}",
            "model_name": f"gold_research_{spec.variant_id}",
            "regression": {
                "n": stats["n_obs"],
                "r_squared": stats["r_squared"],
                "adj_r_squared": stats["adj_r_squared"],
                "intercept": round(intercept, 6),
                "features": beta_map,
            },
            "sign_expectations": sign_expectations,
            "intercept_dominance_ratio": round(intercept_dominance, 2),
            "breakdown_reconciles": True,
            "stale_inputs": bundle.stale,
        },
        reversion_series=reversion_series,
        market="Gold",
    )

    audit = gated.get("institutional_audit") or {}
    blockers = list(audit.get("blockers") or [])
    sign_diag = build_metals_sign_diagnostic(
        blockers=blockers,
        regression_features=beta_map,
        sign_expectations=sign_expectations,
        feature_series=feature_series,
        log_prices=y,
        sample_start=bundle.dates[0] if bundle.dates else "",
        sample_end=bundle.as_of,
        r_squared=stats["r_squared"],
        n_observations=stats["n_obs"],
    )

    failed_signs = [r["feature"] for r in stats["coefficients"] if r.get("sign_passed") is False]

    return {
        "variant_id": spec.variant_id,
        "label": spec.label,
        "cb_feature": spec.cb_feature,
        "cb_engineering": spec.engineer,
        "n_obs": stats["n_obs"],
        "sample_start": bundle.dates[0] if bundle.dates else "",
        "sample_end": bundle.as_of,
        "r_squared": stats["r_squared"],
        "adj_r_squared": stats["adj_r_squared"],
        "coefficients": stats["coefficients"],
        "correlations_with_log_price": correlations,
        "sign_expectations": sign_expectations,
        "failed_sign_gates": failed_signs,
        "sign_gate_diagnostic": sign_diag or None,
        "validation_status": gated.get("model_status"),
        "publish": bool(gated.get("publish")),
        "publish_decision": "PUBLISH" if gated.get("publish") else "WITHHOLD",
        "blockers": blockers,
        "deviation_pct": dev_pct,
        "fair_value": round(fair, 4),
        "spot_price": round(spot, 4),
        "institutional_audit": audit,
    }


def run_gold_model_research() -> dict[str, Any]:
    bundle = build_driver_bundle("Gold")
    cb_meta: dict[str, Any] = {}
    cb_path = PROJECT_ROOT / CB_CACHE_REL
    if cb_path.exists():
        cb_meta = json.loads(cb_path.read_text(encoding="utf-8"))

    variants: list[dict[str, Any]] = []
    errors: list[str] = []
    for spec in GOLD_VARIANTS:
        if bundle.missing_required:
            errors.append(f"{spec.variant_id}: driver bundle missing {bundle.missing_required}")
            continue
        try:
            result = _fit_variant(bundle, spec)
            if result:
                variants.append(result)
            else:
                errors.append(f"{spec.variant_id}: regression failed (insufficient aligned data)")
        except Exception as exc:
            errors.append(f"{spec.variant_id}: {exc}")

    publishable = [v for v in variants if v.get("publish")]
    best_adj_r2 = max(variants, key=lambda v: v.get("adj_r_squared") or -1.0) if variants else None
    best_defensible = None
    for v in sorted(variants, key=lambda x: x.get("adj_r_squared") or -1.0, reverse=True):
        cb = v.get("cb_feature", "")
        cb_coef = next((c for c in v.get("coefficients", []) if c.get("feature") == cb), None)
        if cb_coef and cb_coef.get("sign_passed") and (cb_coef.get("p_value") or 1) < 0.10:
            best_defensible = v
            break

    report = {
        "generated_at": _now_iso(),
        "cb_driver_source": {
            "cache_path": CB_CACHE_REL,
            "source_name": cb_meta.get("source_name"),
            "source_id": cb_meta.get("source_id"),
            "manual_source_path": cb_meta.get("manual_source_path"),
            "frequency": cb_meta.get("frequency"),
            "observation_count": cb_meta.get("observation_count"),
            "latest_date": cb_meta.get("latest_date"),
            "notes": cb_meta.get("notes"),
        },
        "driver_bundle": {
            "n_weeks": bundle.n,
            "as_of": bundle.as_of,
            "missing_required": bundle.missing_required,
            "stale": bundle.stale,
        },
        "production_gate_r2_min": MIN_R2_PRODUCTION,
        "variants_tested": len(GOLD_VARIANTS),
        "variants_fitted": len(variants),
        "variants_publishable": len(publishable),
        "best_adj_r2_variant": best_adj_r2.get("variant_id") if best_adj_r2 else None,
        "best_cb_sign_defensible_variant": best_defensible.get("variant_id") if best_defensible else None,
        "recommendation": (
            "No variant passes full institutional publish gate — withhold Gold valuation."
            if not publishable
            else f"Variant {publishable[0]['variant_id']} passes publish gate."
        ),
        "fit_errors": errors,
        "variants": variants,
    }
    return report


def write_research_artifacts(report: dict[str, Any]) -> tuple[Path, Path]:
    RESEARCH_JSON.parent.mkdir(parents=True, exist_ok=True)
    RESEARCH_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    lines = [
        "# Gold valuation model research (WGC monthly CB driver)",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        "## CB driver source",
        "",
        f"- Source: {report.get('cb_driver_source', {}).get('source_name')} / {report.get('cb_driver_source', {}).get('source_id')}",
        f"- Worksheet: Monthly (global net purchases, tonnes)",
        f"- Observations: {report.get('cb_driver_source', {}).get('observation_count')} through {report.get('cb_driver_source', {}).get('latest_date')}",
        "",
        f"**Recommendation:** {report.get('recommendation')}",
        "",
        "## Variant comparison",
        "",
        "| Variant | Adj R² | CB β | CB p-value | CB corr | Sign pass | Validation | Publish |",
        "| --- | ---: | ---: | ---: | ---: | --- | --- | --- |",
    ]
    for v in report.get("variants") or []:
        cb = v.get("cb_feature", "")
        cb_row = next((c for c in v.get("coefficients", []) if c.get("feature") == cb), {})
        cb_corr = (v.get("correlations_with_log_price") or {}).get(cb)
        sign_ok = "Yes" if cb_row.get("sign_passed") else "No"
        lines.append(
            f"| {v.get('label')} | {v.get('adj_r_squared')} | {cb_row.get('beta')} | "
            f"{cb_row.get('p_value')} | {cb_corr} | {sign_ok} | {v.get('validation_status')} | "
            f"{v.get('publish_decision')} |"
        )

    lines.extend(["", "## Per-variant detail", ""])
    for v in report.get("variants") or []:
        lines.append(f"### {v.get('variant_id')} — {v.get('label')}")
        lines.append("")
        lines.append(f"- Adj R²: {v.get('adj_r_squared')} (R² {v.get('r_squared')})")
        lines.append(f"- n: {v.get('n_obs')} ({v.get('sample_start')} → {v.get('sample_end')})")
        lines.append(f"- Validation: {v.get('validation_status')} | **{v.get('publish_decision')}**")
        if v.get("blockers"):
            lines.append(f"- Blockers: {'; '.join(v['blockers'])}")
        lines.append("")
        lines.append("| Feature | β | p-value | Expected sign | Sign OK | Corr w/ log(price) |")
        lines.append("| --- | ---: | ---: | --- | --- | ---: |")
        corrs = v.get("correlations_with_log_price") or {}
        for c in v.get("coefficients") or []:
            if c.get("feature") == "intercept":
                continue
            fname = c.get("feature", "")
            lines.append(
                f"| {fname} | {c.get('beta')} | {c.get('p_value')} | {c.get('expected_sign')} | "
                f"{'Yes' if c.get('sign_passed') else 'No'} | {corrs.get(fname)} |"
            )
        if v.get("sign_gate_diagnostic", {}).get("summary"):
            lines.append("")
            lines.append(f"Sign diagnostic: {v['sign_gate_diagnostic']['summary']}")
        lines.append("")

    RESEARCH_MD.write_text("\n".join(lines), encoding="utf-8")
    return RESEARCH_JSON, RESEARCH_MD
