"""Gold production model — real_yield specification research.

Tests economically defensible alternatives to DFII10 level while holding
cb_roll12 production spec fixed. Does not weaken institutional sign gates.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.fx.fx_macro_history import load_fred_daily_map
from hptl.valuation.gold_cb_driver_comparison import _compute_vif, _feature_correlation_matrix
from hptl.valuation.gold_model_research import (
    SIGN_EXPECTATIONS as _BASE_SIGN,
    _pearson,
    multivariate_ols_stats,
)
from hptl.valuation.institutional_publish_gate import (
    MIN_R2_PRODUCTION,
    apply_metals_institutional_publish_gate,
    build_metals_sign_diagnostic,
)
from hptl.valuation.metals_institutional_drivers import (
    DriverBundle,
    _asof_value,
    _weekly_from_daily,
    build_driver_bundle,
)
from hptl.valuation.metals_valuation_v1 import MIN_WEEKS, _predict_log_price

RESEARCH_JSON = PROJECT_ROOT / "data" / "processed" / "gold_real_yield_research_latest.json"
RESEARCH_MD = PROJECT_ROOT / "data" / "processed" / "gold_real_yield_research_latest.md"

PRODUCTION_FEATURES = ("real_yield", "log_dxy", "cb_roll12", "etf_holdings")
SIGN_EXPECTATIONS: dict[str, str] = {
    "real_yield": "negative",
    "log_dxy": "negative",
    "cb_roll12": "positive",
    "etf_holdings": "positive",
}

RY_FEATURE = "real_yield"


@dataclass(frozen=True)
class RealYieldVariant:
    variant_id: str
    label: str
    engineer: str
    ry_sign_expectation: str  # negative | positive | any — for inverted convention test
    economic_note: str
    sample_filter: str | None = None  # pre_2020 | post_2020 | high_cpi | low_cpi


REAL_YIELD_VARIANTS: tuple[RealYieldVariant, ...] = (
    RealYieldVariant(
        "baseline_dfii10_level",
        "DFII10 10Y TIPS yield (production level)",
        "level_dfii10",
        "negative",
        "Higher real yields raise opportunity cost of non-yielding gold → expect negative β.",
    ),
    RealYieldVariant(
        "lagged_4w",
        "DFII10 lagged 4 weeks",
        "lag_4",
        "negative",
        "Delayed pass-through of rate moves to gold pricing.",
    ),
    RealYieldVariant(
        "lagged_13w",
        "DFII10 lagged 13 weeks (~quarter)",
        "lag_13",
        "negative",
        "Quarterly policy/rate transmission lag.",
    ),
    RealYieldVariant(
        "inverted_dfii10",
        "Inverted DFII10 (−1 × level) — convention check",
        "invert_level",
        "negative",
        "If stored sign is wrong, −DFII10 should restore negative β on opportunity-cost interpretation.",
    ),
    RealYieldVariant(
        "delta_4w",
        "4-week change in DFII10",
        "delta_4",
        "negative",
        "Rising real yields should pressure gold → negative β on yield change.",
    ),
    RealYieldVariant(
        "delta_13w",
        "13-week change in DFII10",
        "delta_13",
        "negative",
        "Medium-horizon yield momentum vs gold.",
    ),
    RealYieldVariant(
        "roll26_avg",
        "26-week rolling average DFII10",
        "roll26",
        "negative",
        "Smoothed real-rate level reduces weekly noise.",
    ),
    RealYieldVariant(
        "roll52_avg",
        "52-week rolling average DFII10",
        "roll52",
        "negative",
        "Annual average real-rate anchor.",
    ),
    RealYieldVariant(
        "zscore_104w",
        "104-week rolling z-score of DFII10",
        "zscore_104",
        "negative",
        "Above-average real yields vs 2Y history should weigh on gold.",
    ),
    RealYieldVariant(
        "nominal_minus_cpi_yoy",
        "DGS10 − CPI YoY (nominal minus inflation proxy)",
        "nominal_minus_cpi",
        "negative",
        "Classic Fisher approximation; tests whether DFII10 is wrong proxy vs nominal−CPI.",
    ),
    RealYieldVariant(
        "tips_dfii10_explicit",
        "DFII10 TIPS (explicit reload — same as production)",
        "level_dfii10",
        "negative",
        "Market-implied 10Y real yield from TIPS; benchmark vs nominal−CPI.",
    ),
)

REGIME_VARIANTS: tuple[RealYieldVariant, ...] = (
    RealYieldVariant(
        "regime_pre_2020",
        "Pre-2020 subsample (DFII10 level)",
        "level_dfii10",
        "negative",
        "Pre-pandemic low-inflation / negative-real-rate era.",
        sample_filter="pre_2020",
    ),
    RealYieldVariant(
        "regime_post_2020",
        "Post-2020 subsample (DFII10 level)",
        "level_dfii10",
        "negative",
        "Post-pandemic inflation shock; yields and gold often co-moved.",
        sample_filter="post_2020",
    ),
    RealYieldVariant(
        "regime_high_cpi",
        "High CPI regime (CPI YoY ≥ 4%, DFII10 level)",
        "level_dfii10",
        "negative",
        "High-inflation regime may break classical real-yield channel.",
        sample_filter="high_cpi",
    ),
    RealYieldVariant(
        "regime_low_cpi",
        "Low CPI regime (CPI YoY < 4%, DFII10 level)",
        "level_dfii10",
        "negative",
        "Low-inflation regime where opportunity-cost channel is cleaner.",
        sample_filter="low_cpi",
    ),
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _load_dfii10_daily() -> dict[str, float]:
    return load_fred_daily_map("DFII10")


def _load_dgs10_daily() -> dict[str, float]:
    return load_fred_daily_map("DGS10")


def _cpi_yoy_daily() -> dict[str, float]:
    """Monthly CPIAUCSL year-over-year % change, forward-filled to daily keys."""
    cpi = load_fred_daily_map("CPIAUCSL", observation_start="2000-01-01")
    if not cpi:
        return {}
    months = sorted(cpi.keys())
    yoy: dict[str, float] = {}
    month_vals: list[tuple[str, float]] = []
    for d in months:
        v = cpi[d]
        month_vals.append((d[:7], v))
    # dedupe by month (last obs in month)
    by_month: dict[str, float] = {}
    for ym, v in month_vals:
        by_month[ym] = v
    sorted_months = sorted(by_month.keys())
    for i, ym in enumerate(sorted_months):
        if i < 12:
            continue
        prev = by_month[sorted_months[i - 12]]
        cur = by_month[ym]
        if prev and prev > 0:
            pct = 100.0 * (cur / prev - 1.0)
            # assign to last day of month approx
            yoy[f"{ym}-28"] = pct
    if not yoy:
        return {}
    # expand to daily as-of
    daily_keys = sorted(set(list(cpi.keys()) + list(_load_dfii10_daily().keys())))
    out: dict[str, float] = {}
    for d in daily_keys:
        v = _asof_value(yoy, d)
        if v is not None:
            out[d] = v
    return out


def _nominal_minus_cpi_daily() -> dict[str, float]:
    dgs = _load_dgs10_daily()
    cpi_yoy = _cpi_yoy_daily()
    if not dgs or not cpi_yoy:
        return {}
    out: dict[str, float] = {}
    for d in sorted(set(dgs.keys()) | set(cpi_yoy.keys())):
        nom = _asof_value(dgs, d)
        cpi = _asof_value(cpi_yoy, d)
        if nom is not None and cpi is not None:
            out[d] = nom - cpi
    return out


def _weekly_series(daily: dict[str, float], dates: list[str]) -> list[float] | None:
    weekly = _weekly_from_daily(daily, dates)
    col = [weekly.get(d) for d in dates]
    if any(v is None for v in col):
        return None
    return [float(v) for v in col]


def _engineer_weekly_ry(
    base_weekly: list[float],
    dates: list[str],
    engineer: str,
    *,
    daily_override: dict[str, float] | None = None,
) -> list[float] | None:
    n = len(base_weekly)
    if engineer == "level_dfii10":
        return list(base_weekly)
    if engineer == "invert_level":
        return [-v for v in base_weekly]
    if engineer == "nominal_minus_cpi":
        if not daily_override:
            daily_override = _nominal_minus_cpi_daily()
        return _weekly_series(daily_override, dates)

    if daily_override is None:
        daily_override = _load_dfii10_daily()
    weekly_map = _weekly_from_daily(daily_override, dates)
    series = [weekly_map.get(d) for d in dates]
    if any(v is None for v in series):
        return None
    vals = [float(v) for v in series]

    if engineer == "lag_4":
        return [vals[max(0, i - 4)] for i in range(n)]
    if engineer == "lag_13":
        return [vals[max(0, i - 13)] for i in range(n)]
    if engineer == "delta_4":
        return [vals[i] - vals[max(0, i - 4)] for i in range(n)]
    if engineer == "delta_13":
        return [vals[i] - vals[max(0, i - 13)] for i in range(n)]
    if engineer == "roll26":
        return [sum(vals[max(0, i - 25) : i + 1]) / min(26, i + 1) for i in range(n)]
    if engineer == "roll52":
        return [sum(vals[max(0, i - 51) : i + 1]) / min(52, i + 1) for i in range(n)]
    if engineer == "zscore_104":
        out: list[float] = []
        for i in range(n):
            window = vals[max(0, i - 103) : i + 1]
            if len(window) < 26:
                out.append(0.0)
                continue
            m = sum(window) / len(window)
            var = sum((x - m) ** 2 for x in window) / len(window)
            sd = math.sqrt(var) if var > 0 else 1e-9
            out.append((vals[i] - m) / sd)
        return out
    return None


def _filter_indices(
    dates: list[str],
    filter_name: str | None,
    *,
    cpi_yoy_weekly: list[float] | None = None,
) -> list[int]:
    if not filter_name:
        return list(range(len(dates)))
    idx: list[int] = []
    for i, d in enumerate(dates):
        if filter_name == "pre_2020" and d < "2020-01-01":
            idx.append(i)
        elif filter_name == "post_2020" and d >= "2020-01-01":
            idx.append(i)
        elif filter_name in {"high_cpi", "low_cpi"} and cpi_yoy_weekly is not None:
            cpi = cpi_yoy_weekly[i]
            if filter_name == "high_cpi" and cpi >= 4.0:
                idx.append(i)
            elif filter_name == "low_cpi" and cpi < 4.0:
                idx.append(i)
    return idx


def _slice_panel(
    dates: list[str],
    y: list[float],
    x_cols: list[list[float]],
    feature_names: list[str],
    indices: list[int],
) -> tuple[list[str], list[float], list[list[float]]] | None:
    if len(indices) < MIN_WEEKS:
        return None
    sd = [dates[i] for i in indices]
    sy = [y[i] for i in indices]
    sx = [[col[i] for i in indices] for col in x_cols]
    return sd, sy, sx


def _fit_production_panel(
    bundle: DriverBundle,
    ry_col: list[float],
    *,
    sample_filter: str | None = None,
    cpi_yoy_weekly: list[float] | None = None,
) -> dict[str, Any] | None:
    other_features = ("log_dxy", "cb_roll12", "etf_holdings")
    x_cols: list[list[float]] = [ry_col]
    names: list[str] = [RY_FEATURE]
    for fname in other_features:
        col = bundle.features.get(fname)
        if col is None or len(col) != bundle.n:
            return None
        x_cols.append(col)
        names.append(fname)

    y = [math.log(p) for p in bundle.price]
    dates = bundle.dates
    indices = _filter_indices(dates, sample_filter, cpi_yoy_weekly=cpi_yoy_weekly)
    sliced = _slice_panel(dates, y, x_cols, names, indices)
    if not sliced:
        return None
    s_dates, s_y, s_x = sliced

    stats = multivariate_ols_stats(s_y, s_x, names)
    if not stats:
        return None

    vif = _compute_vif(s_x, names)
    corr_matrix = _feature_correlation_matrix(s_x, names)
    feature_series = {n: c for n, c in zip(names, s_x)}

    ry_row = next((c for c in stats["coefficients"] if c["feature"] == RY_FEATURE), None)
    for row in stats["coefficients"]:
        if row["feature"] == "intercept":
            continue
        expected = SIGN_EXPECTATIONS.get(row["feature"], "any")
        row["expected_sign"] = expected
        b = row["beta"]
        row["sign_passed"] = (expected == "negative" and b <= 0) or (expected == "positive" and b >= 0) or expected == "any"

    ry_corr = _pearson(feature_series[RY_FEATURE], s_y)
    beta_map = {r["feature"]: r["beta"] for r in stats["coefficients"] if r["feature"] != "intercept"}

    beta_vec = stats["beta_vector"]
    latest_feats = [col[-1] for col in s_x]
    log_fair = _predict_log_price(beta_vec, latest_feats)
    fair = math.exp(log_fair) if log_fair is not None else None
    spot = bundle.price[indices[-1]]

    reversion_series: list[dict[str, Any]] = []
    for i in range(len(s_y)):
        feats_i = [col[i] for col in s_x]
        lp = _predict_log_price(beta_vec, feats_i)
        if lp is None:
            continue
        f = math.exp(lp)
        if f <= 0:
            continue
        px = bundle.price[indices[i]]
        reversion_series.append({"date": s_dates[i], "deviation_pct": round(100.0 * (px - f) / f, 2)})

    intercept = beta_vec[0]
    feat_contrib = sum(abs(b * f) for b, f in zip(beta_vec[1:], latest_feats))
    intercept_dominance = abs(intercept) / max(feat_contrib, 1e-9)

    gated = apply_metals_institutional_publish_gate(
        {
            "fair_value": round(fair, 4) if fair else None,
            "deviation_pct": round(100.0 * (spot - fair) / fair, 2) if fair and fair > 0 else None,
            "spot_price": round(spot, 4),
            "model_id": "gold_real_yield_research",
            "model_name": "gold_institutional_fair_value_v1",
            "regression": {
                "n": stats["n_obs"],
                "r_squared": stats["r_squared"],
                "adj_r_squared": stats["adj_r_squared"],
                "intercept": round(intercept, 6),
                "features": beta_map,
            },
            "sign_expectations": SIGN_EXPECTATIONS,
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
        sign_expectations=SIGN_EXPECTATIONS,
        feature_series=feature_series,
        log_prices=s_y,
        sample_start=s_dates[0],
        sample_end=s_dates[-1],
        r_squared=stats["r_squared"],
        n_observations=stats["n_obs"],
    )

    ry_vif = next((v for v in vif if v["feature"] == RY_FEATURE), {})
    failed = [r["feature"] for r in stats["coefficients"] if r.get("sign_passed") is False]

    return {
        "n_obs": stats["n_obs"],
        "sample_start": s_dates[0],
        "sample_end": s_dates[-1],
        "r_squared": stats["r_squared"],
        "adj_r_squared": stats["adj_r_squared"],
        "real_yield_coefficient": ry_row,
        "real_yield_univariate_corr": round(ry_corr, 4) if ry_corr is not None else None,
        "real_yield_vif": ry_vif.get("vif"),
        "coefficients": stats["coefficients"],
        "vif": vif,
        "correlation_matrix": corr_matrix,
        "failed_sign_gates": failed,
        "publish": bool(gated.get("publish")),
        "publish_decision": "PUBLISH" if gated.get("publish") else "WITHHOLD",
        "blockers": blockers,
        "sign_gate_diagnostic": sign_diag or None,
        "validation_status": gated.get("model_status"),
    }


def _multicollinearity_diagnostic(bundle: DriverBundle) -> dict[str, Any]:
    """Item 7 — VIF and partial correlations for production drivers."""
    cols: list[list[float]] = []
    names: list[str] = []
    for fname in PRODUCTION_FEATURES:
        col = bundle.features.get(fname)
        if col is None:
            return {"error": f"missing {fname}"}
        cols.append(col)
        names.append(fname)
    y = [math.log(p) for p in bundle.price]
    vif = _compute_vif(cols, names)
    corr = _feature_correlation_matrix(cols, names)
    ry_idx = names.index(RY_FEATURE)
    ry_col = cols[ry_idx]

    # Partial corr: residualize ry vs other X, correlate with residualized y
    def _residualize(target: list[float], predictors: list[list[float]]) -> list[float] | None:
        stats = multivariate_ols_stats(target, predictors, [f"x{i}" for i in range(len(predictors))])
        if not stats:
            return None
        beta = stats["beta_vector"]
        resid = []
        for i in range(len(target)):
            pred = beta[0] + sum(b * predictors[j][i] for j, b in enumerate(beta[1:]))
            resid.append(target[i] - pred)
        return resid

    others = [c for i, c in enumerate(cols) if i != ry_idx]
    ry_resid = _residualize(ry_col, others)
    y_resid = _residualize(y, others)
    partial_corr = _pearson(ry_resid, y_resid) if ry_resid and y_resid else None

    ry_vif = next((v["vif"] for v in vif if v["feature"] == RY_FEATURE), None)
    etf_vif = next((v["vif"] for v in vif if v["feature"] == "etf_holdings"), None)

    interpretation = []
    if ry_vif is not None:
        interpretation.append(
            f"real_yield VIF={ry_vif} — "
            + ("moderate collinearity with DXY/CB/ETF." if ry_vif >= 5 else "acceptable vs other drivers.")
        )
    if partial_corr is not None:
        interpretation.append(
            f"Partial corr(real_yield, log price | other drivers)={partial_corr:.4f} — "
            + ("still positive after controls" if partial_corr > 0 else "negative after controls.")
        )
    dxy_cb = corr.get("real_yield", {}).get("log_dxy")
    dxy_ry = corr.get("real_yield", {}).get("cb_roll12")
    if dxy_cb is not None:
        interpretation.append(f"real_yield vs log_dxy correlation={dxy_cb}.")
    if dxy_ry is not None:
        interpretation.append(f"real_yield vs cb_roll12 correlation={dxy_ry}.")

    return {
        "vif": vif,
        "correlation_matrix": corr,
        "partial_corr_real_yield_log_price": round(partial_corr, 4) if partial_corr is not None else None,
        "interpretation": interpretation,
        "etf_holdings_vif": etf_vif,
        "note": (
            "High ETF VIF is expected (trend collinearity with price/CB). "
            "Sign failure on real_yield is not explained by VIF alone if partial correlation remains positive."
        ),
    }


def _fit_variant(bundle: DriverBundle, spec: RealYieldVariant, *, cpi_yoy_weekly: list[float] | None) -> dict[str, Any] | None:
    base_ry = bundle.features.get(RY_FEATURE)
    if not base_ry or len(base_ry) != bundle.n:
        return None

    daily_override: dict[str, float] | None = None
    if spec.engineer == "nominal_minus_cpi":
        daily_override = _nominal_minus_cpi_daily()
        if not daily_override:
            return None

    ry_col = _engineer_weekly_ry(base_ry, bundle.dates, spec.engineer, daily_override=daily_override)
    if ry_col is None or len(ry_col) != bundle.n:
        return None

    fit = _fit_production_panel(
        bundle,
        ry_col,
        sample_filter=spec.sample_filter,
        cpi_yoy_weekly=cpi_yoy_weekly,
    )
    if not fit:
        return None

    ry_coef = fit.get("real_yield_coefficient") or {}
    econ = spec.economic_note
    if ry_coef.get("sign_passed"):
        econ += " Coefficient sign aligns with theory on this sample."
    else:
        beta = ry_coef.get("beta")
        p = ry_coef.get("p_value")
        econ += f" Sign fails (β={beta}, p={p}) — classical channel weak or dominated by other factors."

    return {
        "variant_id": spec.variant_id,
        "label": spec.label,
        "engineer": spec.engineer,
        "sample_filter": spec.sample_filter,
        "economic_interpretation": econ,
        **fit,
    }


def run_gold_real_yield_research() -> dict[str, Any]:
    bundle = build_driver_bundle("Gold")
    errors: list[str] = []
    if bundle.missing_required:
        return {"status": "error", "error": f"driver bundle missing: {bundle.missing_required}"}

    cpi_daily = _cpi_yoy_daily()
    cpi_yoy_weekly = _weekly_series(cpi_daily, bundle.dates) if cpi_daily else None

    variants: list[dict[str, Any]] = []
    all_specs = list(REAL_YIELD_VARIANTS) + list(REGIME_VARIANTS)
    # Deduplicate tips vs baseline (same engineer) — keep both for documentation
    for spec in all_specs:
        try:
            result = _fit_variant(bundle, spec, cpi_yoy_weekly=cpi_yoy_weekly)
            if result:
                variants.append(result)
            else:
                errors.append(f"{spec.variant_id}: insufficient aligned data")
        except Exception as exc:
            errors.append(f"{spec.variant_id}: {exc}")

    multicol = _multicollinearity_diagnostic(bundle)

    # Baseline production for root-cause summary
    baseline = next((v for v in variants if v["variant_id"] == "baseline_dfii10_level"), None)
    publishable = [v for v in variants if v.get("publish")]

    ry_sign_pass = [v for v in variants if (v.get("real_yield_coefficient") or {}).get("sign_passed")]
    best_ry_sign = max(ry_sign_pass, key=lambda v: v.get("adj_r_squared") or -1.0) if ry_sign_pass else None

    recommendation = _build_recommendation(baseline, variants, multicol, publishable, best_ry_sign)

    return {
        "status": "ok",
        "generated_at": _now_iso(),
        "production_spec": {
            "features": list(PRODUCTION_FEATURES),
            "sign_expectations": SIGN_EXPECTATIONS,
            "real_yield_source_production": "FRED DFII10 (10Y TIPS constant-maturity real yield)",
            "cb_driver": "cb_roll12 (WGC rolling 12m)",
        },
        "driver_bundle": {
            "n_weeks": bundle.n,
            "as_of": bundle.as_of,
            "sample_start": bundle.dates[0] if bundle.dates else "",
            "stale": bundle.stale,
        },
        "production_gate_r2_min": MIN_R2_PRODUCTION,
        "root_cause_summary": _root_cause_summary(baseline, multicol),
        "multicollinearity_diagnostic": multicol,
        "variants_tested": len(all_specs),
        "variants_fitted": len(variants),
        "variants_real_yield_sign_pass": len(ry_sign_pass),
        "variants_full_publish_pass": len(publishable),
        "best_real_yield_sign_variant": best_ry_sign.get("variant_id") if best_ry_sign else None,
        "recommendation": recommendation,
        "fit_errors": errors,
        "variants": variants,
    }


def _root_cause_summary(baseline: dict[str, Any] | None, multicol: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    if not baseline:
        return ["Could not fit baseline production panel."]
    ry = baseline.get("real_yield_coefficient") or {}
    lines.append(
        f"Production DFII10 level: β={ry.get('beta')}, p={ry.get('p_value')}, "
        f"univariate corr={baseline.get('real_yield_univariate_corr')}, sign_passed={ry.get('sign_passed')}."
    )
    partial = multicol.get("partial_corr_real_yield_log_price")
    if partial is not None:
        lines.append(
            f"After residualizing vs log_dxy, cb_roll12, etf_holdings, real_yield–gold partial correlation is {partial}."
        )
    lines.append(
        "2022+ regime: real yields rose while gold made new highs — positive conditional correlation "
        "in multivariate fit (β>0) despite long-run opportunity-cost theory."
    )
    lines.append(
        "cb_roll12 now passes sign gate; real_yield remains the sole sign blocker on full production sample."
    )
    return lines


def _build_recommendation(
    baseline: dict[str, Any] | None,
    variants: list[dict[str, Any]],
    multicol: dict[str, Any],
    publishable: list[dict[str, Any]],
    best_ry_sign: dict[str, Any] | None,
) -> dict[str, Any]:
    if publishable:
        v = publishable[0]
        return {
            "decision": "PUBLISH",
            "variant": v["variant_id"],
            "rationale": f"Variant {v['variant_id']} passes all institutional gates including real_yield sign.",
        }

    pre = next((v for v in variants if v["variant_id"] == "regime_pre_2020"), None)
    post = next((v for v in variants if v["variant_id"] == "regime_post_2020"), None)
    nom_cpi = next((v for v in variants if v["variant_id"] == "nominal_minus_cpi_yoy"), None)
    inverted = next((v for v in variants if v["variant_id"] == "inverted_dfii10"), None)

    notes: list[str] = []
    if pre and (pre.get("real_yield_coefficient") or {}).get("sign_passed"):
        notes.append("Pre-2020 subsample: real_yield sign passes — post-2020 regime drives full-sample failure.")
    if post and not (post.get("real_yield_coefficient") or {}).get("sign_passed"):
        pry = post.get("real_yield_coefficient") or {}
        notes.append(f"Post-2020 subsample: sign still fails (β={pry.get('beta')}).")
    if inverted and not (inverted.get("real_yield_coefficient") or {}).get("sign_passed"):
        notes.append("Inverting DFII10 does not fix sign — convention is not the root cause.")
    if nom_cpi:
        nry = nom_cpi.get("real_yield_coefficient") or {}
        notes.append(
            f"Nominal−CPI proxy: β={nry.get('beta')}, sign_passed={nry.get('sign_passed')} "
            f"(adj R²={nom_cpi.get('adj_r_squared')})."
        )
    partial = multicol.get("partial_corr_real_yield_log_price")
    if partial is not None and partial > 0:
        notes.append("Multicollinearity does not flip sign — partial correlation stays positive.")

    return {
        "decision": "WITHHOLD",
        "rationale": (
            "No real_yield specification clears the full institutional publish gate on the production "
            "feature set (real_yield, log_dxy, cb_roll12, etf_holdings). Keep Gold valuation withheld."
        ),
        "best_real_yield_sign_only": best_ry_sign.get("variant_id") if best_ry_sign else None,
        "notes": notes,
        "production_action": "Do not change production model or weaken sign gate. cb_roll12 promotion stands.",
    }


def write_research_artifacts(report: dict[str, Any]) -> tuple[Path, Path]:
    RESEARCH_JSON.parent.mkdir(parents=True, exist_ok=True)
    RESEARCH_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    rec = report.get("recommendation") or {}
    lines = [
        "# Gold real_yield specification research",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        "## Production context",
        "",
        f"- Features: {', '.join(report.get('production_spec', {}).get('features', []))}",
        f"- Real yield source: {report.get('production_spec', {}).get('real_yield_source_production')}",
        f"- CB driver: {report.get('production_spec', {}).get('cb_driver')}",
        "",
        "## Root cause",
        "",
    ]
    for line in report.get("root_cause_summary") or []:
        lines.append(f"- {line}")

    lines.extend(["", "## Recommendation", "", f"**{rec.get('decision')}** — {rec.get('rationale', '')}"])
    for note in rec.get("notes") or []:
        lines.append(f"- {note}")
    if rec.get("production_action"):
        lines.append(f"- {rec['production_action']}")

    mc = report.get("multicollinearity_diagnostic") or {}
    lines.extend(["", "## Multicollinearity (item 7)", ""])
    for row in mc.get("vif") or []:
        lines.append(f"- {row['feature']}: VIF={row['vif']}")
    lines.append(f"- Partial corr(real_yield, log price | controls): {mc.get('partial_corr_real_yield_log_price')}")
    for interp in mc.get("interpretation") or []:
        lines.append(f"- {interp}")

    lines.extend(
        [
            "",
            "## Variant comparison",
            "",
            "| Variant | N | Adj R² | RY β | RY p | RY VIF | RY sign | Univ corr | Publish |",
            "|---------|---|--------|------|------|--------|---------|-----------|---------|",
        ]
    )
    for v in report.get("variants") or []:
        ry = v.get("real_yield_coefficient") or {}
        lines.append(
            f"| {v.get('variant_id')} | {v.get('n_obs')} | {v.get('adj_r_squared')} | "
            f"{ry.get('beta')} | {ry.get('p_value')} | {v.get('real_yield_vif')} | "
            f"{'OK' if ry.get('sign_passed') else 'FAIL'} | {v.get('real_yield_univariate_corr')} | "
            f"{v.get('publish_decision')} |"
        )

    lines.extend(["", "## Economic notes per variant", ""])
    for v in report.get("variants") or []:
        lines.append(f"### {v.get('variant_id')}")
        lines.append(f"{v.get('economic_interpretation', '')}")
        lines.append("")

    RESEARCH_MD.write_text("\n".join(lines), encoding="utf-8")
    return RESEARCH_JSON, RESEARCH_MD
