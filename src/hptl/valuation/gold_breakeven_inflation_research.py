"""Gold institutional model — breakeven inflation (T10YIE) research driver.

Adds breakeven_10y alongside production drivers without replacing real_yield.
Does not modify production model wiring or weaken publish gates.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.fx.fx_macro_history import load_fred_daily_map
from hptl.valuation.gold_cb_driver_comparison import _compute_vif, _feature_correlation_matrix
from hptl.valuation.gold_model_research import _pearson, multivariate_ols_stats
from hptl.valuation.institutional_publish_gate import (
    MIN_R2_PRODUCTION,
    apply_metals_institutional_publish_gate,
    build_metals_sign_diagnostic,
)
from hptl.valuation.metals_institutional_drivers import (
    DriverBundle,
    _weekly_from_daily,
    build_driver_bundle,
)
from hptl.valuation.metals_valuation_v1 import MIN_WEEKS, _predict_log_price

RESEARCH_JSON = PROJECT_ROOT / "data" / "processed" / "gold_breakeven_inflation_research_latest.json"
RESEARCH_MD = PROJECT_ROOT / "data" / "processed" / "gold_breakeven_inflation_research_latest.md"
SIGN_DIAG_PATH = PROJECT_ROOT / "data" / "processed" / "gold_breakeven_sign_gate_diagnostic_latest.json"

T10YIE_SERIES = "T10YIE"
BREAKEVEN_FEATURE = "breakeven_10y"

PRODUCTION_FEATURES = ("real_yield", "log_dxy", "cb_roll12", "etf_holdings")
RESEARCH_FEATURES = PRODUCTION_FEATURES + (BREAKEVEN_FEATURE,)

PRODUCTION_SIGN_EXPECTATIONS: dict[str, str] = {
    "real_yield": "negative",
    "log_dxy": "negative",
    "cb_roll12": "positive",
    "etf_holdings": "positive",
}
RESEARCH_SIGN_EXPECTATIONS: dict[str, str] = {
    **PRODUCTION_SIGN_EXPECTATIONS,
    BREAKEVEN_FEATURE: "positive",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _load_breakeven_weekly(dates: list[str]) -> list[float] | None:
    daily = load_fred_daily_map(T10YIE_SERIES)
    if not daily:
        return None
    weekly = _weekly_from_daily(daily, dates)
    col = [weekly.get(d) for d in dates]
    if any(v is None for v in col):
        return None
    return [float(v) for v in col]


def _annotate_signs(
    stats: dict[str, Any],
    sign_expectations: dict[str, str],
) -> dict[str, Any]:
    out = dict(stats)
    coefs: list[dict[str, Any]] = []
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
                row["sign_passed"] = (expected == "negative" and b <= 0) or (
                    expected == "positive" and b >= 0
                )
        coefs.append(row)
    out["coefficients"] = coefs
    out["failed_sign_gates"] = [
        c["feature"] for c in coefs if c.get("sign_passed") is False and c["feature"] != "intercept"
    ]
    return out


def _fit_gold_model(
    bundle: DriverBundle,
    feature_names: tuple[str, ...],
    sign_expectations: dict[str, str],
    *,
    model_id: str,
    extra_features: dict[str, list[float]] | None = None,
) -> dict[str, Any] | None:
    x_cols: list[list[float]] = []
    for fname in feature_names:
        if extra_features and fname in extra_features:
            col = extra_features[fname]
        else:
            col = bundle.features.get(fname)
        if col is None or len(col) != bundle.n:
            return None
        x_cols.append(col)

    y = [math.log(p) for p in bundle.price]
    stats = multivariate_ols_stats(y, x_cols, list(feature_names))
    if not stats:
        return None

    stats = _annotate_signs(stats, sign_expectations)
    vif = _compute_vif(x_cols, list(feature_names))
    corr_matrix = _feature_correlation_matrix(x_cols, list(feature_names))
    feature_series = {n: c for n, c in zip(feature_names, x_cols)}

    beta_map = {r["feature"]: r["beta"] for r in stats["coefficients"] if r["feature"] != "intercept"}
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

    breakdown_steps: list[dict[str, Any]] = [
        {"step": 1, "description": "Intercept", "value": round(intercept, 6)},
    ]
    log_check = intercept
    for i, fname in enumerate(feature_names):
        contrib = beta_vec[i + 1] * latest_feats[i]
        log_check += contrib
        breakdown_steps.append(
            {
                "step": i + 2,
                "description": f"beta*{fname}",
                "value": round(contrib, 6),
            }
        )
    reconcile_ok = abs(log_check - log_fair) < 1e-4

    gated = apply_metals_institutional_publish_gate(
        {
            "fair_value": round(fair, 4),
            "deviation_pct": dev_pct,
            "spot_price": round(spot, 4),
            "model_id": model_id,
            "model_name": model_id,
            "regression": {
                "n": stats["n_obs"],
                "r_squared": stats["r_squared"],
                "adj_r_squared": stats["adj_r_squared"],
                "intercept": round(intercept, 6),
                "features": beta_map,
            },
            "sign_expectations": sign_expectations,
            "intercept_dominance_ratio": round(intercept_dominance, 2),
            "breakdown_reconciles": reconcile_ok,
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

    def _coef_row(fname: str) -> dict[str, Any] | None:
        return next((c for c in stats["coefficients"] if c["feature"] == fname), None)

    correlations = {
        fname: round(_pearson(col, y), 4) if _pearson(col, y) is not None else None
        for fname, col in feature_series.items()
    }

    return {
        "model_id": model_id,
        "feature_names": list(feature_names),
        "sign_expectations": sign_expectations,
        "n_obs": stats["n_obs"],
        "sample_start": bundle.dates[0] if bundle.dates else "",
        "sample_end": bundle.as_of,
        "r_squared": stats["r_squared"],
        "adj_r_squared": stats["adj_r_squared"],
        "coefficients": stats["coefficients"],
        "correlations_with_log_price": correlations,
        "vif": vif,
        "correlation_matrix": corr_matrix,
        "failed_sign_gates": stats["failed_sign_gates"],
        "real_yield_coefficient": _coef_row("real_yield"),
        "breakeven_coefficient": _coef_row(BREAKEVEN_FEATURE) if BREAKEVEN_FEATURE in feature_names else None,
        "publish": bool(gated.get("publish")),
        "publish_decision": "PUBLISH" if gated.get("publish") else "WITHHOLD",
        "blockers": blockers,
        "institutional_audit": audit,
        "sign_gate_diagnostic": sign_diag or None,
        "validation_status": gated.get("model_status"),
        "fair_value": round(fair, 4),
        "spot_price": round(spot, 4),
        "deviation_pct": dev_pct,
        "intercept_dominance_ratio": round(intercept_dominance, 2),
        "drivers_snapshot": {fname: round(latest_feats[i], 4) for i, fname in enumerate(feature_names)},
        "breakeven_source": {
            "series_id": T10YIE_SERIES,
            "source_name": "FRED",
            "description": "10-Year breakeven inflation rate (market-implied inflation expectations)",
        },
    }


def run_gold_breakeven_inflation_research() -> dict[str, Any]:
    bundle = build_driver_bundle("Gold")
    if bundle.missing_required:
        return {"status": "error", "error": f"driver bundle missing: {bundle.missing_required}"}

    breakeven_col = _load_breakeven_weekly(bundle.dates)
    if breakeven_col is None:
        return {
            "status": "error",
            "error": f"Could not align {T10YIE_SERIES} breakeven inflation to weekly Gold panel",
        }

    production = _fit_gold_model(
        bundle,
        PRODUCTION_FEATURES,
        PRODUCTION_SIGN_EXPECTATIONS,
        model_id="gold_institutional_fair_value_v1",
    )
    research = _fit_gold_model(
        bundle,
        RESEARCH_FEATURES,
        RESEARCH_SIGN_EXPECTATIONS,
        model_id="gold_research_breakeven_10y_v1",
        extra_features={BREAKEVEN_FEATURE: breakeven_col},
    )

    if not production or not research:
        return {"status": "error", "error": "Regression failed for production and/or research model"}

    ry_prod = production.get("real_yield_coefficient") or {}
    ry_res = research.get("real_yield_coefficient") or {}
    be_res = research.get("breakeven_coefficient") or {}

    be_significant = be_res.get("p_value") is not None and be_res.get("p_value") < 0.05
    ry_sign_restored = bool(ry_res.get("sign_passed"))
    all_signs_pass = len(research.get("failed_sign_gates") or []) == 0

    verdict = _build_verdict(
        production=production,
        research=research,
        ry_sign_restored=ry_sign_restored,
        be_significant=be_significant,
        all_signs_pass=all_signs_pass,
    )

    return {
        "status": "ok",
        "generated_at": _now_iso(),
        "research_scope": "breakeven_10y additive driver — research only, production model unchanged",
        "breakeven_series": T10YIE_SERIES,
        "production_gate_r2_min": MIN_R2_PRODUCTION,
        "driver_bundle": {
            "n_weeks": bundle.n,
            "as_of": bundle.as_of,
            "sample_start": bundle.dates[0] if bundle.dates else "",
            "stale": bundle.stale,
        },
        "verdict": verdict,
        "checks": {
            "real_yield_sign_restored": ry_sign_restored,
            "real_yield_beta_production": ry_prod.get("beta"),
            "real_yield_beta_research": ry_res.get("beta"),
            "real_yield_p_research": ry_res.get("p_value"),
            "breakeven_sign_positive": bool(be_res.get("sign_passed")),
            "breakeven_statistically_significant": be_significant,
            "breakeven_beta": be_res.get("beta"),
            "breakeven_p_value": be_res.get("p_value"),
            "all_sign_gates_pass": all_signs_pass,
            "gold_publishable": bool(research.get("publish")),
        },
        "production_baseline": production,
        "research_model": research,
    }


def _build_verdict(
    *,
    production: dict[str, Any],
    research: dict[str, Any],
    ry_sign_restored: bool,
    be_significant: bool,
    all_signs_pass: bool,
) -> dict[str, Any]:
    publishable = bool(research.get("publish"))
    blockers = list(research.get("blockers") or [])

    if publishable:
        return {
            "decision": "PUBLISH_CANDIDATE",
            "summary": (
                "Research model with breakeven_10y passes all institutional gates. "
                "Requires separate promotion review before production wiring."
            ),
            "production_action": "Research passed — run promotion protocol before changing gold_institutional_fair_value_v1.",
        }

    reasons: list[str] = []
    if not ry_sign_restored:
        ry = research.get("real_yield_coefficient") or {}
        reasons.append(
            f"real_yield sign still fails: beta={ry.get('beta')} (expected negative), p={ry.get('p_value')}"
        )
    else:
        reasons.append("real_yield sign restored to negative.")

    be = research.get("breakeven_coefficient") or {}
    if not be.get("sign_passed"):
        reasons.append(f"breakeven_10y sign fails: beta={be.get('beta')} (expected positive)")
    elif not be_significant:
        reasons.append(f"breakeven_10y not significant at 5%: p={be.get('p_value')}")
    else:
        reasons.append(f"breakeven_10y positive and significant: beta={be.get('beta')}, p={be.get('p_value')}")

    if not all_signs_pass:
        failed = research.get("failed_sign_gates") or []
        reasons.append(f"Sign gate failures on: {', '.join(failed)}")

    non_sign_blockers = [b for b in blockers if "Coefficient sign mismatch" not in b]
    if non_sign_blockers:
        reasons.append(f"Non-sign blockers: {'; '.join(non_sign_blockers)}")

    return {
        "decision": "WITHHOLD",
        "summary": "Research model does not clear institutional publish gate. Gold remains withheld.",
        "production_action": "Do not wire breakeven_10y to production. Keep gold_institutional_fair_value_v1 unchanged.",
        "reasons": reasons,
        "blockers": blockers,
        "adj_r_squared_delta": round(
            (research.get("adj_r_squared") or 0) - (production.get("adj_r_squared") or 0),
            4,
        ),
    }


def write_research_artifacts(report: dict[str, Any]) -> tuple[Path, Path, Path | None]:
    RESEARCH_JSON.parent.mkdir(parents=True, exist_ok=True)
    RESEARCH_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    checks = report.get("checks") or {}
    prod = report.get("production_baseline") or {}
    res = report.get("research_model") or {}
    verdict = report.get("verdict") or {}
    ry_p = prod.get("real_yield_coefficient") or {}
    ry_r = res.get("real_yield_coefficient") or {}
    be_r = res.get("breakeven_coefficient") or {}

    lines = [
        "# Gold breakeven inflation (T10YIE) research",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        f"**Scope:** {report.get('research_scope')}",
        "",
        "## Verdict",
        "",
        f"**{verdict.get('decision')}** — {verdict.get('summary')}",
        "",
        "### Gate checklist",
        "",
        f"| Check | Result |",
        f"|-------|--------|",
        f"| real_yield sign restored (negative) | {'YES' if checks.get('real_yield_sign_restored') else 'NO'} |",
        f"| breakeven_10y sign positive | {'YES' if checks.get('breakeven_sign_positive') else 'NO'} |",
        f"| breakeven_10y p < 0.05 | {'YES' if checks.get('breakeven_statistically_significant') else 'NO'} |",
        f"| All sign gates pass | {'YES' if checks.get('all_sign_gates_pass') else 'NO'} |",
        f"| Gold publishable | {'YES' if checks.get('gold_publishable') else 'NO'} |",
        "",
        "## Model comparison",
        "",
        "| Metric | Production (4-feature) | Research (+ breakeven_10y) |",
        "|--------|------------------------|----------------------------|",
        f"| Adj R² | {prod.get('adj_r_squared')} | {res.get('adj_r_squared')} |",
        f"| R² | {prod.get('r_squared')} | {res.get('r_squared')} |",
        f"| real_yield β | {ry_p.get('beta')} | {ry_r.get('beta')} |",
        f"| real_yield p | {ry_p.get('p_value')} | {ry_r.get('p_value')} |",
        f"| real_yield sign | {'OK' if ry_p.get('sign_passed') else 'FAIL'} | {'OK' if ry_r.get('sign_passed') else 'FAIL'} |",
        f"| breakeven_10y β | — | {be_r.get('beta')} |",
        f"| breakeven_10y p | — | {be_r.get('p_value')} |",
        f"| Publish | {prod.get('publish_decision')} | {res.get('publish_decision')} |",
        "",
        "## Research coefficients",
        "",
        "| Feature | β | p-value | Expected sign | Pass | VIF |",
        "|---------|---|---------|---------------|------|-----|",
    ]
    vif_map = {v["feature"]: v["vif"] for v in res.get("vif") or []}
    for row in res.get("coefficients") or []:
        if row["feature"] == "intercept":
            continue
        lines.append(
            f"| {row['feature']} | {row.get('beta')} | {row.get('p_value')} | "
            f"{row.get('expected_sign')} | {'OK' if row.get('sign_passed') else 'FAIL'} | "
            f"{vif_map.get(row['feature'], '—')} |"
        )

    lines.extend(["", "## Blockers (research model)", ""])
    for b in res.get("blockers") or []:
        lines.append(f"- {b}")
    if not res.get("blockers"):
        lines.append("- None")

    for r in verdict.get("reasons") or []:
        lines.append(f"- {r}")

    if verdict.get("production_action"):
        lines.extend(["", f"**Action:** {verdict['production_action']}"])

    RESEARCH_MD.write_text("\n".join(lines), encoding="utf-8")

    diag_path: Path | None = None
    sign_diag = res.get("sign_gate_diagnostic")
    if sign_diag:
        SIGN_DIAG_PATH.write_text(json.dumps(sign_diag, indent=2), encoding="utf-8")
        diag_path = SIGN_DIAG_PATH
    elif res.get("failed_sign_gates"):
        fallback = {
            "gate": "coefficient_sign",
            "model": "gold_research_breakeven_10y_v1",
            "failed_sign_gates": res.get("failed_sign_gates"),
            "coefficients": res.get("coefficients"),
            "blockers": res.get("blockers"),
            "generated_at": report.get("generated_at"),
        }
        SIGN_DIAG_PATH.write_text(json.dumps(fallback, indent=2), encoding="utf-8")
        diag_path = SIGN_DIAG_PATH

    return RESEARCH_JSON, RESEARCH_MD, diag_path
