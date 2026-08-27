"""Institutional publish gates for non-FX valuation pillars (Phase 3A).

A model may compute fair value internally but must not wire to the scanner unless
validation and usefulness thresholds pass. No confidence scores — evidence only.
"""

from __future__ import annotations

import math
from typing import Any

MAX_PUBLISH_DEVIATION_PCT = 35.0
MIN_R2_PUBLISH = 0.08
MIN_R2_PRODUCTION = 0.15
MIN_AGRI_REGRESSION_N = 24
MIN_REVERSION_60D_PCT = 50.0
REVERSION_DEV_THRESHOLD = 5.0


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def reversion_rate_pct(
    series: list[dict[str, Any]],
    *,
    horizon_days: int = 60,
    dev_threshold: float = REVERSION_DEV_THRESHOLD,
) -> tuple[float | None, int]:
    hits = trials = 0
    for i, row in enumerate(series):
        dev = _num(row.get("deviation_pct"))
        if dev is None or abs(dev) < dev_threshold:
            continue
        trials += 1
        future = series[i + 1 : i + 1 + horizon_days]
        if not future:
            continue
        end_dev = _num(future[-1].get("deviation_pct"))
        if end_dev is not None and abs(end_dev) < abs(dev):
            hits += 1
    if not trials:
        return None, 0
    return round(100.0 * hits / trials, 1), trials


def classify_status(
    *,
    r2: float | None,
    publishable: bool,
    model_exists: bool,
) -> str:
    if not model_exists or not publishable:
        if not model_exists:
            return "REBUILD_REQUIRED"
        return "NEEDS_IMPROVEMENT"
    if r2 is not None and r2 >= MIN_R2_PRODUCTION:
        return "PRODUCTION_READY"
    return "NEEDS_IMPROVEMENT"


def apply_agri_publish_gate(
    result: dict[str, Any],
    *,
    reversion_series: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Mutates export fields: wired only when publishable."""
    out = dict(result)
    model_id = out.get("model_id") or ""
    r2 = _num(out.get("regression_r2")) or _num(
        (out.get("regression") or {}).get("r_squared") if isinstance(out.get("regression"), dict) else None
    )
    n = int(out.get("aligned_n") or out.get("balance_sheet_observations") or out.get("data_depth") or 0)
    dev = _num(out.get("deviation_pct"))
    computed = out.get("fair_value") is not None and dev is not None

    rev30 = rev60 = rev90 = None
    rev_trials = 0
    if reversion_series:
        rev30, t30 = reversion_rate_pct(reversion_series, horizon_days=30)
        rev60, rev_trials = reversion_rate_pct(reversion_series, horizon_days=60)
        rev90, _ = reversion_rate_pct(reversion_series, horizon_days=90)
        if rev_trials == 0:
            rev_trials = t30

    blockers: list[str] = []
    if not computed:
        blockers.append(out.get("unavailable_reason") or "Fair value not computed")
    elif model_id == "agri_stu_percentile_v1":
        blockers.append(
            "Percentile fallback only — S/U→price regression R² below gate or insufficient aligned history"
        )
    elif r2 is not None and r2 < MIN_R2_PUBLISH:
        blockers.append(f"Regression R² {r2:.4f} below publish gate {MIN_R2_PUBLISH}")
    if n < MIN_AGRI_REGRESSION_N and model_id != "agri_stu_regression_v1":
        blockers.append(f"Aligned sample n={n} below regression minimum {MIN_AGRI_REGRESSION_N}")
    if dev is not None and abs(dev) > MAX_PUBLISH_DEVIATION_PCT:
        blockers.append(f"Current |deviation| {abs(dev):.1f}% exceeds {MAX_PUBLISH_DEVIATION_PCT}% cap")
    if rev60 is not None and rev60 < MIN_REVERSION_60D_PCT and rev_trials >= 5:
        blockers.append(f"60d reversion {rev60}% below {MIN_REVERSION_60D_PCT}% ({rev_trials} trials)")

    publishable = computed and not blockers
    status = classify_status(r2=r2, publishable=publishable, model_exists=computed)

    out["institutional_audit"] = {
        "r_squared": r2,
        "reversion_30d_pct": rev30,
        "reversion_60d_pct": rev60,
        "reversion_90d_pct": rev90,
        "reversion_trials": rev_trials,
        "status": status,
        "publish": publishable,
        "blockers": blockers,
    }
    out["publish"] = publishable
    out["model_status"] = "VALIDATED" if publishable else "WITHHELD"
    if publishable:
        out["wired"] = True
        out["pass"] = True
    else:
        out["wired"] = False
        out["pass"] = False
        reason = "; ".join(blockers) if blockers else "Institutional publish gate failed"
        out["withheld_reason"] = reason
        out["valuation_reason"] = f"WITHHELD — {reason}"
    return out


def apply_metals_publish_gate(
    result: dict[str, Any],
    *,
    reversion_series: list[dict[str, Any]] | None = None,
    validation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    out = dict(result)
    reg = out.get("regression") or {}
    r2 = _num(reg.get("r_squared"))
    dev = _num(out.get("deviation_pct"))
    computed = bool(out.get("fair_value") is not None and dev is not None and out.get("model_variant"))

    rev30 = rev60 = rev90 = None
    rev_trials = 0
    if reversion_series:
        rev30, _ = reversion_rate_pct(reversion_series, horizon_days=30)
        rev60, rev_trials = reversion_rate_pct(reversion_series, horizon_days=60)
        rev90, _ = reversion_rate_pct(reversion_series, horizon_days=90)

    blockers: list[str] = []
    if not computed:
        blockers.append(out.get("unavailable_reason") or out.get("valuation_reason") or "Model not selected")
    elif r2 is not None and r2 < MIN_R2_PUBLISH:
        blockers.append(f"R² {r2:.4f} below publish gate {MIN_R2_PUBLISH}")
    if dev is not None and abs(dev) > MAX_PUBLISH_DEVIATION_PCT:
        blockers.append(
            f"Current |deviation| {abs(dev):.1f}% exceeds {MAX_PUBLISH_DEVIATION_PCT}% — "
            "extreme valuation without sufficient reversion support"
        )
    if rev60 is not None and rev60 < MIN_REVERSION_60D_PCT and rev_trials >= 20:
        blockers.append(f"60d reversion {rev60}% below {MIN_REVERSION_60D_PCT}% ({rev_trials} trials)")

    publishable = computed and not blockers
    status = classify_status(r2=r2, publishable=publishable, model_exists=bool(out.get("model_variant")))

    audit = {
        "r_squared": r2,
        "reversion_30d_pct": rev30,
        "reversion_60d_pct": rev60,
        "reversion_90d_pct": rev90,
        "reversion_trials": rev_trials,
        "status": status,
        "publish": publishable,
        "blockers": blockers,
    }
    if validation:
        audit.update({k: validation.get(k) for k in ("mae", "rmse", "avg_deviation_pct", "max_deviation_pct")})
    out["institutional_audit"] = audit
    out["publish"] = publishable
    out["model_status"] = "VALIDATED" if publishable else "WITHHELD"
    if publishable:
        out["wired"] = True
        out["pass"] = True
    else:
        out["wired"] = False
        out["pass"] = False
        reason = "; ".join(blockers) if blockers else "Institutional publish gate failed"
        out["withheld_reason"] = reason
        out["valuation_reason"] = f"WITHHELD — {reason}"
    return out
