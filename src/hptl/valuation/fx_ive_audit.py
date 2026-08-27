"""Phase 1A — FX IVE institutional audit (evidence gathering only).

Audits fx_carry_real_yield_v3 per G10 major pair without modifying models.
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np

from hptl.config import DATA_DIR, PROJECT_ROOT
from hptl.fx.currency_rates import get_currency_rate
from hptl.fx.fx_macro_history import currency_histories, ensure_fx_macro_caches
from hptl.fx.fx_valuation import resolve_pair_currencies
from hptl.valuation.fx_carry_real_yield_v3 import (
    INFLATION_LOG_BETA,
    MIN_R_SQUARED,
    MIN_WEEKLY_OBS,
    MODEL_ID,
    POLICY_LOG_BETA,
    REAL_YIELD_LOG_BETA,
    VALUATION_PHASE,
    _align_daily_panel,
    _ols_log_spot,
    _regime_adjustment_pct,
    _treasury_regime,
    _value_as_of,
    compute_fx_pair_v3,
)

FX_IVE_AUDIT_PAIRS: tuple[str, ...] = (
    "EUR/USD",
    "GBP/USD",
    "AUD/USD",
    "NZD/USD",
    "USD/CHF",
    "USD/CAD",
    "USD/JPY",
)

AUDIT_JSON = DATA_DIR / "audits" / "fx_ive_audit.json"
AUDIT_MD = DATA_DIR / "audits" / "fx_ive_audit.md"
PUBLIC_JSON = PROJECT_ROOT / "web-dashboard/public/data/fx_ive_audit.json"


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _parse_date(s: str) -> date | None:
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


def _extended_panel(
    pair_id: str,
    base: str,
    quote: str,
    histories: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Daily panel with macro diffs (CPI held at latest snapshot — matches model limitation)."""
    br = get_currency_rate(base)
    qr = get_currency_rate(quote)
    base_cpi = br.cpi_yoy
    quote_cpi = qr.cpi_yoy
    panel = _align_daily_panel(pair_id, base, quote, histories)
    out: list[dict[str, Any]] = []
    for row in panel:
        y2d = _num(row.get("y2_diff"))
        pol = _num(row.get("policy_diff"))
        spot = _num(row.get("spot"))
        if spot is None or spot <= 0 or y2d is None:
            continue
        real_diff = None
        infl_diff = None
        if base_cpi is not None and quote_cpi is not None:
            infl_diff = round(base_cpi - quote_cpi, 4)
            by2 = _num(row.get("y2_diff"))
            # Recover leg y2 from panel via histories at date
            d = row["date"]
            base_y2 = _value_as_of(dict((histories.get(base) or {}).get("y2") or {}), d)
            quote_y2 = _value_as_of(dict((histories.get(quote) or {}).get("y2") or {}), d)
            if base_y2 is not None and quote_y2 is not None:
                real_diff = round((base_y2 - base_cpi) - (quote_y2 - quote_cpi), 4)
        out.append(
            {
                "date": row["date"],
                "spot": spot,
                "y2_diff": y2d,
                "policy_diff": pol,
                "real_yield_diff": real_diff,
                "inflation_diff": infl_diff,
            }
        )
    return out


def _fit_regression(panel: list[dict[str, Any]]) -> dict[str, Any]:
    y_log: list[float] = []
    x_y2: list[float] = []
    x_pol: list[float] = []
    for row in panel:
        s = _num(row.get("spot"))
        if s is None or s <= 0:
            continue
        y_log.append(math.log(s))
        x_y2.append(float(row["y2_diff"]))
        if row.get("policy_diff") is not None:
            x_pol.append(float(row["policy_diff"]))
    use_policy = len(x_pol) >= MIN_WEEKLY_OBS and len(x_pol) == len(y_log)
    if use_policy:
        return _ols_log_spot(y_log, x_y2, x_pol)
    return _ols_log_spot(y_log, x_y2, None)


def _regression_stats(y_log: np.ndarray, pred: np.ndarray, k: int) -> dict[str, float | None]:
    n = len(y_log)
    if n < 2:
        return {}
    ss_res = float(((y_log - pred) ** 2).sum())
    ss_tot = float(((y_log - y_log.mean()) ** 2).sum())
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    adj_r2 = 1.0 - (1.0 - r2) * (n - 1) / max(n - k - 1, 1)
    corr = float(np.corrcoef(y_log, pred)[0, 1]) if n > 1 else None
    rse = math.sqrt(ss_res / max(n - k - 1, 1))
    mae_log = float(np.abs(y_log - pred).mean())
    rmse_log = math.sqrt(float(((y_log - pred) ** 2).mean()))
    actual_p = np.exp(y_log)
    pred_p = np.exp(pred)
    mae_price = float(np.abs(actual_p - pred_p).mean())
    rmse_price = math.sqrt(float(((actual_p - pred_p) ** 2).mean()))
    return {
        "r_squared": round(r2, 4),
        "adjusted_r_squared": round(adj_r2, 4),
        "correlation": round(corr, 4) if corr is not None else None,
        "residual_std_error_log": round(rse, 6),
        "mae_log": round(mae_log, 6),
        "rmse_log": round(rmse_log, 6),
        "mae_price": round(mae_price, 6),
        "rmse_price": round(rmse_price, 6),
    }


def _predict_log_fv(
    reg: dict[str, Any],
    *,
    y2_diff: float,
    policy_diff: float | None,
    real_diff: float | None,
    infl_diff: float | None,
    use_policy_in_reg: bool,
) -> float:
    log_fv = float(reg["intercept"]) + float(reg["b_y2"]) * y2_diff
    if use_policy_in_reg and reg.get("b_policy") is not None and policy_diff is not None:
        log_fv += float(reg["b_policy"]) * policy_diff
    elif policy_diff is not None:
        log_fv += POLICY_LOG_BETA * policy_diff
    if real_diff is not None:
        log_fv += REAL_YIELD_LOG_BETA * real_diff
    if infl_diff is not None:
        log_fv += INFLATION_LOG_BETA * infl_diff
    return log_fv


def _fair_value_from_log(log_fv: float, regime_adj_pct: float) -> float:
    return round(math.exp(log_fv) * (1.0 + regime_adj_pct / 100.0), 6)


def _driver_attribution(
    reg: dict[str, Any],
    drivers: dict[str, Any],
    *,
    use_policy_in_reg: bool,
    regime_adj_pct: float,
    fair_value: float,
) -> dict[str, Any]:
    """Multiplicative log decomposition — contributions sum to fair value (pre-regime)."""
    intercept = float(reg["intercept"])
    y2 = _num(drivers.get("yield_2y_diff")) or 0.0
    pol = _num(drivers.get("policy_rate_diff")) or 0.0
    real = _num(drivers.get("real_yield_diff")) or 0.0
    infl = _num(drivers.get("inflation_diff")) or 0.0

    b_pol = float(reg["b_policy"]) if use_policy_in_reg and reg.get("b_policy") is not None else POLICY_LOG_BETA
    terms = {
        "intercept_anchor": intercept,
        "yield_2y": float(reg["b_y2"]) * y2,
        "policy_rate": b_pol * pol,
        "real_yield": REAL_YIELD_LOG_BETA * real,
        "inflation": INFLATION_LOG_BETA * infl,
    }
    log_core = sum(terms.values())
    fv_pre_regime = math.exp(log_core)
    fv_check = _fair_value_from_log(log_core, regime_adj_pct)

    # % price impact vs intercept-only anchor (exp(intercept))
    anchor = math.exp(intercept)
    contributions: list[dict[str, Any]] = []
    for name, log_term in (
        ("2Y Yield Differential", terms["yield_2y"]),
        ("Policy Rate Differential", terms["policy_rate"]),
        ("Real Yield Differential", terms["real_yield"]),
        ("Inflation (CPI YoY) Differential", terms["inflation"]),
    ):
        mult = math.exp(log_term)
        pct = round((mult - 1.0) * 100.0, 2)
        contributions.append({"factor": name, "log_contribution": round(log_term, 6), "price_impact_pct": pct})

    regime_pct = round((1.0 + regime_adj_pct / 100.0 - 1.0) * 100.0, 2)
    total_from_anchor = round((fv_check / anchor - 1.0) * 100.0, 2)

    return {
        "contributions": contributions,
        "regime_adjustment_pct": regime_adj_pct,
        "regime_price_impact_pct": regime_pct,
        "intercept_anchor_price": round(anchor, 6),
        "fair_value_pre_regime": round(fv_pre_regime, 6),
        "fair_value_reconciled": fv_check,
        "fair_value_published": fair_value,
        "reconciliation_error": round(abs(fv_check - (fair_value or fv_check)), 8),
        "total_price_impact_from_anchor_pct": total_from_anchor,
        "equation": (
            "log(Fair Value) = intercept + β_y2×2Y_diff + β_policy×Policy_diff "
            "+ β_real×RealYield_diff + β_infl×Infl_diff; "
            "Fair Value = exp(log_fv) × (1 + regime_adj/100)"
        ),
    }


def _window_panel(panel: list[dict[str, Any]], years: float | None) -> list[dict[str, Any]]:
    if not panel or years is None:
        return panel
    end = _parse_date(panel[-1]["date"])
    if end is None:
        return panel
    start = end - timedelta(days=int(years * 365.25))
    return [r for r in panel if (_parse_date(r["date"]) or date.min) >= start]


def _stability_test(
    panel: list[dict[str, Any]],
    current_drivers: dict[str, Any],
    *,
    base: str,
    quote: str,
    dxy: dict[str, Any],
    treas: dict[str, Any],
) -> dict[str, Any]:
    regime_adj = _regime_adjustment_pct(base, quote, dxy, treas)
    windows: dict[str, Any] = {}
    for label, years in (("3_year", 3.0), ("5_year", 5.0), ("full_sample", None)):
        sub = _window_panel(panel, years)
        reg = _fit_regression(sub)
        if not reg.get("ok"):
            windows[label] = {"ok": False, "n": reg.get("n", 0)}
            continue
        use_pol = reg.get("features") == "y2,policy"
        y2 = _num(current_drivers.get("yield_2y_diff"))
        pol = _num(current_drivers.get("policy_rate_diff"))
        real = _num(current_drivers.get("real_yield_diff"))
        infl = _num(current_drivers.get("inflation_diff"))
        log_fv = _predict_log_fv(
            reg, y2_diff=y2 or 0.0, policy_diff=pol, real_diff=real, infl_diff=infl, use_policy_in_reg=use_pol
        )
        fv = _fair_value_from_log(log_fv, regime_adj) if y2 is not None else None
        windows[label] = {
            "ok": True,
            "n": reg.get("n"),
            "sample_start": sub[0]["date"] if sub else None,
            "sample_end": sub[-1]["date"] if sub else None,
            "b_y2": reg.get("b_y2"),
            "b_policy": reg.get("b_policy"),
            "intercept": reg.get("intercept"),
            "r_squared": reg.get("r_squared"),
            "fair_value_at_current_drivers": fv,
        }

    full = windows.get("full_sample") or {}
    drift: dict[str, Any] = {}
    if full.get("ok"):
        fv_full = full.get("fair_value_at_current_drivers")
        for label in ("3_year", "5_year"):
            w = windows.get(label) or {}
            if w.get("ok") and fv_full and w.get("fair_value_at_current_drivers"):
                drift[label] = {
                    "fair_value_drift_pct": round(
                        (w["fair_value_at_current_drivers"] - fv_full) / fv_full * 100.0, 2
                    ),
                    "b_y2_drift": round((w.get("b_y2") or 0) - (full.get("b_y2") or 0), 6),
                    "coefficient_stable": abs((w.get("b_y2") or 0) - (full.get("b_y2") or 0)) < 0.02,
                }
    return {"windows": windows, "coefficient_drift": drift}


def _in_sample_predictions(reg: dict[str, Any], panel: list[dict[str, Any]]) -> tuple[np.ndarray, np.ndarray]:
    use_pol = reg.get("features") == "y2,policy"
    y: list[float] = []
    pred: list[float] = []
    k = 3 if use_pol else 2
    for row in panel:
        s = _num(row.get("spot"))
        y2 = _num(row.get("y2_diff"))
        if s is None or s <= 0 or y2 is None:
            continue
        if use_pol and row.get("policy_diff") is None:
            continue
        log_s = math.log(s)
        log_p = _predict_log_fv(
            reg,
            y2_diff=y2,
            policy_diff=_num(row.get("policy_diff")),
            real_diff=_num(row.get("real_yield_diff")),
            infl_diff=_num(row.get("inflation_diff")),
            use_policy_in_reg=use_pol,
        )
        y.append(log_s)
        pred.append(log_p)
    return np.array(y), np.array(pred)


def _performance_track(
    reg: dict[str, Any],
    panel: list[dict[str, Any]],
    *,
    base: str,
    quote: str,
    dxy: dict[str, Any],
    treas: dict[str, Any],
) -> dict[str, Any]:
    regime_adj = _regime_adjustment_pct(base, quote, dxy, treas)
    use_pol = reg.get("features") == "y2,policy"
    series: list[dict[str, Any]] = []
    deviations: list[float] = []
    for row in panel:
        spot = _num(row.get("spot"))
        y2 = _num(row.get("y2_diff"))
        if spot is None or y2 is None:
            continue
        log_fv = _predict_log_fv(
            reg,
            y2_diff=y2,
            policy_diff=_num(row.get("policy_diff")),
            real_diff=_num(row.get("real_yield_diff")),
            infl_diff=_num(row.get("inflation_diff")),
            use_policy_in_reg=use_pol,
        )
        fv = _fair_value_from_log(log_fv, regime_adj)
        dev = (spot - fv) / fv * 100.0
        deviations.append(dev)
        series.append({"date": row["date"], "spot": spot, "fair_value": fv, "deviation_pct": round(dev, 2)})

    def _slice_years(years: float) -> list[dict[str, Any]]:
        sub = _window_panel(series, years)
        return sub

    def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
        if not rows:
            return {}
        devs = [r["deviation_pct"] for r in rows]
        return {
            "avg_deviation_pct": round(float(np.mean(devs)), 2),
            "max_abs_deviation_pct": round(max(abs(d) for d in devs), 2),
            "observations": len(rows),
        }

    # Reversion: after |dev|>5%, does deviation shrink within 60 trading days?
    reversion_hits = 0
    reversion_trials = 0
    for i, row in enumerate(series):
        if abs(row["deviation_pct"]) < 5.0:
            continue
        reversion_trials += 1
        future = series[i + 1 : i + 61]
        if not future:
            continue
        if abs(future[-1]["deviation_pct"]) < abs(row["deviation_pct"]):
            reversion_hits += 1
    reversion_accuracy = round(reversion_hits / reversion_trials * 100.0, 1) if reversion_trials else None

    return {
        "full_sample_metrics": _metrics(series),
        "last_1y": _metrics(_slice_years(1.0)),
        "last_3y": _metrics(_slice_years the 3.0)),
        "last_5y": _metrics(_slice_years(5.0)),
        "reversion_accuracy_pct_60d": reversion_accuracy,
        "reversion_trials": reversion_trials,
        "chart_series_weekly": series[::5][-400:],  # ~weekly subsample for JSON size
    }


def _input_audit(base: str, quote: str, drivers: dict[str, Any], panel: list[dict[str, Any]]) -> list[dict[str, Any]]:
    br = get_currency_rate(base)
    qr = get_currency_rate(quote)

    def _hist_range(key: str) -> dict[str, float | None]:
        vals = [_num(r.get(key)) for r in panel]
        vals = [v for v in vals if v is not None]
        if not vals:
            return {"min": None, "max": None}
        return {"min": round(min(vals), 4), "max": round(max(vals), 4)}

    inputs = [
        {
            "name": "Policy Rate Differential",
            "current_value": drivers.get("policy_rate_diff"),
            "historical_range": _hist_range("policy_diff"),
            "latest_source_date": qr.latest_as_of,
            "legs": {
                base: {"value": drivers.get("base_policy_rate"), "as_of": br.policy_rate_as_of},
                quote: {"value": drivers.get("quote_policy_rate"), "as_of": qr.policy_rate_as_of},
            },
        },
        {
            "name": "2Y Yield Differential",
            "current_value": drivers.get("yield_2y_diff"),
            "historical_range": _hist_range("y2_diff"),
            "latest_source_date": max(filter(None, [br.y2_as_of, qr.y2_as_of]), default=None),
            "legs": {
                base: {"value": drivers.get("base_yield_2y"), "as_of": br.y2_as_of},
                quote: {"value": drivers.get("quote_yield_2y"), "as_of": qr.y2_as_of},
            },
        },
        {
            "name": "Real Yield Differential",
            "current_value": drivers.get("real_yield_diff"),
            "historical_range": _hist_range("real_yield_diff"),
            "latest_source_date": max(filter(None, [br.y2_as_of, br.cpi_yoy_as_of, qr.y2_as_of, qr.cpi_yoy_as_of]), default=None),
            "note": "CPI YoY is snapshot-only (not daily history); real yield history tracks 2Y with fixed CPI.",
        },
        {
            "name": "Inflation (CPI YoY) Differential",
            "current_value": drivers.get("inflation_diff"),
            "historical_range": {"min": drivers.get("inflation_diff"), "max": drivers.get("inflation_diff")},
            "latest_source_date": max(filter(None, [br.cpi_yoy_as_of, qr.cpi_yoy_as_of]), default=None),
            "note": "Single CPI snapshot per leg — no historical CPI series in engine.",
        },
    ]
    return inputs


def _institutional_rating(
    pair_id: str,
    block: dict[str, Any],
    stats: dict[str, Any],
    stability: dict[str, Any],
    performance: dict[str, Any],
) -> dict[str, Any]:
    r2 = _num(block.get("regression", {}).get("r_squared"))
    audit = block.get("audit_status")
    wired = block.get("wired")
    stale = block.get("stale_inputs") or []
    missing = block.get("missing_inputs") or []
    reg_n = block.get("regression", {}).get("n") or 0
    dev = abs(_num(block.get("deviation_pct")) or 0)
    rmse = _num(stats.get("rmse_price"))
    reversion = performance.get("reversion_accuracy_pct_60d")

    if audit == "FAIL" or not wired or r2 is None or r2 < MIN_R_SQUARED:
        rating = "REBUILD_REQUIRED"
        if pair_id == "GBP/USD" and r2 is not None and r2 < MIN_R_SQUARED:
            justification = (
                f"R²={r2:.4f} below gate ({MIN_R_SQUARED}); fair value not published. "
                f"Regression n={reg_n} but 2Y/policy fit explains <8% of log-spot variance."
            )
        else:
            justification = (
                f"Model audit FAIL or not wired. missing={missing or 'none'}, "
                f"R²={r2}, regression_n={reg_n}."
            )
    elif r2 >= 0.35 and reg_n >= 500 and not missing and dev < 15:
        rating = "PRODUCTION_READY"
        justification = (
            f"R²={r2:.4f}, n={reg_n}, RMSE={rmse}, current |deviation|={dev:.1f}%. "
            f"60d reversion accuracy={reversion}%. Stale inputs: {stale or 'none'}."
        )
    else:
        rating = "NEEDS_IMPROVEMENT"
        reasons: list[str] = []
        if r2 is not None and r2 < 0.25:
            reasons.append(f"moderate R²={r2:.4f}")
        if stale:
            reasons.append(f"stale inputs {stale}")
        if dev >= 15:
            reasons.append(f"current deviation {block.get('deviation_pct'):+.1f}% is extreme")
        if reg_n < 500:
            reasons.append(f"policy-aligned regression n={reg_n} (sparse policy history for some pairs)")
        drift = stability.get("coefficient_drift") or {}
        for w, d in drift.items():
            if not d.get("coefficient_stable"):
                reasons.append(f"{w} β_y2 drift {d.get('b_y2_drift')}")
        if reversion is not None and reversion < 50:
            reasons.append(f"60d reversion accuracy only {reversion}%")
        justification = "; ".join(reasons) or "Borderline fit or data quality."

    return {"rating": rating, "justification": justification}


def audit_fx_pair(pair_id: str, histories: dict[str, dict[str, Any]]) -> dict[str, Any]:
    resolved = resolve_pair_currencies(pair_id)
    if not resolved:
        return {"pair": pair_id, "error": "unsupported pair"}
    base, quote, _ = resolved
    block = compute_fx_pair_v3(pair_id, histories=histories).as_dict()
    panel = _extended_panel(pair_id, base, quote, histories)
    reg = block.get("regression") or _fit_regression(panel)
    use_pol = reg.get("features") == "y2,policy"
    k = 3 if use_pol else 2

    y_arr, pred_arr = _in_sample_predictions(reg, panel) if reg.get("ok") else (np.array([]), np.array([]))
    stats = _regression_stats(y_arr, pred_arr, k) if len(y_arr) else {}

    dxy = block.get("dxy_regime") or {}
    treas = block.get("treasury_regime") or {}
    drivers = block.get("drivers") or {}
    regime_adj = _regime_adjustment_pct(base, quote, dxy, treas)

    stability = _stability_test(panel, drivers, base=base, quote=quote, dxy=dxy, treas=treas)
    performance = _performance_track(reg, panel, base=base, quote=quote, dxy=dxy, treas=treas) if reg.get("ok") else {}

    attribution = _driver_attribution(
        reg,
        drivers,
        use_policy_in_reg=use_pol,
        regime_adj_pct=regime_adj,
        fair_value=_num(block.get("fair_value")) or 0.0,
    )

    sample_start = panel[0]["date"] if panel else None
    sample_end = panel[-1]["date"] if panel else None

    b_pol = reg.get("b_policy") if use_pol else POLICY_LOG_BETA

    return {
        "model_information": {
            "pair": pair_id,
            "model_name": MODEL_ID,
            "model_version": VALUATION_PHASE,
            "sample_period": f"{sample_start} → {sample_end}" if sample_start else None,
            "observation_count": len(panel),
            "regression_observations": reg.get("n"),
            "last_updated": block.get("input_freshness", {}).get("quote_rates_as_of"),
            "audit_status": block.get("audit_status"),
            "wired": block.get("wired"),
            "model_status": block.get("model_status"),
        },
        "inputs": _input_audit(base, quote, drivers, panel),
        "regression": {
            "equation": attribution["equation"],
            "intercept": reg.get("intercept"),
            "coefficients": {
                "yield_2y_diff": reg.get("b_y2"),
                "policy_rate_diff": b_pol,
                "real_yield_diff_fixed": REAL_YIELD_LOG_BETA,
                "inflation_diff_fixed": INFLATION_LOG_BETA,
            },
            "features_in_ols": reg.get("features"),
            "fixed_betas_note": "Real yield and inflation enter fair value with fixed log-betas (not OLS-estimated).",
        },
        "statistical_validation": stats,
        "stability_testing": stability,
        "fair_value_performance": {
            "current_price": block.get("spot_price"),
            "current_fair_value": block.get("fair_value"),
            "current_valuation_pct": block.get("deviation_pct"),
            "valuation_state": block.get("valuation_state"),
            **performance,
        },
        "driver_attribution": attribution,
        "institutional_review": _institutional_rating(
            pair_id, block, stats, stability, performance
        ),
        "data_quality_flags": {
            "missing_inputs": block.get("missing_inputs"),
            "stale_inputs": block.get("stale_inputs"),
            "cpi_snapshot_only": True,
        },
        "_ranking": {
            "r_squared": reg.get("r_squared"),
            "mae_price": stats.get("mae_price"),
            "rmse_price": stats.get("rmse_price"),
            "audit_status": block.get("audit_status"),
            "institutional_rating": None,  # filled after review
        },
    }


def run_fx_ive_audit(*, refresh_caches: bool = True) -> dict[str, Any]:
    if refresh_caches:
        ensure_fx_macro_caches()
    histories = currency_histories()
    pairs: dict[str, Any] = {}
    for pid in FX_IVE_AUDIT_PAIRS:
        pairs[pid] = audit_fx_pair(pid, histories)
        pairs[pid]["_ranking"]["institutional_rating"] = pairs[pid]["institutional_review"]["rating"]

    ranking = sorted(
        [
            {
                "pair": pid,
                "r_squared": pairs[pid]["_ranking"].get("r_squared"),
                "mae": pairs[pid]["_ranking"].get("mae_price"),
                "rmse": pairs[pid]["_ranking"].get("rmse_price"),
                "model_status": pairs[pid]["model_information"].get("audit_status"),
                "institutional_rating": pairs[pid]["institutional_review"]["rating"],
            }
            for pid in FX_IVE_AUDIT_PAIRS
        ],
        key=lambda r: (_num(r.get("r_squared")) or -1.0),
        reverse=True,
    )

    return {
        "phase": "1A",
        "title": "FX IVE Institutional Audit",
        "model_id": MODEL_ID,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pairs_audited": list(FX_IVE_AUDIT_PAIRS),
        "pairs": pairs,
        "summary_ranking": ranking,
    }


def _md_pair_section(pair_id: str, doc: dict[str, Any]) -> list[str]:
    mi = doc.get("model_information") or {}
    lines = [
        f"## {pair_id}",
        "",
        "### Model Information",
        "",
        f"- **Pair:** {pair_id}",
        f"- **Model Name:** {mi.get('model_name')}",
        f"- **Model Version:** {mi.get('model_version')}",
        f"- **Sample Period:** {mi.get('sample_period')}",
        f"- **Observation Count:** {mi.get('observation_count')} daily ({mi.get('regression_observations')} in OLS)",
        f"- **Last Updated:** {mi.get('last_updated')}",
        f"- **Audit Status:** {mi.get('audit_status')} | Wired: {mi.get('wired')}",
        "",
        "### Inputs",
        "",
        "| Input | Current | Historical Range | Latest Source |",
        "|---|---:|---|---|",
    ]
    for inp in doc.get("inputs") or []:
        rng = inp.get("historical_range") or {}
        rstr = f"{rng.get('min')} → {rng.get('max')}" if rng.get("min") is not None else "—"
        lines.append(
            f"| {inp.get('name')} | {inp.get('current_value')} | {rstr} | {inp.get('latest_source_date')} |"
        )

    reg = doc.get("regression") or {}
    coef = reg.get("coefficients") or {}
    lines.extend(
        [
            "",
            "### Regression Details",
            "",
            "```",
            "log(Fair Value) = intercept",
            "  + β_y2 × 2Y Yield Differential",
            "  + β_policy × Policy Rate Differential",
            "  + β_real × Real Yield Differential  (fixed β=0.055)",
            "  + β_infl × CPI YoY Differential       (fixed β=0.025)",
            "Fair Value = exp(log_fv) × (1 + regime_adj/100)",
            "```",
            "",
            f"- **Intercept:** {reg.get('intercept')}",
            f"- **β_y2:** {coef.get('yield_2y_diff')}",
            f"- **β_policy:** {coef.get('policy_rate_diff')} ({'OLS' if reg.get('features') == 'y2,policy' else 'fixed 0.045'})",
            f"- **β_real (fixed):** {coef.get('real_yield_diff_fixed')}",
            f"- **β_infl (fixed):** {coef.get('inflation_diff_fixed')}",
            f"- **OLS features:** {reg.get('features_in_ols')}",
            "",
            "### Statistical Validation",
            "",
        ]
    )
    sv = doc.get("statistical_validation") or {}
    lines.append(
        f"R²={sv.get('r_squared')} · Adj R²={sv.get('adjusted_r_squared')} · "
        f"Corr={sv.get('correlation')} · RSE(log)={sv.get('residual_std_error_log')} · "
        f"MAE={sv.get('mae_price')} · RMSE={sv.get('rmse_price')}"
    )

    lines.extend(["", "### Stability Testing", ""])
    st = doc.get("stability_testing") or {}
    for wname, w in (st.get("windows") or {}).items():
        if not w.get("ok"):
            lines.append(f"- **{wname}:** insufficient data (n={w.get('n')})")
            continue
        lines.append(
            f"- **{wname}** ({w.get('sample_start')} → {w.get('sample_end')}, n={w.get('n')}): "
            f"β_y2={w.get('b_y2'):.6f}, R²={w.get('r_squared')}, FV@now={w.get('fair_value_at_current_drivers')}"
        )
    for wname, d in (st.get("coefficient_drift") or {}).items():
        lines.append(
            f"  - Drift vs full: FV {d.get('fair_value_drift_pct'):+.2f}%, β_y2 drift {d.get('b_y2_drift')}"
        )

    fvp = doc.get("fair_value_performance") or {}
    lines.extend(
        [
            "",
            "### Fair Value Performance",
            "",
            f"- **Current Price:** {fvp.get('current_price')}",
            f"- **Current Fair Value:** {fvp.get('current_fair_value')}",
            f"- **Current Valuation %:** {fvp.get('current_valuation_pct')}",
            "",
            "| Window | Avg |dev| % | Max |dev| % | Obs |",
            "|---|---:|---:|---:|",
        ]
    )
    for key, label in (
        ("last_1y", "1 Year"),
        ("last_3y", "3 Year"),
        ("last_5y", "5 Year"),
        ("full_sample_metrics", "Full Sample"),
    ):
        m = fvp.get(key) or {}
        if m:
            lines.append(
                f"| {label} | {m.get('avg_deviation_pct')} | {m.get('max_abs_deviation_pct')} | {m.get('observations')} |"
            )
    lines.append(
        f"\n60-day reversion accuracy (|dev|>5%): **{fvp.get('reversion_accuracy_pct_60d')}%** "
        f"({fvp.get('reversion_trials')} trials)"
    )

    lines.extend(["", "### Driver Attribution", ""])
    attr = doc.get("driver_attribution") or {}
    for c in attr.get("contributions") or []:
        lines.append(f"- **{c.get('factor')}:** {c.get('price_impact_pct'):+.2f}% price impact")
    lines.append(
        f"- **Regime adjustment:** {attr.get('regime_adjustment_pct'):+.2f}% "
        f"({attr.get('regime_price_impact_pct'):+.2f}% price impact)"
    )
    lines.append(
        f"- **Total from intercept anchor:** {attr.get('total_price_impact_from_anchor_pct'):+.2f}% → "
        f"FV={attr.get('fair_value_reconciled')} (published {attr.get('fair_value_published')})"
    )

    ir = doc.get("institutional_review") or {}
    lines.extend(
        [
            "",
            "### Institutional Review",
            "",
            f"**{ir.get('rating')}**",
            "",
            ir.get("justification"),
            "",
        ]
    )
    return lines


def write_fx_ive_audit_artifacts(*, refresh_caches: bool = True) -> dict[str, Path]:
    report = run_fx_ive_audit(refresh_caches=refresh_caches)
    AUDIT_JSON.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    PUBLIC_JSON.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")

    md: list[str] = [
        "# FX IVE Audit — Phase 1A",
        "",
        f"**Model:** `{MODEL_ID}` · Generated {report['generated_at']}",
        "",
        "Evidence-only audit. No model changes. Determines institutional defensibility before Phase 1B.",
        "",
        "## Final Summary Ranking",
        "",
        "| Rank | Pair | R² | MAE | RMSE | Model Status | Institutional Rating |",
        "|---:|---|---:|---:|---:|---|---|",
    ]
    for i, row in enumerate(report["summary_ranking"], 1):
        md.append(
            f"| {i} | {row['pair']} | {row.get('r_squared')} | {row.get('mae')} | "
            f"{row.get('rmse')} | {row.get('model_status')} | **{row.get('institutional_rating')}** |"
        )
    md.append("")
    for pid in FX_IVE_AUDIT_PAIRS:
        md.extend(_md_pair_section(pid, report["pairs"][pid]))

    AUDIT_MD.write_text("\n".join(md), encoding="utf-8")
    return {"audit_json": AUDIT_JSON, "audit_md": AUDIT_MD, "public_json": PUBLIC_JSON}
