"""Rates curve fair value V1 — US Treasury yields and curve spread.

Methodology (rates_curve_fair_value_v1):
  Fair yield = macro-anchored OLS on weekly FRED history.
  - 2Y (DGS2):     yield ~ Fed funds (DFF)
  - 10Y (DGS10):   yield ~ DFF + 10Y real yield (DFII10)
  - 30Y (DGS30):   yield ~ DGS10 + DFF (term-structure anchor)
  - Real 10Y:      DFII10 ~ DFF + nominal 10Y breakeven proxy
  - 2s10s spread:  fair = fair_10Y − fair_2Y (derived from sibling models)

Deviation % = (actual − fair) / fair × 100 on the yield (or spread) level.
"""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Literal

from hptl.valuation.engine import BIAS_UNAVAILABLE

MODEL_ID = "rates_curve_fair_value_v1"
VALUATION_PHASE = "V3.7 Rates"

RATES_MARKETS: tuple[str, ...] = (
    "US 2-Year Treasury Yield",
    "US 10-Year Treasury Yield",
    "US 30-Year Treasury Yield",
    "10-Year Real Yield",
    "2s10s Yield Curve",
)

MIN_WEEKS = 52
MIN_WEEKS_GRADE_A = 156
MIN_R2 = 0.08
MIN_R2_GRADE_A = 0.15
DEV_UNDER_PCT = -5.0
DEV_OVER_PCT = 5.0

TrustGrade = Literal["A", "B", "C"]

MARKET_SPECS: dict[str, dict[str, Any]] = {
    "US 2-Year Treasury Yield": {
        "target": "DGS2",
        "drivers": ["DFF"],
        "method": "2Y yield ~ Fed effective rate (policy anchor)",
    },
    "US 10-Year Treasury Yield": {
        "target": "DGS10",
        "drivers": ["DFF", "DFII10"],
        "method": "10Y yield ~ Fed funds + 10Y real yield (term & inflation anchor)",
    },
    "US 30-Year Treasury Yield": {
        "target": "DGS30",
        "drivers": ["DGS10", "DFF"],
        "method": "30Y yield ~ 10Y yield + Fed funds (long-end term premium)",
    },
    "10-Year Real Yield": {
        "target": "DFII10",
        "drivers": ["DFF", "BREAKEVEN10"],
        "method": "10Y TIPS real yield ~ Fed funds + 10Y breakeven (DGS10−DFII10)",
    },
    "2s10s Yield Curve": {
        "derived": True,
        "method": "2s10s spread = fair_10Y − fair_2Y vs actual DGS10−DGS2",
    },
}


def is_rates_valuation_market(market: str) -> bool:
    return market in RATES_MARKETS


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _load_series(series_id: str) -> dict[str, float]:
    from hptl.fx.fx_macro_history import load_fred_daily_map

    return load_fred_daily_map(series_id)


def _breakeven_series(dgs10: dict[str, float], dfii10: dict[str, float]) -> dict[str, float]:
    out: dict[str, float] = {}
    for d in sorted(set(dgs10.keys()) & set(dfii10.keys())):
        n = dgs10.get(d)
        r = dfii10.get(d)
        if n is not None and r is not None:
            out[d] = round(float(n) - float(r), 4)
    return out


def _weekly_last(daily: dict[str, float]) -> list[tuple[str, float]]:
    """ISO week-end (Friday) last observation per week."""
    if not daily:
        return []
    by_week: dict[tuple[int, int], tuple[str, float]] = {}
    for d in sorted(daily.keys()):
        v = _num(daily[d])
        if v is None:
            continue
        dt = date.fromisoformat(d[:10])
        iso = dt.isocalendar()
        key = (iso.year, iso.week)
        prev = by_week.get(key)
        if prev is None or d > prev[0]:
            by_week[key] = (d, v)
    return [(d, v) for d, v in sorted(by_week.values(), key=lambda x: x[0])]


@dataclass(frozen=True)
class RatesObs:
    date: str
    target: float
    drivers: dict[str, float]


def _build_panel(target_id: str, driver_ids: list[str]) -> tuple[list[RatesObs], dict[str, str]]:
    series_maps: dict[str, dict[str, float]] = {}
    sources: dict[str, str] = {target_id: f"FRED {target_id}"}
    target_daily = _load_series(target_id)
    series_maps["TARGET"] = target_daily

    dgs10 = _load_series("DGS10")
    dfii10 = _load_series("DFII10")

    for did in driver_ids:
        if did == "BREAKEVEN10":
            series_maps[did] = _breakeven_series(dgs10, dfii10)
            sources[did] = "derived DGS10 − DFII10 (10Y breakeven proxy)"
        else:
            series_maps[did] = _load_series(did)
            sources[did] = f"FRED {did}"

    target_w = _weekly_last(series_maps["TARGET"])
    driver_weekly: dict[str, list[tuple[str, float]]] = {}
    for did in driver_ids:
        driver_weekly[did] = _weekly_last(series_maps[did])

    driver_by_date: dict[str, dict[str, float]] = {did: dict(w) for did, w in zip(driver_ids, [driver_weekly[d] for d in driver_ids])}

    # Rebuild driver lookup by date from weekly lists
    driver_lookup: dict[str, dict[str, float]] = {}
    for did in driver_ids:
        driver_lookup[did] = {d: v for d, v in driver_weekly[did]}

    out: list[RatesObs] = []
    for d, tgt in target_w:
        drivers: dict[str, float] = {}
        ok = True
        for did in driver_ids:
            v = _asof_value(driver_lookup[did], d)
            if v is None:
                ok = False
                break
            drivers[did] = v
        if ok:
            out.append(RatesObs(date=d, target=tgt, drivers=drivers))
    return out, sources


def _asof_value(series: dict[str, float], iso_date: str) -> float | None:
    if not series:
        return None
    d = str(iso_date)[:10]
    best: float | None = None
    for k in sorted(series.keys()):
        if k <= d:
            best = series[k]
        else:
            break
    return best


def _multivariate_ols(y: list[float], x_cols: list[list[float]]) -> tuple[list[float], float | None]:
    n = len(y)
    if n < 3 or not x_cols or any(len(col) != n for col in x_cols):
        return [], None
    try:
        import numpy as np

        X = np.column_stack([np.ones(n)] + [np.array(col, dtype=float) for col in x_cols])
        yv = np.array(y, dtype=float)
        beta, _, _, _ = np.linalg.lstsq(X, yv, rcond=None)
        yhat = X @ beta
        ss_res = float(((yv - yhat) ** 2).sum())
        ss_tot = float(((yv - yv.mean()) ** 2).sum())
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else None
        return [float(b) for b in beta], r2
    except Exception:
        return [], None


def _predict(beta: list[float], features: list[float]) -> float | None:
    if not beta or len(beta) != len(features) + 1:
        return None
    return beta[0] + sum(b * f for b, f in zip(beta[1:], features))


def _bias_from_deviation(dev_pct: float | None) -> str:
    if dev_pct is None or not math.isfinite(dev_pct):
        return BIAS_UNAVAILABLE
    if dev_pct <= DEV_UNDER_PCT:
        return "Undervalued"
    if dev_pct >= DEV_OVER_PCT:
        return "Overvalued"
    return "Fair Value"


def _trust_grade(n: int, r2: float | None, inputs_fresh: bool) -> TrustGrade:
    if n >= MIN_WEEKS_GRADE_A and r2 is not None and r2 >= MIN_R2_GRADE_A and inputs_fresh:
        return "A"
    if n >= MIN_WEEKS and r2 is not None and r2 >= MIN_R2:
        return "B"
    return "C"


def _confidence(trust: TrustGrade) -> str:
    if trust == "A":
        return "medium"
    if trust == "B":
        return "low"
    return "none"


def _compute_direct(market: str, *, as_of_week: str | None = None) -> dict[str, Any]:
    spec = MARKET_SPECS[market]
    target_id = spec["target"]
    driver_ids: list[str] = list(spec["drivers"])
    panel, sources = _build_panel(target_id, driver_ids)
    if as_of_week:
        panel = [o for o in panel if o.date <= str(as_of_week)[:10]]
    if len(panel) < MIN_WEEKS:
        return {"ok": False, "reason": f"Insufficient aligned history ({len(panel)} weeks, need {MIN_WEEKS}+)."}

    y = [o.target for o in panel]
    x_cols = [[o.drivers[d] for o in panel] for d in driver_ids]
    beta, r2 = _multivariate_ols(y, x_cols)
    if not beta or r2 is None:
        return {"ok": False, "reason": "Macro regression failed."}
    if r2 < MIN_R2:
        return {"ok": False, "reason": f"Model R² {r2:.3f} below gate {MIN_R2}."}

    latest = panel[-1]
    features = [latest.drivers[d] for d in driver_ids]
    fair = _predict(beta, features)
    if fair is None:
        return {"ok": False, "reason": "Fair value prediction failed."}
    spot = latest.target
    dev_pct = round(100.0 * (spot - fair) / fair, 2) if fair != 0 else None

    target_daily = _load_series(target_id)
    dates = sorted(target_daily.keys())
    inputs_fresh = bool(dates and panel[-1].date >= dates[-1][:10])
    trust = _trust_grade(len(panel), r2, inputs_fresh)

    driver_parts = [f"{d}={latest.drivers[d]:.2f}" for d in driver_ids]
    return {
        "ok": True,
        "n_obs": len(panel),
        "r_squared": round(r2, 4),
        "beta": {driver_ids[i]: round(beta[i + 1], 6) for i in range(len(driver_ids))},
        "intercept": round(beta[0], 6),
        "fair_value": round(fair, 4),
        "spot_price": round(spot, 4),
        "deviation_pct": dev_pct,
        "as_of_date": latest.date,
        "trust_grade": trust,
        "confidence": _confidence(trust),
        "inputs_fresh": inputs_fresh,
        "method": spec["method"],
        "inputs": sources,
        "drivers_current": latest.drivers,
        "driver_summary": f"{spec['method']}; drivers: {', '.join(driver_parts)}.",
    }


def _compute_2s10s(*, as_of_week: str | None = None) -> dict[str, Any]:
    r2 = _compute_direct("US 2-Year Treasury Yield", as_of_week=as_of_week)
    r10 = _compute_direct("US 10-Year Treasury Yield", as_of_week=as_of_week)
    if not r2.get("ok") or not r10.get("ok"):
        reasons = []
        if not r2.get("ok"):
            reasons.append(f"2Y: {r2.get('reason')}")
        if not r10.get("ok"):
            reasons.append(f"10Y: {r10.get('reason')}")
        return {"ok": False, "reason": "; ".join(reasons)}

    dgs2 = _load_series("DGS2")
    dgs10 = _load_series("DGS10")
    spread_daily: dict[str, float] = {}
    for d in sorted(set(dgs2.keys()) & set(dgs10.keys())):
        spread_daily[d] = round(float(dgs10[d]) - float(dgs2[d]), 4)
    spread_w = _weekly_last(spread_daily)
    if as_of_week:
        spread_w = [(d, v) for d, v in spread_w if d <= str(as_of_week)[:10]]
    if not spread_w:
        return {"ok": False, "reason": "No 2s10s spread history."}

    spot_spread = spread_w[-1][1]
    fair_spread = round(float(r10["fair_value"]) - float(r2["fair_value"]), 4)
    dev_pct = round(100.0 * (spot_spread - fair_spread) / fair_spread, 2) if fair_spread != 0 else None

    n = min(int(r2["n_obs"]), int(r10["n_obs"]))
    r2_avg = statistics.mean([float(r2["r_squared"]), float(r10["r_squared"])])
    trust = _trust_grade(n, r2_avg, bool(r2.get("inputs_fresh") and r10.get("inputs_fresh")))

    return {
        "ok": True,
        "n_obs": n,
        "r_squared": round(r2_avg, 4),
        "fair_value": fair_spread,
        "spot_price": spot_spread,
        "deviation_pct": dev_pct,
        "as_of_date": spread_w[-1][0],
        "trust_grade": trust,
        "confidence": _confidence(trust),
        "inputs_fresh": r2.get("inputs_fresh") and r10.get("inputs_fresh"),
        "method": MARKET_SPECS["2s10s Yield Curve"]["method"],
        "inputs": {"2Y_fair": r2["fair_value"], "10Y_fair": r10["fair_value"], "spread": "DGS10−DGS2"},
        "drivers_current": {"fair_2y": r2["fair_value"], "fair_10y": r10["fair_value"], "actual_2y": r2["spot_price"], "actual_10y": r10["spot_price"]},
        "driver_summary": (
            f"2s10s fair {fair_spread:.2f}bp (10Y fair {r10['fair_value']}% − 2Y fair {r2['fair_value']}%); "
            f"actual spread {spot_spread:.2f}bp."
        ),
        "components": {"2y": r2, "10y": r10},
    }


def compute_rates_valuation(*, market: str, as_of_week: str | None = None) -> dict[str, Any]:
    base: dict[str, Any] = {
        "market": market,
        "as_of_week": as_of_week,
        "asset_class": "rates",
        "wired": False,
        "valuation_state": BIAS_UNAVAILABLE,
        "valuation_bias": BIAS_UNAVAILABLE,
        "valuation_score": None,
        "fair_value": None,
        "deviation_pct": None,
        "spot_price": None,
        "confidence": "none",
        "model_id": MODEL_ID,
        "valuation_phase": VALUATION_PHASE,
        "valuation_pillar": "rates_curve_fair_value",
        "driver_summary": MARKET_SPECS.get(market, {}).get("method", "US rates macro fair value"),
        "pass": False,
    }
    if not is_rates_valuation_market(market):
        base["valuation_reason"] = f"{market} is not a rates valuation market."
        return base

    if MARKET_SPECS[market].get("derived"):
        result = _compute_2s10s(as_of_week=as_of_week)
    else:
        result = _compute_direct(market, as_of_week=as_of_week)

    if not result.get("ok"):
        reason = f"Rates valuation unavailable — {result.get('reason', 'unknown')}"
        base["valuation_reason"] = reason
        base["unavailable_reason"] = reason
        return base

    dev = result["deviation_pct"]
    bias = _bias_from_deviation(dev)
    model_note = f"{result['method']} (R²={result['r_squared']}, n={result['n_obs']})"

    base.update(
        {
            "wired": True,
            "valuation_state": bias,
            "valuation_bias": bias,
            "fair_value": result["fair_value"],
            "deviation_pct": dev,
            "spot_price": result["spot_price"],
            "confidence": result["confidence"],
            "trust_grade": result["trust_grade"],
            "valuation_reason": model_note,
            "model_note": model_note,
            "regression": {
                "n": result["n_obs"],
                "r_squared": result["r_squared"],
                "intercept": result.get("intercept"),
                "features": result.get("beta"),
            },
            "drivers": result.get("drivers_current"),
            "inputs": result.get("inputs"),
            "driver_summary": result.get("driver_summary") or model_note,
            "pass": bias in {"Undervalued", "Overvalued", "Fair Value"},
        }
    )
    return base


def build_all_rates_valuations(*, as_of_week: str | None = None) -> dict[str, Any]:
    from datetime import datetime, timezone

    instruments: dict[str, Any] = {}
    for market in RATES_MARKETS:
        instruments[market] = compute_rates_valuation(market=market, as_of_week=as_of_week)
    wired = sum(1 for v in instruments.values() if v.get("wired"))
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": MODEL_ID,
        "valuation_phase": VALUATION_PHASE,
        "summary": {"total_instruments": len(RATES_MARKETS), "wired_count": wired},
        "instruments": instruments,
    }
