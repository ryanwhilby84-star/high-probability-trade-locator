"""USD broad fair value V1 — DXY / broad USD index macro fair value.

Methodology (usd_broad_fair_value_v1):
  log(DXY) ~ avg G10 2Y yield differential vs USD + Fed funds + 10Y real yield
  Plus historical percentile context for driver narrative.

Inputs:
  - Broad USD index (DTWEXBGS / canonical DX timeline)
  - Fed effective rate (DFF)
  - 10Y real yield (DFII10)
  - Average 2Y differential: mean(base_2Y − USD_2Y) across G10 legs
"""
from __future__ import annotations

import math
import statistics
from datetime import datetime, timezone
from typing import Any, Literal

from hptl.fx.fx_macro_history import build_differential_series, currency_histories
from hptl.prices.canonical_timeline import load_canonical_timeline
from hptl.valuation.engine import BIAS_UNAVAILABLE

MODEL_ID = "usd_broad_fair_value_v1"
VALUATION_PHASE = "V3.8 USD Index"
DXY_MARKET = "US Dollar Index / DX"

REAL_YIELD_SERIES = "DFII10"
FED_FUNDS_SERIES = "DFF"
DXY_FRED = "DTWEXBGS"

G10_VS_USD: tuple[tuple[str, str], ...] = (
    ("EUR", "USD"),
    ("GBP", "USD"),
    ("JPY", "USD"),
    ("CHF", "USD"),
    ("CAD", "USD"),
    ("AUD", "USD"),
    ("NZD", "USD"),
)

MIN_WEEKS = 52
MIN_WEEKS_GRADE_A = 156
MIN_R2 = 0.08
MIN_R2_GRADE_A = 0.15
DEV_UNDER_PCT = -5.0
DEV_OVER_PCT = 5.0

TrustGrade = Literal["A", "B", "C"]


def is_usd_index_valuation_market(market: str) -> bool:
    return market == DXY_MARKET


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


def _load_dxy_daily() -> dict[str, float]:
    dxy = _load_series(DXY_FRED)
    tl = load_canonical_timeline(DXY_MARKET)
    if tl:
        for d, c in tl.daily_closes():
            iso = str(d)[:10]
            v = _num(c)
            if iso and v is not None and v > 0:
                dxy[iso] = float(v)
    return dxy


def _weekly_last(daily: dict[str, float]) -> list[tuple[str, float]]:
    from datetime import date

    by_week: dict[tuple[int, int], tuple[str, float]] = {}
    for d in sorted(daily.keys()):
        v = _num(daily[d])
        if v is None:
            continue
        dt = date.fromisoformat(d[:10])
        key = (dt.isocalendar().year, dt.isocalendar().week)
        prev = by_week.get(key)
        if prev is None or d > prev[0]:
            by_week[key] = (d, v)
    return sorted(by_week.values(), key=lambda x: x[0])


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


def _avg_g10_2y_diff_daily(histories: dict[str, dict[str, Any]]) -> dict[str, float]:
    diffs_by_date: dict[str, list[float]] = {}
    for base, quote in G10_VS_USD:
        rows = build_differential_series(base, quote, "y2", histories)
        for row in rows:
            diffs_by_date.setdefault(row["date"], []).append(float(row["value"]))
    return {d: round(statistics.mean(vals), 4) for d, vals in diffs_by_date.items() if vals}


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


def _percentile_rank(values: list[float], current: float) -> float | None:
    if not values:
        return None
    below = sum(1 for v in values if v < current)
    return round(100.0 * below / len(values), 1)


def compute_usd_broad_valuation(*, market: str, as_of_week: str | None = None) -> dict[str, Any]:
    base: dict[str, Any] = {
        "market": market,
        "as_of_week": as_of_week,
        "asset_class": "fx",
        "subgroup": "usd_index",
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
        "valuation_pillar": "usd_broad_fair_value",
        "driver_summary": "Broad USD fair value from G10 rate diffs, real yield, and Fed policy",
        "pass": False,
    }
    if market != DXY_MARKET:
        base["valuation_reason"] = f"{market} is not the USD broad index market."
        return base

    dxy_daily = _load_dxy_daily()
    dff = _load_series(FED_FUNDS_SERIES)
    real10 = _load_series(REAL_YIELD_SERIES)
    histories = currency_histories()
    avg_diff = _avg_g10_2y_diff_daily(histories)

    dxy_w = _weekly_last(dxy_daily)
    dff_w = dict(_weekly_last(dff))
    ry_w = dict(_weekly_last(real10))
    diff_w = dict(_weekly_last(avg_diff))

    panel: list[tuple[str, float, float, float, float]] = []
    for d, spot in dxy_w:
        if spot <= 0:
            continue
        ff = _asof_value(dff_w, d)
        ry = _asof_value(ry_w, d)
        ad = _asof_value(diff_w, d)
        if ff is None or ry is None or ad is None:
            continue
        panel.append((d, spot, ff, ry, ad))

    if as_of_week:
        panel = [p for p in panel if p[0] <= str(as_of_week)[:10]]
    if len(panel) < MIN_WEEKS:
        base["valuation_reason"] = f"USD broad valuation unavailable — insufficient history ({len(panel)} weeks)."
        base["unavailable_reason"] = base["valuation_reason"]
        return base

    y = [math.log(p[1]) for p in panel]
    x_ff = [p[2] for p in panel]
    x_ry = [p[3] for p in panel]
    x_ad = [p[4] for p in panel]
    beta, r2 = _multivariate_ols(y, [x_ff, x_ry, x_ad])
    if not beta or r2 is None or r2 < MIN_R2:
        base["valuation_reason"] = f"USD broad valuation unavailable — R² {r2 if r2 is not None else 'n/a'} below gate."
        base["unavailable_reason"] = base["valuation_reason"]
        return base

    latest = panel[-1]
    log_fair = beta[0] + beta[1] * latest[2] + beta[2] * latest[3] + beta[3] * latest[4]
    fair = math.exp(log_fair)
    spot = latest[1]
    dev_pct = round(100.0 * (spot - fair) / fair, 2) if fair > 0 else None
    bias = _bias_from_deviation(dev_pct)

    dxy_hist = [p[1] for p in panel]
    pct_rank = _percentile_rank(dxy_hist, spot)

    dxy_dates = sorted(dxy_daily.keys())
    inputs_fresh = bool(dxy_dates and latest[0] >= dxy_dates[-1][:10])
    trust = _trust_grade(len(panel), r2, inputs_fresh)
    model_note = f"log(DXY) ~ DFF + DFII10 + avg_G10_2Y_diff (R²={round(r2, 4)}, n={len(panel)})"

    base.update(
        {
            "wired": True,
            "valuation_state": bias,
            "valuation_bias": bias,
            "fair_value": round(fair, 4),
            "deviation_pct": dev_pct,
            "spot_price": round(spot, 4),
            "confidence": _confidence(trust),
            "trust_grade": trust,
            "valuation_reason": model_note,
            "model_note": model_note,
            "regression": {
                "n": len(panel),
                "r_squared": round(r2, 4),
                "intercept": round(beta[0], 6),
                "features": {
                    "fed_funds": round(beta[1], 6),
                    "real_yield_10y": round(beta[2], 6),
                    "avg_g10_2y_diff": round(beta[3], 6),
                },
            },
            "drivers": {
                "fed_funds": round(latest[2], 3),
                "real_yield_10y": round(latest[3], 3),
                "avg_g10_2y_diff": round(latest[4], 3),
                "dxy_percentile_10y": pct_rank,
            },
            "inputs": {
                "dxy": f"FRED {DXY_FRED} + canonical DX timeline",
                "fed_funds": f"FRED {FED_FUNDS_SERIES}",
                "real_yield": f"FRED {REAL_YIELD_SERIES}",
                "g10_2y_diff": "mean(EUR,GBP,JPY,CHF,CAD,AUD,NZD 2Y − USD 2Y)",
            },
            "driver_summary": (
                f"DXY {spot:.2f} vs fair {fair:.2f} ({dev_pct:+.1f}%); "
                f"Fed {latest[2]:.2f}%, real yield {latest[3]:.2f}%, "
                f"avg G10 2Y diff {latest[4]:+.2f}bp; "
                f"10Y history percentile {pct_rank if pct_rank is not None else '—'}."
            ),
            "pass": bias in {"Undervalued", "Overvalued", "Fair Value"},
        }
    )
    return base


def build_usd_broad_valuation(*, as_of_week: str | None = None) -> dict[str, Any]:
    block = compute_usd_broad_valuation(market=DXY_MARKET, as_of_week=as_of_week)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": MODEL_ID,
        "valuation_phase": VALUATION_PHASE,
        "summary": {"total_instruments": 1, "wired_count": 1 if block.get("wired") else 0},
        "instruments": {DXY_MARKET: block},
    }
