"""Metals Valuation V1 — real yield + DXY fair value (institutional macro anchor).

Uses existing FRED macro_cache (DFII10, DTWEXBGS) and canonical metal prices.
Does not substitute location percentile or COT for valuation.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from hptl.prices.canonical_timeline import load_canonical_timeline
from hptl.valuation.engine import BIAS_UNAVAILABLE

MODEL_ID = "metals_real_yield_v1"
VALUATION_PHASE = "V3.1 Metals"

METALS_MARKETS: tuple[str, ...] = (
    "Gold",
    "Silver",
    "Copper / HG",
    "Platinum",
    "Palladium",
)

PREMIUM_METALS: frozenset[str] = frozenset({"Gold", "Silver"})
PGM_METALS: frozenset[str] = frozenset({"Platinum", "Palladium"})
INDUSTRIAL_METALS: frozenset[str] = frozenset({"Copper / HG"})

REAL_YIELD_SERIES = "DFII10"
DXY_SERIES = "DTWEXBGS"
CHINA_PMI_SERIES = "CHINAMANUFPMIMEI"

MIN_WEEKS = 52
MIN_WEEKS_GRADE_A = 156
MIN_R2 = 0.08
MIN_R2_GRADE_A = 0.15
DEV_UNDER_PCT = -5.0
DEV_OVER_PCT = 5.0

TrustGrade = Literal["A", "B", "C"]


def is_metals_valuation_market(market: str) -> bool:
    return market in METALS_MARKETS


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _asof_value(series: dict[str, float], date: str) -> float | None:
    if not series:
        return None
    d = str(date)[:10]
    best: float | None = None
    for k in series:
        if k <= d:
            best = series[k]
        elif k > d:
            break
    return best


def _load_macro_series(series_id: str) -> dict[str, float]:
    from hptl.fx.fx_macro_history import load_fred_daily_map

    return load_fred_daily_map(series_id)


def _load_dxy_series() -> dict[str, float]:
    dxy = _load_macro_series(DXY_SERIES)
    if len(dxy) >= MIN_WEEKS:
        return dxy
    tl = load_canonical_timeline("US Dollar Index / DX")
    if not tl:
        return dxy
    out = dict(dxy)
    for d, c in tl.daily_closes():
        iso = str(d)[:10]
        v = _num(c)
        if iso and v is not None and v > 0:
            out[iso] = float(v)
    return out


def _bias_from_deviation(dev_pct: float | None) -> str:
    if dev_pct is None or not math.isfinite(dev_pct):
        return BIAS_UNAVAILABLE
    if dev_pct <= DEV_UNDER_PCT:
        return "Undervalued"
    if dev_pct >= DEV_OVER_PCT:
        return "Overvalued"
    return "Fair Value"


def _percentile_rank(values: list[float], current: float) -> float | None:
    if not values:
        return None
    below = sum(1 for v in values if v < current)
    return round(100.0 * below / len(values), 1)


@dataclass(frozen=True)
class WeeklyObs:
    date: str
    price: float
    real_yield: float
    dxy: float
    china_pmi: float | None = None


def _build_weekly_panel(market: str) -> list[WeeklyObs]:
    tl = load_canonical_timeline(market)
    if not tl:
        return []
    real_yield = _load_macro_series(REAL_YIELD_SERIES)
    dxy = _load_dxy_series()
    if not real_yield or not dxy:
        return []

    china: dict[str, float] = {}
    if market in INDUSTRIAL_METALS:
        china = _load_macro_series(CHINA_PMI_SERIES)

    weekly_pairs, _ = tl.derive_weekly_iso()
    out: list[WeeklyObs] = []
    for date, price in weekly_pairs:
        px = _num(price)
        if px is None or px <= 0:
            continue
        iso = str(date)[:10]
        ry = _asof_value(real_yield, iso)
        dx = _asof_value(dxy, iso)
        if ry is None or dx is None or dx <= 0:
            continue
        pmi = _asof_value(china, iso) if china else None
        out.append(WeeklyObs(date=iso, price=px, real_yield=ry, dxy=dx, china_pmi=pmi))
    return out


def _multivariate_ols(
    y: list[float],
    x_cols: list[list[float]],
) -> tuple[list[float], float | None]:
    """OLS with intercept; returns [intercept, b1, b2, ...], r_squared."""
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


def _predict_log_price(beta: list[float], features: list[float]) -> float | None:
    if not beta or len(beta) != len(features) + 1:
        return None
    return beta[0] + sum(b * f for b, f in zip(beta[1:], features))


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


def _driver_summary(market: str, *, real_yield: float, dxy: float, dev_pct: float | None) -> str:
    parts: list[str] = []
    parts.append(f"10Y real yield {real_yield:.2f}%")
    parts.append(f"broad USD index {dxy:.2f}")
    if market in INDUSTRIAL_METALS:
        parts.append("China PMI layer reserved (not yet wired)")
    if dev_pct is not None:
        direction = "below" if dev_pct < 0 else "above" if dev_pct > 0 else "near"
        parts.append(f"spot {abs(dev_pct):.1f}% {direction} macro fair value")
    return "; ".join(parts) + "."


def _china_pmi_block(panel: list[WeeklyObs]) -> dict[str, Any]:
    pmi_vals = [o.china_pmi for o in panel if o.china_pmi is not None]
    available = len(pmi_vals) >= MIN_WEEKS
    return {
        "available": available,
        "wired": False,
        "placeholder": True,
        "series_id": CHINA_PMI_SERIES,
        "observations": len(pmi_vals),
        "note": "China PMI integration reserved for Copper V1.1 — macro regression runs without PMI term until audit pass.",
    }


def _compute_fair_value(
    panel: list[WeeklyObs],
    market: str,
    *,
    as_of_week: str | None = None,
) -> dict[str, Any]:
    """Core regression + fair value at as_of (or latest)."""
    if as_of_week:
        panel = [o for o in panel if o.date <= str(as_of_week)[:10]]
    if len(panel) < MIN_WEEKS:
        return {"ok": False, "reason": f"Insufficient aligned history ({len(panel)} weeks, need {MIN_WEEKS}+)."}

    use_china = market in INDUSTRIAL_METALS and sum(1 for o in panel if o.china_pmi is not None) >= MIN_WEEKS

    y = [math.log(o.price) for o in panel]
    x_ry = [o.real_yield for o in panel]
    x_dxy = [math.log(o.dxy) for o in panel]
    x_cols = [x_ry, x_dxy]
    feature_names = ["real_yield", "log_dxy"]
    if use_china:
        x_cols.append([o.china_pmi if o.china_pmi is not None else 0.0 for o in panel])
        feature_names.append("china_pmi")

    beta, r2 = _multivariate_ols(y, x_cols)
    if not beta or r2 is None:
        return {"ok": False, "reason": "Macro regression failed (singular or insufficient data)."}
    if r2 < MIN_R2:
        return {"ok": False, "reason": f"Model R² {r2:.3f} below gate {MIN_R2}."}

    latest = panel[-1]
    features = [latest.real_yield, math.log(latest.dxy)]
    if use_china and latest.china_pmi is not None:
        features.append(latest.china_pmi)

    log_fair = _predict_log_price(beta, features)
    if log_fair is None:
        return {"ok": False, "reason": "Fair value prediction failed."}
    fair = math.exp(log_fair)
    spot = latest.price
    dev_pct = round(100.0 * (spot - fair) / fair, 2) if fair > 0 else None

    # Historical deviation percentile + composite score (Gold/Silver/PGM)
    ratio_history: list[float] = []
    residual_history: list[float] = []
    for obs in panel:
        feats = [obs.real_yield, math.log(obs.dxy)]
        if use_china and obs.china_pmi is not None:
            feats.append(obs.china_pmi)
        lp = _predict_log_price(beta, feats)
        if lp is None:
            continue
        f = math.exp(lp)
        if f > 0:
            ratio_history.append(obs.price / f)
            residual_history.append(obs.price - f)

    ratio_pct = _percentile_rank(ratio_history, spot / fair if fair > 0 else 0.0)
    residual_pct = _percentile_rank(residual_history, spot - fair)

    composite_score: float | None = None
    if market in PREMIUM_METALS:
        # High percentile ratio = expensive vs macro history; map to score for display
        if ratio_pct is not None:
            composite_score = round(max(-100.0, min(100.0, (ratio_pct - 50.0) * 2.0)), 1)
    elif market in PGM_METALS or market in INDUSTRIAL_METALS:
        if residual_pct is not None:
            composite_score = round(max(-100.0, min(100.0, (residual_pct - 50.0) * 2.0)), 1)

    ry_dates = sorted(_load_macro_series(REAL_YIELD_SERIES))
    dxy_dates = sorted(_load_dxy_series())
    inputs_fresh = bool(ry_dates and dxy_dates and panel[-1].date >= ry_dates[-1][:10] and panel[-1].date >= dxy_dates[-1][:10])
    trust = _trust_grade(len(panel), r2, inputs_fresh)

    return {
        "ok": True,
        "n_obs": len(panel),
        "r_squared": round(r2, 4),
        "beta": {name: round(beta[i + 1], 6) for i, name in enumerate(feature_names)},
        "intercept": round(beta[0], 6),
        "fair_value": round(fair, 4),
        "spot_price": round(spot, 4),
        "deviation_pct": dev_pct,
        "as_of_date": latest.date,
        "real_yield": round(latest.real_yield, 3),
        "dxy": round(latest.dxy, 3),
        "valuation_ratio_percentile": ratio_pct,
        "residual_percentile": residual_pct,
        "composite_score": composite_score,
        "trust_grade": trust,
        "confidence": _confidence(trust),
        "use_china_pmi": use_china,
        "inputs_fresh": inputs_fresh,
    }


def compute_metals_valuation(*, market: str, as_of_week: str | None = None) -> dict[str, Any]:
    """Return metals fair value or explicit UNAVAILABLE."""
    base: dict[str, Any] = {
        "market": market,
        "as_of_week": as_of_week,
        "asset_class": "metals",
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
        "valuation_pillar": "metals_real_yield",
        "driver_summary": "10Y real yield (DFII10) + broad USD index (DTWEXBGS) macro fair value",
        "pass": False,
    }

    if not is_metals_valuation_market(market):
        base["valuation_reason"] = f"{market} is not a metals valuation market."
        return base

    panel = _build_weekly_panel(market)
    result = _compute_fair_value(panel, market, as_of_week=as_of_week)
    if market in INDUSTRIAL_METALS:
        base["china_pmi"] = _china_pmi_block(panel)

    if not result.get("ok"):
        reason = f"Metals valuation unavailable — {result.get('reason', 'unknown error')}"
        base["valuation_reason"] = reason
        base["unavailable_reason"] = reason
        base["data_depth"] = len(panel)
        return base

    dev = result["deviation_pct"]
    bias = _bias_from_deviation(dev)
    model_note = (
        f"log(price) ~ real_yield + log(DXY)"
        + (" + china_pmi" if result.get("use_china_pmi") else "")
        + f" (R²={result['r_squared']}, n={result['n_obs']})"
    )

    base.update(
        {
            "wired": True,
            "valuation_state": bias,
            "valuation_bias": bias,
            "valuation_score": result.get("composite_score"),
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
                "intercept": result["intercept"],
                "features": result["beta"],
            },
            "drivers": {
                "real_yield_10y": result["real_yield"],
                "dxy_broad": result["dxy"],
                "valuation_ratio_percentile": result.get("valuation_ratio_percentile"),
                "residual_percentile": result.get("residual_percentile"),
                "composite_score": result.get("composite_score"),
            },
            "input_freshness": {
                "price_as_of": result["as_of_date"],
                "real_yield_series": REAL_YIELD_SERIES,
                "dxy_series": DXY_SERIES,
                "inputs_fresh": result.get("inputs_fresh"),
            },
            "pass": bias != BIAS_UNAVAILABLE,
        }
    )
    base["driver_summary"] = _driver_summary(
        market,
        real_yield=result["real_yield"],
        dxy=result["dxy"],
        dev_pct=dev,
    )
    return base


def build_all_metals_valuations(*, as_of_week: str | None = None) -> dict[str, Any]:
    instruments: dict[str, Any] = {}
    wired = 0
    for market in METALS_MARKETS:
        val = compute_metals_valuation(market=market, as_of_week=as_of_week)
        instruments[market] = val
        if val.get("wired"):
            wired += 1
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "engine": MODEL_ID,
        "valuation_phase": VALUATION_PHASE,
        "summary": {
            "total_instruments": len(METALS_MARKETS),
            "wired_count": wired,
            "unavailable_count": len(METALS_MARKETS) - wired,
        },
        "instruments": instruments,
    }


def run_backtest_diagnostics(*, forward_weeks: int = 4) -> dict[str, Any]:
    """In-sample regression stats + simple forward-return correlation on deviations."""
    markets_out: dict[str, Any] = {}
    for market in METALS_MARKETS:
        panel = _build_weekly_panel(market)
        if len(panel) < MIN_WEEKS + forward_weeks:
            markets_out[market] = {"available": False, "reason": "insufficient panel"}
            continue

        result = _compute_fair_value(panel, market)
        if not result.get("ok"):
            markets_out[market] = {"available": False, "reason": result.get("reason")}
            continue

        beta = [result["intercept"], *result["beta"].values()]
        feature_names = list(result["beta"].keys())
        devs: list[float] = []
        fwd_rets: list[float] = []
        for i in range(MIN_WEEKS, len(panel) - forward_weeks):
            obs = panel[i]
            feats = [obs.real_yield, math.log(obs.dxy)]
            if "china_pmi" in feature_names and obs.china_pmi is not None:
                feats.append(obs.china_pmi)
            lp = _predict_log_price(beta, feats)
            if lp is None:
                continue
            fair_i = math.exp(lp)
            if fair_i <= 0:
                continue
            dev = 100.0 * (obs.price - fair_i) / fair_i
            p0 = obs.price
            p1 = panel[i + forward_weeks].price
            if p0 > 0 and p1 > 0:
                devs.append(dev)
                fwd_rets.append(100.0 * (p1 / p0 - 1.0))

        corr: float | None = None
        if len(devs) >= 20:
            try:
                import numpy as np

                if np.std(devs) > 0 and np.std(fwd_rets) > 0:
                    corr = round(float(np.corrcoef(devs, fwd_rets)[0, 1]), 4)
            except Exception:
                corr = None

        markets_out[market] = {
            "available": True,
            "n_obs": result["n_obs"],
            "r_squared": result["r_squared"],
            "trust_grade": result["trust_grade"],
            "forward_weeks": forward_weeks,
            "forward_return_correlation": corr,
            "mean_abs_deviation_pct": round(sum(abs(d) for d in devs) / len(devs), 2) if devs else None,
            "sample_forward_pairs": len(devs),
            "latest_deviation_pct": result["deviation_pct"],
            "latest_bias": _bias_from_deviation(result["deviation_pct"]),
        }

    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "model_id": MODEL_ID,
        "markets": markets_out,
    }
