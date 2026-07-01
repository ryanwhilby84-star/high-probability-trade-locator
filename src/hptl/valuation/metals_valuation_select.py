"""Metals valuation model selection — Phase 3A per-market variant testing.

Tests real_yield-only, DXY-only, combined, rolling-window, and Silver gold/silver ratio.
Selects the variant that passes institutional publish gates when possible.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable

from hptl.valuation.institutional_publish_gate import (
    MAX_PUBLISH_DEVIATION_PCT,
    MIN_R2_PUBLISH,
    MIN_REVERSION_60D_PCT,
    apply_metals_publish_gate,
    reversion_rate_pct,
)
from hptl.valuation.metals_valuation_v1 import (
    MIN_WEEKS,
    WeeklyObs,
    _build_weekly_panel,
    _multivariate_ols,
    _predict_log_price,
    _bias_from_deviation,
)

ROLLING_WEEKS = 156


@dataclass(frozen=True)
class MetalsVariant:
    model_id: str
    feature_names: tuple[str, ...]
    build_features: Callable[[WeeklyObs, list[WeeklyObs], int], list[float] | None]


def _feat_real_yield(obs: WeeklyObs, _panel: list[WeeklyObs], _i: int) -> list[float] | None:
    return [obs.real_yield]


def _feat_log_dxy(obs: WeeklyObs, _panel: list[WeeklyObs], _i: int) -> list[float] | None:
    return [math.log(obs.dxy)]


def _feat_ry_dxy(obs: WeeklyObs, _panel: list[WeeklyObs], _i: int) -> list[float] | None:
    return [obs.real_yield, math.log(obs.dxy)]


def _feat_ry_dxy_gs_ratio(obs: WeeklyObs, panel: list[WeeklyObs], i: int) -> list[float] | None:
    gold_panel = _build_weekly_panel("Gold")
    if not gold_panel:
        return None
    # Match gold price on same week
    dates = {o.date: o.price for o in gold_panel}
    gp = dates.get(obs.date)
    if gp is None or gp <= 0 or obs.price <= 0:
        return None
    return [obs.real_yield, math.log(obs.dxy), math.log(gp / obs.price)]


def _fit_metrics(actual: list[float], predicted: list[float]) -> dict[str, float | None]:
    if not actual or len(actual) != len(predicted):
        return {"mae": None, "rmse": None, "avg_deviation_pct": None, "max_deviation_pct": None}
    n = len(actual)
    mae = sum(abs(a - p) for a, p in zip(actual, predicted)) / n
    rmse = math.sqrt(sum((a - p) ** 2 for a, p in zip(actual, predicted)) / n)
    devs = [100.0 * (a - p) / p if p else 0.0 for a, p in zip(actual, predicted)]
    return {
        "mae": round(mae, 4),
        "rmse": round(rmse, 4),
        "avg_deviation_pct": round(sum(devs) / n, 2),
        "max_deviation_pct": round(max(abs(d) for d in devs), 2),
    }


def _run_variant(
    panel: list[WeeklyObs],
    variant: MetalsVariant,
    *,
    window: int | None = None,
) -> dict[str, Any] | None:
    use = panel[-window:] if window and len(panel) >= window else panel
    if len(use) < MIN_WEEKS:
        return None

    y = [math.log(o.price) for o in use]
    x_cols: list[list[float]] = []
    valid = True
    for j, _ in enumerate(use):
        feats = variant.build_features(use[j], use, j)
        if feats is None:
            valid = False
            break
        if not x_cols:
            x_cols = [[] for _ in feats]
        for k, f in enumerate(feats):
            x_cols[k].append(f)
    if not valid or not x_cols:
        return None

    beta, r2 = _multivariate_ols(y, x_cols)
    if not beta or r2 is None or r2 < MIN_R2_PUBLISH:
        return None

    latest = use[-1]
    feats_now = variant.build_features(latest, use, len(use) - 1)
    if feats_now is None:
        return None
    log_fair = _predict_log_price(beta, feats_now)
    if log_fair is None:
        return None
    fair = math.exp(log_fair)
    spot = latest.price
    dev_pct = round(100.0 * (spot - fair) / fair, 2) if fair > 0 else None

    actuals: list[float] = []
    preds: list[float] = []
    series: list[dict[str, Any]] = []
    for j, obs in enumerate(use):
        feats = variant.build_features(obs, use, j)
        if feats is None:
            continue
        lp = _predict_log_price(beta, feats)
        if lp is None:
            continue
        f = math.exp(lp)
        if f <= 0:
            continue
        actuals.append(obs.price)
        preds.append(f)
        series.append({"date": obs.date, "deviation_pct": round(100.0 * (obs.price - f) / f, 2)})

    metrics = _fit_metrics(actuals, preds)
    rev60, rev_trials = reversion_rate_pct(series, horizon_days=60)

    return {
        "model_variant": variant.model_id,
        "window": window or len(use),
        "n_obs": len(use),
        "r_squared": round(r2, 4),
        "intercept": round(beta[0], 6),
        "beta": {name: round(beta[i + 1], 6) for i, name in enumerate(variant.feature_names)},
        "fair_value": round(fair, 4),
        "spot_price": round(spot, 4),
        "deviation_pct": dev_pct,
        "as_of_date": latest.date,
        "real_yield": round(latest.real_yield, 3),
        "dxy": round(latest.dxy, 3),
        "validation": metrics,
        "reversion_series": series,
        "reversion_60d_pct": rev60,
        "reversion_trials": rev_trials,
    }


def _variants_for_market(market: str) -> list[MetalsVariant]:
    base = [
        MetalsVariant("metals_real_yield_only_v2", ("real_yield",), _feat_real_yield),
        MetalsVariant("metals_log_dxy_only_v2", ("log_dxy",), _feat_log_dxy),
        MetalsVariant("metals_real_yield_v1", ("real_yield", "log_dxy"), _feat_ry_dxy),
        MetalsVariant(
            f"metals_real_yield_v1_roll{ROLLING_WEEKS}",
            ("real_yield", "log_dxy"),
            _feat_ry_dxy,
        ),
    ]
    if market == "Silver":
        base.insert(
            2,
            MetalsVariant(
                "metals_silver_gs_ratio_v2",
                ("real_yield", "log_dxy", "log_gold_silver_ratio"),
                _feat_ry_dxy_gs_ratio,
            ),
        )
    return base


def _score_candidate(c: dict[str, Any]) -> tuple[float, float, float]:
    """Higher is better: publishable first, then lower |dev|, then higher R²."""
    dev = abs(c.get("deviation_pct") or 999.0)
    r2 = float(c.get("r_squared") or 0.0)
    rev60 = c.get("reversion_60d_pct")
    rev_ok = 1.0 if rev60 is not None and rev60 >= MIN_REVERSION_60D_PCT else 0.0
    publish_ok = 1.0 if dev <= MAX_PUBLISH_DEVIATION_PCT else 0.0
    return (publish_ok + rev_ok * 0.5, -dev, r2)


def select_metals_model(market: str) -> dict[str, Any]:
    panel = _build_weekly_panel(market)
    if len(panel) < MIN_WEEKS:
        return {"ok": False, "reason": f"Insufficient panel ({len(panel)} weeks)"}

    candidates: list[dict[str, Any]] = []
    for variant in _variants_for_market(market):
        windows: list[int | None] = [None]
        if variant.model_id.endswith(str(ROLLING_WEEKS)):
            windows = [ROLLING_WEEKS]
        for w in windows:
            hit = _run_variant(panel, variant, window=w)
            if hit:
                candidates.append(hit)

    if not candidates:
        return {"ok": False, "reason": "No variant passed R² gate"}

    candidates.sort(key=_score_candidate, reverse=True)
    best = candidates[0]
    gated = apply_metals_publish_gate(
        {
            "fair_value": best["fair_value"],
            "deviation_pct": best["deviation_pct"],
            "spot_price": best["spot_price"],
            "model_variant": best["model_variant"],
            "regression": {
                "n": best["n_obs"],
                "r_squared": best["r_squared"],
                "intercept": best["intercept"],
                "features": best["beta"],
            },
        },
        reversion_series=best["reversion_series"],
        validation=best["validation"],
    )

    return {
        "ok": True,
        "selected": best,
        "gated": gated,
        "candidates_tested": len(candidates),
        "all_variants": [
            {
                "model": c["model_variant"],
                "r2": c["r_squared"],
                "dev_pct": c["deviation_pct"],
                "rev60": c.get("reversion_60d_pct"),
            }
            for c in candidates[:8]
        ],
    }


def compute_metals_valuation_v2(*, market: str, as_of_week: str | None = None) -> dict[str, Any]:
    from hptl.valuation.engine import BIAS_UNAVAILABLE
    from hptl.valuation.metals_valuation_v1 import (
        MODEL_ID,
        VALUATION_PHASE,
        _driver_summary,
        is_metals_valuation_market,
    )

    base: dict[str, Any] = {
        "market": market,
        "as_of_week": as_of_week,
        "asset_class": "metals",
        "wired": False,
        "publish": False,
        "valuation_state": BIAS_UNAVAILABLE,
        "valuation_bias": BIAS_UNAVAILABLE,
        "valuation_score": None,
        "fair_value": None,
        "deviation_pct": None,
        "spot_price": None,
        "model_id": MODEL_ID,
        "valuation_phase": VALUATION_PHASE,
        "valuation_pillar": "metals_real_yield",
    }

    if not is_metals_valuation_market(market):
        base["valuation_reason"] = f"{market} is not a metals valuation market."
        return base

    sel = select_metals_model(market)
    if not sel.get("ok"):
        reason = f"Metals valuation unavailable — {sel.get('reason')}"
        base["valuation_reason"] = reason
        base["unavailable_reason"] = reason
        return base

    best = sel["selected"]
    gated = sel["gated"]
    dev = best["deviation_pct"]
    bias = _bias_from_deviation(dev)
    model_note = (
        f"{best['model_variant']}: log(price) ~ {', '.join(best['beta'].keys())} "
        f"(R²={best['r_squared']}, n={best['n_obs']}, window={best['window']})"
    )

    base.update(
        {
            "model_id": best["model_variant"],
            "model_variant": best["model_variant"],
            "fair_value": best["fair_value"],
            "deviation_pct": dev,
            "spot_price": best["spot_price"],
            "valuation_state": bias if gated.get("publish") else BIAS_UNAVAILABLE,
            "valuation_bias": bias if gated.get("publish") else BIAS_UNAVAILABLE,
            "valuation_reason": gated.get("valuation_reason") or model_note,
            "model_note": model_note,
            "regression": gated.get("regression") or {
                "n": best["n_obs"],
                "r_squared": best["r_squared"],
                "intercept": best["intercept"],
                "features": best["beta"],
            },
            "drivers": {
                "real_yield_10y": best["real_yield"],
                "dxy_broad": best["dxy"],
            },
            "institutional_audit": gated.get("institutional_audit"),
            "wired": gated.get("wired", False),
            "publish": gated.get("publish", False),
            "withheld_reason": gated.get("withheld_reason"),
            "model_status": gated.get("model_status"),
            "pass": gated.get("pass", False),
            "variant_selection": {
                "candidates_tested": sel["candidates_tested"],
                "top_variants": sel["all_variants"],
            },
        }
    )
    base["driver_summary"] = _driver_summary(
        market,
        real_yield=best["real_yield"],
        dxy=best["dxy"],
        dev_pct=dev,
    )
    return base
