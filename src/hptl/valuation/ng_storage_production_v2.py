"""Natural Gas Valuation — ng_storage_production_v2.

Validated two-driver fair value:
  log(P) = intercept + β_storage * storage_surplus_bcf + β_yoy * production_yoy_pct

Preserves ng_storage_v1 (storage-only) as an internal benchmark.
Falls back to v1 when production YoY is missing or stale beyond monthly cadence.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from hptl.prices.price_freshness import (
    build_instrument_price_freshness,
    valuation_deviation_gate,
)
from hptl.prices.price_store import load_instrument_record_internal
from hptl.valuation.energy_ng_drivers import MARKET, NgDriverBundle, build_ng_driver_bundle
from hptl.valuation.energy_natural_gas_valuation_v1 import (
    FEATURE_LABELS as V1_FEATURE_LABELS,
    _confidence,
    _expanding_walk_forward,
    _extreme_fv_rate,
    _institutional_bias_label,
    _ols_stats,
    _scale_position,
)
from hptl.valuation.engine import BIAS_UNAVAILABLE
from hptl.valuation.metals_valuation_v1 import (
    MIN_WEEKS,
    _bias_from_deviation,
    _predict_log_price,
)

MODEL_V1 = "ng_storage_v1"
MODEL_V2 = "ng_storage_production_v2"
VALUATION_PHASE = "Validated Two-Driver Fair Value"
VALIDATED_DRIVERS_V2 = ["storage_surplus_bcf", "production_yoy_pct"]
VALIDATED_DRIVERS_V1 = ["storage_surplus_bcf"]

# EIA dry-gas is monthly with multi-week publish lag; within this window as-of is accepted.
MAX_PRODUCTION_STALENESS_DAYS = 100
PRODUCTION_SOURCE_CADENCE = "monthly"

FEATURE_LABELS = {
    **V1_FEATURE_LABELS,
    "production_yoy_pct": "Production YoY %",
    "storage_surplus_bcf": "Storage surplus/deficit",
}

REJECTED_PRODUCTION_TRANSFORMS = [
    "raw_level",
    "seasonal_deviation",
    "trailing_zscore_156",
    "chg_4w",
    "chg_12w",
    "v1_fullsample_zscore",
    "dry_gas_production",  # full-sample z used in legacy ladder
    "dry_gas_production_level",
]


def _parse_date(d: str | None) -> datetime | None:
    if not d:
        return None
    try:
        return datetime.strptime(str(d)[:10], "%Y-%m-%d")
    except ValueError:
        return None


def _days_between(a: str | None, b: str | None) -> int | None:
    da, db = _parse_date(a), _parse_date(b)
    if da is None or db is None:
        return None
    return abs((db - da).days)


def production_yoy_freshness(
    *,
    as_of_week: str | None,
    observation_date: str | None,
    yoy_value: float | None,
    using_proxy: bool = False,
) -> dict[str, Any]:
    """Return freshness / usability of the tip production YoY observation."""
    if using_proxy:
        return {
            "usable": False,
            "reason": "proxy_or_fallback_production_not_allowed_for_v2",
            "stale": True,
            "observation_date": observation_date,
            "age_days": _days_between(observation_date, as_of_week),
            "max_age_days": MAX_PRODUCTION_STALENESS_DAYS,
        }
    if yoy_value is None or not math.isfinite(float(yoy_value)):
        return {
            "usable": False,
            "reason": "production_yoy_unavailable",
            "stale": True,
            "observation_date": observation_date,
            "age_days": _days_between(observation_date, as_of_week),
            "max_age_days": MAX_PRODUCTION_STALENESS_DAYS,
        }
    age = _days_between(observation_date, as_of_week)
    if age is None:
        return {
            "usable": False,
            "reason": "missing_production_observation_date",
            "stale": True,
            "observation_date": observation_date,
            "age_days": None,
            "max_age_days": MAX_PRODUCTION_STALENESS_DAYS,
        }
    if age > MAX_PRODUCTION_STALENESS_DAYS:
        return {
            "usable": False,
            "reason": "production_yoy_stale_beyond_monthly_cadence",
            "stale": True,
            "observation_date": observation_date,
            "age_days": age,
            "max_age_days": MAX_PRODUCTION_STALENESS_DAYS,
        }
    return {
        "usable": True,
        "reason": "ok",
        "stale": False,
        "observation_date": observation_date,
        "age_days": age,
        "max_age_days": MAX_PRODUCTION_STALENESS_DAYS,
    }


def _align_panel(
    bundle: NgDriverBundle,
    feature_names: list[str],
) -> tuple[list[int], list[str], list[float], list[list[float]]]:
    """Return indices/dates/y/cols where all requested features are finite."""
    keep: list[int] = []
    for i in range(bundle.n):
        if bundle.price[i] is None or bundle.price[i] <= 0:
            continue
        ok = True
        for name in feature_names:
            col = bundle.features.get(name) or []
            if i >= len(col):
                ok = False
                break
            v = col[i]
            if v is None or not math.isfinite(float(v)):
                ok = False
                break
        if ok:
            keep.append(i)
    dates = [bundle.dates[i] for i in keep]
    y = [math.log(bundle.price[i]) for i in keep]
    cols = [[float(bundle.features[name][i]) for i in keep] for name in feature_names]
    return keep, dates, y, cols


def _fit_model(
    *,
    model_id: str,
    feature_names: list[str],
    dates: list[str],
    y: list[float],
    cols: list[list[float]],
    prices: list[float],
) -> dict[str, Any] | None:
    if len(y) < MIN_WEEKS or any(len(c) != len(y) for c in cols):
        return None
    beta, r2, adj, t_stats, p_values = _ols_stats(y, cols)
    if not beta or r2 is None:
        return None
    wf = _expanding_walk_forward(y, cols, feature_names=feature_names)

    history: list[dict[str, Any]] = []
    for i in range(len(y)):
        feats = [col[i] for col in cols]
        lp = _predict_log_price(beta, feats)
        if lp is None:
            continue
        fair_i = math.exp(lp)
        spot_i = prices[i]
        history.append(
            {
                "date": dates[i],
                "spot_price": round(spot_i, 4),
                "fair_value": round(fair_i, 4),
                "deviation_pct": round(100.0 * (spot_i - fair_i) / fair_i, 2),
            }
        )

    latest_feats = [col[-1] for col in cols]
    log_fair = _predict_log_price(beta, latest_feats)
    if log_fair is None:
        return None
    fair = math.exp(log_fair)
    spot = prices[-1]
    extreme = _extreme_fv_rate(history)
    conf = _confidence(
        r2,
        len(y),
        len(feature_names),
        oos_r2=wf.get("oos_r2"),
        extreme_fv_rate=extreme,
    )
    return {
        "model_id": model_id,
        "features": list(feature_names),
        "n": len(y),
        "sample_start": dates[0],
        "sample_end": dates[-1],
        "intercept": round(beta[0], 6),
        "coefficients": {
            feature_names[i]: round(beta[i + 1], 6) for i in range(len(feature_names))
        },
        "r_squared": round(r2, 4),
        "adj_r_squared": round(adj, 4) if adj is not None else None,
        "p_values": {
            feature_names[i]: p_values[i + 1]
            for i in range(len(feature_names))
            if i + 1 < len(p_values)
        },
        "t_stats": {
            feature_names[i]: t_stats[i + 1]
            for i in range(len(feature_names))
            if i + 1 < len(t_stats)
        },
        "oos_r2": wf.get("oos_r2"),
        "oos_rmse": wf.get("oos_rmse"),
        "oos_mae": wf.get("oos_mae"),
        "n_oos": wf.get("n_oos"),
        "coefficient_stability": wf.get("coefficient_stability"),
        "extreme_fv_rate_25pct": extreme,
        "fair_value": round(fair, 4),
        "spot_price": round(spot, 4),
        "deviation_pct": round(100.0 * (spot - fair) / fair, 2) if fair > 0 else None,
        "log_fair": round(log_fair, 6),
        "latest_features": {
            feature_names[i]: round(latest_feats[i], 6) for i in range(len(feature_names))
        },
        "beta_full": beta,
        "latest_feats_list": latest_feats,
        "history": history,
        "confidence": conf,
        "equation": (
            "log(P) = "
            + f"{round(beta[0], 6)}"
            + "".join(
                f" + ({round(beta[i + 1], 6)}) * {feature_names[i]}"
                for i in range(len(feature_names))
            )
            + "; fair = exp(log(P))"
        ),
    }


def _driver_contributions(
    *,
    feature_names: list[str],
    beta: list[float],
    latest_feats: list[float],
    raw_values: dict[str, Any],
    spot: float,
) -> dict[str, Any]:
    intercept = float(beta[0])
    contrib_map: dict[str, Any] = {}
    log_sum = intercept
    rows = []
    for i, name in enumerate(feature_names):
        x = float(latest_feats[i])
        coef = float(beta[i + 1])
        log_c = coef * x
        log_sum += log_c
        price_impact_pct = 100.0 * (math.exp(log_c) - 1.0)
        entry = {
            "value": raw_values.get(name, x),
            "transformed_input": round(x, 6),
            "coefficient": round(coef, 6),
            "log_contribution": round(log_c, 6),
            "price_impact_pct": round(price_impact_pct, 4),
            "direction": (
                "raises fair value"
                if log_c > 0
                else "lowers fair value"
                if log_c < 0
                else "neutral"
            ),
            "label": FEATURE_LABELS.get(name, name),
        }
        contrib_map[name] = entry
        rows.append({"feature": name, **entry, "raw_observation": raw_values.get(name, x)})

    fair = math.exp(log_sum)
    return {
        "space": "log_price",
        "identity": "log(fair) = intercept + Σ (βᵢ · xᵢ); fair = exp(log(fair))",
        "intercept_log_contribution": round(intercept, 6),
        "driver_contributions": contrib_map,
        "drivers": rows,
        "sum_log_contributions": round(log_sum, 6),
        "reconstructed_log_fair": round(log_sum, 6),
        "reconstructed_fair_value": round(fair, 4),
        "reconciliation_ok": abs(math.exp(log_sum) - fair) < 1e-9,
        "market_price": round(spot, 4),
        "deviation_pct": round(100.0 * (spot - fair) / fair, 2) if fair > 0 else None,
        "note": (
            "price_impact_pct = 100*(exp(log_contribution)-1) is the multiplicative "
            "price-space effect of that driver term alone."
        ),
    }


def _annotate_cards_for_active(
    cards: dict[str, dict[str, Any]],
    *,
    active_model: str,
    contrib: dict[str, Any] | None,
    fallback: bool,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    contrib_map = (contrib or {}).get("driver_contributions") or {}
    for card in cards.values():
        c = dict(card)
        cid = c.get("id")
        if cid == "storage":
            c["valuation_role"] = "VALIDATED VALUATION DRIVER"
            c["valuation_badge"] = "VALIDATED VALUATION DRIVER"
            c["in_fair_value"] = True
            c["valuation_note"] = "Included in fair value"
            stor = contrib_map.get("storage_surplus_bcf") or {}
            if stor:
                c["model_contribution"] = stor
                c["contribution_direction"] = stor.get("direction")
                c["contribution_magnitude_log"] = stor.get("log_contribution")
        elif cid == "production":
            if active_model == MODEL_V2 and not fallback:
                c["valuation_role"] = "VALIDATED VALUATION DRIVER"
                c["valuation_badge"] = "VALIDATED VALUATION DRIVER"
                c["in_fair_value"] = True
                c["valuation_note"] = "Included in fair value as production_yoy_pct"
                yoy = contrib_map.get("production_yoy_pct") or {}
                if yoy:
                    c["model_contribution"] = yoy
                    c["contribution_direction"] = yoy.get("direction")
                    c["contribution_magnitude_log"] = yoy.get("log_contribution")
            else:
                c["valuation_role"] = "VALIDATED DRIVER — UNAVAILABLE / FALLBACK"
                c["valuation_badge"] = "NOT IN ACTIVE FAIR VALUE (V1 FALLBACK)"
                c["in_fair_value"] = False
                c["valuation_note"] = "v2 inactive — storage-only fallback"
            c["production_transformation"] = "production_yoy_pct"
            c["raw_level_used_in_fair_value"] = False
            c["interpretation"] = (
                "Faster year-over-year production growth increases available supply and "
                "lowers modeled fair value."
            )
        elif cid == "seasonality":
            c["valuation_role"] = "INFORMATIONAL ONLY"
            c["valuation_badge"] = "INFORMATIONAL ONLY — NOT INCLUDED IN FAIR VALUE"
            c["in_fair_value"] = False
        else:
            # LNG / weather / USD remain experimental — do not change their status semantics.
            if cid in {"lng_exports", "hdd", "cdd", "dxy"}:
                c["valuation_role"] = "EXPERIMENTAL DRIVER"
                c["valuation_badge"] = "EXPERIMENTAL DRIVER — NOT INCLUDED IN FAIR VALUE"
                c["in_fair_value"] = False
                c["valuation_note"] = "NOT INCLUDED IN FAIR VALUE"
        out.append(c)
    return out


def compute_ng_storage_production_v2(*, as_of_week: str | None = None) -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    bundle = build_ng_driver_bundle(as_of_week=as_of_week)
    cards = bundle.driver_cards
    spot = bundle.price[-1] if bundle.price else None
    as_of = bundle.as_of or as_of_week

    base: dict[str, Any] = {
        "market": MARKET,
        "model_id": MODEL_V2,
        "valuation_phase": VALUATION_PHASE,
        "valuation_pillar": "energy_natural_gas",
        "headline": VALUATION_PHASE,
        "generated_at": generated_at,
        "as_of_week": as_of,
        "wired": False,
        "publish": False,
        "fair_value": None,
        "spot_price": round(spot, 4) if spot is not None else None,
        "deviation_pct": None,
        "valuation_bias": BIAS_UNAVAILABLE,
        "institutional_bias": "Unavailable",
        "confidence": "None",
        "confidence_reasons": [],
        "active_model": None,
        "fallback_to_v1": False,
        "fallback_reason": None,
        "validated_drivers": [],
        "production_transformation": "production_yoy_pct",
        "rejected_production_transforms": list(REJECTED_PRODUCTION_TRANSFORMS),
        "raw_level_used_in_fair_value": False,
        "production_source_cadence": PRODUCTION_SOURCE_CADENCE,
        "max_production_staleness_days": MAX_PRODUCTION_STALENESS_DAYS,
        "driver_cards": list(cards.values()),
        "history": [],
        "awaiting_drivers": [c["label"] for c in cards.values() if not c.get("available")],
    }

    if not bundle.price or bundle.n < MIN_WEEKS or "storage_surplus_bcf" not in bundle.features:
        base["summary_text"] = "Insufficient storage/price history for valuation."
        return base

    # --- v1 storage-only benchmark ---
    keep_v1, dates_v1, y_v1, cols_v1 = _align_panel(bundle, VALIDATED_DRIVERS_V1)
    prices_v1 = [bundle.price[i] for i in keep_v1]
    v1_fit = _fit_model(
        model_id=MODEL_V1,
        feature_names=VALIDATED_DRIVERS_V1,
        dates=dates_v1,
        y=y_v1,
        cols=cols_v1,
        prices=prices_v1,
    )
    if not v1_fit:
        base["summary_text"] = "Storage-only v1 benchmark failed to fit."
        return base

    prod_card = cards.get("production") or {}
    tip_yoy = None
    tip_obs = None
    yoy_col = bundle.features.get("production_yoy_pct") or []
    obs_col = bundle.features.get("production_yoy_observation_date") or []
    if yoy_col:
        tip_yoy = yoy_col[-1]
        tip_obs = obs_col[-1] if obs_col else prod_card.get("observation_date")
    freshness = production_yoy_freshness(
        as_of_week=as_of,
        observation_date=str(tip_obs)[:10] if tip_obs else None,
        yoy_value=float(tip_yoy) if tip_yoy is not None else None,
        using_proxy=bool(prod_card.get("proxy") or prod_card.get("fallback")),
    )

    # --- v2 storage + production YoY ---
    keep_v2, dates_v2, y_v2, cols_v2 = _align_panel(bundle, VALIDATED_DRIVERS_V2)
    prices_v2 = [bundle.price[i] for i in keep_v2]
    v2_fit = _fit_model(
        model_id=MODEL_V2,
        feature_names=VALIDATED_DRIVERS_V2,
        dates=dates_v2,
        y=y_v2,
        cols=cols_v2,
        prices=prices_v2,
    )

    v1_benchmark = {
        "model_id": MODEL_V1,
        "fair_value": v1_fit["fair_value"],
        "deviation_pct": v1_fit["deviation_pct"],
        "confidence": v1_fit["confidence"],
        "equation": v1_fit["equation"],
        "coefficients": v1_fit["coefficients"],
        "intercept": v1_fit["intercept"],
        "validated_drivers": VALIDATED_DRIVERS_V1,
        "oos_r2": v1_fit["oos_r2"],
        "oos_rmse": v1_fit["oos_rmse"],
        "r_squared": v1_fit["r_squared"],
        "n": v1_fit["n"],
    }

    use_v2 = bool(v2_fit and freshness.get("usable"))
    fallback = not use_v2
    active = v2_fit if use_v2 else v1_fit
    active_model = MODEL_V2 if use_v2 else MODEL_V1
    validated = VALIDATED_DRIVERS_V2 if use_v2 else VALIDATED_DRIVERS_V1

    raw_obs = {
        "storage_surplus_bcf": (cards.get("storage") or {}).get("difference"),
        "production_yoy_pct": (
            float(tip_yoy) if tip_yoy is not None and math.isfinite(float(tip_yoy)) else None
        ),
    }
    # When v2 active, contributions use v2 betas; when fallback, storage-only.
    if use_v2 and v2_fit:
        contrib = _driver_contributions(
            feature_names=VALIDATED_DRIVERS_V2,
            beta=v2_fit["beta_full"],
            latest_feats=v2_fit["latest_feats_list"],
            raw_values=raw_obs,
            spot=float(spot),
        )
        # Prefer tip features from full bundle (same as aligned tip).
        fair = contrib["reconstructed_fair_value"]
        dev_pct = contrib["deviation_pct"]
        history = v2_fit["history"]
        regression = {
            "n": v2_fit["n"],
            "r_squared": v2_fit["r_squared"],
            "adj_r_squared": v2_fit["adj_r_squared"],
            "intercept": v2_fit["intercept"],
            "features": v2_fit["coefficients"],
            "p_values": v2_fit["p_values"],
            "t_stats": v2_fit["t_stats"],
            "oos_r2": v2_fit["oos_r2"],
            "oos_rmse": v2_fit["oos_rmse"],
            "oos_mae": v2_fit["oos_mae"],
        }
        equation = v2_fit["equation"]
        conf = v2_fit["confidence"]
        conf_reasons = [
            f"active_model={MODEL_V2}",
            f"validated_drivers={validated}",
            f"in_sample_R2={v2_fit['r_squared']}",
            f"oos_R2={v2_fit['oos_r2']}",
            f"oos_RMSE={v2_fit['oos_rmse']}",
            f"n={v2_fit['n']}",
            f"production_observation_date={freshness.get('observation_date')}",
            f"production_age_days={freshness.get('age_days')}",
        ]
        if conf == "High":
            conf_reasons.append("cleared High thresholds (OOS R², sample, features, extremes)")
        elif conf == "Medium":
            conf_reasons.append("OOS R² ≥ 0.15 and in-sample R² ≥ 0.12 with ≥1 validated feature")
        else:
            conf_reasons.append("below Medium OOS/in-sample thresholds")
    else:
        contrib = _driver_contributions(
            feature_names=VALIDATED_DRIVERS_V1,
            beta=v1_fit["beta_full"],
            latest_feats=v1_fit["latest_feats_list"],
            raw_values={"storage_surplus_bcf": raw_obs["storage_surplus_bcf"]},
            spot=float(spot),
        )
        fair = contrib["reconstructed_fair_value"]
        dev_pct = contrib["deviation_pct"]
        history = v1_fit["history"]
        regression = {
            "n": v1_fit["n"],
            "r_squared": v1_fit["r_squared"],
            "adj_r_squared": v1_fit["adj_r_squared"],
            "intercept": v1_fit["intercept"],
            "features": v1_fit["coefficients"],
            "p_values": v1_fit["p_values"],
            "t_stats": v1_fit["t_stats"],
            "oos_r2": v1_fit["oos_r2"],
            "oos_rmse": v1_fit["oos_rmse"],
            "oos_mae": v1_fit["oos_mae"],
        }
        equation = v1_fit["equation"]
        conf = v1_fit["confidence"]
        conf_reasons = [
            f"active_model={MODEL_V1} (fallback)",
            f"fallback_reason={freshness.get('reason')}",
            f"validated_drivers={validated}",
            f"in_sample_R2={v1_fit['r_squared']}",
            f"oos_R2={v1_fit['oos_r2']}",
            f"production_age_days={freshness.get('age_days')}",
            f"max_age_days={MAX_PRODUCTION_STALENESS_DAYS}",
        ]

    # Price freshness (live snapshot / completed OHLC) — gates trusted market deviation only.
    price_rec = load_instrument_record_internal(MARKET) or {}
    scale = price_rec.get("price_scale") or {}
    price_freshness = build_instrument_price_freshness(
        price_rec,
        provider=scale.get("source") or "oanda",
        symbol=scale.get("symbol") or "NATGAS_USD",
    )
    price_gate = valuation_deviation_gate(
        price_freshness,
        spot_for_model=float(spot) if spot is not None else None,
        fair_value=fair,
    )
    display_dev = price_gate.get("deviation_pct")
    bias = _bias_from_deviation(display_dev if display_dev is not None else 0.0)
    if not price_gate.get("deviation_pct_trusted"):
        bias = BIAS_UNAVAILABLE
        inst = "Market comparison unavailable — price stale"
    else:
        inst = _institutional_bias_label(display_dev, bias, conf)
    v2_fair = v2_fit["fair_value"] if v2_fit else None
    v1_fair = v1_fit["fair_value"]
    v1_v2_diff = (
        round(v2_fair - v1_fair, 4) if v2_fair is not None and v1_fair is not None else None
    )

    annotated = _annotate_cards_for_active(
        cards, active_model=active_model, contrib=contrib, fallback=fallback
    )
    # Annotate market_price card with live vs model-anchor separation.
    for i, card in enumerate(annotated):
        if card.get("id") != "market_price":
            continue
        annotated[i] = {
            **card,
            "model_anchor_price": price_gate.get("model_anchor_price"),
            "model_anchor_as_of": as_of,
            "live_quote": (price_freshness.get("live_quote") or {}),
            "latest_completed_daily": (price_freshness.get("latest_completed_daily") or {}),
            "latest_completed_weekly": (price_freshness.get("latest_completed_weekly") or {}),
            "forming_daily": price_freshness.get("forming_daily"),
            "market_comparison": (price_freshness.get("market_comparison") or {}),
            "price_status": price_freshness.get("overall_status"),
            "interpretation": (
                "Model anchor = latest completed weekly ISO close from canonical daily history. "
                "Market comparison uses the live OANDA snapshot when fresh; otherwise comparison "
                "is marked stale and deviation is not trusted."
            ),
        }

    warnings: list[str] = []
    if fallback:
        warnings.append(
            f"FALLBACK TO {MODEL_V1}: {freshness.get('reason')} "
            f"(production obs={freshness.get('observation_date')}, "
            f"age_days={freshness.get('age_days')}, max={MAX_PRODUCTION_STALENESS_DAYS})"
        )
    if freshness.get("stale") and freshness.get("usable"):
        warnings.append("Production observation near cadence limit")
    if price_gate.get("warning"):
        warnings.append(price_gate["warning"])

    summary_text = (
        f"{VALUATION_PHASE}. Active model `{active_model}`. "
        f"Fair value {fair}. "
        f"Model anchor (weekly) {spot:.4f}. "
        + (
            f"Market comparison {price_gate.get('market_comparison_price')} "
            f"({price_gate.get('market_price_status')})"
            + (
                f"; trusted deviation {display_dev:+.2f}%."
                if display_dev is not None
                else "; deviation not trusted."
            )
        )
        + f" v1 benchmark fair {v1_fair}"
        + (f"; v2 fair {v2_fair}; Δ(v2−v1)={v1_v2_diff}" if v2_fair is not None else "")
        + f". Confidence={conf}."
    )
    if fallback:
        summary_text += f" Fallback active: {freshness.get('reason')}."

    base.update(
        {
            "wired": True,
            "publish": conf != "None",
            "model_id": active_model,
            "active_model": active_model,
            "headline": VALUATION_PHASE if use_v2 else f"{VALUATION_PHASE} (v1 fallback)",
            "fair_value": fair,
            "spot_price": round(float(spot), 4),
            "model_anchor_price": round(float(spot), 4),
            "deviation_pct": display_dev,
            "deviation_pct_model_anchor": dev_pct,
            "deviation_pct_trusted": bool(price_gate.get("deviation_pct_trusted")),
            "deviation_pct_stale_untrusted": price_gate.get("deviation_pct_stale_untrusted"),
            "price_freshness": price_freshness,
            "price_comparison_gate": price_gate,
            "valuation_bias": bias,
            "valuation_state": bias if price_gate.get("deviation_pct_trusted") else "Unavailable",
            "institutional_bias": inst,
            "confidence": conf,
            "confidence_reasons": conf_reasons,
            "fallback_to_v1": fallback,
            "fallback_reason": None if use_v2 else freshness.get("reason"),
            "freshness_warnings": warnings,
            "validated_drivers": validated,
            "active_features": validated,
            "validated_features": validated,
            "experimental_features": ["lng_exports", "log_dxy", "hdd_anomaly", "cdd_anomaly"],
            "informational_features": ["working_gas_storage_level", "seasonality_factor"],
            "rejected_features": list(REJECTED_PRODUCTION_TRANSFORMS),
            "equation": equation,
            "regression": regression,
            "driver_contributions": contrib.get("driver_contributions"),
            "contribution_breakdown": contrib,
            "v1_benchmark": v1_benchmark,
            "v2_model": (
                {
                    "model_id": MODEL_V2,
                    "available": bool(v2_fit),
                    "fair_value": v2_fair,
                    "deviation_pct": v2_fit["deviation_pct"] if v2_fit else None,
                    "confidence": v2_fit["confidence"] if v2_fit else None,
                    "equation": v2_fit["equation"] if v2_fit else None,
                    "coefficients": v2_fit["coefficients"] if v2_fit else None,
                    "intercept": v2_fit["intercept"] if v2_fit else None,
                    "validated_drivers": VALIDATED_DRIVERS_V2,
                    "oos_r2": v2_fit["oos_r2"] if v2_fit else None,
                    "oos_rmse": v2_fit["oos_rmse"] if v2_fit else None,
                    "r_squared": v2_fit["r_squared"] if v2_fit else None,
                    "n": v2_fit["n"] if v2_fit else None,
                    "production_freshness": freshness,
                }
                if v2_fit
                else {
                    "model_id": MODEL_V2,
                    "available": False,
                    "production_freshness": freshness,
                }
            ),
            "v1_fair_value": v1_fair,
            "v2_fair_value": v2_fair,
            "v1_v2_fair_value_diff": v1_v2_diff,
            "production_observation_date": freshness.get("observation_date"),
            "production_yoy_value": raw_obs.get("production_yoy_pct"),
            "storage_surplus_bcf_value": raw_obs.get("storage_surplus_bcf"),
            "history": history,
            "scale": _scale_position(
                display_dev
                if display_dev is not None
                and (conf not in {"Low", "None"} or abs(display_dev or 0) < 15)
                else 0.0
            ),
            "driver_cards": annotated,
            "summary_text": summary_text,
            "model_note": (
                f"{active_model}: features={validated} R²={regression.get('r_squared')} "
                f"OOS_R²={regression.get('oos_r2')} OOS_RMSE={regression.get('oos_rmse')} "
                f"production_transformation=production_yoy_pct "
                f"raw_level_used_in_fair_value=false"
            ),
            "valuation_reason": (
                f"Active={active_model}; fair {fair}; model_anchor {spot:.4f}; "
                f"market_comparison={price_gate.get('market_comparison_price')} "
                f"status={price_gate.get('market_price_status')}; "
                f"trusted_dev={display_dev}; confidence={conf}"
                + (f"; fallback={freshness.get('reason')}" if fallback else "")
            ),
            "pass": True,
            "phase2_audit_ref": (
                "data/audits/ng_driver_validation_phase2_production/"
                "phase2_production_validation.json"
            ),
            "source_lineage": list(bundle.lineage.values()),
        }
    )
    # Avoid leaking internal fit arrays
    return base


def build_natural_gas_valuation_document(*, as_of_week: str | None = None) -> dict[str, Any]:
    block = compute_ng_storage_production_v2(as_of_week=as_of_week)
    return {
        "version": 4,
        "generated_at": block.get("generated_at"),
        "engine": block.get("active_model") or MODEL_V2,
        "valuation_phase": VALUATION_PHASE,
        "market": MARKET,
        "active_model": block.get("active_model"),
        "fallback_to_v1": block.get("fallback_to_v1"),
        "summary": {
            "wired": bool(block.get("wired")),
            "publish": bool(block.get("publish")),
            "headline": block.get("headline"),
            "active_model": block.get("active_model"),
            "fallback_to_v1": block.get("fallback_to_v1"),
            "fallback_reason": block.get("fallback_reason"),
            "validated_features": block.get("validated_features") or [],
            "validated_drivers": block.get("validated_drivers") or [],
            "experimental_features": block.get("experimental_features") or [],
            "informational_features": block.get("informational_features") or [],
            "rejected_features": block.get("rejected_features") or [],
            "production_transformation": "production_yoy_pct",
            "raw_level_used_in_fair_value": False,
            "v1_fair_value": block.get("v1_fair_value"),
            "v2_fair_value": block.get("v2_fair_value"),
            "awaiting_drivers": block.get("awaiting_drivers") or [],
            "confidence": block.get("confidence"),
            "confidence_reasons": block.get("confidence_reasons") or [],
            "freshness_warnings": block.get("freshness_warnings") or [],
        },
        "instrument": block,
    }
