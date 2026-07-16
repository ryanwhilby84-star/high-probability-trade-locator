"""Natural Gas Institutional Valuation V1.

Reuses metals OLS helpers (log-price multivariate regression). Missing EIA drivers
do not block — the model fits on whichever aligned features are available.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.valuation.energy_ng_drivers import MARKET, NgDriverBundle, build_ng_driver_bundle
from hptl.valuation.engine import BIAS_UNAVAILABLE
from hptl.valuation.metals_valuation_v1 import (
    MIN_WEEKS,
    _bias_from_deviation,
    _multivariate_ols,
    _predict_log_price,
)

MODEL_ID = "energy_natural_gas_v1"
VALUATION_PHASE = "Energy NG V1"
CONFIG_PATH = PROJECT_ROOT / "data" / "config" / "energy_ng_valuation_sources.json"

# Prefer this feature order when present
FEATURE_PRIORITY = (
    "storage_surplus_bcf",
    "dry_gas_production",
    "lng_exports",
    "hdd_anomaly",
    "cdd_anomaly",
    "log_dxy",
    "seasonality_factor",
)


def _load_sign_expectations() -> dict[str, str]:
    if not CONFIG_PATH.exists():
        return {}
    try:
        return (json.loads(CONFIG_PATH.read_text(encoding="utf-8")).get("sign_expectations") or {})
    except Exception:
        return {}


def _confidence(r2: float | None, n: int, n_features: int, missing_required_like: int) -> str:
    if r2 is not None and r2 >= 0.25 and n >= 156 and n_features >= 3 and missing_required_like == 0:
        return "High"
    if r2 is not None and r2 >= 0.12 and n >= MIN_WEEKS and n_features >= 2:
        return "Medium"
    if n >= MIN_WEEKS and n_features >= 1:
        return "Low"
    return "None"


def _institutional_bias_label(dev_pct: float | None, bias: str) -> str:
    if bias == BIAS_UNAVAILABLE or dev_pct is None:
        return "Unavailable"
    if abs(dev_pct) < 5:
        return "Neutral"
    if dev_pct <= -15:
        return "Strongly Bullish"
    if dev_pct <= -5:
        return "Bullish"
    if dev_pct >= 15:
        return "Strongly Bearish"
    return "Bearish"


def _scale_position(dev_pct: float | None) -> dict[str, Any]:
    """Map deviation onto a 0–100 institutional scale bar."""
    if dev_pct is None or not math.isfinite(dev_pct):
        return {"pct": 50.0, "band": "Fair Value"}
    # Clamp to ±30% for display
    clamped = max(-30.0, min(30.0, float(dev_pct)))
    pct = 50.0 + (clamped / 30.0) * 50.0
    if clamped <= -15:
        band = "Strongly Undervalued"
    elif clamped <= -5:
        band = "Moderately Undervalued"
    elif clamped < 5:
        band = "Fair Value"
    elif clamped < 15:
        band = "Moderately Overvalued"
    else:
        band = "Strongly Overvalued"
    return {"pct": round(pct, 1), "band": band, "deviation_pct": round(clamped, 2)}


def _build_summary(
    *,
    market: str,
    dev_pct: float | None,
    bias: str,
    cards: dict[str, dict[str, Any]],
) -> str:
    if dev_pct is None:
        return (
            f"{market} institutional fair value is not yet publishable. "
            "Driver interfaces are live; awaiting sufficient aligned fundamentals."
        )

    direction = "undervalued" if dev_pct < 0 else "overvalued" if dev_pct > 0 else "near fair value"
    lines = [
        f"Natural Gas currently appears approximately {abs(dev_pct):.1f}% {direction}."
    ]

    scored: list[tuple[str, str, str]] = []
    for card in cards.values():
        if card.get("id") == "market_price":
            continue
        if not card.get("available"):
            continue
        effect = str(card.get("institutional_effect") or "")
        if effect in {"Bullish", "Bearish"}:
            scored.append((card.get("label") or card.get("id"), effect, card.get("interpretation") or ""))

    bullish = [s for s in scored if s[1] == "Bullish"]
    bearish = [s for s in scored if s[1] == "Bearish"]
    if bullish:
        lines.append(f"The strongest bullish driver is {bullish[0][0].lower()}.")
    if bearish:
        lines.append(f"The strongest bearish driver is {bearish[0][0].lower()}.")
    for label, effect, _ in scored:
        if "LNG" in label and effect == "Bullish":
            lines.append("LNG exports remain supportive.")
            break
    inst = _institutional_bias_label(dev_pct, bias)
    lines.append(f"Overall institutional bias remains {inst.replace('Strongly ', '')}.")
    return " ".join(lines)


def _fit(bundle: NgDriverBundle) -> dict[str, Any] | None:
    names = [f for f in FEATURE_PRIORITY if f in bundle.features and len(bundle.features[f]) == bundle.n]
    if not names or bundle.n < MIN_WEEKS:
        return None

    y = [math.log(p) for p in bundle.price]
    x_cols = [bundle.features[n] for n in names]
    beta, r2 = _multivariate_ols(y, x_cols)
    if not beta or r2 is None:
        return None

    series: list[dict[str, Any]] = []
    for i in range(bundle.n):
        feats_i = [col[i] for col in x_cols]
        lp = _predict_log_price(beta, feats_i)
        if lp is None:
            continue
        fair = math.exp(lp)
        if fair <= 0:
            continue
        spot = bundle.price[i]
        series.append(
            {
                "date": bundle.dates[i],
                "spot_price": round(spot, 4),
                "fair_value": round(fair, 4),
                "deviation_pct": round(100.0 * (spot - fair) / fair, 2),
            }
        )

    latest_feats = [col[-1] for col in x_cols]
    log_fair = _predict_log_price(beta, latest_feats)
    if log_fair is None:
        return None
    fair = math.exp(log_fair)
    spot = bundle.price[-1]
    dev_pct = round(100.0 * (spot - fair) / fair, 2) if fair > 0 else None

    return {
        "feature_names": names,
        "n_obs": bundle.n,
        "r_squared": round(r2, 4),
        "intercept": round(beta[0], 6),
        "beta": {name: round(beta[i + 1], 6) for i, name in enumerate(names)},
        "fair_value": round(fair, 4),
        "spot_price": round(spot, 4),
        "deviation_pct": dev_pct,
        "as_of_date": bundle.as_of,
        "history": series,
        "drivers_snapshot": {name: round(latest_feats[i], 4) for i, name in enumerate(names)},
        "sign_expectations": {k: v for k, v in _load_sign_expectations().items() if k in names},
    }


def compute_natural_gas_valuation(*, as_of_week: str | None = None) -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    bundle = build_ng_driver_bundle(as_of_week=as_of_week)
    cards = bundle.driver_cards
    awaiting = [c["label"] for c in cards.values() if not c.get("available")]

    base: dict[str, Any] = {
        "market": MARKET,
        "model_id": MODEL_ID,
        "valuation_phase": VALUATION_PHASE,
        "valuation_pillar": "energy_natural_gas",
        "generated_at": generated_at,
        "wired": False,
        "publish": False,
        "fair_value": None,
        "spot_price": bundle.price[-1] if bundle.price else None,
        "deviation_pct": None,
        "valuation_bias": BIAS_UNAVAILABLE,
        "valuation_state": BIAS_UNAVAILABLE,
        "institutional_bias": "Unavailable",
        "confidence": "None",
        "as_of_week": bundle.as_of or as_of_week,
        "driver_cards": list(cards.values()),
        "history": [],
        "awaiting_drivers": awaiting,
        "summary_text": "",
        "scale": _scale_position(None),
    }

    if not bundle.price:
        base["valuation_reason"] = "Natural Gas canonical price history unavailable."
        base["summary_text"] = base["valuation_reason"]
        return base

    fit = _fit(bundle)
    if not fit:
        base["valuation_reason"] = (
            "Insufficient aligned drivers for OLS fit — showing price and driver interfaces."
        )
        base["summary_text"] = _build_summary(
            market=MARKET, dev_pct=None, bias=BIAS_UNAVAILABLE, cards=cards
        )
        base["drivers"] = {k: cards[k] for k in cards}
        return base

    bias = _bias_from_deviation(fit["deviation_pct"])
    conf = _confidence(fit["r_squared"], fit["n_obs"], len(fit["feature_names"]), len(awaiting))
    inst = _institutional_bias_label(fit["deviation_pct"], bias)
    scale = _scale_position(fit["deviation_pct"])

    base.update(
        {
            "wired": True,
            "publish": True,
            "fair_value": fit["fair_value"],
            "spot_price": fit["spot_price"],
            "deviation_pct": fit["deviation_pct"],
            "valuation_bias": bias,
            "valuation_state": bias,
            "institutional_bias": inst,
            "confidence": conf,
            "regression": {
                "n": fit["n_obs"],
                "r_squared": fit["r_squared"],
                "intercept": fit["intercept"],
                "features": fit["beta"],
            },
            "active_features": fit["feature_names"],
            "sign_expectations": fit["sign_expectations"],
            "drivers_snapshot": fit["drivers_snapshot"],
            "history": fit["history"],
            "scale": scale,
            "model_note": (
                f"{MODEL_ID}: log(price) ~ {', '.join(fit['feature_names'])} "
                f"(R²={fit['r_squared']}, n={fit['n_obs']})"
            ),
            "valuation_reason": (
                f"{MODEL_ID}: spot {fit['spot_price']} vs fair {fit['fair_value']} "
                f"({fit['deviation_pct']:+.2f}%)"
            ),
            "pass": True,
            "source_lineage": list(bundle.lineage.values()),
        }
    )
    base["summary_text"] = _build_summary(
        market=MARKET, dev_pct=fit["deviation_pct"], bias=bias, cards=cards
    )
    return base


def build_natural_gas_valuation_document(*, as_of_week: str | None = None) -> dict[str, Any]:
    block = compute_natural_gas_valuation(as_of_week=as_of_week)
    return {
        "version": 1,
        "generated_at": block.get("generated_at"),
        "engine": MODEL_ID,
        "valuation_phase": VALUATION_PHASE,
        "market": MARKET,
        "summary": {
            "wired": bool(block.get("wired")),
            "publish": bool(block.get("publish")),
            "active_features": block.get("active_features") or [],
            "awaiting_drivers": block.get("awaiting_drivers") or [],
        },
        "instrument": block,
    }
