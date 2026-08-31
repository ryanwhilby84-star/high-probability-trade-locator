"""Soybean dual-anchor fundamental valuation research model.

V1 deliberately keeps COT/seasonality out of fair value.  It combines:
1) nonlinear scarcity value from USDA stocks-to-use; and
2) board-crush economics from soybean meal/oil prices.

Inputs are explicit and timestampable so historical runs can remain point-in-time safe.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
import math
from typing import Any, Iterable

MODEL_ID = "soybean_dual_anchor_v1"


def _finite(v: Any) -> float | None:
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def stocks_to_use(ending_stocks: float, total_use: float) -> float:
    if total_use <= 0:
        raise ValueError("total_use must be positive")
    return ending_stocks / total_use


def gross_crush_value(meal_usd_per_short_ton: float, oil_cents_per_lb: float) -> float:
    """Approximate product value per 60-lb soybean bushel.

    Standard physical yields: 44 lb meal + 11 lb oil.  CBOT soybean oil is
    conventionally quoted in cents/lb, hence /100 below.
    """
    meal = (meal_usd_per_short_ton / 2000.0) * 44.0
    oil = (oil_cents_per_lb / 100.0) * 11.0
    return meal + oil


def crush_implied_bean_value(
    meal_usd_per_short_ton: float,
    oil_cents_per_lb: float,
    required_crush_margin_usd_per_bushel: float = 1.50,
) -> float:
    return gross_crush_value(meal_usd_per_short_ton, oil_cents_per_lb) - required_crush_margin_usd_per_bushel


def fit_inverse_stu(history: Iterable[tuple[float, float]]) -> dict[str, float | int | None]:
    """OLS: soybean $/bu = alpha + beta * (1 / stocks-to-use).

    history entries are (stocks_to_use_fraction, soybean_usd_per_bushel).
    """
    rows = [(float(s), float(p)) for s, p in history if s and s > 0 and p and p > 0]
    if len(rows) < 8:
        return {"alpha": None, "beta": None, "r2": None, "n": len(rows)}
    xs = [1.0 / s for s, _ in rows]
    ys = [p for _, p in rows]
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0:
        return {"alpha": None, "beta": None, "r2": None, "n": len(rows)}
    beta = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den
    alpha = my - beta * mx
    fitted = [alpha + beta * x for x in xs]
    ss_res = sum((y - f) ** 2 for y, f in zip(ys, fitted))
    ss_tot = sum((y - my) ** 2 for y in ys)
    r2 = 1.0 - ss_res / ss_tot if ss_tot else None
    return {"alpha": alpha, "beta": beta, "r2": r2, "n": len(rows)}


def scarcity_fair_value(current_stu: float, fit: dict[str, Any]) -> float | None:
    alpha, beta = _finite(fit.get("alpha")), _finite(fit.get("beta"))
    if alpha is None or beta is None or current_stu <= 0:
        return None
    return alpha + beta * (1.0 / current_stu)


def _state(deviation_pct: float | None) -> str:
    if deviation_pct is None:
        return "UNAVAILABLE"
    if deviation_pct <= -10:
        return "Deeply Undervalued"
    if deviation_pct <= -5:
        return "Undervalued"
    if deviation_pct >= 10:
        return "Deeply Overvalued"
    if deviation_pct >= 5:
        return "Overvalued"
    return "Fair Value"


@dataclass(frozen=True)
class SoybeanInputs:
    as_of: str
    market_price_usd_per_bushel: float
    ending_stocks: float
    total_use: float
    meal_usd_per_short_ton: float | None = None
    oil_cents_per_lb: float | None = None
    required_crush_margin_usd_per_bushel: float = 1.50
    scarcity_weight: float = 0.65
    crush_weight: float = 0.35


def compute(inputs: SoybeanInputs, scarcity_fit: dict[str, Any]) -> dict[str, Any]:
    stu = stocks_to_use(inputs.ending_stocks, inputs.total_use)
    scarcity = scarcity_fair_value(stu, scarcity_fit)
    crush = None
    if inputs.meal_usd_per_short_ton is not None and inputs.oil_cents_per_lb is not None:
        crush = crush_implied_bean_value(
            inputs.meal_usd_per_short_ton,
            inputs.oil_cents_per_lb,
            inputs.required_crush_margin_usd_per_bushel,
        )

    anchors = []
    if scarcity is not None:
        anchors.append((scarcity, max(0.0, inputs.scarcity_weight)))
    if crush is not None and crush > 0:
        anchors.append((crush, max(0.0, inputs.crush_weight)))
    weight_sum = sum(w for _, w in anchors)
    fair = sum(v * w for v, w in anchors) / weight_sum if weight_sum > 0 else None
    dev = ((inputs.market_price_usd_per_bushel / fair) - 1.0) * 100.0 if fair and fair > 0 else None

    return {
        "market": "Soybeans",
        "model_id": MODEL_ID,
        "as_of": inputs.as_of,
        "market_price": inputs.market_price_usd_per_bushel,
        "stocks_to_use": stu,
        "scarcity_fair_value": scarcity,
        "crush_implied_value": crush,
        "fair_value": fair,
        "deviation_pct": dev,
        "valuation_state": _state(dev),
        "scarcity_fit": scarcity_fit,
        "inputs": asdict(inputs),
        "drivers": {
            "scarcity": {"value": scarcity, "weight": inputs.scarcity_weight},
            "crush": {"value": crush, "weight": inputs.crush_weight},
        },
    }


def historical_snapshots(rows: Iterable[dict[str, Any]], scarcity_fit: dict[str, Any]) -> list[dict[str, Any]]:
    """Build a chartable point-in-time valuation-vs-price series.

    Each row must contain values known on its `as_of` date.  No forward fill is
    performed here: ingestion owns release-date semantics.
    """
    out: list[dict[str, Any]] = []
    for row in rows:
        try:
            inp = SoybeanInputs(
                as_of=str(row["as_of"]),
                market_price_usd_per_bushel=float(row["market_price_usd_per_bushel"]),
                ending_stocks=float(row["ending_stocks"]),
                total_use=float(row["total_use"]),
                meal_usd_per_short_ton=_finite(row.get("meal_usd_per_short_ton")),
                oil_cents_per_lb=_finite(row.get("oil_cents_per_lb")),
                required_crush_margin_usd_per_bushel=float(row.get("required_crush_margin_usd_per_bushel", 1.50)),
            )
            out.append(compute(inp, scarcity_fit))
        except (KeyError, TypeError, ValueError):
            continue
    return out
