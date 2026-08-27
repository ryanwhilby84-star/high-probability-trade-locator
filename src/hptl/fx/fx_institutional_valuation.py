"""FX Institutional Macro V2 — macro-driven currency valuation (SECONDARY / PARALLEL).

.. warning::
    **SECONDARY — not the dashboard valuation pillar.** Do not wire into
    ``valuation_latest.json`` or replace ``fx_carry_real_yield_v3`` on the
    main scanner valuation column.

    Used by: ``fx_valuation_export`` → ``fx_valuation_latest.json`` and
    ``FxValuationPanel`` (legacy drawer).

Estimates whether a currency is fundamentally cheap or expensive using live
macro inputs only — **not** price action, COT positioning, or seasonality.

Primary drivers (per currency)
------------------------------
1. Central bank policy rate
2. 2-year sovereign yield
3. CPI inflation (YoY %)
4. Real yield = 2Y yield − CPI

Each driver is cross-sectionally ranked across the G10 basket and combined into
a normalized **−100 … +100** currency score (+ = undervalued / attractive).

Pair valuation compares base vs quote currency scores to produce:
* model fair value (spot × (1 + gap%))
* valuation gap %
* directional bias for setup ranking / secondary panels (not pillar export)

This module is a **secondary** macro valuation layer for HPTL.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from hptl.fx.currency_rates import CurrencyRate, SUPPORTED_CURRENCIES, all_currency_rates, get_currency_rate
from hptl.fx.fx_valuation import (
    BIAS_BEARISH,
    BIAS_BULLISH,
    BIAS_NEUTRAL,
    CONF_LOW,
    CONF_MEDIUM,
    resolve_pair_currencies,
    value_condition_from_bias,
)

VALUATION_MODEL_TYPE = "FX Institutional Macro V2"

# Component weights (sum = 1.0).
_W_POLICY = 0.20
_W_YIELD = 0.30
_W_REAL = 0.35
_W_INFLATION = 0.15

# Pair gap: each score-point differential ≈ this % fair-value gap.
_GAP_PCT_PER_SCORE_POINT = 0.056

# Pair bias thresholds on score differential (base − quote).
_PAIR_BULL_THRESHOLD = 15.0
_PAIR_BEAR_THRESHOLD = -15.0

# Currency status bands.
_STATUS_EXTREME_UNDER = 75.0
_STATUS_UNDER = 25.0
_STATUS_OVER = -25.0
_STATUS_EXTREME_OVER = -75.0


@dataclass(frozen=True)
class CurrencyValuation:
    """Single-currency institutional macro score."""

    code: str
    policy_rate: float | None
    y2: float | None
    cpi_yoy: float | None
    real_yield: float | None

    policy_rate_score: float
    yield_score: float
    real_yield_score: float
    inflation_score: float

    valuation_score: float
    status: str
    missing_fields: list[str] = field(default_factory=list)
    stale_fields: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "policy_rate": self.policy_rate,
            "y2": self.y2,
            "cpi_yoy": self.cpi_yoy,
            "real_yield": self.real_yield,
            "policy_rate_score": self.policy_rate_score,
            "yield_score": self.yield_score,
            "real_yield_score": self.real_yield_score,
            "inflation_score": self.inflation_score,
            "valuation_score": self.valuation_score,
            "status": self.status,
            "missing_fields": list(self.missing_fields),
            "stale_fields": list(self.stale_fields),
        }


@dataclass(frozen=True)
class InstitutionalFxValuation:
    """Auditable pair-level institutional macro valuation."""

    pair: str
    base: str
    quote: str
    spot: float | None

    base_currency_score: float
    quote_currency_score: float
    pair_score_differential: float

    base_policy_rate: float | None
    quote_policy_rate: float | None
    policy_rate_diff: float | None

    base_2y: float | None
    quote_2y: float | None
    yield_2y_diff: float | None

    base_cpi_yoy: float | None
    quote_cpi_yoy: float | None
    base_real_yield: float | None
    quote_real_yield: float | None
    real_yield_diff: float | None

    base_10y: float | None
    quote_10y: float | None
    yield_10y_diff: float | None

    valuation_bias: str
    value_condition: str
    valuation_score: float
    confidence: str
    valuation_model_type: str

    fair_value_estimate: float | None
    spot_deviation_pct: float | None
    valuation_gap_pct: float | None
    pair_status: str

    explanation: str
    missing_fields: list[str] = field(default_factory=list)
    stale_fields: list[str] = field(default_factory=list)
    sources: dict[str, Any] = field(default_factory=dict)
    base_currency: dict[str, Any] = field(default_factory=dict)
    quote_currency: dict[str, Any] = field(default_factory=dict)

    def engine_fields(self) -> dict[str, Any]:
        return {
            "fx_policy_rate_diff": self.policy_rate_diff,
            "fx_2y_yield_diff": self.yield_2y_diff,
            "fx_10y_yield_diff": self.yield_10y_diff,
            "fx_real_yield_diff": self.real_yield_diff,
            "fx_valuation_bias": self.valuation_bias,
            "fx_valuation_condition": self.value_condition,
            "fx_valuation_score": self.valuation_score,
            "fx_valuation_confidence": self.confidence,
            "fx_valuation_model_type": self.valuation_model_type,
            "fx_fair_value_estimate": self.fair_value_estimate,
            "fx_spot_deviation_pct": self.spot_deviation_pct,
            "fx_valuation_gap_pct": self.valuation_gap_pct,
            "fx_base_currency_score": self.base_currency_score,
            "fx_quote_currency_score": self.quote_currency_score,
        }

    def as_block(self) -> dict[str, Any]:
        return {
            "pair": self.pair,
            "base": self.base,
            "quote": self.quote,
            "spot": self.spot,
            "base_currency_score": self.base_currency_score,
            "quote_currency_score": self.quote_currency_score,
            "pair_score_differential": self.pair_score_differential,
            "base_policy_rate": self.base_policy_rate,
            "quote_policy_rate": self.quote_policy_rate,
            "policy_rate_diff": self.policy_rate_diff,
            "base_2y": self.base_2y,
            "quote_2y": self.quote_2y,
            "yield_2y_diff": self.yield_2y_diff,
            "base_cpi_yoy": self.base_cpi_yoy,
            "quote_cpi_yoy": self.quote_cpi_yoy,
            "base_real_yield": self.base_real_yield,
            "quote_real_yield": self.quote_real_yield,
            "real_yield_diff": self.real_yield_diff,
            "base_10y": self.base_10y,
            "quote_10y": self.quote_10y,
            "yield_10y_diff": self.yield_10y_diff,
            "valuation_bias": self.valuation_bias,
            "value_condition": self.value_condition,
            "valuation_score": self.valuation_score,
            "confidence": self.confidence,
            "valuation_model_type": self.valuation_model_type,
            "fair_value_estimate": self.fair_value_estimate,
            "spot_deviation_pct": self.spot_deviation_pct,
            "valuation_gap_pct": self.valuation_gap_pct,
            "pair_status": self.pair_status,
            "explanation": self.explanation,
            "missing_fields": list(self.missing_fields),
            "stale_fields": list(self.stale_fields),
            "sources": dict(self.sources),
            "base_currency": dict(self.base_currency),
            "quote_currency": dict(self.quote_currency),
        }


def currency_status_label(score: float) -> str:
    if score >= _STATUS_EXTREME_UNDER:
        return "Extremely Undervalued"
    if score >= _STATUS_UNDER:
        return "Undervalued"
    if score > _STATUS_OVER:
        return "Fair Value"
    if score > _STATUS_EXTREME_OVER:
        return "Overvalued"
    return "Extremely Overvalued"


def _cross_section_scores(values: dict[str, float | None], *, invert: bool = False) -> dict[str, float]:
    """Map raw values to −100…+100 via min-max rank across available currencies."""
    valid = {k: v for k, v in values.items() if v is not None}
    if len(valid) < 2:
        return {k: 0.0 for k in values}
    lo = min(valid.values())
    hi = max(valid.values())
    out: dict[str, float] = {}
    for k, v in values.items():
        if v is None:
            out[k] = 0.0
        elif hi == lo:
            out[k] = 0.0
        else:
            rank = (v - lo) / (hi - lo)
            if invert:
                rank = 1.0 - rank
            out[k] = round(rank * 200.0 - 100.0, 1)
    return out


def _real_yield(y2: float | None, cpi: float | None) -> float | None:
    if y2 is None or cpi is None:
        return None
    return round(y2 - cpi, 3)


def score_currencies(
    rates: dict[str, CurrencyRate] | None = None,
) -> dict[str, CurrencyValuation]:
    """Score every supported currency on macro fundamentals."""
    rates = rates or all_currency_rates()

    policy_raw = {c: rates[c].policy_rate for c in SUPPORTED_CURRENCIES if c in rates}
    yield_raw = {c: rates[c].y2 for c in SUPPORTED_CURRENCIES if c in rates}
    cpi_raw = {c: rates[c].cpi_yoy for c in SUPPORTED_CURRENCIES if c in rates}
    real_raw = {
        c: _real_yield(rates[c].y2, rates[c].cpi_yoy)
        for c in SUPPORTED_CURRENCIES
        if c in rates
    }

    policy_scores = _cross_section_scores(policy_raw)
    yield_scores = _cross_section_scores(yield_raw)
    real_scores = _cross_section_scores(real_raw)
    inflation_scores = _cross_section_scores(cpi_raw, invert=True)

    out: dict[str, CurrencyValuation] = {}
    for code in SUPPORTED_CURRENCIES:
        rec = rates.get(code) or get_currency_rate(code)
        composite = round(
            _W_POLICY * policy_scores.get(code, 0.0)
            + _W_YIELD * yield_scores.get(code, 0.0)
            + _W_REAL * real_scores.get(code, 0.0)
            + _W_INFLATION * inflation_scores.get(code, 0.0),
            1,
        )
        out[code] = CurrencyValuation(
            code=code,
            policy_rate=rec.policy_rate,
            y2=rec.y2,
            cpi_yoy=rec.cpi_yoy,
            real_yield=real_raw.get(code),
            policy_rate_score=policy_scores.get(code, 0.0),
            yield_score=yield_scores.get(code, 0.0),
            real_yield_score=real_scores.get(code, 0.0),
            inflation_score=inflation_scores.get(code, 0.0),
            valuation_score=composite,
            status=currency_status_label(composite),
            missing_fields=list(rec.missing_fields),
            stale_fields=list(rec.stale_fields),
        )
    return out


def _diff(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    return round(a - b, 4)


def _pair_bias(score_diff: float) -> str:
    if score_diff >= _PAIR_BULL_THRESHOLD:
        return BIAS_BULLISH
    if score_diff <= _PAIR_BEAR_THRESHOLD:
        return BIAS_BEARISH
    return BIAS_NEUTRAL


def _pair_status(base: str, quote: str, score_diff: float, bias: str) -> str:
    if bias == BIAS_BULLISH:
        return f"{base} Undervalued vs {quote}"
    if bias == BIAS_BEARISH:
        return f"{base} Overvalued vs {quote}"
    return f"{base}/{quote} Fair Value"


def value_fx_pair_institutional(
    base: str,
    quote: str,
    *,
    spot: float | None = None,
    base_rate: CurrencyRate | None = None,
    quote_rate: CurrencyRate | None = None,
    currency_scores: dict[str, CurrencyValuation] | None = None,
    config_path: str | None = None,
) -> InstitutionalFxValuation:
    """Compute institutional macro valuation for ``base/quote``."""
    base = base.upper()
    quote = quote.upper()
    pair = f"{base}/{quote}"

    br = base_rate or get_currency_rate(base, config_path=config_path)
    qr = quote_rate or get_currency_rate(quote, config_path=config_path)

    if currency_scores is None:
        rates = {base: br, quote: qr}
        for c in SUPPORTED_CURRENCIES:
            if c not in rates:
                rates[c] = get_currency_rate(c, config_path=config_path)
        currency_scores = score_currencies(rates)

    base_cv = currency_scores[base]
    quote_cv = currency_scores[quote]
    score_diff = round(base_cv.valuation_score - quote_cv.valuation_score, 1)

    policy_diff = _diff(br.policy_rate, qr.policy_rate)
    y2_diff = _diff(br.y2, qr.y2)
    y10_diff = _diff(br.y10, qr.y10)
    real_diff = _diff(base_cv.real_yield, quote_cv.real_yield)

    missing: list[str] = []
    for leg, rec in ((base, br), (quote, qr)):
        for fld in rec.missing_fields:
            missing.append(f"{leg}.{fld}")
    stale = [f"{base}.{f}" for f in br.stale_fields] + [f"{quote}.{f}" for f in qr.stale_fields]

    # Confidence: macro completeness — CPI staleness ignored (annual series).
    def _critical_stale(rec: CurrencyRate) -> list[str]:
        return [f for f in rec.stale_fields if f in {"policy_rate", "y2", "y10"}]

    have_core = br.has_2y and qr.has_2y and br.cpi_yoy is not None and qr.cpi_yoy is not None
    stale_critical = _critical_stale(br) or _critical_stale(qr)
    data_unreliable = not have_core or bool(stale_critical)
    confidence = CONF_LOW
    if have_core and not stale:
        confidence = CONF_MEDIUM

    if data_unreliable or missing:
        bias = BIAS_NEUTRAL
        gap_pct = None
        fair_value = None
        spot_dev = None
        pair_score = 0.0
    else:
        bias = _pair_bias(score_diff)
        gap_pct = round(score_diff * _GAP_PCT_PER_SCORE_POINT, 2)
        fair_value = round(spot * (1.0 + gap_pct / 100.0), 6) if spot and gap_pct is not None else None
        spot_dev = (
            round((spot - fair_value) / fair_value * 100.0, 2)
            if spot and fair_value
            else None
        )
        pair_score = score_diff

    explanation = _build_explanation(
        base=base,
        quote=quote,
        base_cv=base_cv,
        quote_cv=quote_cv,
        score_diff=score_diff,
        gap_pct=gap_pct,
        bias=bias,
        confidence=confidence,
        missing=missing,
        stale=stale,
    )

    return InstitutionalFxValuation(
        pair=pair,
        base=base,
        quote=quote,
        spot=spot,
        base_currency_score=base_cv.valuation_score,
        quote_currency_score=quote_cv.valuation_score,
        pair_score_differential=score_diff,
        base_policy_rate=br.policy_rate,
        quote_policy_rate=qr.policy_rate,
        policy_rate_diff=policy_diff,
        base_2y=br.y2,
        quote_2y=qr.y2,
        yield_2y_diff=y2_diff,
        base_cpi_yoy=br.cpi_yoy,
        quote_cpi_yoy=qr.cpi_yoy,
        base_real_yield=base_cv.real_yield,
        quote_real_yield=quote_cv.real_yield,
        real_yield_diff=real_diff,
        base_10y=br.y10,
        quote_10y=qr.y10,
        yield_10y_diff=y10_diff,
        valuation_bias=bias,
        value_condition=value_condition_from_bias(bias),
        valuation_score=pair_score,
        confidence=confidence,
        valuation_model_type=VALUATION_MODEL_TYPE,
        fair_value_estimate=fair_value,
        spot_deviation_pct=spot_dev,
        valuation_gap_pct=gap_pct,
        pair_status=_pair_status(base, quote, score_diff, bias),
        explanation=explanation,
        missing_fields=missing,
        stale_fields=stale,
        sources={"base": br.as_dict(), "quote": qr.as_dict()},
        base_currency=base_cv.as_dict(),
        quote_currency=quote_cv.as_dict(),
    )


def value_fx_market_institutional(
    market_id: str,
    *,
    spot: float | None = None,
    config_path: str | None = None,
) -> InstitutionalFxValuation | None:
    resolved = resolve_pair_currencies(market_id)
    if not resolved:
        return None
    base, quote, _pair = resolved
    return value_fx_pair_institutional(base, quote, spot=spot, config_path=config_path)


def _build_explanation(
    *,
    base: str,
    quote: str,
    base_cv: CurrencyValuation,
    quote_cv: CurrencyValuation,
    score_diff: float,
    gap_pct: float | None,
    bias: str,
    confidence: str,
    missing: list[str],
    stale: list[str],
) -> str:
    if missing:
        return (
            f"{base}/{quote} institutional valuation held at Fair Value (Low confidence) — "
            f"missing macro inputs: {', '.join(missing)}."
        )
    if stale:
        return (
            f"{base}/{quote} institutional valuation held at Fair Value (Low confidence) — "
            f"stale macro data: {', '.join(stale)}."
        )

    base_clause = (
        f"{base} macro score {base_cv.valuation_score:+.0f} ({base_cv.status}); "
        f"{quote} {quote_cv.valuation_score:+.0f} ({quote_cv.status})"
    )
    gap_clause = ""
    if gap_pct is not None:
        gap_clause = (
            f" Model fair-value gap {gap_pct:+.2f}% "
            f"(real yield {base}: {base_cv.real_yield}, {quote}: {quote_cv.real_yield})."
        )

    bias_clause = {
        BIAS_BULLISH: f"{base} appears undervalued vs {quote} on macro fundamentals",
        BIAS_BEARISH: f"{base} appears overvalued vs {quote} on macro fundamentals",
        BIAS_NEUTRAL: f"{base}/{quote} appears fairly valued on macro fundamentals",
    }[bias]

    conf_clause = (
        "Confidence Medium — policy rate, 2Y yield, and CPI available on both legs."
        if confidence == CONF_MEDIUM
        else "Confidence Low — incomplete or stale macro inputs."
    )
    return f"{base_clause}. {bias_clause}.{gap_clause} {conf_clause}"


# Re-export for convenience
__all__ = [
    "CurrencyValuation",
    "InstitutionalFxValuation",
    "VALUATION_MODEL_TYPE",
    "currency_status_label",
    "score_currencies",
    "value_fx_pair_institutional",
    "value_fx_market_institutional",
    "value_condition_from_bias",
]
