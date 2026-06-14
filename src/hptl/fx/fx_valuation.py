"""FX Valuation V1 — yield/rate-differential valuation (LEGACY).

.. warning::
    **LEGACY — not the dashboard valuation pillar.** Do not use for
    ``valuation_latest.json``, scanner ValuationCell, or thesis pillar wiring.
    The canonical FX engine is ``hptl.valuation.fx_carry_real_yield_v3``.

    This module remains for confluence row attach (``fx_valuation_attach``)
    and historical yield-diff charts only.

Answers: "Is this FX pair cheap, fair, or expensive relative to yield/rate support?"

This is *not* a black-box signal. Every output carries the raw differentials, the
confidence rationale, and a plain-English explanation so it can be audited. Fair
value is intentionally left null until a historical regression exists
(see ``fx_fair_value``); we do not fabricate precision.

Design notes
------------
* Primary driver = 2Y government yield differential (most FX-relevant front-end
  rate expectation). Policy-rate differential is the confirming signal.
* 10Y differential is reported when available but does not drive V1 scoring.
* Confidence is about *data quality / agreement*, bias is about *direction*.
* Data safety: when required data is missing or stale, we return Neutral / Low
  and explain why — we never force a bullish/bearish answer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from hptl.fx.currency_map import parse_fx_pair, COT_CURRENCY_SOURCES
from hptl.fx.currency_rates import CurrencyRate, get_currency_rate, SUPPORTED_CURRENCIES

VALUATION_MODEL_TYPE = "FX Yield Differential V1"

# Internal directional bias (kept for scoring / pillar pass-fail; NOT the
# user-facing valuation label). The display layer uses the value condition below.
BIAS_BULLISH = "Bullish"
BIAS_NEUTRAL = "Neutral"
BIAS_BEARISH = "Bearish"

# User-facing valuation condition — describes *value* (cheap/fair/expensive),
# not trade direction. This is the label shown on the dashboard / thesis tracker.
VALUE_UNDERVALUED = "Undervalued"
VALUE_FAIR = "Fair Value"
VALUE_OVERVALUED = "Overvalued"

_BIAS_TO_CONDITION = {
    BIAS_BULLISH: VALUE_UNDERVALUED,
    BIAS_NEUTRAL: VALUE_FAIR,
    BIAS_BEARISH: VALUE_OVERVALUED,
}


def value_condition_from_bias(bias: str | None) -> str:
    """Map internal directional bias -> value condition (cheap/fair/expensive).

    A positive (bullish) yield-support read means the pair is *undervalued*
    relative to that support; a negative (bearish) read means *overvalued*.
    """
    return _BIAS_TO_CONDITION.get(str(bias or ""), VALUE_FAIR)


CONF_LOW = "Low"
CONF_MEDIUM = "Medium"
CONF_HIGH = "High"

# Scoring constants (transparent + auditable).
_SCORE_MIDPOINT = 5.0
_SCORE_PER_PCT = 2.0          # each +1.00pp of 2Y differential -> +2.0 score points
_SCORE_CLAMP = 4.0            # max push away from neutral midpoint
_DEADZONE_PP = 0.15           # |diff| below this is treated as "flat"
_BIAS_BULL_THRESHOLD = 6.5    # score >= -> Bullish
_BIAS_BEAR_THRESHOLD = 3.5    # score <= -> Bearish
_EXTENDED_PCTL = 80.0         # 52w price percentile considered "extended up"
_CHEAP_PCTL = 20.0            # 52w price percentile considered "extended down"
_EXTENSION_DAMPEN = 1.0       # max score dampening from price extension overlay


@dataclass(frozen=True)
class FxValuation:
    """Auditable FX valuation result for a single pair."""

    pair: str
    base: str
    quote: str
    spot: float | None

    base_policy_rate: float | None
    quote_policy_rate: float | None
    policy_rate_diff: float | None

    base_2y: float | None
    quote_2y: float | None
    yield_2y_diff: float | None

    base_10y: float | None
    quote_10y: float | None
    yield_10y_diff: float | None

    valuation_bias: str          # internal directional read (scoring only)
    value_condition: str         # user-facing: Undervalued / Fair Value / Overvalued
    valuation_score: float
    confidence: str
    valuation_model_type: str

    fair_value_estimate: float | None
    spot_deviation_pct: float | None

    explanation: str
    missing_fields: list[str] = field(default_factory=list)
    stale_fields: list[str] = field(default_factory=list)
    sources: dict[str, Any] = field(default_factory=dict)

    def engine_fields(self) -> dict[str, Any]:
        """Flat scalars exposed to the scoring engine + dashboard records."""
        return {
            "fx_policy_rate_diff": self.policy_rate_diff,
            "fx_2y_yield_diff": self.yield_2y_diff,
            "fx_10y_yield_diff": self.yield_10y_diff,
            "fx_valuation_bias": self.valuation_bias,
            "fx_valuation_condition": self.value_condition,
            "fx_valuation_score": self.valuation_score,
            "fx_valuation_confidence": self.confidence,
            "fx_valuation_model_type": self.valuation_model_type,
            "fx_fair_value_estimate": self.fair_value_estimate,
            "fx_spot_deviation_pct": self.spot_deviation_pct,
        }

    def as_block(self) -> dict[str, Any]:
        """Full nested audit block for the instrument view."""
        return {
            "pair": self.pair,
            "base": self.base,
            "quote": self.quote,
            "spot": self.spot,
            "base_policy_rate": self.base_policy_rate,
            "quote_policy_rate": self.quote_policy_rate,
            "policy_rate_diff": self.policy_rate_diff,
            "base_2y": self.base_2y,
            "quote_2y": self.quote_2y,
            "yield_2y_diff": self.yield_2y_diff,
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
            "explanation": self.explanation,
            "missing_fields": list(self.missing_fields),
            "stale_fields": list(self.stale_fields),
            "sources": dict(self.sources),
        }


def resolve_pair_currencies(market_id: str) -> tuple[str, str, str] | None:
    """Resolve a confluence market id to ``(base, quote, canonical_pair_id)``.

    Handles both FX cross ids (``"GBP/NZD"``) and legacy COT major market names
    (``"Euro FX / 6E"`` -> ``EUR/USD``). Returns ``None`` if not a supported pair.
    """
    legs = parse_fx_pair(market_id)
    if legs and legs.base in SUPPORTED_CURRENCIES and legs.quote in SUPPORTED_CURRENCIES:
        return legs.base, legs.quote, f"{legs.base}/{legs.quote}"

    for code, spec in COT_CURRENCY_SOURCES.items():
        if str(spec.get("market")) == market_id:
            quote_pair = str(spec.get("quote") or "")
            qlegs = parse_fx_pair(quote_pair)
            if qlegs and qlegs.base in SUPPORTED_CURRENCIES and qlegs.quote in SUPPORTED_CURRENCIES:
                return qlegs.base, qlegs.quote, f"{qlegs.base}/{qlegs.quote}"
    return None


def _diff(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    return round(a - b, 4)


def _round_score(v: float) -> float:
    return round(min(10.0, max(0.0, v)), 1)


def _sign(v: float | None) -> int:
    if v is None or abs(v) < _DEADZONE_PP:
        return 0
    return 1 if v > 0 else -1


def value_fx_pair(
    base: str,
    quote: str,
    *,
    spot: float | None = None,
    price_percentile_52w: float | None = None,
    base_rate: CurrencyRate | None = None,
    quote_rate: CurrencyRate | None = None,
    config_path: str | None = None,
) -> FxValuation:
    """Compute a yield-differential valuation for ``base/quote``.

    ``price_percentile_52w`` (0-100) is an optional extension overlay: a strong
    bullish yield read into an already-extended price is dampened (not flipped).
    """
    base = base.upper()
    quote = quote.upper()
    pair = f"{base}/{quote}"

    br = base_rate or get_currency_rate(base, config_path=config_path)
    qr = quote_rate or get_currency_rate(quote, config_path=config_path)

    policy_diff = _diff(br.policy_rate, qr.policy_rate)
    y2_diff = _diff(br.y2, qr.y2)
    y10_diff = _diff(br.y10, qr.y10)

    missing: list[str] = []
    for leg, rec in ((base, br), (quote, qr)):
        for fld in rec.missing_fields:
            missing.append(f"{leg}.{fld}")
    stale = [f"{base}.{f}" for f in br.stale_fields] + [f"{quote}.{f}" for f in qr.stale_fields]

    sources = {
        "base": br.as_dict(),
        "quote": qr.as_dict(),
        "primary_driver": "2y_yield_differential",
    }

    # --- Confidence (data availability + agreement) -------------------------
    have_2y = br.has_2y and qr.has_2y
    have_policy = br.has_policy and qr.has_policy
    have_2y_or_policy_stale = bool(br.stale_fields or qr.stale_fields)

    # Data safety: missing required data, stale data, or no spot => Low + Neutral.
    data_unreliable = (not have_2y) or have_2y_or_policy_stale
    confidence = CONF_LOW

    if have_policy and have_2y and not have_2y_or_policy_stale:
        if _sign(policy_diff) == _sign(y2_diff) or _sign(y2_diff) == 0:
            confidence = CONF_MEDIUM
        else:
            confidence = CONF_LOW  # mixed signals
    # HIGH intentionally unreachable in V1 (requires validated regression).

    # --- Bias + score -------------------------------------------------------
    if confidence == CONF_LOW or data_unreliable:
        bias = BIAS_NEUTRAL
        score = _SCORE_MIDPOINT
    else:
        push = max(-_SCORE_CLAMP, min(_SCORE_CLAMP, (y2_diff or 0.0) * _SCORE_PER_PCT))
        score = _SCORE_MIDPOINT + push

        # Extension overlay (optional, does not flip direction).
        if price_percentile_52w is not None:
            if push > 0 and price_percentile_52w >= _EXTENDED_PCTL:
                score -= min(_EXTENSION_DAMPEN, push)
            elif push < 0 and price_percentile_52w <= _CHEAP_PCTL:
                score += min(_EXTENSION_DAMPEN, -push)

        score = _round_score(score)
        if score >= _BIAS_BULL_THRESHOLD:
            bias = BIAS_BULLISH
        elif score <= _BIAS_BEAR_THRESHOLD:
            bias = BIAS_BEARISH
        else:
            bias = BIAS_NEUTRAL

    explanation = _build_explanation(
        base=base,
        quote=quote,
        y2_diff=y2_diff,
        policy_diff=policy_diff,
        bias=bias,
        confidence=confidence,
        missing=missing,
        stale=stale,
        price_percentile_52w=price_percentile_52w,
    )

    return FxValuation(
        pair=pair,
        base=base,
        quote=quote,
        spot=spot,
        base_policy_rate=br.policy_rate,
        quote_policy_rate=qr.policy_rate,
        policy_rate_diff=policy_diff,
        base_2y=br.y2,
        quote_2y=qr.y2,
        yield_2y_diff=y2_diff,
        base_10y=br.y10,
        quote_10y=qr.y10,
        yield_10y_diff=y10_diff,
        valuation_bias=bias,
        value_condition=value_condition_from_bias(bias),
        valuation_score=score,
        confidence=confidence,
        valuation_model_type=VALUATION_MODEL_TYPE,
        fair_value_estimate=None,   # TODO(fx_fair_value): set once regression validated
        spot_deviation_pct=None,    # TODO(fx_fair_value): requires fair_value_estimate
        explanation=explanation,
        missing_fields=missing,
        stale_fields=stale,
        sources=sources,
    )


def value_fx_market(
    market_id: str,
    *,
    spot: float | None = None,
    price_percentile_52w: float | None = None,
    config_path: str | None = None,
) -> FxValuation | None:
    """Resolve a confluence market id and value it, or ``None`` if unsupported."""
    resolved = resolve_pair_currencies(market_id)
    if not resolved:
        return None
    base, quote, _pair = resolved
    return value_fx_pair(
        base,
        quote,
        spot=spot,
        price_percentile_52w=price_percentile_52w,
        config_path=config_path,
    )


def _fmt_pp(v: float | None) -> str:
    if v is None:
        return "n/a"
    return f"{v:+.2f}pp"


def _build_explanation(
    *,
    base: str,
    quote: str,
    y2_diff: float | None,
    policy_diff: float | None,
    bias: str,
    confidence: str,
    missing: list[str],
    stale: list[str],
    price_percentile_52w: float | None,
) -> str:
    if missing:
        return (
            f"{base}/{quote} valuation is held at Fair Value with Low confidence because required "
            f"rate data is missing ({', '.join(missing)}). No value read is taken without yield "
            "support on both legs."
        )
    if stale:
        return (
            f"{base}/{quote} valuation is held at Fair Value with Low confidence because rate data "
            f"is stale ({', '.join(stale)}). Refresh the currency rate config before relying on this read."
        )

    if y2_diff is None:
        return (
            f"{base}/{quote}: only policy-rate data is available (no 2Y yields), so confidence is "
            "Low and valuation is held at Fair Value pending front-end yield data."
        )

    # Who has yield support.
    if abs(y2_diff) < _DEADZONE_PP:
        support_clause = (
            f"the 2Y yield differential is essentially flat ({_fmt_pp(y2_diff)}), so neither leg has "
            "a clear yield advantage"
        )
    elif y2_diff > 0:
        support_clause = (
            f"{base} has a positive 2Y yield advantage over {quote} ({_fmt_pp(y2_diff)}), giving "
            f"{base}/{quote} yield support"
        )
    else:
        support_clause = (
            f"{quote} has the 2Y yield advantage ({_fmt_pp(y2_diff)} for {base} less {quote}), so yield "
            f"support sits with the quote currency"
        )

    bias_clause = {
        BIAS_BULLISH: f"{base}/{quote} appears undervalued (cheap) relative to this yield support",
        BIAS_BEARISH: f"{base}/{quote} appears overvalued (expensive) relative to this yield support",
        BIAS_NEUTRAL: f"{base}/{quote} appears fairly valued relative to this yield support",
    }[bias]

    conf_clause = {
        CONF_MEDIUM: (
            "Confidence is Medium because policy-rate and 2Y yield differentials are both available "
            "and point the same way, but no historical fair-value regression has been applied yet"
        ),
        CONF_LOW: (
            "Confidence is Low because the policy-rate and 2Y differentials disagree (mixed signal)"
        ),
        CONF_HIGH: "Confidence is High (regression-backed)",
    }[confidence]

    ext = ""
    if price_percentile_52w is not None and bias == BIAS_BULLISH and price_percentile_52w >= _EXTENDED_PCTL:
        ext = f" Price already sits in the upper {price_percentile_52w:.0f}th percentile of its 52-week range, so the score is dampened for extension."

    policy_note = f" Policy-rate differential is {_fmt_pp(policy_diff)}."
    return f"{support_clause[0].upper()}{support_clause[1:]}; {bias_clause}. {conf_clause}.{policy_note}{ext}"
