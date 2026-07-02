"""Valuation V3.0 — fx_carry_real_yield_v3 (G10 FX fair value).

**CANONICAL FX VALUATION ENGINE** — single source of truth for the dashboard
valuation pillar, scanner ValuationCell (FX), FxValuationV3Panel, and thesis
tracker V3 snaps.

Do NOT wire V1 (``hptl.fx.fx_valuation``) or V2 (``hptl.fx.fx_institutional_valuation``)
into the pillar export or main dashboard valuation column.

Estimates where FX spot *should* trade using macro drivers only:
policy / 2Y / real-yield / inflation differentials, DXY regime, Treasury regime,
and a validated log-linear fair-value regression on weekly history.

Does not use: price percentile, location, COT, seasonality, or ranking scores.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

import pandas as pd

from hptl.fx.currency_map import COT_CURRENCY_SOURCES
from hptl.fx.currency_rates import CurrencyRate, SUPPORTED_CURRENCIES, get_currency_rate
from hptl.fx.fx_rate_history_loaders import currency_histories
from hptl.fx.fx_valuation import resolve_pair_currencies
from hptl.fx.fx_valuation_attach import _spot_and_percentile
from hptl.valuation.confidence_v2 import compute_confidence_v2, fx_confidence_display_label
from hptl.prices.canonical_timeline import load_canonical_timeline
from hptl.prices.price_store import load_price_store
from hptl.fx.fx_spot_history import get_daily_spot_series

MODEL_ID = "fx_carry_real_yield_v3"
VALUATION_PHASE = "V3.0 FX"

FX_V3_PAIRS: tuple[str, ...] = (
    "EUR/USD",
    "GBP/USD",
    "AUD/USD",
    "NZD/USD",
    "USD/JPY",
    "USD/CHF",
    "USD/CAD",
    "EUR/JPY",
    "AUD/JPY",
    "NZD/JPY",
    "EUR/GBP",
    "EUR/AUD",
    "GBP/JPY",
)

# V3.0 live scope — only these pairs may wire to dashboard / thesis when audits pass.
FX_V3_LIVE_PAIRS: tuple[str, ...] = (
    "EUR/USD",
    "GBP/USD",
    "AUD/USD",
    "NZD/USD",
    "USD/JPY",
    "USD/CAD",
    "USD/CHF",
    "EUR/GBP",
    "EUR/AUD",
)

# One registry instrument per live-wired pair in valuation_latest.json (pillar export).
FX_V3_CANONICAL_MARKET_BY_PAIR: dict[str, str] = {
    "EUR/USD": "Euro FX / 6E",
    "GBP/USD": "British Pound / 6B",
    "AUD/USD": "Australian Dollar / 6A",
    "NZD/USD": "NZ Dollar / 6N",
    "USD/JPY": "Japanese Yen / 6J",
    "USD/CAD": "Canadian Dollar / 6C",
    "USD/CHF": "Swiss Franc / 6S",
    "EUR/GBP": "EUR/GBP",
    "EUR/AUD": "EUR/AUD",
}

# Spot-format pair ids that duplicate a COT major — pillar wires the canonical market only.
FX_V3_PILLAR_ALIAS_OF: dict[str, str] = {
    "AUD/USD": "Australian Dollar / 6A",
    "NZD/USD": "NZ Dollar / 6N",
    "EUR/USD": "Euro FX / 6E",
    "GBP/USD": "British Pound / 6B",
    "USD/JPY": "Japanese Yen / 6J",
    "USD/CAD": "Canadian Dollar / 6C",
    "USD/CHF": "Swiss Franc / 6S",
}


def apply_pillar_canonical_gate(market_id: str, block: dict[str, Any]) -> dict[str, Any]:
    """Ensure one wired pillar row per FX pair — aliases defer to canonical COT markets."""
    canonical = FX_V3_PILLAR_ALIAS_OF.get(market_id)
    out = dict(block)
    if canonical and market_id != canonical:
        if out.get("wired"):
            pair = out.get("pair") or market_id
            out["wired"] = False
            out["valuation_pillar_role"] = "alias"
            out["valuation_canonical_market"] = canonical
            out["valuation_reason"] = (
                f"Pillar valuation is published on canonical instrument «{canonical}» "
                f"(same {pair})."
            )
        return out
    if out.get("wired"):
        out["valuation_pillar_role"] = "canonical"
    return out


def dxy_regime_display(regime: str | None) -> str:
    """Trader-facing DXY label."""
    key = str(regime or "").lower()
    if key == "strong_usd":
        return "Bullish"
    if key == "weak_usd":
        return "Bearish"
    return "Neutral"


def treasury_regime_display(regime: str | None) -> str:
    """Trader-facing Treasury / risk label from 2s10s structure."""
    key = str(regime or "").lower()
    if key == "steepening":
        return "Bullish risk"
    if key == "flat_or_inverted":
        return "Defensive"
    return "Neutral"


def is_live_scope_pair(pair_id: str, *, foundation_pass: bool = False) -> bool:
    """Whether pair is in dashboard live scope (foundation checked separately in wiring gate)."""
    return pair_id in FX_V3_LIVE_PAIRS


def _plain_explanation(
    *,
    pair: str,
    state: ValuationState,
    deviation_pct: float | None,
    driver_summary: str,
    confidence: Confidence,
) -> str:
    if state == "Unavailable" or deviation_pct is None:
        return f"VALUATION UNAVAILABLE — {driver_summary}"
    pct_abs = abs(deviation_pct)
    if deviation_pct < -0.05:
        lead = f"{pair} trades {pct_abs:.1f}% below estimated fair value."
    elif deviation_pct > 0.05:
        lead = f"{pair} trades {pct_abs:.1f}% above estimated fair value."
    else:
        lead = f"{pair} is near estimated fair value."
    return f"{lead}\n\n{driver_summary}\n\nConfidence:\n{confidence}"


def apply_live_wiring_gate(
    block: dict[str, Any],
    *,
    pair_id: str,
    foundation_pass: bool,
) -> dict[str, Any]:
    """Restrict dashboard wiring to live-scope pairs with foundation PASS."""
    out = dict(block)
    in_scope = is_live_scope_pair(pair_id, foundation_pass=foundation_pass)
    v3_pass = out.get("audit_status") == "PASS"
    if not in_scope:
        reason = f"{pair_id} outside V3.0 live scope — foundation not cleared for wiring."
        out.update(
            {
                "wired": False,
                "valuation_state": "Unavailable",
                "valuation_bias": "UNAVAILABLE",
                "fair_value": None,
                "deviation_pct": None,
                "confidence": "None",
                "valuation_reason": reason,
                "driver_summary": reason,
                "explanation": f"VALUATION UNAVAILABLE — {reason}",
                "live_scope": False,
            }
        )
        return out
    if not foundation_pass:
        reason = f"{pair_id} foundation audit FAIL — valuation not published."
        out.update(
            {
                "wired": False,
                "valuation_state": "Unavailable",
                "valuation_bias": "UNAVAILABLE",
                "fair_value": None,
                "deviation_pct": None,
                "confidence": "None",
                "valuation_reason": reason,
                "driver_summary": reason,
                "explanation": f"VALUATION UNAVAILABLE — {reason}",
                "live_scope": True,
                "foundation_status": "FAIL",
            }
        )
        return out
    out["live_scope"] = True
    out["foundation_status"] = "PASS"
    if v3_pass and out.get("wired"):
        conf_label = str(out.get("confidence") or "None")
        if out.get("confidence_explanation"):
            out["explanation"] = (
                _plain_explanation(
                    pair=pair_id,
                    state=out.get("valuation_state") or "Unavailable",
                    deviation_pct=_num(out.get("deviation_pct")),
                    driver_summary=str(out.get("driver_summary") or ""),
                    confidence=conf_label,  # type: ignore[arg-type]
                ).split("\n\nConfidence:")[0]
                + f"\n\n{out['confidence_explanation']}"
            )
        else:
            out["explanation"] = _plain_explanation(
                pair=pair_id,
                state=out.get("valuation_state") or "Unavailable",
                deviation_pct=_num(out.get("deviation_pct")),
                driver_summary=str(out.get("driver_summary") or ""),
                confidence=conf_label,  # type: ignore[arg-type]
            )
    else:
        reason = str(out.get("driver_summary") or "V3 audit gate not cleared.")
        out.update(
            {
                "wired": False,
                "valuation_state": "Unavailable",
                "valuation_bias": "UNAVAILABLE",
                "fair_value": None,
                "deviation_pct": None,
                "confidence": "None",
                "explanation": f"VALUATION UNAVAILABLE — {reason}",
            }
        )
    return out


COT_MARKET_BY_PAIR: dict[str, str] = {
    str(spec["quote"]): str(spec["market"]) for spec in COT_CURRENCY_SOURCES.values()
}
PAIR_BY_COT_MARKET: dict[str, str] = {v: k for k, v in COT_MARKET_BY_PAIR.items()}

MIN_WEEKLY_OBS = 52
MIN_R_SQUARED = 0.08
POLICY_LOG_BETA = 0.045
REAL_YIELD_LOG_BETA = 0.055
INFLATION_LOG_BETA = 0.025
DEV_UNDER_PCT = -2.0
DEV_OVER_PCT = 2.0

Confidence = Literal["High", "Medium", "Low", "None"]
ValuationState = Literal["Undervalued", "Fair Value", "Overvalued", "Unavailable"]


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _spot_price_stale(spot_as_of: str | None) -> bool:
    from hptl.valuation.fx_valuation_diagnostics import DEFAULT_STALE_DAYS, _parse_date

    d = _parse_date(spot_as_of)
    if d is None:
        return True
    from datetime import date

    return (date.today() - d).days > DEFAULT_STALE_DAYS


def _diff(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    return round(a - b, 4)


def _real_yield(y2: float | None, cpi: float | None) -> float | None:
    if y2 is None or cpi is None:
        return None
    return round(y2 - cpi, 3)


def _weekly_spot_series(pair_id: str) -> list[dict[str, Any]]:
    """Canonical weekly spot from price store (production weekly bars)."""
    resolved = resolve_pair_currencies(pair_id)
    if not resolved:
        return []
    _base, _quote, canonical = resolved
    instruments = load_price_store().get("instruments") or {}
    store_keys = [canonical, pair_id]
    for _code, spec in COT_CURRENCY_SOURCES.items():
        if str(spec.get("quote")) == canonical:
            store_keys.append(str(spec.get("market")))
    rec: dict[str, Any] = {}
    for key in store_keys:
        if key and key in instruments:
            rec = instruments[key]
            break
    weekly = rec.get("weekly") or []
    out: list[dict[str, Any]] = []
    for bar in weekly:
        if not isinstance(bar, dict):
            continue
        d = str(bar.get("date") or "")[:10]
        c = _num(bar.get("close"))
        if d and c is not None and c > 0:
            out.append({"date": d, "spot": c})
    out.sort(key=lambda x: x["date"])
    return out


def _dxy_regime() -> dict[str, Any]:
    tl = load_canonical_timeline("US Dollar Index / DX")
    if not tl:
        return {"available": False, "percentile_52w": None, "regime": "unavailable", "as_of": None}
    closes = [c for _, c in tl.daily_closes() if _num(c) is not None]
    if len(closes) < 20:
        return {"available": False, "percentile_52w": None, "regime": "unavailable", "as_of": tl.date_end}
    window = closes[-252:] if len(closes) >= 252 else closes
    current = window[-1]
    pct = sum(1 for x in window if x <= current) / len(window) * 100.0
    if pct >= 67:
        regime = "strong_usd"
    elif pct <= 33:
        regime = "weak_usd"
    else:
        regime = "neutral"
    return {
        "available": True,
        "percentile_52w": round(pct, 1),
        "regime": regime,
        "regime_label": dxy_regime_display(regime),
        "as_of": tl.date_end,
        "source": tl.canonical_source,
    }


def _treasury_regime() -> dict[str, Any]:
    usd = get_currency_rate("USD")
    y2, y10 = usd.y2, usd.y10
    if y2 is None or y10 is None:
        return {"available": False, "slope_2s10s": None, "regime": "unavailable"}
    slope = round(y10 - y2, 3)
    if slope >= 0.35:
        regime = "steepening"
    elif slope <= 0.05:
        regime = "flat_or_inverted"
    else:
        regime = "moderate"
    return {
        "available": True,
        "y2": y2,
        "y10": y10,
        "slope_2s10s": slope,
        "regime": regime,
        "regime_label": treasury_regime_display(regime),
        "as_of": usd.y2_as_of or usd.y10_as_of,
    }


def _regime_adjustment_pct(base: str, quote: str, dxy: dict[str, Any], treas: dict[str, Any]) -> float:
    """Macro regime tilt on fair value (%), not positioning."""
    adj = 0.0
    dxy_pct = _num(dxy.get("percentile_52w"))
    if dxy_pct is not None:
        usd_strength = (dxy_pct - 50.0) / 50.0
        if quote == "USD":
            adj -= usd_strength * 1.5
        elif base == "USD":
            adj += usd_strength * 1.5
    slope = _num(treas.get("slope_2s10s"))
    if slope is not None:
        if quote == "USD":
            adj += (slope - 0.20) * 0.8
        elif base == "USD":
            adj -= (slope - 0.20) * 0.8
    return round(max(-3.0, min(3.0, adj)), 2)


def _ols_log_spot(y_log: list[float], x1: list[float], x2: list[float] | None = None) -> dict[str, Any]:
    n = len(y_log)
    if x2 is None:
        if len(x1) != n or n < 8:
            return {"ok": False, "n": n}
        df = pd.DataFrame({"y": y_log, "x1": x1}).dropna()
    else:
        if min(len(x1), len(x2)) != n or n < 8:
            return {"ok": False, "n": n}
        df = pd.DataFrame({"y": y_log, "x1": x1, "x2": x2}).dropna()
    if len(df) < 8:
        return {"ok": False, "n": len(df)}
    if x2 is None:
        X = df[["x1"]].assign(intercept=1.0).values
    else:
        X = df[["x1", "x2"]].assign(intercept=1.0).values
    y = df["y"].values
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)  # type: ignore[name-defined]
    pred = X @ coef
    ss_res = float(((y - pred) ** 2).sum())
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    out: dict[str, Any] = {
        "ok": True,
        "n": len(df),
        "b_y2": float(coef[0]),
        "intercept": float(coef[-1]),
        "r_squared": round(r2, 4),
        "features": "y2,policy" if x2 is not None else "y2",
    }
    if x2 is not None:
        out["b_policy"] = float(coef[1])
    return out


try:
    import numpy as np
except ImportError:  # pragma: no cover
    np = None  # type: ignore


def _ols_log_spot_safe(y_log: list[float], x1: list[float], x2: list[float] | None = None) -> dict[str, Any]:
    if np is None:
        return {"ok": False, "n": 0, "error": "numpy unavailable"}
    return _ols_log_spot(y_log, x1, x2)


def _value_as_of(daily_map: dict[str, float], iso_date: str) -> float | None:
    if not daily_map:
        return None
    best: str | None = None
    for d in daily_map:
        if d <= iso_date and (best is None or d > best):
            best = d
    return daily_map.get(best) if best else None


def _daily_map_from_series(series: list[dict[str, Any]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in series:
        d = str(row.get("date") or "")[:10]
        v = _num(row.get("value"))
        if d and v is not None:
            out[d] = v
    return out


def _daily_spot_series(pair_id: str) -> list[dict[str, Any]]:
    """Daily canonical spot for regression depth (direct or derived_cross)."""
    series, _meta = get_daily_spot_series(pair_id)
    return series


def _align_daily_panel(
    pair_id: str,
    base: str,
    quote: str,
    histories: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    spot_daily = _daily_spot_series(pair_id)
    base_y2 = dict((histories.get(base) or {}).get("y2") or {})
    quote_y2 = dict((histories.get(quote) or {}).get("y2") or {})

    base_pol = dict((histories.get(base) or {}).get("policy") or {})
    quote_pol = dict((histories.get(quote) or {}).get("policy") or {})
    br = get_currency_rate(base)
    qr = get_currency_rate(quote)
    if br.policy_rate is not None and br.policy_rate_as_of:
        base_pol[str(br.policy_rate_as_of)[:10]] = float(br.policy_rate)
    if qr.policy_rate is not None and qr.policy_rate_as_of:
        quote_pol[str(qr.policy_rate_as_of)[:10]] = float(qr.policy_rate)

    rows: list[dict[str, Any]] = []
    for pt in spot_daily:
        d = pt["date"]
        by2 = _value_as_of(base_y2, d)
        qy2 = _value_as_of(quote_y2, d)
        if by2 is None or qy2 is None:
            continue
        y2d = round(by2 - qy2, 4)
        bp = _value_as_of(base_pol, d)
        qp = _value_as_of(quote_pol, d)
        policy_diff = round(bp - qp, 4) if bp is not None and qp is not None else None
        rows.append({"date": d, "spot": pt["spot"], "y2_diff": y2d, "policy_diff": policy_diff})
    return rows


def _valuation_state(deviation_pct: float | None) -> ValuationState:
    if deviation_pct is None:
        return "Unavailable"
    if deviation_pct <= DEV_UNDER_PCT:
        return "Undervalued"
    if deviation_pct >= DEV_OVER_PCT:
        return "Overvalued"
    return "Fair Value"


def _confidence(n: int, r2: float | None, missing: list[str], stale: list[str]) -> Confidence:
    if missing or n < MIN_WEEKLY_OBS or r2 is None or r2 < MIN_R_SQUARED:
        return "None"
    if stale:
        return "Low"
    if n >= 156 and r2 >= 0.25:
        return "High"
    if n >= MIN_WEEKLY_OBS and r2 >= 0.18:
        return "Medium"
    return "Low"


def _driver_summary(
    *,
    pair: str,
    base: str,
    quote: str,
    state: ValuationState,
    deviation_pct: float | None,
    policy_diff: float | None,
    y2_diff: float | None,
    real_diff: float | None,
    infl_diff: float | None,
    dxy: dict[str, Any],
    treas: dict[str, Any],
) -> str:
    if state == "Unavailable" or deviation_pct is None:
        return "Insufficient validated macro inputs or regression history for fair value."
    parts: list[str] = []
    if real_diff is not None and abs(real_diff) >= 0.15:
        parts.append(
            f"{base} real-yield differential vs {quote} is {'higher' if real_diff > 0 else 'lower'}"
        )
    elif y2_diff is not None and abs(y2_diff) >= 0.10:
        parts.append(f"2Y yield differential favours {'base' if y2_diff > 0 else 'quote'} ({base}/{quote})")
    if policy_diff is not None and abs(policy_diff) >= 0.15:
        parts.append(f"policy rate gap supports {'base' if policy_diff > 0 else 'quote'}")
    if infl_diff is not None and abs(infl_diff) >= 0.3:
        parts.append(f"inflation differential {'base higher' if infl_diff > 0 else 'quote higher'}")
    dxy_reg = str(dxy.get("regime") or "")
    if dxy_reg == "strong_usd" and quote == "USD":
        parts.append("broad USD (DXY) regime is strong")
    elif dxy_reg == "weak_usd" and quote == "USD":
        parts.append("broad USD (DXY) regime is weak")
    treas_reg = str(treas.get("regime") or "")
    if treas_reg == "steepening" and quote == "USD":
        parts.append("US Treasury curve is steep (USD supportive)")
    elif treas_reg == "flat_or_inverted":
        parts.append("US Treasury curve is flat/inverted")
    if not parts:
        parts.append("macro differentials are near neutral versus historical fair-value anchor")
    action = {
        "Undervalued": "trades below estimated fair value because",
        "Overvalued": "trades above estimated fair value because",
        "Fair Value": "is near estimated fair value with",
    }.get(state, "—")
    return f"{pair} {action} " + ", ".join(parts) + "."


@dataclass
class FxV3PairResult:
    pair: str
    base: str
    quote: str
    spot_price: float | None
    fair_value: float | None
    deviation_pct: float | None
    valuation_state: ValuationState
    confidence: Confidence
    driver_summary: str
    input_freshness: dict[str, Any]
    missing_inputs: list[str]
    stale_inputs: list[str]
    audit_status: Literal["PASS", "FAIL"]
    explanation: str = ""
    regression: dict[str, Any] = field(default_factory=dict)
    drivers: dict[str, Any] = field(default_factory=dict)
    dxy_regime: dict[str, Any] = field(default_factory=dict)
    treasury_regime: dict[str, Any] = field(default_factory=dict)
    confidence_meta: dict[str, Any] = field(default_factory=dict)
    deviation_pct_raw: float | None = None
    valuation_diagnostics: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        diag = dict(self.valuation_diagnostics or {})
        publishable = (
            self.deviation_pct is not None
            and self.fair_value is not None
            and self.valuation_state != "Unavailable"
        )
        wired = publishable
        explanation = self.explanation or _plain_explanation(
            pair=self.pair,
            state=self.valuation_state,
            deviation_pct=self.deviation_pct,
            driver_summary=self.driver_summary,
            confidence=self.confidence,
        )
        out = {
            "pair": self.pair,
            "base": self.base,
            "quote": self.quote,
            "spot_price": self.spot_price,
            "fair_value": self.fair_value,
            "deviation_pct": self.deviation_pct,
            "deviation_pct_raw": self.deviation_pct_raw,
            "valuation_diagnostics": dict(self.valuation_diagnostics),
            "valuation_state": self.valuation_state,
            "valuation_bias": self.valuation_state if self.valuation_state != "Unavailable" else "UNAVAILABLE",
            "confidence": self.confidence,
            "model_id": MODEL_ID,
            "valuation_phase": VALUATION_PHASE,
            "valuation_model_id": MODEL_ID,
            "driver_summary": self.driver_summary,
            "valuation_reason": self.driver_summary,
            "explanation": explanation,
            "input_freshness": dict(self.input_freshness),
            "missing_inputs": list(self.missing_inputs),
            "stale_inputs": list(self.stale_inputs),
            "audit_status": self.audit_status,
            "wired": wired,
            "publishable": publishable,
            "inputs_stale": diag.get("inputs_stale") is True,
            "regression": dict(self.regression),
            "drivers": dict(self.drivers),
            "dxy_regime": dict(self.dxy_regime),
            "treasury_regime": dict(self.treasury_regime),
        }
        out.update(self.confidence_meta)
        return out


def compute_fx_pair_v3(
    pair_id: str,
    *,
    histories: dict[str, dict[str, Any]] | None = None,
    base_rate: CurrencyRate | None = None,
    quote_rate: CurrencyRate | None = None,
) -> FxV3PairResult:
    resolved = resolve_pair_currencies(pair_id)
    if not resolved:
        return FxV3PairResult(
            pair=pair_id,
            base="",
            quote="",
            spot_price=None,
            fair_value=None,
            deviation_pct=None,
            valuation_state="Unavailable",
            confidence="None",
            driver_summary="Unsupported FX pair.",
            input_freshness={},
            missing_inputs=["pair"],
            stale_inputs=[],
            audit_status="FAIL",
        )
    base, quote, canonical = resolved
    pair = f"{base}/{quote}"
    histories = histories or currency_histories()

    br = base_rate or get_currency_rate(base)
    qr = quote_rate or get_currency_rate(quote)
    spot, _pctl = _spot_and_percentile(canonical)

    missing: list[str] = []
    stale: list[str] = []
    for leg, rec in ((base, br), (quote, qr)):
        for fld in rec.missing_fields:
            missing.append(f"{leg}.{fld}")
        for fld in rec.stale_fields:
            if fld in {"policy_rate", "y2", "y10", "cpi_yoy"}:
                stale.append(f"{leg}.{fld}")
    if spot is None:
        missing.append("spot_price")

    core_missing = [m for m in missing if not m.endswith(".cpi_yoy")]

    policy_diff = _diff(br.policy_rate, qr.policy_rate)
    y2_diff = _diff(br.y2, qr.y2)
    infl_diff = _diff(br.cpi_yoy, qr.cpi_yoy)
    base_real = _real_yield(br.y2, br.cpi_yoy)
    quote_real = _real_yield(qr.y2, qr.cpi_yoy)
    real_diff = _diff(base_real, quote_real)

    dxy = _dxy_regime()
    treas = _treasury_regime()

    panel = _align_daily_panel(pair, base, quote, histories)
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

    use_policy_in_reg = len(x_pol) >= MIN_WEEKLY_OBS and len(x_pol) == len(y_log)
    if use_policy_in_reg:
        reg = _ols_log_spot_safe(y_log, x_y2, x_pol)
    else:
        reg = _ols_log_spot_safe(y_log, x_y2, None)

    fair_value: float | None = None
    deviation_pct: float | None = None
    deviation_pct_raw: float | None = None

    if (
        spot is not None
        and reg.get("ok")
        and reg.get("n", 0) >= MIN_WEEKLY_OBS
        and y2_diff is not None
        and policy_diff is not None
        and real_diff is not None
        and not core_missing
    ):
        log_fv = float(reg["intercept"]) + float(reg["b_y2"]) * y2_diff
        if use_policy_in_reg and reg.get("b_policy") is not None:
            log_fv += float(reg["b_policy"]) * policy_diff
        else:
            log_fv += POLICY_LOG_BETA * policy_diff
        log_fv += REAL_YIELD_LOG_BETA * real_diff
        if infl_diff is not None:
            log_fv += INFLATION_LOG_BETA * infl_diff
        fair_value = math.exp(log_fv)
        regime_adj = _regime_adjustment_pct(base, quote, dxy, treas)
        fair_value = round(fair_value * (1.0 + regime_adj / 100.0), 6)
        deviation_pct_raw = (spot - fair_value) / fair_value * 100.0
        deviation_pct = round(deviation_pct_raw, 2)

    state = _valuation_state(deviation_pct)
    conf_v1 = _confidence(int(reg.get("n") or 0), _num(reg.get("r_squared")), core_missing, stale)
    publishable = (
        spot is not None
        and reg.get("ok")
        and int(reg.get("n") or 0) >= MIN_WEEKLY_OBS
        and _num(reg.get("r_squared")) is not None
        and (_num(reg.get("r_squared")) or 0) >= MIN_R_SQUARED
        and not core_missing
    )
    v2 = compute_confidence_v2(
        model_id=MODEL_ID,
        publishable=publishable,
        n=int(reg.get("n") or 0),
        r_squared=_num(reg.get("r_squared")),
        stale_inputs=stale,
        missing_inputs=core_missing,
        confidence_v1=conf_v1,
    )
    conf = fx_confidence_display_label(v2.confidence)
    if state != "Unavailable" and v2.confidence == "none" and core_missing:
        state = "Unavailable"
        fair_value = None
        deviation_pct = None
        deviation_pct_raw = None

    spot_series, _spot_meta = get_daily_spot_series(canonical)
    spot_as_of = str(spot_series[-1]["date"])[:10] if spot_series else None
    valuation_date = datetime.now(timezone.utc).date().isoformat()
    generated_at = datetime.now(timezone.utc).isoformat()

    input_freshness = {
        "spot_as_of": spot_as_of,
        "base_policy_as_of": br.policy_rate_as_of,
        "quote_policy_as_of": qr.policy_rate_as_of,
        "base_y2_as_of": br.y2_as_of,
        "quote_y2_as_of": qr.y2_as_of,
        "base_cpi_as_of": br.cpi_yoy_as_of,
        "quote_cpi_as_of": qr.cpi_yoy_as_of,
        "base_real_yield_as_of": br.y2_as_of,
        "quote_real_yield_as_of": qr.y2_as_of,
        "dxy_as_of": dxy.get("as_of"),
        "treasury_as_of": treas.get("as_of"),
    }

    from hptl.valuation.fx_valuation_diagnostics import build_fx_valuation_diagnostics

    valuation_diagnostics = build_fx_valuation_diagnostics(
        valuation_date=valuation_date,
        spot_date=spot_as_of,
        spot=spot,
        fair_value=fair_value,
        raw_gap_pct_unrounded=deviation_pct_raw,
        gap_pct_rounded=deviation_pct,
        input_latest_dates={
            "spot": spot_as_of,
            f"{base}.policy_rate": br.policy_rate_as_of,
            f"{quote}.policy_rate": qr.policy_rate_as_of,
            f"{base}.y2": br.y2_as_of,
            f"{quote}.y2": qr.y2_as_of,
            f"{base}.cpi_yoy": br.cpi_yoy_as_of,
            f"{quote}.cpi_yoy": qr.cpi_yoy_as_of,
            "dxy": dxy.get("as_of"),
            "treasury": treas.get("as_of"),
        },
        cache_generated_at=generated_at,
        source_file="web-dashboard/public/data/fx_valuation_v3_latest.json",
        stale_inputs=stale,
        missing_inputs=missing,
        price_stale=_spot_price_stale(spot_as_of),
    )

    audit_pass = (
        fair_value is not None
        and spot is not None
        and deviation_pct is not None
        and state != "Unavailable"
        and not core_missing
    )
    inputs_stale = valuation_diagnostics.get("inputs_stale") is True

    summary = _driver_summary(
        pair=pair,
        base=base,
        quote=quote,
        state=state,
        deviation_pct=deviation_pct,
        policy_diff=policy_diff,
        y2_diff=y2_diff,
        real_diff=real_diff,
        infl_diff=infl_diff,
        dxy=dxy,
        treas=treas,
    )

    return FxV3PairResult(
        pair=pair,
        base=base,
        quote=quote,
        spot_price=spot,
        fair_value=fair_value,
        deviation_pct=deviation_pct,
        valuation_state=state,
        confidence=conf,
        driver_summary=summary,
        input_freshness=input_freshness,
        missing_inputs=missing,
        stale_inputs=stale,
        audit_status="PASS" if audit_pass else "FAIL",
        deviation_pct_raw=deviation_pct_raw,
        valuation_diagnostics=valuation_diagnostics,
        explanation=_plain_explanation(
            pair=pair,
            state=state,
            deviation_pct=deviation_pct,
            driver_summary=summary,
            confidence=conf,
        ),
        regression=reg,
        drivers={
            "base_policy_rate": br.policy_rate,
            "quote_policy_rate": qr.policy_rate,
            "policy_rate_diff": policy_diff,
            "base_yield_2y": br.y2,
            "quote_yield_2y": qr.y2,
            "yield_2y_diff": y2_diff,
            "base_real_yield": base_real,
            "quote_real_yield": quote_real,
            "real_yield_diff": real_diff,
            "base_cpi_yoy": br.cpi_yoy,
            "quote_cpi_yoy": qr.cpi_yoy,
            "inflation_diff": infl_diff,
        },
        dxy_regime=dxy,
        treasury_regime=treas,
        confidence_meta=v2.as_export_fields(),
    )


def compute_fx_market_v3(market_id: str, *, as_of_week: str | None = None) -> dict[str, Any]:
    """Map a COT FX market or cross pair id to V3 valuation payload."""
    pair_id = PAIR_BY_COT_MARKET.get(market_id)
    if pair_id is None:
        resolved = resolve_pair_currencies(market_id)
        pair_id = f"{resolved[0]}/{resolved[1]}" if resolved else None
    if pair_id is None or pair_id not in FX_V3_PAIRS:
        return _unavailable_market(market_id, as_of_week, reason="Pair outside V3.0 FX scope or unsupported.")
    result = compute_fx_pair_v3(pair_id)
    out = result.as_dict()
    out["market"] = market_id
    out["as_of_week"] = as_of_week
    out["asset_class"] = "fx"
    if result.audit_status != "PASS":
        out["wired"] = False
        out["valuation_state"] = "Unavailable"
        out["valuation_bias"] = "UNAVAILABLE"
        out["confidence"] = "none"
    return out


def _unavailable_market(market_id: str, as_of_week: str | None, *, reason: str) -> dict[str, Any]:
    return {
        "market": market_id,
        "as_of_week": as_of_week,
        "asset_class": "fx",
        "wired": False,
        "valuation_state": "Unavailable",
        "valuation_bias": "UNAVAILABLE",
        "fair_value": None,
        "spot_price": None,
        "deviation_pct": None,
        "confidence": "none",
        "model_id": MODEL_ID,
        "valuation_phase": VALUATION_PHASE,
        "valuation_model_id": MODEL_ID,
        "driver_summary": reason,
        "valuation_reason": reason,
        "input_freshness": {},
        "missing_inputs": ["model"],
        "stale_inputs": [],
        "audit_status": "FAIL",
        "pass": False,
    }


def build_all_fx_v3_pairs() -> dict[str, Any]:
    histories = currency_histories()
    results = {pid: compute_fx_pair_v3(pid, histories=histories).as_dict() for pid in FX_V3_PAIRS}
    passed = sum(1 for r in results.values() if r.get("audit_status") == "PASS")
    return {
        "model_id": MODEL_ID,
        "valuation_phase": VALUATION_PHASE,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pairs": results,
        "summary": {
            "total_pairs": len(FX_V3_PAIRS),
            "audit_pass": passed,
            "audit_fail": len(FX_V3_PAIRS) - passed,
        },
    }
