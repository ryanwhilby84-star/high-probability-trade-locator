"""Currency futures IVE — Phase 1C futures-native fair value engine.

Each CME instrument (DX, 6E, …, 6N) is valued from its own continuous futures
price history. Macro inputs are aligned daily and combined via OLS on log(price).

This module does NOT call fx_carry_real_yield_v3, pair fair values, or fixed
legacy betas (0.055 / 0.025 / 0.045).
"""
from __future__ import annotations

import json
import math
import statistics
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd

from hptl.config import DATA_DIR, PROJECT_ROOT
from hptl.fx.currency_map import DX_INSTRUMENT_ID
from hptl.fx.currency_rates import get_currency_rate
from hptl.fx.fx_macro_history import build_differential_series, currency_histories, load_fred_daily_map
from hptl.prices.price_store import load_instrument_record_internal
from hptl.valuation.ive_schema import (
    CONFIDENCE_EXPORT_KEYS,
    CalculationStep,
    IVEOutput,
    SourceLineage,
    strip_confidence_fields,
    valuation_grade_from_pct,
)
from hptl.valuation.series_asof import value_as_of

ENGINE_ID = "currency_futures_ive_v1"
VALUATION_PHASE = "1C Currency Futures IVE"

MIN_DAILY_OBS = 252
MIN_R_SQUARED = 0.08
PRICE_MAX_STALE_DAYS = 10
DEPENDENT_SERIES = "log_continuous_futures_close"

PUBLIC_JSON = PROJECT_ROOT / "web-dashboard/public/data/currency_futures_ive_latest.json"
DATA_JSON = DATA_DIR / "processed/currency_futures_ive_latest.json"

BANNED_PAIR_SYMBOLS = frozenset(
    {"EUR/USD", "GBP/USD", "AUD/USD", "NZD/USD", "USD/JPY", "USD/CHF", "USD/CAD", "EURUSD", "GBPUSD"}
)

ModelStatus = Literal["VALIDATED", "MODEL_INCOMPLETE", "DATA_STALE", "DATA_MISSING"]


@dataclass(frozen=True)
class FuturesSpec:
    symbol: str
    instrument_id: str
    currency: str
    usd_quoted: bool
    model_name: str
    model_family: str
    cot_market_code: str
    feature_names: tuple[str, ...]
    safe_haven: bool = False


FUTURES_REGISTRY: dict[str, FuturesSpec] = {
    "DX": FuturesSpec(
        symbol="DX",
        instrument_id=DX_INSTRUMENT_ID,
        currency="USD",
        usd_quoted=False,
        model_name="dx_futures_broad_macro_v1",
        model_family="futures_ols_log_macro",
        cot_market_code="098662",
        feature_names=("avg_g10_2y_vs_usd", "fed_funds", "real_yield_10y"),
    ),
    "6E": FuturesSpec("6E", "Euro FX / 6E", "EUR", True, "eur_futures_macro_v1", "futures_ols_log_macro", "EUR_USD", ("y2_diff", "policy_diff")),
    "6B": FuturesSpec("6B", "British Pound / 6B", "GBP", True, "gbp_futures_macro_v1", "futures_ols_log_macro", "GBP_USD", ("y2_diff", "policy_diff")),
    "6A": FuturesSpec("6A", "Australian Dollar / 6A", "AUD", True, "aud_futures_macro_v1", "futures_ols_log_macro", "AUD_USD", ("y2_diff", "policy_diff")),
    "6C": FuturesSpec("6C", "Canadian Dollar / 6C", "CAD", False, "cad_futures_macro_v1", "futures_ols_log_macro", "USD_CAD", ("y2_diff", "policy_diff")),
    "6J": FuturesSpec("6J", "Japanese Yen / 6J", "JPY", False, "jpy_futures_macro_v1", "futures_ols_log_macro", "097741", ("y2_diff", "policy_diff")),
    "6S": FuturesSpec(
        "6S",
        "Swiss Franc / 6S",
        "CHF",
        False,
        "chf_futures_safe_haven_v1",
        "futures_ols_log_safe_haven",
        "USD_CHF",
        ("y2_diff", "policy_diff", "broad_usd_index"),
        safe_haven=True,
    ),
    "6N": FuturesSpec("6N", "NZ Dollar / 6N", "NZD", True, "nzd_futures_macro_v1", "futures_ols_log_macro", "NZD_USD", ("y2_diff", "policy_diff")),
}

FUTURES_INSTRUMENT_IDS: frozenset[str] = frozenset(s.instrument_id for s in FUTURES_REGISTRY.values())


def valuation_label_from_pct(valuation_pct: float | None) -> str:
    if valuation_pct is None:
        return "—"
    if valuation_pct <= -2.0:
        return "Undervalued"
    if valuation_pct >= 2.0:
        return "Overvalued"
    return "Fair Value"


def is_currency_futures_instrument(market_id: str) -> bool:
    return str(market_id or "").strip() in FUTURES_INSTRUMENT_IDS


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _parse_date(s: str) -> date | None:
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


def _load_futures_daily(instrument_id: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    doc = load_instrument_record_internal(instrument_id) or {}
    daily = doc.get("daily") or []
    rows: list[dict[str, Any]] = []
    for bar in daily:
        if not isinstance(bar, dict):
            continue
        d = str(bar.get("date") or "")[:10]
        c = _num(bar.get("close"))
        if d and c is not None and c > 0:
            rows.append({"date": d, "close": c})
    rows.sort(key=lambda x: x["date"])
    meta = {
        "instrument_id": instrument_id,
        "bar_count": len(rows),
        "earliest": rows[0]["date"] if rows else None,
        "latest": rows[-1]["date"] if rows else None,
        "roll_method": doc.get("_historical_via") or doc.get("_fetched_via") or "price_store",
        "price_as_of": (doc.get("price") or {}).get("as_of"),
        "stored_at": doc.get("stored_at"),
    }
    return rows, meta


def _current_futures_price(instrument_id: str) -> tuple[float | None, str | None]:
    doc = load_instrument_record_internal(instrument_id) or {}
    price = doc.get("price") or {}
    mid = _num(price.get("mid"))
    if mid is None and doc.get("daily"):
        mid = _num(doc["daily"][-1].get("close"))
    as_of = str(price.get("as_of") or (doc.get("daily") or [{}])[-1].get("date") or "")[:10]
    return mid, as_of or None


def _price_stale(as_of: str | None, reference: date | None = None) -> bool:
    ref = reference or date.today()
    d = _parse_date(as_of or "")
    if d is None:
        return True
    return (ref - d).days > PRICE_MAX_STALE_DAYS


def _rate_diff(leg_val: float | None, usd_val: float | None, *, usd_quoted: bool) -> float | None:
    if leg_val is None or usd_val is None:
        return None
    if usd_quoted:
        return round(leg_val - usd_val, 4)
    return round(usd_val - leg_val, 4)


def _model_input_flags(spec: FuturesSpec) -> tuple[list[str], list[str]]:
    """Freshness/missing checks for inputs actually used by the instrument model."""
    missing: list[str] = []
    stale: list[str] = []

    def check_ccy(ccy: str, fields: tuple[str, ...]) -> None:
        rec = get_currency_rate(ccy)
        for fld in fields:
            key = f"{ccy}.{fld}"
            if fld in rec.missing_fields:
                missing.append(key)
            if fld in rec.stale_fields:
                stale.append(key)

    if spec.symbol == "DX":
        check_ccy("USD", ("y2", "policy_rate"))
        return missing, stale

    check_ccy(spec.currency, ("y2", "policy_rate"))
    check_ccy("USD", ("y2", "policy_rate"))

    if spec.safe_haven:
        # Broad USD index is loaded from FRED macro_cache; stale if empty
        dxy = load_fred_daily_map("DTWEXBGS")
        if not dxy:
            missing.append("DTWEXBGS")

    return missing, stale


def _ols_log_futures(
    panel: list[dict[str, Any]],
    feature_names: tuple[str, ...],
) -> dict[str, Any]:
    """OLS: log(close) ~ intercept + Σ βᵢ·featureᵢ (all β fitted, no fixed legacy betas)."""
    if len(panel) < MIN_DAILY_OBS:
        return {"ok": False, "n": len(panel), "error": "insufficient_panel"}

    cols: dict[str, list[float]] = {"y": [math.log(r["close"]) for r in panel]}
    for name in feature_names:
        cols[name] = [_num(r.get(name)) for r in panel]

    df = pd.DataFrame(cols).dropna()
    if len(df) < MIN_DAILY_OBS:
        return {"ok": False, "n": len(df), "error": "insufficient_aligned_obs"}

    y = df["y"].values
    X = df[list(feature_names)].assign(intercept=1.0).values
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    pred = X @ coef
    ss_res = float(((y - pred) ** 2).sum())
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    coefficients: dict[str, float] = {"intercept": float(coef[-1])}
    for i, name in enumerate(feature_names):
        coefficients[name] = float(coef[i])

    return {
        "ok": True,
        "n": len(df),
        "r_squared": round(r2, 4),
        "features": list(feature_names),
        "coefficients": coefficients,
        "dependent_series": DEPENDENT_SERIES,
        "legacy_fixed_betas_used": False,
        "legacy_fx_v3_used": False,
        "legacy_pair_model_used": False,
    }


def _predict_log_fv(reg: dict[str, Any], drivers: dict[str, float | None]) -> float | None:
    if not reg.get("ok"):
        return None
    coef = reg.get("coefficients") or {}
    intercept = coef.get("intercept")
    if intercept is None:
        return None
    log_fv = float(intercept)
    for name in reg.get("features") or []:
        val = drivers.get(name)
        beta = coef.get(name)
        if val is None or beta is None:
            return None
        log_fv += float(beta) * float(val)
    return log_fv


def _build_breakdown(
    reg: dict[str, Any],
    drivers: dict[str, float | None],
    fair_value: float,
) -> list[dict[str, Any]]:
    steps: list[CalculationStep] = []
    n = 1
    coef = reg.get("coefficients") or {}
    intercept = float(coef.get("intercept") or 0)
    steps.append(CalculationStep(n, "Intercept anchor (log)", round(intercept, 6)))
    n += 1
    for name in reg.get("features") or []:
        val = drivers.get(name)
        beta = coef.get(name)
        if val is not None and beta is not None:
            term = float(beta) * float(val)
            steps.append(CalculationStep(n, f"{name} × β_{name}", round(term, 6)))
            n += 1
    steps.append(CalculationStep(n, "Fair value = exp(log linear model)", round(fair_value, 6)))
    return [s.to_dict() for s in steps]


def _reconciles(reg: dict[str, Any], drivers: dict[str, float | None], fair_value: float) -> bool:
    log_fv = _predict_log_fv(reg, drivers)
    if log_fv is None:
        return False
    return abs(math.exp(log_fv) - fair_value) < max(1e-6, fair_value * 1e-4)


def _lineage_for_spec(spec: FuturesSpec, generated_at: str) -> list[dict[str, str]]:
    rows: list[SourceLineage] = []
    _, price_meta = _load_futures_daily(spec.instrument_id)
    rows.append(
        SourceLineage(
            source_name="Futures price store",
            source_id=spec.instrument_id,
            source_date=str(price_meta.get("latest") or "—")[:10],
            last_refresh=generated_at[:10],
            field="current_price",
        )
    )
    if spec.symbol == "DX":
        usd = get_currency_rate("USD")
        rows.append(
            SourceLineage(
                source_name="FRED DFF",
                source_id="DFF",
                source_date=str(usd.policy_rate_as_of or "—")[:10],
                last_refresh=generated_at[:10],
                field="fed_funds",
            )
        )
        rows.append(
            SourceLineage(
                source_name="FRED DFII10",
                source_id="DFII10",
                source_date=str(usd.y10_as_of or "—")[:10],
                last_refresh=generated_at[:10],
                field="real_yield_10y",
            )
        )
        return [ln.to_dict() for ln in rows]

    leg = get_currency_rate(spec.currency)
    usd = get_currency_rate("USD")
    rows.append(
        SourceLineage(
            source_name=f"{spec.currency} 2Y yield",
            source_id=leg.y2_as_of or "y2",
            source_date=str(leg.y2_as_of or "—")[:10],
            last_refresh=generated_at[:10],
            field=f"{spec.currency}.y2",
        )
    )
    rows.append(
        SourceLineage(
            source_name="USD 2Y yield",
            source_id=usd.y2_as_of or "DGS2",
            source_date=str(usd.y2_as_of or "—")[:10],
            last_refresh=generated_at[:10],
            field="USD.y2",
        )
    )
    if spec.safe_haven:
        rows.append(
            SourceLineage(
                source_name="FRED DTWEXBGS",
                source_id="DTWEXBGS",
                source_date=generated_at[:10],
                last_refresh=generated_at[:10],
                field="broad_usd_index",
            )
        )
    return [ln.to_dict() for ln in rows]


def _determine_status(
    *,
    missing: list[str],
    stale: list[str],
    price_stale: bool,
    reg_ok: bool,
    n: int,
    r2: float | None,
    reconciled: bool,
    panel_len: int,
) -> ModelStatus:
    if missing:
        return "DATA_MISSING"
    if stale or price_stale:
        return "DATA_STALE"
    if panel_len < MIN_DAILY_OBS or not reg_ok or n < MIN_DAILY_OBS:
        return "MODEL_INCOMPLETE"
    if r2 is None or r2 < MIN_R_SQUARED or not reconciled:
        return "MODEL_INCOMPLETE"
    return "VALIDATED"


def _build_g10_panel(spec: FuturesSpec, futures_daily: list[dict[str, Any]], histories: dict[str, Any]) -> list[dict[str, Any]]:
    leg = spec.currency
    leg_rec = get_currency_rate(leg)
    usd_rec = get_currency_rate("USD")

    leg_y2 = dict((histories.get(leg) or {}).get("y2") or {})
    usd_y2 = dict((histories.get("USD") or {}).get("y2") or {})
    leg_pol = dict((histories.get(leg) or {}).get("policy") or {})
    usd_pol = dict((histories.get("USD") or {}).get("policy") or {})
    if leg_rec.policy_rate is not None and leg_rec.policy_rate_as_of:
        leg_pol[str(leg_rec.policy_rate_as_of)[:10]] = float(leg_rec.policy_rate)
    if usd_rec.policy_rate is not None and usd_rec.policy_rate_as_of:
        usd_pol[str(usd_rec.policy_rate_as_of)[:10]] = float(usd_rec.policy_rate)

    dxy_map = load_fred_daily_map("DTWEXBGS") if spec.safe_haven else {}

    panel: list[dict[str, Any]] = []
    for row in futures_daily:
        d = row["date"]
        ly2 = value_as_of(leg_y2, d)
        uy2 = value_as_of(usd_y2, d)
        if ly2 is None or uy2 is None:
            continue
        y2d = _rate_diff(ly2, uy2, usd_quoted=spec.usd_quoted)
        lp = value_as_of(leg_pol, d)
        up = value_as_of(usd_pol, d)
        pol = _rate_diff(lp, up, usd_quoted=spec.usd_quoted)
        rec: dict[str, Any] = {"date": d, "close": row["close"], "y2_diff": y2d, "policy_diff": pol}
        if spec.safe_haven:
            dxy = value_as_of(dxy_map, d)
            if dxy is None or y2d is None or pol is None:
                continue
            rec["broad_usd_index"] = round(dxy, 4)
        elif y2d is None:
            continue
        panel.append(rec)
    return panel


def _build_dx_panel(futures_daily: list[dict[str, Any]], histories: dict[str, Any]) -> list[dict[str, Any]]:
    dff = load_fred_daily_map("DFF")
    real10 = load_fred_daily_map("DFII10")
    avg_diff: dict[str, list[float]] = {}
    for base in ("EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD"):
        rows = build_differential_series(base, "USD", "y2", histories)
        for row in rows:
            avg_diff.setdefault(row["date"], []).append(float(row["value"]))
    avg_y2 = {d: round(statistics.mean(v), 4) for d, v in avg_diff.items() if v}

    panel: list[dict[str, Any]] = []
    for row in futures_daily:
        d = row["date"]
        ff = value_as_of(dff, d)
        ry = value_as_of(real10, d)
        ad = value_as_of(avg_y2, d)
        if ff is None or ry is None or ad is None:
            continue
        panel.append(
            {
                "date": d,
                "close": row["close"],
                "fed_funds": ff,
                "real_yield_10y": ry,
                "avg_g10_2y_vs_usd": ad,
            }
        )
    return panel


def _drivers_from_panel(spec: FuturesSpec, panel: list[dict[str, Any]]) -> dict[str, float | None]:
    last = panel[-1] if panel else {}
    return {name: _num(last.get(name)) for name in spec.feature_names}


def _compute_instrument(spec: FuturesSpec, histories: dict[str, Any], generated_at: str) -> dict[str, Any]:
    futures_daily, price_meta = _load_futures_daily(spec.instrument_id)
    current, price_as_of = _current_futures_price(spec.instrument_id)
    missing, stale = _model_input_flags(spec)

    if spec.symbol == "DX":
        panel = _build_dx_panel(futures_daily, histories)
    else:
        panel = _build_g10_panel(spec, futures_daily, histories)

    reg = _ols_log_futures(panel, spec.feature_names)
    drivers = _drivers_from_panel(spec, panel)

    fair_value: float | None = None
    valuation_pct: float | None = None
    reconciled = False
    if reg.get("ok"):
        log_fv = _predict_log_fv(reg, drivers)
        if log_fv is not None:
            fair_value = round(math.exp(log_fv), 6 if spec.symbol != "DX" else 4)
            reconciled = _reconciles(reg, drivers, fair_value)
            if current is not None and fair_value:
                valuation_pct = round((current - fair_value) / fair_value * 100.0, 2)

    status = _determine_status(
        missing=missing,
        stale=stale,
        price_stale=_price_stale(price_as_of),
        reg_ok=bool(reg.get("ok")),
        n=int(reg.get("n") or 0),
        r2=_num(reg.get("r_squared")),
        reconciled=reconciled,
        panel_len=len(panel),
    )
    publish = status == "VALIDATED"
    if not publish:
        fair_value = None
        valuation_pct = None

    breakdown = (
        _build_breakdown(reg, drivers, fair_value)
        if publish and fair_value is not None
        else []
    )

    return _pack_ive(
        spec=spec,
        current=current,
        fair_value=fair_value,
        valuation_pct=valuation_pct,
        status=status,
        reg=reg,
        drivers=drivers,
        missing=missing,
        stale=stale,
        breakdown=breakdown,
        price_meta=price_meta,
        generated_at=generated_at,
        price_as_of=price_as_of,
        panel_len=len(panel),
    )


def _pack_ive(
    *,
    spec: FuturesSpec,
    current: float | None,
    fair_value: float | None,
    valuation_pct: float | None,
    status: str,
    reg: dict[str, Any],
    drivers: dict[str, Any],
    missing: list[str],
    stale: list[str],
    breakdown: list[dict[str, Any]],
    price_meta: dict[str, Any],
    generated_at: str,
    price_as_of: str | None,
    panel_len: int,
) -> dict[str, Any]:
    label = valuation_label_from_pct(valuation_pct) if fair_value is not None else "—"
    grade = valuation_grade_from_pct(valuation_pct) if valuation_pct is not None else "FAIR"
    lineage = _lineage_for_spec(spec, generated_at)

    ive = IVEOutput(
        instrument=spec.instrument_id,
        current_price=current,
        fair_value=fair_value,
        valuation_pct=valuation_pct,
        valuation_label=label,
        valuation_grade=grade,
        model_name=spec.model_name,
        source_names=[ln["source_name"] for ln in lineage],
        source_dates=[ln["source_date"] for ln in lineage],
        inputs={
            **{k: v for k, v in drivers.items() if v is not None},
            "futures_symbol": spec.symbol,
            "dependent_series": DEPENDENT_SERIES,
            "cot_market_code": spec.cot_market_code,
            "regression": reg,
            "panel_observations": panel_len,
            "_missing_inputs": missing,
            "_stale_inputs": stale,
            "price_history": price_meta,
        },
        calculation_breakdown=breakdown,
        last_updated=str(price_as_of or generated_at)[:10],
        model_status=status,
        source_lineage=lineage,
    )
    out = strip_confidence_fields(ive.to_dict())
    out.update(
        {
            "futures_symbol": spec.symbol,
            "dependent_series": DEPENDENT_SERIES,
            "model_family": spec.model_family,
            "model_id": spec.model_name,
            "wired": status == "VALIDATED",
            "legacy_pair_model_used": False,
            "legacy_fx_v3_used": False,
            "pair_derived": False,
        }
    )
    if spec.safe_haven:
        out["chf_dedicated_model"] = True
        out["usdchf_pair_model_used"] = False
    return out


def compute_futures_instrument(symbol: str, *, histories: dict[str, Any] | None = None) -> dict[str, Any]:
    spec = FUTURES_REGISTRY.get(symbol)
    if not spec:
        raise ValueError(f"Unknown futures symbol: {symbol}")
    histories = histories or currency_histories()
    generated_at = datetime.now(timezone.utc).isoformat()
    return _compute_instrument(spec, histories, generated_at)


def build_currency_futures_ive_export() -> dict[str, Any]:
    histories = currency_histories()
    generated_at = datetime.now(timezone.utc).isoformat()
    by_symbol: dict[str, Any] = {}
    by_instrument: dict[str, Any] = {}

    for symbol, spec in FUTURES_REGISTRY.items():
        block = compute_futures_instrument(symbol, histories=histories)
        by_symbol[symbol] = block
        by_instrument[block["instrument"]] = block

    for banned in BANNED_PAIR_SYMBOLS:
        if banned in by_symbol or banned in by_instrument:
            raise RuntimeError(f"Banned pair symbol in export: {banned}")

    for block in by_instrument.values():
        if block.get("legacy_fx_v3_used") or block.get("legacy_pair_model_used"):
            raise RuntimeError(f"Legacy FX model flagged on {block.get('futures_symbol')}")
        reg = (block.get("inputs") or {}).get("regression") or {}
        if reg.get("legacy_fixed_betas_used"):
            raise RuntimeError(f"Fixed legacy betas on {block.get('futures_symbol')}")

    return {
        "model_id": ENGINE_ID,
        "valuation_phase": VALUATION_PHASE,
        "generated_at": generated_at,
        "engine": ENGINE_ID,
        "note": "Futures-native IVE — OLS on continuous futures close; no pair fair value or fx_carry_real_yield_v3.",
        "roll_method_default": "oanda_backfill continuous (front-month implicit)",
        "symbols": list(FUTURES_REGISTRY.keys()),
        "instruments": by_instrument,
        "by_symbol": by_symbol,
    }


def write_currency_futures_ive_export() -> dict[str, Path]:
    doc = build_currency_futures_ive_export()
    PUBLIC_JSON.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_JSON.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    DATA_JSON.parent.mkdir(parents=True, exist_ok=True)
    DATA_JSON.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    return {"public_json": PUBLIC_JSON, "data_json": DATA_JSON}
