"""FX Valuation Data Foundation Audit — inputs for fx_carry_real_yield_v3.

Reports coverage, alignment, sourcing, and blockers before any valuation model
is promoted. Does not loosen audit gates or synthesize placeholder fair values.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal

from hptl.config import DATA_DIR, PROJECT_ROOT
from hptl.fx.currency_map import COT_CURRENCY_SOURCES
from hptl.fx.currency_rates import SUPPORTED_CURRENCIES, get_currency_rate
from hptl.fx.fred_inflation_adapter import CPI_FRED_SERIES
from hptl.fx.fx_macro_history import (
    CACHE_DIR,
    audit_g10_currency_legs,
    build_differential_series,
    currency_histories,
    ensure_fx_macro_caches,
    load_usd_combined_history,
)
from hptl.fx.fx_valuation import resolve_pair_currencies
from hptl.prices.canonical_timeline import load_canonical_timeline
from hptl.fx.fx_spot_history import get_daily_spot_series
from hptl.valuation.fx_carry_real_yield_v3 import (
    FX_V3_PAIRS,
    MIN_R_SQUARED,
    MIN_WEEKLY_OBS,
    MODEL_ID,
    _align_daily_panel,
    _value_as_of,
    compute_fx_pair_v3,
)

AUDIT_JSON = DATA_DIR / "audits" / "fx_valuation_data_foundation_audit.json"
AUDIT_MD = DATA_DIR / "audits" / "fx_valuation_data_foundation_audit.md"
PUBLIC_JSON = PROJECT_ROOT / "web-dashboard/public/data/fx_valuation_data_foundation_audit.json"

# Foundation bar — matches fx_carry_real_yield_v3 regression gate (not loosened).
MIN_ALIGNED_OBS = MIN_WEEKLY_OBS
MIN_SPOT_OBS = 252
YIELD_FRESHNESS_DAYS = 10
REFERENCE_LOOKBACK_DAYS = 365

AuditStatus = Literal["PASS", "FAIL"]

CURRENCY_SOURCES: dict[str, dict[str, str]] = {
    "USD": {
        "policy": "NY Fed EFFR (latest-only cache: usd_effr.txt)",
        "y2": "US Treasury daily par yield curve (usd_treasury.txt)",
        "y10": "US Treasury daily par yield curve (usd_treasury.txt)",
        "cpi": f"FRED {CPI_FRED_SERIES['USD']} (annual OECD CPI YoY)",
    },
    "EUR": {
        "policy": "ECB deposit facility rate (eur_dfr.txt)",
        "y2": "ECB euro area 2Y yield (eur_2y.txt)",
        "y10": "ECB euro area 10Y yield (eur_10y.txt)",
        "cpi": f"FRED {CPI_FRED_SERIES['EUR']} (annual OECD CPI YoY)",
    },
    "GBP": {
        "policy": "BoE Bank Rate IUDBEDR (gbp_bank_rate.txt)",
        "y2": "BoE GLC nominal spot curve (adapter: latest month only; no history loader)",
        "y10": "BoE GLC nominal spot curve (adapter: latest month only; no history loader)",
        "cpi": f"FRED {CPI_FRED_SERIES['GBP']} (annual OECD CPI YoY)",
    },
    "JPY": {
        "policy": "BIS WS_CBPOL deep history (bis_cbpol_jp_history.txt)",
        "y2": "Japan MoF JGB + FRED IR3TIB01JPM156N fallback (jpy_jgb.txt)",
        "y10": "Japan MoF JGB + FRED IRLTLT01JPM156N fallback (jpy_jgb.txt)",
        "cpi": f"FRED {CPI_FRED_SERIES['JPY']} (annual OECD CPI YoY)",
    },
    "CAD": {
        "policy": "Bank of Canada Valet (cad_valet.txt)",
        "y2": "Bank of Canada Valet 2Y (cad_valet.txt)",
        "y10": "Bank of Canada Valet 10Y (cad_valet.txt)",
        "cpi": f"FRED {CPI_FRED_SERIES['CAD']} (annual OECD CPI YoY)",
    },
    "AUD": {
        "policy": "RBA F1 cash rate (aud_f1.bin — latest only via adapter)",
        "y2": "RBA F2 AGB 2Y (aud_f2.bin — latest only via adapter)",
        "y10": "RBA F2 AGB 10Y (aud_f2.bin — latest only via adapter)",
        "cpi": f"FRED {CPI_FRED_SERIES['AUD']} (annual OECD CPI YoY)",
    },
    "NZD": {
        "policy": "BIS WS_CBPOL deep history (bis_cbpol_nz_history.txt)",
        "y2": "FRED IR3TIB01NZM156N (OECD short-term daily)",
        "y10": "FRED IRLTLT01NZM156N (OECD long-term daily)",
        "cpi": f"FRED {CPI_FRED_SERIES['NZD']} (annual OECD CPI YoY)",
    },
    "CHF": {
        "policy": "BIS WS_CBPOL deep history (bis_cbpol_ch_history.txt)",
        "y2": "SNB rendoblid cube (chf_rendoblid.bin)",
        "y10": "SNB rendoblid cube (chf_rendoblid.bin)",
        "cpi": f"FRED {CPI_FRED_SERIES['CHF']} (annual OECD CPI YoY)",
    },
}

GLOBAL_SOURCES = {
    "dxy": "Canonical timeline: US Dollar Index / DX (price_store + timeline)",
    "treasury_regime": "US Treasury 2s10s slope from live USD y2/y10 adapters",
}


def _today() -> date:
    return date.today()


def _parse_date(iso: str | None) -> date | None:
    if not iso:
        return None
    try:
        return date.fromisoformat(str(iso)[:10])
    except ValueError:
        return None


def _business_days_between(start: date, end: date) -> int:
    if end < start:
        return 0
    days = 0
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            days += 1
        cur += timedelta(days=1)
    return days


def _sorted_dates(mapping: dict[str, float]) -> list[str]:
    return sorted(mapping.keys())


def _audit_daily_series(
    *,
    input_name: str,
    currency: str,
    series: dict[str, float],
    source: str,
    update_frequency: str,
    reference_start: date,
    reference_end: date,
    min_obs: int,
    freshness_days: int | None = None,
) -> dict[str, Any]:
    dates = _sorted_dates(series)
    earliest = dates[0] if dates else None
    latest = dates[-1] if dates else None
    count = len(dates)

    ref_bdays = _business_days_between(reference_start, reference_end) or 1
    in_window = [d for d in dates if reference_start.isoformat() <= d <= reference_end.isoformat()]
    coverage_pct = round(len(in_window) / ref_bdays * 100.0, 1)

    missing_periods: list[str] = []
    if not dates:
        missing_periods.append(f"Entire reference window {reference_start} → {reference_end}")
    else:
        ed = _parse_date(earliest)
        sd = _parse_date(latest)
        if ed and ed > reference_start:
            missing_periods.append(f"History starts {earliest}; need from {reference_start.isoformat()}")
        if sd and sd < reference_end - timedelta(days=(freshness_days or 10)):
            missing_periods.append(f"Latest observation {latest} is stale vs reference end {reference_end.isoformat()}")
        if count < min_obs:
            missing_periods.append(f"Only {count} observations; model requires ≥ {min_obs}")

    stale = False
    if freshness_days and latest:
        ld = _parse_date(latest)
        if ld and (_today() - ld).days > freshness_days:
            stale = True
            missing_periods.append(f"Last update {latest} exceeds {freshness_days}d freshness window")

    audit_status: AuditStatus = "PASS"
    if count < min_obs or coverage_pct < 50.0 or stale or not dates:
        audit_status = "FAIL"

    return {
        "input": input_name,
        "currency": currency,
        "earliest_date": earliest,
        "latest_date": latest,
        "observation_count": count,
        "coverage_pct": coverage_pct,
        "reference_window": f"{reference_start.isoformat()} → {reference_end.isoformat()}",
        "missing_periods": missing_periods,
        "source": source,
        "update_frequency": update_frequency,
        "audit_status": audit_status,
    }


def _audit_cpi_point(currency: str, reference_end: date) -> dict[str, Any]:
    rec = get_currency_rate(currency)
    source = CURRENCY_SOURCES.get(currency, {}).get("cpi", "FRED OECD CPI YoY")
    as_of = rec.cpi_yoy_as_of
    val = rec.cpi_yoy
    missing: list[str] = []
    if val is None:
        missing.append("No current CPI YoY on currency rate record")
    if as_of is None:
        missing.append("CPI as_of missing")
    else:
        ad = _parse_date(as_of)
        if ad and (reference_end - ad).days > 400:
            missing.append(f"CPI as_of {as_of} older than 400d annual window")

    audit_status: AuditStatus = "PASS" if val is not None and not missing else "FAIL"
    return {
        "input": "cpi_yoy",
        "currency": currency,
        "earliest_date": as_of,
        "latest_date": as_of,
        "observation_count": 1 if val is not None else 0,
        "coverage_pct": 100.0 if val is not None else 0.0,
        "reference_window": f"point-in-time as of {reference_end.isoformat()}",
        "missing_periods": missing,
        "source": source,
        "update_frequency": "annual (OECD harmonized CPI YoY via FRED; no daily history in pipeline)",
        "audit_status": audit_status,
        "value": val,
    }


def _audit_real_yield_point(currency: str, reference_end: date) -> dict[str, Any]:
    rec = get_currency_rate(currency)
    y2 = rec.y2
    cpi = rec.cpi_yoy
    real = round(y2 - cpi, 3) if y2 is not None and cpi is not None else None
    missing: list[str] = []
    if y2 is None:
        missing.append("Current 2Y yield missing — cannot derive real yield")
    if cpi is None:
        missing.append("Current CPI YoY missing — cannot derive real yield")
    audit_status: AuditStatus = "PASS" if real is not None else "FAIL"
    return {
        "input": "real_yield",
        "currency": currency,
        "earliest_date": rec.y2_as_of or rec.cpi_yoy_as_of,
        "latest_date": rec.y2_as_of or rec.cpi_yoy_as_of,
        "observation_count": 1 if real is not None else 0,
        "coverage_pct": 100.0 if real is not None else 0.0,
        "reference_window": f"derived point as of {reference_end.isoformat()}",
        "missing_periods": missing,
        "source": "derived: 2Y yield − CPI YoY (current adapters)",
        "update_frequency": "point-in-time (not a historical series in fx_carry_real_yield_v3)",
        "audit_status": audit_status,
        "value": real,
    }


def _audit_dxy(reference_start: date, reference_end: date) -> dict[str, Any]:
    tl = load_canonical_timeline("US Dollar Index / DX")
    if not tl:
        return {
            "input": "dxy",
            "currency": "USD",
            "earliest_date": None,
            "latest_date": None,
            "observation_count": 0,
            "coverage_pct": 0.0,
            "reference_window": f"{reference_start.isoformat()} → {reference_end.isoformat()}",
            "missing_periods": ["Canonical DXY timeline unavailable"],
            "source": GLOBAL_SOURCES["dxy"],
            "update_frequency": "daily",
            "audit_status": "FAIL",
        }
    closes = tl.daily_closes()
    dates = [d for d, _ in closes if reference_start.isoformat() <= d <= reference_end.isoformat()]
    all_dates = [d for d, _ in closes]
    ref_bdays = _business_days_between(reference_start, reference_end) or 1
    coverage = round(len(dates) / ref_bdays * 100.0, 1)
    audit_status: AuditStatus = "PASS" if len(all_dates) >= MIN_SPOT_OBS and tl.date_end else "FAIL"
    return {
        "input": "dxy",
        "currency": "USD",
        "earliest_date": tl.date_start,
        "latest_date": tl.date_end,
        "observation_count": len(all_dates),
        "coverage_pct": coverage,
        "reference_window": f"{reference_start.isoformat()} → {reference_end.isoformat()}",
        "missing_periods": [] if audit_status == "PASS" else ["DXY history shorter than 252d or timeline gap"],
        "source": tl.canonical_source or GLOBAL_SOURCES["dxy"],
        "update_frequency": "daily",
        "audit_status": audit_status,
        "regime_note": "DXY 52w percentile used for regime tilt only — not a valuation percentile score",
    }


def _audit_us_treasury(reference_start: date, reference_end: date) -> dict[str, Any]:
    usd = load_usd_combined_history()
    y2 = usd.get("y2") or {}
    y10 = usd.get("y10") or {}
    y2_audit = _audit_daily_series(
        input_name="us_treasury_y2",
        currency="USD",
        series=y2,
        source=CURRENCY_SOURCES["USD"]["y2"],
        update_frequency="daily",
        reference_start=reference_start,
        reference_end=reference_end,
        min_obs=MIN_SPOT_OBS,
        freshness_days=YIELD_FRESHNESS_DAYS,
    )
    y10_audit = _audit_daily_series(
        input_name="us_treasury_y10",
        currency="USD",
        series=y10,
        source=CURRENCY_SOURCES["USD"]["y10"],
        update_frequency="daily",
        reference_start=reference_start,
        reference_end=reference_end,
        min_obs=MIN_SPOT_OBS,
        freshness_days=YIELD_FRESHNESS_DAYS,
    )
    slope_ok = bool(y2 and y10)
    regime_status: AuditStatus = "PASS" if slope_ok and y2_audit["audit_status"] == "PASS" else "FAIL"
    return {
        "input": "treasury_regime",
        "currency": "USD",
        "earliest_date": y2_audit.get("earliest_date"),
        "latest_date": y2_audit.get("latest_date"),
        "observation_count": min(y2_audit["observation_count"], y10_audit["observation_count"]),
        "coverage_pct": min(y2_audit["coverage_pct"], y10_audit["coverage_pct"]),
        "reference_window": y2_audit["reference_window"],
        "missing_periods": list(set(y2_audit["missing_periods"] + y10_audit["missing_periods"])),
        "source": GLOBAL_SOURCES["treasury_regime"],
        "update_frequency": "daily (2s10s slope for regime adjustment)",
        "audit_status": regime_status,
        "y2_detail": y2_audit,
        "y10_detail": y10_audit,
    }


def _spot_audit_summary(pair_id: str, reference_start: date, reference_end: date) -> dict[str, Any]:
    resolved = resolve_pair_currencies(pair_id)
    if not resolved:
        return {"audit_status": "FAIL", "summary": "Unsupported pair", "observation_count": 0}
    _base, _quote, canonical = resolved
    daily, spot_meta = get_daily_spot_series(pair_id)
    dates = [r["date"] for r in daily]
    count = len(dates)
    in_window = [d for d in dates if reference_start.isoformat() <= d <= reference_end.isoformat()]
    ref_bdays = _business_days_between(reference_start, reference_end) or 1
    coverage = round(len(in_window) / ref_bdays * 100.0, 1)
    missing: list[str] = []
    if count == 0:
        missing.append("No canonical daily spot — cross pair may need USD-leg synthesis")
    elif count < MIN_SPOT_OBS:
        missing.append(f"Only {count} spot days; foundation expects ≥ {MIN_SPOT_OBS}")
    store_keys = [canonical, pair_id]
    for _code, spec in COT_CURRENCY_SOURCES.items():
        if str(spec.get("quote")) == canonical:
            store_keys.append(str(spec.get("market")))
    audit_status: AuditStatus = "PASS" if count >= MIN_SPOT_OBS else "FAIL"
    return {
        "audit_status": audit_status,
        "summary": f"{'PASS' if audit_status == 'PASS' else 'FAIL'} ({count}d, {coverage}% cov)",
        "observation_count": count,
        "earliest_date": dates[0] if dates else None,
        "latest_date": dates[-1] if dates else None,
        "coverage_pct": coverage,
        "missing_periods": missing,
        "source": f"price_store daily via keys {store_keys[:3]}",
        "update_frequency": "daily",
        "canonical": canonical,
    }


def _leg_histories(histories: dict[str, dict[str, Any]], ccy: str, field: str) -> dict[str, float]:
    return dict((histories.get(ccy) or {}).get(field) or {})


def _pair_policy_audit(
    pair_id: str,
    base: str,
    quote: str,
    histories: dict[str, dict[str, Any]],
    spot_dates: list[str],
) -> dict[str, Any]:
    base_map = _leg_histories(histories, base, "policy")
    quote_map = _leg_histories(histories, quote, "policy")
    br = get_currency_rate(base)
    qr = get_currency_rate(quote)
    if br.policy_rate is not None and br.policy_rate_as_of:
        base_map[str(br.policy_rate_as_of)[:10]] = float(br.policy_rate)
    if qr.policy_rate is not None and qr.policy_rate_as_of:
        quote_map[str(qr.policy_rate_as_of)[:10]] = float(qr.policy_rate)

    aligned = 0
    for d in spot_dates:
        bp = _value_as_of(base_map, d)
        qp = _value_as_of(quote_map, d)
        if bp is not None and qp is not None:
            aligned += 1

    base_audit = _audit_daily_series(
        input_name="policy_rate",
        currency=base,
        series=base_map,
        source=CURRENCY_SOURCES.get(base, {}).get("policy", "unknown"),
        update_frequency="step (central bank decisions)",
        reference_start=_parse_date(spot_dates[0]) or _today() - timedelta(days=REFERENCE_LOOKBACK_DAYS),
        reference_end=_parse_date(spot_dates[-1]) or _today(),
        min_obs=1,
    )
    quote_audit = _audit_daily_series(
        input_name="policy_rate",
        currency=quote,
        series=quote_map,
        source=CURRENCY_SOURCES.get(quote, {}).get("policy", "unknown"),
        update_frequency="step (central bank decisions)",
        reference_start=_parse_date(spot_dates[0]) or _today() - timedelta(days=REFERENCE_LOOKBACK_DAYS),
        reference_end=_parse_date(spot_dates[-1]) or _today(),
        min_obs=1,
    )

    missing: list[str] = []
    if base_audit["observation_count"] == 0:
        missing.append(f"{base} policy history empty")
    if quote_audit["observation_count"] == 0:
        missing.append(f"{quote} policy history empty")
    if aligned < MIN_ALIGNED_OBS:
        missing.append(
            f"Policy differential alignable on {aligned} spot days; model requires ≥ {MIN_ALIGNED_OBS}"
        )

    audit_status: AuditStatus = "PASS" if aligned >= MIN_ALIGNED_OBS and not missing[:2] else "FAIL"
    return {
        "audit_status": audit_status,
        "summary": f"{'PASS' if audit_status == 'PASS' else 'FAIL'} (align {aligned}d; {base} {base_audit['observation_count']} / {quote} {quote_audit['observation_count']} steps)",
        "aligned_days": aligned,
        "base": base_audit,
        "quote": quote_audit,
        "missing_periods": missing,
    }


def _pair_yield_audit(
    pair_id: str,
    base: str,
    quote: str,
    histories: dict[str, dict[str, Any]],
    spot_dates: list[str],
) -> dict[str, Any]:
    base_y2 = _leg_histories(histories, base, "y2")
    quote_y2 = _leg_histories(histories, quote, "y2")

    intersection = build_differential_series(base, quote, "y2", histories)
    intersection_count = len(intersection)

    asof_aligned = 0
    for d in spot_dates:
        bv = _value_as_of(base_y2, d)
        qv = _value_as_of(quote_y2, d)
        if bv is not None and qv is not None:
            asof_aligned += 1

    ref_start = _parse_date(spot_dates[0]) or _today() - timedelta(days=REFERENCE_LOOKBACK_DAYS)
    ref_end = _parse_date(spot_dates[-1]) or _today()

    base_audit = _audit_daily_series(
        input_name="yield_2y",
        currency=base,
        series=base_y2,
        source=CURRENCY_SOURCES.get(base, {}).get("y2", "unknown"),
        update_frequency="daily",
        reference_start=ref_start,
        reference_end=ref_end,
        min_obs=MIN_ALIGNED_OBS,
        freshness_days=YIELD_FRESHNESS_DAYS,
    )
    quote_audit = _audit_daily_series(
        input_name="yield_2y",
        currency=quote,
        series=quote_y2,
        source=CURRENCY_SOURCES.get(quote, {}).get("y2", "unknown"),
        update_frequency="daily",
        reference_start=ref_start,
        reference_end=ref_end,
        min_obs=MIN_ALIGNED_OBS,
        freshness_days=YIELD_FRESHNESS_DAYS,
    )

    missing: list[str] = []
    if base_audit["audit_status"] == "FAIL":
        missing.append(f"{base} 2Y: {base_audit['observation_count']} obs ({base_audit['earliest_date']} → {base_audit['latest_date']})")
    if quote_audit["audit_status"] == "FAIL":
        missing.append(f"{quote} 2Y: {quote_audit['observation_count']} obs ({quote_audit['earliest_date']} → {quote_audit['latest_date']})")
    if intersection_count < MIN_ALIGNED_OBS:
        missing.append(
            f"Date-intersection y2 differential only {intersection_count} days — alignment bottleneck"
        )
    if asof_aligned < MIN_ALIGNED_OBS:
        missing.append(
            f"As-of y2 differential on spot days only {asof_aligned}; model requires ≥ {MIN_ALIGNED_OBS}"
        )

    # Model uses build_differential_series intersection mapped as-of — effective panel capped by intersection.
    effective = min(asof_aligned, intersection_count if intersection_count else asof_aligned)
    audit_status: AuditStatus = "PASS" if effective >= MIN_ALIGNED_OBS else "FAIL"

    return {
        "audit_status": audit_status,
        "summary": (
            f"{'PASS' if audit_status == 'PASS' else 'FAIL'} "
            f"(intersect {intersection_count}d, as-of {asof_aligned}d; "
            f"{base} {base_audit['observation_count']}d / {quote} {quote_audit['observation_count']}d)"
        ),
        "intersection_days": intersection_count,
        "asof_aligned_days": asof_aligned,
        "base": base_audit,
        "quote": quote_audit,
        "missing_periods": missing,
        "alignment_issue": intersection_count < asof_aligned,
    }


def _pair_cpi_audit(base: str, quote: str, reference_end: date) -> dict[str, Any]:
    ba = _audit_cpi_point(base, reference_end)
    qa = _audit_cpi_point(quote, reference_end)
    audit_status: AuditStatus = "PASS" if ba["audit_status"] == "PASS" and qa["audit_status"] == "PASS" else "FAIL"
    missing = ba["missing_periods"] + qa["missing_periods"]
    return {
        "audit_status": audit_status,
        "summary": f"{'PASS' if audit_status == 'PASS' else 'FAIL'} ({base} {ba.get('value')} / {quote} {qa.get('value')})",
        "base": ba,
        "quote": qa,
        "missing_periods": missing,
    }


def _pair_real_yield_audit(base: str, quote: str, reference_end: date) -> dict[str, Any]:
    ba = _audit_real_yield_point(base, reference_end)
    qa = _audit_real_yield_point(quote, reference_end)
    audit_status: AuditStatus = "PASS" if ba["audit_status"] == "PASS" and qa["audit_status"] == "PASS" else "FAIL"
    return {
        "audit_status": audit_status,
        "summary": f"{'PASS' if audit_status == 'PASS' else 'FAIL'} ({base} {ba.get('value')} / {quote} {qa.get('value')})",
        "base": ba,
        "quote": qa,
        "missing_periods": ba["missing_periods"] + qa["missing_periods"],
    }


def _classify_root_cause(pair_block: dict[str, Any]) -> str:
    """A=missing, B=misaligned, C=sourcing, D=formula."""
    yield_blk = pair_block.get("yield_history") or {}
    if yield_blk.get("alignment_issue"):
        return "B — Misaligned history (intersection << as-of potential)"
    missing = pair_block.get("v3_blocker", {}).get("missing_inputs") or []
    if any("y2" in m or "policy" in m for m in missing):
        return "A — Missing history"
    spot = pair_block.get("spot_history") or {}
    if spot.get("observation_count", 0) == 0:
        return "A — Missing history (spot)"
    reg = pair_block.get("v3_blocker", {}).get("regression") or {}
    if reg.get("n", 0) >= MIN_ALIGNED_OBS and reg.get("r_squared") is not None and reg["r_squared"] < MIN_R_SQUARED:
        return "D — Formula / model fit (R² below gate with sufficient data)"
    if pair_block.get("overall_status") == "FAIL":
        return "A — Missing history"
    return "C — Sourcing (partial feeds / shallow caches)"


def _diagnose_v3_blocker(pair_id: str, histories: dict[str, dict[str, Any]]) -> dict[str, Any]:
    result = compute_fx_pair_v3(pair_id, histories=histories)
    panel = _align_daily_panel(pair_id, result.base, result.quote, histories)
    blockers: list[str] = []
    if result.spot_price is None:
        blockers.append("spot_price missing")
    if result.missing_inputs:
        blockers.extend(result.missing_inputs)
    reg = result.regression or {}
    if reg.get("n", 0) < MIN_ALIGNED_OBS:
        blockers.append(f"aligned panel n={reg.get('n')} < {MIN_ALIGNED_OBS}")
    if reg.get("r_squared") is not None and reg["r_squared"] < MIN_R_SQUARED:
        blockers.append(f"R²={reg.get('r_squared')} < {MIN_R_SQUARED}")
    if result.fair_value is None:
        blockers.append("fair_value not computed")
    if result.confidence == "None":
        blockers.append("confidence None — audit gate blocks wiring")

    category = "D — Formula issue" if (
        reg.get("n", 0) >= MIN_ALIGNED_OBS and reg.get("r_squared") is not None and reg["r_squared"] < MIN_R_SQUARED
    ) else "A — Missing history"
    if any("align" in b.lower() or "intersection" in b.lower() for b in blockers):
        category = "B — Misaligned history"

    return {
        "model_id": MODEL_ID,
        "audit_status": result.audit_status,
        "fair_value": result.fair_value,
        "aligned_panel_days": len(panel),
        "regression_n": reg.get("n"),
        "regression_r_squared": reg.get("r_squared"),
        "missing_inputs": result.missing_inputs,
        "blockers": blockers,
        "primary_category": category,
    }


def audit_currency_inputs(reference_end: date | None = None) -> dict[str, Any]:
    reference_end = reference_end or _today()
    reference_start = reference_end - timedelta(days=REFERENCE_LOOKBACK_DAYS)
    histories = currency_histories()
    rows: dict[str, Any] = {}

    for ccy in SUPPORTED_CURRENCIES:
        h = histories.get(ccy) or {}
        rows[ccy] = {
            "policy": _audit_daily_series(
                input_name="policy_rate",
                currency=ccy,
                series=dict(h.get("policy") or {}),
                source=CURRENCY_SOURCES.get(ccy, {}).get("policy", "unknown"),
                update_frequency="step",
                reference_start=reference_start,
                reference_end=reference_end,
                min_obs=1 if ccy != "USD" else 1,
            ),
            "yield_2y": _audit_daily_series(
                input_name="yield_2y",
                currency=ccy,
                series=dict(h.get("y2") or {}),
                source=CURRENCY_SOURCES.get(ccy, {}).get("y2", "unknown"),
                update_frequency="daily",
                reference_start=reference_start,
                reference_end=reference_end,
                min_obs=MIN_ALIGNED_OBS,
                freshness_days=YIELD_FRESHNESS_DAYS,
            ),
            "yield_10y": _audit_daily_series(
                input_name="yield_10y",
                currency=ccy,
                series=dict(h.get("y10") or {}),
                source=CURRENCY_SOURCES.get(ccy, {}).get("y10", "unknown"),
                update_frequency="daily",
                reference_start=reference_start,
                reference_end=reference_end,
                min_obs=MIN_ALIGNED_OBS,
                freshness_days=YIELD_FRESHNESS_DAYS,
            ),
            "cpi": _audit_cpi_point(ccy, reference_end),
            "real_yield": _audit_real_yield_point(ccy, reference_end),
        }

    return {
        "reference_window": f"{reference_start.isoformat()} → {reference_end.isoformat()}",
        "currencies": rows,
    }


def audit_pair(pair_id: str, histories: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    histories = histories or currency_histories()
    resolved = resolve_pair_currencies(pair_id)
    if not resolved:
        return {"pair": pair_id, "overall_status": "FAIL", "reason": "Unsupported pair"}

    base, quote, _canonical = resolved
    reference_end = _today()
    reference_start = reference_end - timedelta(days=REFERENCE_LOOKBACK_DAYS)

    spot = _spot_audit_summary(pair_id, reference_start, reference_end)
    spot_dates = [r["date"] for r in get_daily_spot_series(pair_id)[0]]
    if not spot_dates:
        spot_dates = [
            (reference_end - timedelta(days=i)).isoformat()
            for i in range(REFERENCE_LOOKBACK_DAYS, 0, -1)
        ]

    policy = _pair_policy_audit(pair_id, base, quote, histories, spot_dates)
    yield_hist = _pair_yield_audit(pair_id, base, quote, histories, spot_dates)
    cpi = _pair_cpi_audit(base, quote, reference_end)
    real_y = _pair_real_yield_audit(base, quote, reference_end)
    v3 = _diagnose_v3_blocker(pair_id, histories)

    statuses = [
        spot["audit_status"],
        policy["audit_status"],
        yield_hist["audit_status"],
        cpi["audit_status"],
        real_y["audit_status"],
    ]
    overall: AuditStatus = "PASS" if all(s == "PASS" for s in statuses) and v3["audit_status"] == "PASS" else "FAIL"

    block = {
        "pair": pair_id,
        "base": base,
        "quote": quote,
        "spot_history": spot,
        "policy_history": policy,
        "yield_history": yield_hist,
        "cpi_history": cpi,
        "real_yield_history": real_y,
        "v3_blocker": v3,
        "overall_status": overall,
        "root_cause": _classify_root_cause(
            {
                "spot_history": spot,
                "yield_history": yield_hist,
                "v3_blocker": v3,
                "overall_status": overall,
            }
        ),
    }
    return block


def _missing_data_report(
    currency_audit: dict[str, Any],
    pair_audits: list[dict[str, Any]],
    global_audit: dict[str, Any],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []

    for ccy, blocks in (currency_audit.get("currencies") or {}).items():
        for key in ("policy", "yield_2y", "cpi", "real_yield"):
            blk = blocks.get(key) or {}
            if blk.get("audit_status") == "FAIL":
                items.append(
                    {
                        "scope": ccy,
                        "input": key,
                        "issue": "; ".join(blk.get("missing_periods") or ["audit FAIL"]),
                        "source": blk.get("source"),
                        "observation_count": blk.get("observation_count"),
                    }
                )

    for gkey, gblk in global_audit.items():
        if gblk.get("audit_status") == "FAIL":
            items.append(
                {
                    "scope": "GLOBAL",
                    "input": gkey,
                    "issue": "; ".join(gblk.get("missing_periods") or ["audit FAIL"]),
                    "source": gblk.get("source"),
                    "observation_count": gblk.get("observation_count"),
                }
            )

    for pblk in pair_audits:
        if pblk.get("overall_status") == "PASS":
            continue
        items.append(
            {
                "scope": pblk["pair"],
                "input": "pair_foundation",
                "issue": "; ".join(pblk.get("v3_blocker", {}).get("blockers") or []),
                "source": MODEL_ID,
                "observation_count": pblk.get("v3_blocker", {}).get("aligned_panel_days"),
                "root_cause": pblk.get("root_cause"),
            }
        )

    return items


def _recommended_fix_order(
    missing: list[dict[str, Any]],
    pair_audits: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Ordered fixes — data foundation only, no model changes."""
    fixes: list[dict[str, Any]] = []

    def add(priority: int, fix: str, impact: str, pairs: list[str]) -> None:
        fixes.append({"priority": priority, "fix": fix, "impact": impact, "pairs_unblocked": pairs})

    # P1 — USD yield depth (blocks all USD crosses)
    usd_y2_shallow = any(
        m.get("scope") == "USD" and m.get("input") == "yield_2y" for m in missing
    )
    if usd_y2_shallow:
        add(
            1,
            "Deepen USD 2Y/10Y history: merge multi-year Treasury CSV caches (2016–present) or FRED DGS2/DGS10 daily series into currency_histories()",
            "Unblocks every XXX/USD and USD/XXX pair regression alignment",
            list(FX_V3_PAIRS),
        )

    # P2 — Missing leg 2Y loaders
    for ccy in ("GBP", "AUD", "NZD", "CHF"):
        if any(m.get("scope") == ccy and "yield" in str(m.get("input")) for m in missing):
            add(
                2,
                f"Add daily {ccy} 2Y history loader (central-bank archive or FRED sovereign yield series) — adapter currently latest-only",
                f"Unblocks {ccy} pairs",
                [p for p in FX_V3_PAIRS if ccy in p],
            )

    # P3 — JPY shallow JGB cache
    if any(m.get("scope") == "JPY" and m.get("input") == "yield_2y" for m in missing):
        add(
            3,
            "Fetch full MoF JGB CSV history (not recent slice) into jpy_jgb.txt cache",
            "Unblocks USD/JPY and JPY crosses",
            [p for p in FX_V3_PAIRS if "JPY" in p],
        )

    # P4 — USD policy history
    if any(m.get("scope") == "USD" and m.get("input") == "policy" for m in missing):
        add(
            4,
            "Add USD policy step history (FRED DFF / EFFR daily archive) — currently latest-only EFFR",
            "Improves policy differential alignment on historical spot dates",
            list(FX_V3_PAIRS),
        )

    # P5 — Cross spot synthesis history
    cross_spot = any(
        p.get("spot_history", {}).get("observation_count", 0) == 0
        for p in pair_audits
    )
    if not cross_spot:
        for p in missing:
            if p.get("scope") in FX_V3_PAIRS and "spot" in str(p.get("issue", "")).lower():
                cross_spot = True
                break
    if cross_spot:
        add(
            5,
            "Build daily cross spot series from USD leg synthesis (EUR/JPY = EUR/USD × USD/JPY) for pairs without direct price_store keys",
            "Unblocks cross pairs without direct OANDA/store instruments",
            [p for p in FX_V3_PAIRS if p.count("/") and p.split("/")[0] != "USD" and p.split("/")[1] != "USD"],
        )

    # P6 — Alignment method (data pipeline, not formula)
    if any("intersection" in str(m.get("issue", "")).lower() for m in missing):
        add(
            6,
            "Replace date-intersection yield merge with per-leg as-of alignment in regression panel (after histories exist)",
            "Recovers aligned days when leg calendars differ — data alignment fix, not audit loosening",
            list(FX_V3_PAIRS),
        )

    fixes.sort(key=lambda x: x["priority"])
    return fixes


def _estimated_confidence_after_fixes(fixes: list[dict[str, Any]], pair_audits: list[dict[str, Any]]) -> dict[str, Any]:
    """Honest confidence estimate once foundation fixes land — not current state."""
    pass_count = sum(1 for p in pair_audits if p.get("overall_status") == "PASS")
    if pass_count == len(FX_V3_PAIRS):
        level = "High on all 13 pairs (subject to live R² gate)"
    elif pass_count >= 7:
        level = "Medium on major USD pairs; Low on crosses until cross-spot + JPY depth fixed"
    elif any(f["priority"] == 1 for f in fixes):
        level = (
            "After P1 USD yield depth: Medium on EUR/USD, USD/CAD; Low on GBP/AUD/NZD/CHF until leg loaders added; "
            "None on crosses until spot synthesis"
        )
    else:
        level = "Low — multiple leg histories still missing"

    return {
        "current_pairs_passing_foundation": pass_count,
        "total_pairs": len(FX_V3_PAIRS),
        "estimated_confidence_after_p1_p4": level,
        "note": (
            "Confidence refers to fx_carry_real_yield_v3 audit gate (≥52 aligned obs, R²≥0.08, no missing core inputs). "
            "CPI remains point-in-time annual — fair-value level adjustment only, not a regression series."
        ),
    }


def run_fx_valuation_data_foundation_audit(*, refresh_caches: bool = True) -> dict[str, Any]:
    if refresh_caches:
        ensure_fx_macro_caches()
    reference_end = _today()
    reference_start = reference_end - timedelta(days=REFERENCE_LOOKBACK_DAYS)
    histories = currency_histories()

    currency_audit = audit_currency_inputs(reference_end)
    global_audit = {
        "dxy": _audit_dxy(reference_start, reference_end),
        "treasury_regime": _audit_us_treasury(reference_start, reference_end),
    }
    pair_audits = [audit_pair(pid, histories) for pid in FX_V3_PAIRS]
    missing = _missing_data_report(currency_audit, pair_audits, global_audit)
    fixes = _recommended_fix_order(missing, pair_audits)
    confidence = _estimated_confidence_after_fixes(fixes, pair_audits)

    v3_summary = {
        "model_id": MODEL_ID,
        "pairs_with_fair_value": sum(1 for p in pair_audits if p.get("v3_blocker", {}).get("fair_value") is not None),
        "pairs_passing_v3_audit": sum(1 for p in pair_audits if p.get("v3_blocker", {}).get("audit_status") == "PASS"),
        "primary_blocker": (
            "Missing/shallow macro yield history and date-intersection alignment — not valuation formula logic"
            if not any(p.get("v3_blocker", {}).get("audit_status") == "PASS" for p in pair_audits)
            or sum(1 for p in pair_audits if p.get("v3_blocker", {}).get("audit_status") == "PASS") < len(FX_V3_PAIRS)
            else "Partial — review per-pair root_cause"
        ),
        "root_cause_breakdown": {
            "A_missing_history": sum(1 for p in pair_audits if str(p.get("root_cause", "")).startswith("A")),
            "B_misaligned_history": sum(1 for p in pair_audits if str(p.get("root_cause", "")).startswith("B")),
            "C_sourcing": sum(1 for p in pair_audits if str(p.get("root_cause", "")).startswith("C")),
            "D_formula": sum(1 for p in pair_audits if str(p.get("root_cause", "")).startswith("D")),
        },
    }

    g10_table = audit_g10_currency_legs(reference_end)

    return {
        "audit_type": "fx_valuation_data_foundation",
        "model_target": MODEL_ID,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "reference_window_days": REFERENCE_LOOKBACK_DAYS,
        "gates": {
            "min_aligned_obs": MIN_ALIGNED_OBS,
            "min_r_squared": MIN_R_SQUARED,
            "min_spot_obs": MIN_SPOT_OBS,
            "note": "Gates mirror fx_carry_real_yield_v3 — not loosened for this audit",
        },
        "cache_dir": str(CACHE_DIR),
        "g10_currency_table": g10_table,
        "currency_inputs": currency_audit,
        "global_inputs": global_audit,
        "pairs": {p["pair"]: p for p in pair_audits},
        "summary_table": [
            {
                "instrument": p["pair"],
                "spot_history": p["spot_history"]["summary"],
                "policy_history": p["policy_history"]["summary"],
                "yield_history": p["yield_history"]["summary"],
                "cpi_history": p["cpi_history"]["summary"],
                "real_yield_history": p["real_yield_history"]["summary"],
                "pass_fail": p["overall_status"],
            }
            for p in pair_audits
        ],
        "missing_data_report": missing,
        "recommended_fix_order": fixes,
        "estimated_confidence_after_fixes": confidence,
        "fx_carry_real_yield_v3_diagnosis": v3_summary,
        "verdict": {
            "valuation_problem_is": "A — Missing history (primary) with B — Misaligned history (secondary)",
            "formula_issue": sum(1 for p in pair_audits if str(p.get("root_cause", "")).startswith("D")) == 0,
            "proceed_with_model_development": False,
            "note": "Do not wire valuation to dashboard until data foundation PASS and fx_carry_real_yield_v3 audit PASS.",
        },
    }


def _md_g10_table(rows: list[dict[str, Any]]) -> str:
    lines = [
        "| Currency | Policy obs | 2Y obs | 10Y obs | CPI obs | Real yield obs | Earliest | Latest | PASS/FAIL |",
        "|---|---:|---:|---:|---:|---:|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r['currency']} | {r['policy_obs']} | {r['yield_2y_obs']} | {r['yield_10y_obs']} | "
            f"{r['cpi_obs']} | {r['real_yield_obs']} | {r.get('earliest') or '—'} | {r.get('latest') or '—'} | "
            f"**{r['pass_fail']}** |"
        )
    return "\n".join(lines)


def _md_summary_table(rows: list[dict[str, Any]]) -> str:
    lines = [
        "| Instrument | Spot History | Policy History | Yield History | CPI History | Real Yield History | PASS/FAIL |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r['instrument']} | {r['spot_history']} | {r['policy_history']} | {r['yield_history']} | "
            f"{r['cpi_history']} | {r['real_yield_history']} | **{r['pass_fail']}** |"
        )
    return "\n".join(lines)


def _md_input_detail(title: str, blocks: list[dict[str, Any]]) -> str:
    lines = [f"### {title}", ""]
    for b in blocks:
        lines.append(
            f"- **{b.get('currency', b.get('input', '?'))} / {b.get('input', '')}**: "
            f"{b.get('audit_status')} — {b.get('observation_count', 0)} obs, "
            f"{b.get('earliest_date') or '—'} → {b.get('latest_date') or '—'}, "
            f"coverage {b.get('coverage_pct', 0)}%, source: {b.get('source', '—')}"
        )
        if b.get("missing_periods"):
            lines.append(f"  - Missing: {'; '.join(b['missing_periods'][:2])}")
    return "\n".join(lines)


def write_fx_valuation_data_foundation_audit(report: dict[str, Any] | None = None) -> dict[str, str]:
    report = report or run_fx_valuation_data_foundation_audit()
    AUDIT_JSON.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    PUBLIC_JSON.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")

    pairs_pass = sum(1 for r in report["summary_table"] if r["pass_fail"] == "PASS")
    v3 = report["fx_carry_real_yield_v3_diagnosis"]

    md: list[str] = [
        "# FX Valuation Data Foundation Audit",
        "",
        f"- Generated: {report['generated_at']}",
        f"- Target model: `{report['model_target']}`",
        f"- Reference window: {report['currency_inputs']['reference_window']}",
        f"- Pairs passing foundation: **{pairs_pass} / {len(FX_V3_PAIRS)}**",
        f"- Pairs with fair_value today: **{v3['pairs_with_fair_value']}**",
        "",
        "## Verdict",
        "",
        f"**{report['verdict']['valuation_problem_is']}**",
        "",
        f"- Formula issue detected: **{'Yes' if not report['verdict']['formula_issue'] else 'No'}**",
        f"- Proceed with model development: **{report['verdict']['proceed_with_model_development']}**",
        f"- {report['verdict']['note']}",
        "",
        "---",
        "",
        "## G10 Macro Leg Audit",
        "",
        _md_g10_table(report.get("g10_currency_table") or []),
        "",
        "---",
        "",
        "## 1. Data Foundation Audit — Pair Summary",
        "",
        _md_summary_table(report["summary_table"]),
        "",
        "---",
        "",
        "## 2. Missing Data Report",
        "",
    ]

    if not report["missing_data_report"]:
        md.append("No missing inputs detected.")
    else:
        md.append("| Scope | Input | Issue | Obs | Source |")
        md.append("|---|---|---|---:|---|")
        for m in report["missing_data_report"][:40]:
            md.append(
                f"| {m.get('scope')} | {m.get('input')} | {str(m.get('issue', ''))[:80]} | "
                f"{m.get('observation_count', '—')} | {str(m.get('source', ''))[:40]} |"
            )

    md.extend(["", "---", "", "## 3. Recommended Fix Order", ""])
    for fix in report["recommended_fix_order"]:
        md.append(f"{fix['priority']}. **{fix['fix']}**")
        md.append(f"   - Impact: {fix['impact']}")
        md.append(f"   - Pairs: {', '.join(fix.get('pairs_unblocked') or [])[:120]}")
        md.append("")

    conf = report["estimated_confidence_after_fixes"]
    md.extend(
        [
            "---",
            "",
            "## 4. Estimated Confidence Once Completed",
            "",
            f"- Current pairs passing foundation: **{conf['current_pairs_passing_foundation']} / {conf['total_pairs']}**",
            f"- Estimated confidence after fixes: **{conf['estimated_confidence_after_p1_p4']}**",
            f"- {conf['note']}",
            "",
            "---",
            "",
            "## fx_carry_real_yield_v3 Blocker Diagnosis",
            "",
            f"- Primary blocker: {v3['primary_blocker']}",
            f"- Root cause breakdown: {json.dumps(v3['root_cause_breakdown'])}",
            "",
        ]
    )

    for pid in FX_V3_PAIRS:
        p = report["pairs"][pid]
        vb = p.get("v3_blocker") or {}
        md.append(f"### {pid} — {p.get('root_cause', '—')}")
        md.append(f"- Foundation: **{p['overall_status']}** | V3 audit: **{vb.get('audit_status')}**")
        md.append(f"- Aligned panel: {vb.get('aligned_panel_days')}d | R²: {vb.get('regression_r_squared')} | n: {vb.get('regression_n')}")
        if vb.get("blockers"):
            md.append(f"- Blockers: {'; '.join(vb['blockers'])}")
        md.append("")

    AUDIT_MD.write_text("\n".join(md), encoding="utf-8")
    return {"json": str(AUDIT_JSON), "markdown": str(AUDIT_MD), "public_json": str(PUBLIC_JSON)}
