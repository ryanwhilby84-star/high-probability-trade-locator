"""Seasonality V2 — audit-only 10-year rolling ISO weekly seasonality engine.

Not wired to live seasonality pillar, scanner, or thesis panel.
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Sequence

MAX_LOOKBACK_YEARS = 10
STALE_PRICE_DAYS = 14
MIN_SAMPLE_BIAS = 7
BULL_WIN_RATE = 65.0
BEAR_WIN_RATE = 35.0
BULL_AVG_RETURN = 0.5
BEAR_AVG_RETURN = -0.5
HIGH_SAMPLE = 10
MEDIUM_SAMPLE = 7
HIGH_Z = 0.75
MEDIUM_Z = 0.4
HIGH_YEARS = 8
MEDIUM_YEARS = 5
TRADING_MIN_YEARS = 5

BIAS_BULLISH = "Bullish"
BIAS_BEARISH = "Bearish"
BIAS_NEUTRAL = "Neutral"
CONF_HIGH = "High"
CONF_MEDIUM = "Medium"
CONF_LOW = "Low"


@dataclass(frozen=True)
class AuditAssetSpec:
    asset: str
    category: str
    fmp_symbols: tuple[str, ...]
    price_store_keys: tuple[str, ...] = ()


AUDIT_ASSETS: tuple[AuditAssetSpec, ...] = (
    AuditAssetSpec("EURUSD", "FX", ("EURUSD",), ("Euro FX / 6E", "EUR/USD")),
    AuditAssetSpec("NZDUSD", "FX", ("NZDUSD",), ("NZ Dollar / 6N", "NZD/USD")),
    AuditAssetSpec("GBPUSD", "FX", ("GBPUSD",), ("British Pound / 6B", "GBP/USD")),
    AuditAssetSpec("AUDUSD", "FX", ("AUDUSD",), ("Australian Dollar / 6A", "AUD/USD")),
    AuditAssetSpec("USDJPY", "FX", ("USDJPY",), ("Japanese Yen / 6J", "USD/JPY")),
    AuditAssetSpec("USDCAD", "FX", ("USDCAD",), ("Canadian Dollar / 6C", "USD/CAD")),
    AuditAssetSpec("GCUSD", "Metals", ("GCUSD", "XAUUSD"), ("Gold",)),
    AuditAssetSpec("SIUSD", "Metals", ("SIUSD", "XAGUSD"), ("Silver",)),
    AuditAssetSpec("HGUSD", "Metals", ("HGUSD", "CPER"), ("Copper / HG", "Copper")),
    AuditAssetSpec("NGUSD", "Energy", ("NGUSD", "NG"), ("Natural Gas / NG",)),
    AuditAssetSpec("Wheat", "Grains", ("ZWUSD", "ZOUSX"), ("Wheat",)),
    AuditAssetSpec("^GSPC", "Indices", ("^GSPC", "SPY"), ("S&P 500 / ES", "US SPX 500")),
    AuditAssetSpec("^IXIC", "Indices", ("^IXIC", "QQQ"), ("NASDAQ / NQ", "US Nas 100")),
    AuditAssetSpec("BTCUSD", "Crypto", ("BTCUSD",), ("Bitcoin",)),
)


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def iso_year_week(date_str: str) -> tuple[int, int]:
    dt = datetime.strptime(str(date_str)[:10], "%Y-%m-%d").date()
    cal = dt.isocalendar()
    year = int(cal.year)
    week = int(cal.week)
    if week > 52:
        week = 52
    return year, week


def trim_to_lookback_years(
    daily: Sequence[dict[str, Any]],
    *,
    years: int = MAX_LOOKBACK_YEARS,
    as_of: date | None = None,
) -> list[dict[str, Any]]:
    if not daily:
        return []
    ref = as_of or date.today()
    cutoff = ref - timedelta(days=years * 366)
    out: list[dict[str, Any]] = []
    for bar in daily:
        d = str(bar.get("date") or "")[:10]
        if not d:
            continue
        try:
            dt = datetime.strptime(d, "%Y-%m-%d").date()
        except ValueError:
            continue
        if dt >= cutoff:
            out.append(dict(bar))
    out.sort(key=lambda b: str(b.get("date") or ""))
    return out


def normalize_daily_bars(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows or []:
        d = str(row.get("date") or "")[:10]
        if not d or d in seen:
            continue
        close = _num(row.get("close"))
        if close is None:
            continue
        open_px = _num(row.get("open"))
        if open_px is None:
            open_px = close
        out.append(
            {
                "date": d,
                "open": open_px,
                "high": _num(row.get("high")) or close,
                "low": _num(row.get("low")) or close,
                "close": close,
            }
        )
        seen.add(d)
    out.sort(key=lambda b: b["date"])
    return out


def parse_fmp_historical_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        rows = payload.get("historical") or []
    elif isinstance(payload, list):
        rows = payload
    else:
        return []
    return normalize_daily_bars(rows)


def aggregate_daily_to_iso_weeks(daily: Sequence[dict[str, Any]]) -> dict[tuple[int, int], dict[str, Any]]:
    """Map (iso_year, iso_week) -> weekly open/close from daily OHLC."""
    buckets: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for bar in daily:
        y, w = iso_year_week(str(bar["date"]))
        buckets.setdefault((y, w), []).append(bar)

    weekly: dict[tuple[int, int], dict[str, Any]] = {}
    for key, bars in buckets.items():
        bars.sort(key=lambda b: b["date"])
        open_px = _num(bars[0].get("open")) or _num(bars[0].get("close"))
        close_px = _num(bars[-1].get("close"))
        if open_px is None or close_px is None or open_px <= 0:
            continue
        weekly[key] = {
            "iso_year": key[0],
            "iso_week": key[1],
            "open": open_px,
            "close": close_px,
            "open_date": bars[0]["date"],
            "close_date": bars[-1]["date"],
        }
    return weekly


def weekly_return_pct(open_price: float, close_price: float) -> float | None:
    if open_price <= 0:
        return None
    return (close_price / open_price - 1.0) * 100.0


def years_spanned(daily: Sequence[dict[str, Any]]) -> float:
    if not daily:
        return 0.0
    start = str(daily[0]["date"])[:10]
    end = str(daily[-1]["date"])[:10]
    try:
        d0 = datetime.strptime(start, "%Y-%m-%d").date()
        d1 = datetime.strptime(end, "%Y-%m-%d").date()
    except ValueError:
        return 0.0
    return max(0.0, (d1 - d0).days / 365.25)


def distinct_years_in_daily(daily: Sequence[dict[str, Any]]) -> int:
    years = {str(b["date"])[:4] for b in daily if b.get("date")}
    return len(years)


def price_age_days(latest_date: str | None, *, as_of: date | None = None) -> int | None:
    if not latest_date:
        return None
    try:
        latest = datetime.strptime(str(latest_date)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None
    ref = as_of or date.today()
    return (ref - latest).days


def collect_iso_week_returns(
    weekly_map: dict[tuple[int, int], dict[str, Any]],
    *,
    target_week: int,
    exclude_year: int | None = None,
) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for (y, w), row in weekly_map.items():
        if w != target_week:
            continue
        if exclude_year is not None and y >= exclude_year:
            continue
        ret = weekly_return_pct(float(row["open"]), float(row["close"]))
        if ret is None:
            continue
        samples.append(
            {
                "iso_year": y,
                "iso_week": w,
                "return_pct": ret,
                "open": row["open"],
                "close": row["close"],
                "positive": ret > 0,
            }
        )
    samples.sort(key=lambda s: s["iso_year"])
    return samples


def compute_week_statistics(samples: Sequence[dict[str, Any]]) -> dict[str, Any]:
    returns = [float(s["return_pct"]) for s in samples]
    n = len(returns)
    if n == 0:
        return {
            "sample_size": 0,
            "win_rate_pct": None,
            "avg_return_pct": None,
            "median_return_pct": None,
            "std_dev_pct": None,
            "z_score": None,
            "positive_years": 0,
            "negative_years": 0,
            "best_year": None,
            "best_return_pct": None,
            "worst_year": None,
            "worst_return_pct": None,
        }

    positive = sum(1 for r in returns if r > 0)
    negative = sum(1 for r in returns if r < 0)
    avg = statistics.mean(returns)
    med = statistics.median(returns)
    stdev = statistics.stdev(returns) if n >= 2 else None
    z_score = (avg / stdev) if stdev and stdev > 0 else None

    best = max(samples, key=lambda s: s["return_pct"])
    worst = min(samples, key=lambda s: s["return_pct"])

    return {
        "sample_size": n,
        "win_rate_pct": round(positive / n * 100.0, 2),
        "avg_return_pct": round(avg, 4),
        "median_return_pct": round(med, 4),
        "std_dev_pct": round(stdev, 4) if stdev is not None else None,
        "z_score": round(z_score, 4) if z_score is not None else None,
        "positive_years": positive,
        "negative_years": negative,
        "best_year": best.get("iso_year"),
        "best_return_pct": round(float(best["return_pct"]), 4),
        "worst_year": worst.get("iso_year"),
        "worst_return_pct": round(float(worst["return_pct"]), 4),
    }


def classify_bias(
    *,
    sample_size: int,
    win_rate_pct: float | None,
    avg_return_pct: float | None,
) -> str:
    if sample_size < MIN_SAMPLE_BIAS or win_rate_pct is None or avg_return_pct is None:
        return BIAS_NEUTRAL
    if win_rate_pct >= BULL_WIN_RATE and avg_return_pct > BULL_AVG_RETURN:
        return BIAS_BULLISH
    if win_rate_pct <= BEAR_WIN_RATE and avg_return_pct < BEAR_AVG_RETURN:
        return BIAS_BEARISH
    return BIAS_NEUTRAL


def classify_confidence(
    *,
    sample_size: int,
    z_score: float | None,
    years_covered: float,
    price_stale: bool,
    std_dev_pct: float | None,
) -> str:
    if sample_size < MEDIUM_SAMPLE or price_stale:
        return CONF_LOW
    if std_dev_pct is not None and std_dev_pct == 0:
        return CONF_LOW
    if z_score is None:
        return CONF_LOW

    abs_z = abs(z_score)
    if sample_size >= HIGH_SAMPLE and abs_z >= HIGH_Z and years_covered >= HIGH_YEARS:
        return CONF_HIGH
    if sample_size >= MEDIUM_SAMPLE and abs_z >= MEDIUM_Z and years_covered >= MEDIUM_YEARS:
        return CONF_MEDIUM
    return CONF_LOW


def build_warnings(
    *,
    years_covered: float,
    sample_size: int,
    price_stale: bool,
    price_age_days: int | None,
    data_source: str,
    extra: Sequence[str] | None = None,
) -> list[str]:
    warnings: list[str] = list(extra or [])
    if years_covered < TRADING_MIN_YEARS:
        warnings.append("Seasonality not reliable enough for trading decisions.")
    if sample_size < MEDIUM_SAMPLE:
        warnings.append(f"Sample size {sample_size} below minimum {MEDIUM_SAMPLE} for medium confidence.")
    if price_stale:
        warnings.append(
            f"Latest price is stale ({price_age_days} days old); confidence capped at Low."
        )
    if data_source.startswith("none"):
        warnings.append("No usable daily price history from price store or FMP.")
    return warnings


def audit_pass_fail(
    *,
    confidence: str,
    years_covered: float,
    sample_size: int,
    warnings: Sequence[str],
) -> str:
    if years_covered < TRADING_MIN_YEARS:
        return "FAIL"
    if sample_size < MEDIUM_SAMPLE:
        return "FAIL"
    if confidence == CONF_LOW:
        return "FAIL"
    if any("stale" in w.lower() for w in warnings):
        return "WARN"
    if confidence in {CONF_HIGH, CONF_MEDIUM}:
        return "PASS"
    return "FAIL"


def compute_seasonality_v2_from_daily(
    daily: Sequence[dict[str, Any]],
    *,
    asset: str,
    data_source: str = "unknown",
    as_of: date | None = None,
    extra_warnings: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Full Seasonality V2 audit block for one asset from normalized daily OHLC."""
    ref = as_of or date.today()
    trimmed = trim_to_lookback_years(list(daily), years=MAX_LOOKBACK_YEARS, as_of=ref)
    normalized = normalize_daily_bars(trimmed)

    earliest = normalized[0]["date"] if normalized else None
    latest = normalized[-1]["date"] if normalized else None
    daily_count = len(normalized)
    years_cov = years_spanned(normalized)
    distinct_years = distinct_years_in_daily(normalized)
    age_days = price_age_days(latest, as_of=ref)
    stale = age_days is not None and age_days > STALE_PRICE_DAYS

    current_week = ref.isocalendar().week
    if current_week > 52:
        current_week = 52
    current_year = ref.isocalendar().year

    weekly_map = aggregate_daily_to_iso_weeks(normalized)
    samples = collect_iso_week_returns(
        weekly_map,
        target_week=current_week,
        exclude_year=current_year,
    )
    stats = compute_week_statistics(samples)

    bias = classify_bias(
        sample_size=int(stats["sample_size"]),
        win_rate_pct=stats["win_rate_pct"],
        avg_return_pct=stats["avg_return_pct"],
    )
    confidence = classify_confidence(
        sample_size=int(stats["sample_size"]),
        z_score=stats["z_score"],
        years_covered=years_cov,
        price_stale=stale,
        std_dev_pct=stats["std_dev_pct"],
    )
    warnings = build_warnings(
        years_covered=years_cov,
        sample_size=int(stats["sample_size"]),
        price_stale=stale,
        price_age_days=age_days,
        data_source=data_source,
        extra=extra_warnings,
    )
    status = audit_pass_fail(
        confidence=confidence,
        years_covered=years_cov,
        sample_size=int(stats["sample_size"]),
        warnings=warnings,
    )

    return {
        "asset": asset,
        "data_source": data_source,
        "earliest_date": earliest,
        "latest_date": latest,
        "daily_bars": daily_count,
        "years_covered": round(years_cov, 2),
        "distinct_years": distinct_years,
        "current_iso_week": current_week,
        "current_iso_year": current_year,
        "sample_size": stats["sample_size"],
        "win_rate_pct": stats["win_rate_pct"],
        "avg_return_pct": stats["avg_return_pct"],
        "median_return_pct": stats["median_return_pct"],
        "std_dev_pct": stats["std_dev_pct"],
        "z_score": stats["z_score"],
        "positive_years": stats["positive_years"],
        "negative_years": stats["negative_years"],
        "best_year": stats["best_year"],
        "best_return_pct": stats["best_return_pct"],
        "worst_year": stats["worst_year"],
        "worst_return_pct": stats["worst_return_pct"],
        "bias": bias,
        "confidence": confidence,
        "pass_fail_status": status,
        "warnings": warnings,
        "price_age_days": age_days,
        "price_stale": stale,
        "audit_only": True,
        "live_wired": False,
    }
