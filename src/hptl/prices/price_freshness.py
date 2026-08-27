"""Price freshness classification for live quotes vs completed OHLC.

Does not alter valuation formulas or COT pipelines.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

# Live/snapshot quote: Current if observed within this window (offline refresh path).
LIVE_SNAPSHOT_CURRENT_HOURS = 6
# Completed daily tip may lag over weekends; allow calendar buffer.
COMPLETED_DAILY_CURRENT_DAYS = 4
# Valuation market comparison uses the same live-snapshot threshold.
VALUATION_COMPARISON_MAX_AGE_HOURS = 24


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def age_hours(timestamp: str | None, *, now: datetime | None = None) -> float | None:
    dt = _parse_dt(timestamp)
    if dt is None:
        return None
    now = now or datetime.now(timezone.utc)
    return max(0.0, (now - dt).total_seconds() / 3600.0)


def age_days_from_date(date_str: str | None, *, now: datetime | None = None) -> int | None:
    if not date_str:
        return None
    try:
        d = datetime.fromisoformat(str(date_str)[:10]).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    now = now or datetime.now(timezone.utc)
    return max(0, (now.date() - d.date()).days)


def classify_status(
    *,
    age_h: float | None = None,
    age_d: int | None = None,
    failed: bool = False,
    current_hours: float = LIVE_SNAPSHOT_CURRENT_HOURS,
    current_days: int = COMPLETED_DAILY_CURRENT_DAYS,
) -> str:
    if failed:
        return "Failed"
    if age_h is not None:
        return "Current" if age_h <= current_hours else "Stale"
    if age_d is not None:
        return "Current" if age_d <= current_days else "Stale"
    return "Failed"


def build_instrument_price_freshness(
    record: dict[str, Any] | None,
    *,
    now: datetime | None = None,
    provider: str | None = None,
    symbol: str | None = None,
) -> dict[str, Any]:
    """Build Live / completed-daily / weekly freshness block from a price-store record."""
    now = now or datetime.now(timezone.utc)
    rec = record or {}
    price = rec.get("price") or {}
    daily = rec.get("daily") or []
    weekly = rec.get("weekly") or []
    forming_daily = rec.get("forming_daily")
    forming_weekly = rec.get("forming_weekly")
    last_daily = daily[-1] if daily else None
    last_weekly = weekly[-1] if weekly else None

    live_as_of = price.get("as_of")
    live_mid = price.get("mid")
    live_age_h = age_hours(live_as_of, now=now)
    live_status = classify_status(
        age_h=live_age_h,
        failed=live_mid is None,
        current_hours=LIVE_SNAPSHOT_CURRENT_HOURS,
    )

    daily_date = (last_daily or {}).get("date")
    daily_age_d = age_days_from_date(daily_date, now=now)
    daily_status = classify_status(
        age_d=daily_age_d,
        failed=last_daily is None,
        current_days=COMPLETED_DAILY_CURRENT_DAYS,
    )

    weekly_date = (last_weekly or {}).get("date")
    weekly_age_d = age_days_from_date(weekly_date, now=now)
    weekly_status = classify_status(
        age_d=weekly_age_d,
        failed=last_weekly is None,
        current_days=COMPLETED_DAILY_CURRENT_DAYS + 3,
    )

    comparison_price = live_mid
    comparison_as_of = live_as_of
    comparison_kind = "live_snapshot"
    if comparison_price is None and forming_daily and forming_daily.get("close") is not None:
        comparison_price = forming_daily.get("close")
        comparison_as_of = forming_daily.get("date")
        comparison_kind = "forming_daily"
    if comparison_price is None and last_daily:
        comparison_price = last_daily.get("close")
        comparison_as_of = last_daily.get("date")
        comparison_kind = "completed_daily"

    comp_age_h = age_hours(comparison_as_of, now=now)
    if comp_age_h is None and comparison_as_of:
        # date-only
        d = age_days_from_date(str(comparison_as_of)[:10], now=now)
        comp_age_h = float(d * 24) if d is not None else None
    comparison_status = classify_status(
        age_h=comp_age_h,
        failed=comparison_price is None,
        current_hours=VALUATION_COMPARISON_MAX_AGE_HOURS,
    )

    return {
        "provider": provider or ((rec.get("price_scale") or {}).get("source")),
        "symbol": symbol or ((rec.get("price_scale") or {}).get("symbol")),
        "timezone": "UTC",
        "evaluated_at": now.isoformat(),
        "live_quote": {
            "price": live_mid,
            "bid": price.get("bid"),
            "ask": price.get("ask"),
            "as_of": live_as_of,
            "age_hours": round(live_age_h, 3) if live_age_h is not None else None,
            "status": live_status,
        },
        "latest_completed_daily": {
            "date": daily_date,
            "close": (last_daily or {}).get("close"),
            "age_days": daily_age_d,
            "status": daily_status,
        },
        "latest_completed_weekly": {
            "date": weekly_date,
            "close": (last_weekly or {}).get("close"),
            "age_days": weekly_age_d,
            "status": weekly_status,
        },
        "forming_daily": forming_daily,
        "forming_weekly": forming_weekly,
        "market_comparison": {
            "price": comparison_price,
            "as_of": comparison_as_of,
            "kind": comparison_kind,
            "age_hours": round(comp_age_h, 3) if comp_age_h is not None else None,
            "status": comparison_status,
            "trusted": comparison_status == "Current",
        },
        "overall_status": (
            "Current"
            if live_status == "Current" or comparison_status == "Current"
            else daily_status
            if daily_status != "Failed"
            else "Failed"
        ),
    }


def valuation_deviation_gate(
    freshness: dict[str, Any],
    *,
    spot_for_model: float | None,
    fair_value: float | None,
) -> dict[str, Any]:
    """Keep fair value; gate trusted market deviation on price freshness."""
    comp = freshness.get("market_comparison") or {}
    trusted = bool(comp.get("trusted"))
    market = comp.get("price")
    deviation = None
    if trusted and market is not None and fair_value not in (None, 0):
        deviation = round(100.0 * (float(market) - float(fair_value)) / float(fair_value), 2)
    return {
        "fair_value": fair_value,
        "model_anchor_price": spot_for_model,
        "market_comparison_price": market,
        "market_comparison_as_of": comp.get("as_of"),
        "market_comparison_kind": comp.get("kind"),
        "market_price_status": comp.get("status") or freshness.get("overall_status"),
        "deviation_pct_trusted": trusted,
        "deviation_pct": deviation if trusted else None,
        "deviation_pct_stale_untrusted": (
            None
            if trusted or market is None or fair_value in (None, 0)
            else round(100.0 * (float(market) - float(fair_value)) / float(fair_value), 2)
        ),
        "warning": (
            None
            if trusted
            else "Market price is stale or unavailable — fair value retained; "
            "do not treat over/undervaluation % as current."
        ),
    }
