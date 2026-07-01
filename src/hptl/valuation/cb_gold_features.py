"""WGC monthly CB gold purchases — production feature engineering (rolling 12m)."""

from __future__ import annotations

from typing import Any

from hptl.valuation.metals_institutional_drivers import (
    DriverBundle,
    _load_cache_series,
    _series_freshness,
    _weekly_from_daily,
    _cache_max_stale_days,
)

CB_CACHE_REL = "data/cache/metals_drivers/wgc_cb_gold_net_purchases.json"
GOLD_CB_FEATURE = "cb_roll12"
GOLD_CB_ENGINEERING = "rolling_12m_sum"


def load_monthly_cb() -> list[tuple[str, float]]:
    daily = _load_cache_series(CB_CACHE_REL)
    return sorted((d, v) for d, v in daily.items())


def engineer_monthly_cb(monthly: list[tuple[str, float]], engineer: str) -> dict[str, float]:
    dates = [d for d, _ in monthly]
    values = [v for _, v in monthly]
    out: dict[str, float] = {}
    for i, d in enumerate(dates):
        if engineer == "level":
            out[d] = values[i]
        elif engineer == "roll12" and i >= 11:
            out[d] = sum(values[i - 11 : i + 1])
        elif engineer == "lag1" and i >= 1:
            out[d] = values[i - 1]
        elif engineer == "yoy" and i >= 12:
            out[d] = values[i] - values[i - 12]
    return out


def weekly_cb_feature(weekly_dates: list[str], engineer: str = "roll12") -> dict[str, float]:
    monthly = load_monthly_cb()
    daily = engineer_monthly_cb(monthly, engineer)
    return _weekly_from_daily(daily, weekly_dates)


def add_gold_cb_roll12_to_bundle(
    bundle: DriverBundle,
    cache_map: dict[str, str],
    dates: list[str],
    as_of: str,
) -> None:
    """Attach rolling 12-month global CB net purchases (tonnes) to Gold driver bundle."""
    rel = cache_map.get("central_bank_gold_net_purchases", CB_CACHE_REL)
    weekly = weekly_cb_feature(dates, "roll12")
    col = [weekly.get(d) for d in dates]
    if col and not any(v is None for v in col) and len(weekly) >= 52:
        stale_limit = _cache_max_stale_days(rel)
        fresh, latest = _series_freshness(weekly, as_of, max_stale_days=stale_limit)
        if not fresh:
            bundle.stale.append(GOLD_CB_FEATURE)
        bundle.features[GOLD_CB_FEATURE] = [float(v) for v in col]
        bundle.lineage[GOLD_CB_FEATURE] = {
            "source_name": "WGC / IMF IFS (rolling 12m)",
            "source_id": rel,
            "source_date": latest or as_of,
            "engineering": GOLD_CB_ENGINEERING,
            "notes": "Sum of global monthly net CB gold purchases over trailing 12 months (tonnes).",
        }
    else:
        bundle.missing_required.append("central_bank_net_purchases")
