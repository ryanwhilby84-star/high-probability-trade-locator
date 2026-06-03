"""Generic FRED price vs macro driver: rebased overlay + rolling correlations for dashboard JSON.

Does not touch COT. Uses public FRED graph CSV (no API key).
"""

from __future__ import annotations

import math
from typing import Any, Literal, TypedDict

import numpy as np
import pandas as pd

from hptl.macro import fred_client, macro_freshness
from hptl.validation import safe_eq, safe_float

# Public endpoint kept for backward-compatible imports/tests; the actual fetch
# now goes through the resilient cache-first client in ``fred_client``.
FRED_GRAPH_CSV = fred_client.FRED_GRAPH_CSV
DEFAULT_OBS_START = "2018-01-01"
MAX_EXPORT_ROWS_DAILY = 520
MAX_EXPORT_ROWS_MONTHLY = 120
MAX_EXPORT_ROWS_QUARTERLY = 80


def _fred_series_csv(series_id: str, observation_start: str) -> pd.DataFrame:
    """Cache-first, live-second FRED fetch (delegates to ``fred_client``).

    Kept as a thin wrapper so existing callers/tests that patch this symbol keep
    working. Resilience (retry/backoff/cache fallback) lives in ``fred_client``.
    """
    return fred_client.get_series_df(series_id, observation_start)


def _series_freshness_entry(series_id: str, obs: str, role: str, display: str) -> dict[str, Any]:
    """Per-series provenance/freshness drawn from the cache metadata sidecar."""
    meta = fred_client.read_meta(series_id, obs)
    src = fred_client.last_source(series_id, obs) or ("live" if meta else "unknown")
    fetched_at = meta.get("fetched_at") if meta else None
    obs_end = meta.get("observation_end") if meta else None
    rows = meta.get("row_count") if meta else None
    return {
        "series_id": series_id,
        "role": role,
        "display": display,
        "source": src,
        "fetched_at": fetched_at,
        "observation_end": obs_end,
        "rows": rows,
        "refresh_age_days": macro_freshness.age_days_from(fetched_at),
    }


def _relationship_freshness(profile: "RelationshipProfile", obs: str, latest_date: str | None) -> dict[str, Any]:
    """Aggregate freshness metadata for a built relationship map."""
    price_entry = _series_freshness_entry(
        profile["price_fred_id"], obs, "price", profile.get("price_display", "")
    )
    driver_entry = _series_freshness_entry(
        profile["driver_fred_id"], obs, "driver", profile.get("driver_display", "")
    )
    entries = [price_entry, driver_entry]

    fetched = [e["fetched_at"] for e in entries if e["fetched_at"]]
    # Map is "last fully fresh" at the OLDER of its two series' fetch times.
    last_successful_refresh = min(fetched) if fetched else None
    ages = [e["refresh_age_days"] for e in entries if e["refresh_age_days"] is not None]
    refresh_age_days = max(ages) if ages else 0  # got data with no meta -> treat as fresh
    latency_days = macro_freshness.latency_days_from_date(latest_date)
    status = macro_freshness.data_status(available=True, refresh_age_days=refresh_age_days, has_data=True)

    return {
        "data_status": status,
        "freshness_band": macro_freshness.band_for_age(refresh_age_days),
        "source_series_ids": [profile["price_fred_id"], profile["driver_fred_id"]],
        "source_series": entries,
        "last_successful_refresh": last_successful_refresh,
        "refresh_age_days": refresh_age_days,
        "latency_days": latency_days,
        "latest_observation_date": latest_date,
    }


def _sanitize_float(x: Any) -> float | None:
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


class RelationshipProfile(TypedDict, total=False):
    market: str
    price_fred_id: str
    price_display: str
    driver_fred_id: str
    driver_id: str
    driver_display: str
    driver_is_yield: bool
    cadence: Literal["daily", "monthly", "quarterly"]
    observation_start: str
    rolling_primary: int
    rolling_secondary: int
    rolling_tertiary: int | None


def _to_month_end(df: pd.DataFrame) -> pd.DataFrame:
    s = df.set_index("date").sort_index()["value"]
    m = s.resample("ME").last().dropna()
    return m.reset_index()


def _dgs10_monthly_from_daily(dgs_daily: pd.DataFrame) -> pd.DataFrame:
    return _to_month_end(dgs_daily)


def _to_quarter_end(df: pd.DataFrame) -> pd.DataFrame:
    s = df.set_index("date").sort_index()["value"]
    q = s.resample("QE").last().dropna()
    return q.reset_index()


def _merge_daily(price: pd.DataFrame, driver: pd.DataFrame) -> pd.DataFrame:
    p = price.rename(columns={"value": "price"})
    d = driver.rename(columns={"value": "driver"})
    return pd.merge(p, d, on="date", how="inner").sort_values("date").reset_index(drop=True)


def _merge_monthly_both(price_raw: pd.DataFrame, driver_raw: pd.DataFrame) -> pd.DataFrame:
    pm = _to_month_end(price_raw)
    dm = _to_month_end(driver_raw)
    p = pm.rename(columns={"value": "price"})
    d = dm.rename(columns={"value": "driver"})
    return pd.merge(p, d, on="date", how="inner").sort_values("date").reset_index(drop=True)


def _merge_quarterly_price_with_dgs10(price_raw: pd.DataFrame, dgs_daily: pd.DataFrame) -> pd.DataFrame:
    pq = _to_quarter_end(price_raw)
    dq = _to_quarter_end(dgs_daily)
    p = pq.rename(columns={"value": "price"})
    d = dq.rename(columns={"value": "driver"})
    return pd.merge(p, d, on="date", how="inner").sort_values("date").reset_index(drop=True)


def _merge_quarterly_both(price_raw: pd.DataFrame, driver_raw: pd.DataFrame) -> pd.DataFrame:
    pq = _to_quarter_end(price_raw)
    dq = _to_quarter_end(driver_raw)
    p = pq.rename(columns={"value": "price"})
    d = dq.rename(columns={"value": "driver"})
    return pd.merge(p, d, on="date", how="inner").sort_values("date").reset_index(drop=True)


def _corr_regime(rolling: pd.Series) -> str:
    s = rolling.dropna()
    if len(s) < 15:
        return "weak"
    tail = s.tail(30)
    last = float(tail.iloc[-1])
    sd = float(tail.std()) if len(tail) > 5 else 0.0
    if sd > 0.28:
        return "unstable"
    if abs(last) < 0.12:
        return "weak"
    head = s.iloc[-25 : -5] if len(s) > 30 else s.iloc[:-1]
    if len(head) > 8:
        mid = float(head.median())
        if last * mid < 0 and abs(last - mid) > 0.15:
            return "diverging"
    return "active"


def format_relationship_digest(
    c20: float | None,
    price_name: str,
    driver_name: str,
    cadence: str,
) -> str:
    if c20 is None:
        return (
            f"The short-term statistical link between {price_name} and {driver_name} is still filling in — "
            "use a longer window or check data alignment."
        )
    ac = abs(c20)
    cad = "day-to-day" if cadence == "daily" else "period-to-period"
    if ac < 0.12:
        return (
            f"Over this window, {cad} moves in {price_name} and {driver_name} show only a loose linear relationship — "
            "other themes are likely dominating."
        )
    if c20 < -0.22:
        return (
            f"Lately, {cad} changes tend to run opposite: when {driver_name} pushes one way, {price_name} often leans the other — "
            "useful context, not a timing signal."
        )
    if c20 > 0.22:
        return (
            f"Lately, {cad} changes have been more positively aligned than usual — worth noticing if it matches or fights your base case for this market."
        )
    return (
        f"Co-movement between {price_name} and {driver_name} is in a normal middling range for this sample — read it alongside positioning and headlines."
    )


def build_relationship_payload(profile: RelationshipProfile) -> dict[str, Any]:
    """Build standardized macro relationship JSON for one market / primary driver pair."""
    market = profile["market"]
    driver_id = profile["driver_id"]
    obs = profile.get("observation_start") or DEFAULT_OBS_START
    cadence = profile.get("cadence") or "daily"
    driver_is_yield = bool(profile.get("driver_is_yield", False))
    rp = int(profile.get("rolling_primary") or 20)
    rs = int(profile.get("rolling_secondary") or 30)
    rt = profile.get("rolling_tertiary")
    rt_i = int(rt) if rt is not None else None

    price_display = profile["price_display"]
    driver_display = profile["driver_display"]

    base_err: dict[str, Any] = {
        "available": False,
        "market": market,
        "driver_id": driver_id,
        "driver_label": driver_display,
        "price_series_display": price_display,
        "driver_series_display": driver_display,
    }

    try:
        price_raw = _fred_series_csv(profile["price_fred_id"], obs)
        driver_raw = _fred_series_csv(profile["driver_fred_id"], obs)
    except Exception as exc:
        base_err["error"] = f"FRED fetch failed: {type(exc).__name__}: {exc}"
        return base_err

    merged: pd.DataFrame
    max_rows = MAX_EXPORT_ROWS_DAILY
    min_rows = 80
    rolling_min_p = min(max(10, rp - 5), rp)
    rolling_min_s = min(max(12, rs - 8), rs)

    if cadence == "daily":
        merged = _merge_daily(price_raw, driver_raw)
    elif cadence == "monthly":
        max_rows = MAX_EXPORT_ROWS_MONTHLY
        min_rows = 36
        rolling_min_p = min(max(4, rp - 2), rp)
        rolling_min_s = min(max(5, rs - 2), rs)
        merged = _merge_monthly_both(price_raw, driver_raw)
    elif cadence == "quarterly":
        max_rows = MAX_EXPORT_ROWS_QUARTERLY
        min_rows = 24
        rolling_min_p = min(max(3, rp - 1), rp)
        rolling_min_s = min(max(4, rs - 1), rs)
        if driver_is_yield:
            merged = _merge_quarterly_price_with_dgs10(price_raw, driver_raw)
        else:
            merged = _merge_quarterly_both(price_raw, driver_raw)
    else:
        base_err["error"] = f"Unknown cadence: {cadence}"
        return base_err

    merged = merged.dropna(subset=["price", "driver"])
    if len(merged) < min_rows:
        base_err["error"] = f"Insufficient overlapping rows after merge ({len(merged)} < {min_rows})."
        return base_err

    merged = merged.sort_values("date").reset_index(drop=True)
    tail = merged.iloc[-max_rows:].copy()

    p0 = safe_float(tail["price"].iloc[0])
    d0 = safe_float(tail["driver"].iloc[0])
    if p0 is None or d0 is None or safe_eq(p0, 0) or safe_eq(d0, 0):
        base_err["error"] = "Invalid zero or non-finite baseline for rebasing."
        return base_err

    tail["price_rebased_pct"] = 100.0 * (tail["price"] / p0 - 1.0)
    tail["driver_rebased_pct"] = 100.0 * (tail["driver"] / d0 - 1.0)

    p_ret = tail["price"].pct_change()
    if driver_is_yield:
        d_ret = tail["driver"].diff()
    else:
        d_ret = tail["driver"].pct_change()

    tail["rolling_corr_primary"] = p_ret.rolling(rp, min_periods=rolling_min_p).corr(d_ret)
    tail["rolling_corr_secondary"] = p_ret.rolling(rs, min_periods=rolling_min_s).corr(d_ret)
    if rt_i is not None:
        tm = min(max(3, rt_i - 5), rt_i)
        tail["rolling_corr_tertiary"] = p_ret.rolling(rt_i, min_periods=tm).corr(d_ret)
    else:
        tail["rolling_corr_tertiary"] = np.nan

    last = tail.iloc[-1]
    c_p = _sanitize_float(last["rolling_corr_primary"])
    c_s = _sanitize_float(last["rolling_corr_secondary"])
    c_t = _sanitize_float(last["rolling_corr_tertiary"]) if rt_i is not None else None

    regime = _corr_regime(tail["rolling_corr_primary"])

    digest = format_relationship_digest(c_p, price_display, driver_display, cadence)
    dates = [d.strftime("%Y-%m-%d") for d in tail["date"]]

    out: dict[str, Any] = {
        "available": True,
        "market": market,
        "driver_id": driver_id,
        "driver_label": driver_display,
        "price_series_id": profile["price_fred_id"],
        "driver_series_id": profile["driver_fred_id"],
        "price_series_display": price_display,
        "driver_series_display": driver_display,
        "observation_start": tail["date"].iloc[0].strftime("%Y-%m-%d"),
        "observation_end": tail["date"].iloc[-1].strftime("%Y-%m-%d"),
        "cadence": cadence,
        "normalization": "rebased_pct_window",
        "normalization_note": (
            "Both series are rebased to 0% at the first date of the exported window "
            f"({cadence} observations)."
        ),
        "correlation_method": "pearson_rolling_on_aligned_changes",
        "correlation_note": (
            "Rolling Pearson correlation between price percent changes and "
            + ("yield changes in percentage points per period." if driver_is_yield else "driver percent changes.")
        ),
        "driver_is_yield": driver_is_yield,
        "rolling_primary_n": rp,
        "rolling_secondary_n": rs,
        "rolling_tertiary_n": rt_i,
        "dates": dates,
        "price_rebased_pct": [_sanitize_float(x) for x in tail["price_rebased_pct"]],
        "driver_rebased_pct": [_sanitize_float(x) for x in tail["driver_rebased_pct"]],
        "rolling_corr_20": [_sanitize_float(x) for x in tail["rolling_corr_primary"]],
        "rolling_corr_30": [_sanitize_float(x) for x in tail["rolling_corr_secondary"]],
        "latest_date": last["date"].strftime("%Y-%m-%d"),
        "latest_rolling_corr_20": c_p,
        "latest_rolling_corr_30": c_s,
        "correlation_regime": regime,
        "digest": digest,
        "interpretation_summary": digest,
    }
    if rt_i is not None:
        out["rolling_corr_60"] = [_sanitize_float(x) for x in tail["rolling_corr_tertiary"]]
        out["latest_rolling_corr_60"] = c_t

    # Stage B: attach provenance + freshness metadata (does not affect any
    # scoring/confluence/valuation; purely informational for the dashboard).
    try:
        out.update(_relationship_freshness(profile, obs, out.get("latest_date")))
    except Exception:
        # Freshness metadata is best-effort; never fail a built map over it.
        out.setdefault("data_status", "live")

    # Back-compat for legacy dashboard series keys
    if profile["price_fred_id"] == "NASDAQCOM" and profile["driver_fred_id"] == "DGS10":
        out["nasdaq_rebased_pct"] = out["price_rebased_pct"]
        out["dgs10_rebased_pct"] = out["driver_rebased_pct"]
    if profile["price_fred_id"] == "SP500" and profile["driver_fred_id"] == "DGS10":
        out["sp500_rebased_pct"] = out["price_rebased_pct"]
        out["dgs10_rebased_pct"] = out["driver_rebased_pct"]

    return out


def build_nasdaq_dgs10_relationship_payload() -> dict[str, Any]:
    """Backward-compatible entry used by tests and older imports."""
    prof: RelationshipProfile = {
        "market": "NASDAQ / NQ",
        "price_fred_id": "NASDAQCOM",
        "price_display": "Nasdaq Composite",
        "driver_fred_id": "DGS10",
        "driver_id": "dgs10",
        "driver_display": "US 10Y Treasury yield",
        "driver_is_yield": True,
        "cadence": "daily",
        "observation_start": DEFAULT_OBS_START,
        "rolling_primary": 20,
        "rolling_secondary": 30,
        "rolling_tertiary": 60,
    }
    return build_relationship_payload(prof)
