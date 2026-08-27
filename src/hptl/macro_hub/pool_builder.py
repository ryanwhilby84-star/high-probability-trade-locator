"""Assemble the Macro Hub pooled payload from existing HTPL data layers."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from hptl.macro_hub.config import (
    BITCOIN_INSTRUMENT_ID,
    COT_CFTC_BITCOIN,
    COT_CFTC_USD_INDEX,
    CROSS_ASSETS,
    FRED_TREASURY_SERIES,
    FRED_USD_DXY,
    RATES_CLEAN_PATH,
    SCHEMA_VERSION,
    STALE_FRED_DAYS,
    STALE_PRICE_DAYS,
)
from hptl.macro_hub.cot_snapshot import cot_block_for_instrument, cot_block_from_cftc_code, cot_payload_template
from hptl.macro_hub.freshness import freshness_status
from hptl.macro_hub.price_history import fred_series_block, price_block_from_store


def _latest_as_of(*dates: str | None) -> str | None:
    parsed = [d[:10] for d in dates if d]
    return max(parsed) if parsed else None


def _treasuries_from_rates_csv() -> dict[str, Any] | None:
    if not RATES_CLEAN_PATH.exists():
        return None
    df = pd.read_csv(RATES_CLEAN_PATH, parse_dates=["date"])
    if df.empty:
        return None
    df = df.sort_values("date")
    core = df[["dgs2", "dgs10", "dgs30"]].apply(pd.to_numeric, errors="coerce")
    valid = df.loc[core.notna().any(axis=1)]
    if valid.empty:
        return None
    row = valid.iloc[-1]
    as_of = pd.Timestamp(row["date"]).strftime("%Y-%m-%d")

    def _val(col: str) -> float | None:
        v = row.get(col)
        if pd.isna(v):
            return None
        try:
            return round(float(v), 4)
        except (TypeError, ValueError):
            return None

    dgs2 = _val("dgs2")
    dgs10 = _val("dgs10")
    dgs30 = _val("dgs30")
    curve_2s10s = _val("yield_curve_10y2y")
    if curve_2s10s is None and dgs2 is not None and dgs10 is not None:
        curve_2s10s = round(dgs10 - dgs2, 4)
    curve_10s30s = None
    if dgs10 is not None and dgs30 is not None:
        curve_10s30s = round(dgs30 - dgs10, 4)

    return {
        "us_2y_yield": dgs2,
        "us_10y_yield": dgs10,
        "us_30y_yield": dgs30,
        "curve_2s10s": curve_2s10s,
        "curve_10s30s": curve_10s30s,
        "real_yield_10y": None,
        "latest_date": as_of,
        "source": "rates_clean.csv",
        "freshness": freshness_status(as_of, stale_after_days=STALE_FRED_DAYS),
    }


def _treasuries_block(*, allow_live: bool | None) -> dict[str, Any]:
    base = _treasuries_from_rates_csv() or {
        "us_2y_yield": None,
        "us_10y_yield": None,
        "us_30y_yield": None,
        "curve_2s10s": None,
        "curve_10s30s": None,
        "real_yield_10y": None,
        "latest_date": None,
        "source": None,
        "freshness": {"status": "missing", "as_of": None, "age_days": None},
    }

    # Fill gaps / real yield from FRED when CSV missing fields.
    fred_fields: dict[str, Any] = {}
    for spec in FRED_TREASURY_SERIES:
        if base.get(spec.key) is not None and spec.key != "real_yield_10y":
            continue
        block = fred_series_block(
            spec.series_id,
            label=spec.label,
            obs_start=spec.obs_start,
            stale_after_days=STALE_FRED_DAYS,
            allow_live=allow_live,
        )
        fred_fields[spec.key] = block.get("latest_value")
        if base.get("latest_date") is None and block.get("latest_date"):
            base["latest_date"] = block["latest_date"]
            base["source"] = f"fred:{spec.series_id}"
            base["freshness"] = block.get("freshness")

    for key, val in fred_fields.items():
        if base.get(key) is None and val is not None:
            base[key] = val

    # Recompute derived curves if we now have components.
    if base.get("curve_2s10s") is None:
        y2, y10 = base.get("us_2y_yield"), base.get("us_10y_yield")
        if y2 is not None and y10 is not None:
            base["curve_2s10s"] = round(float(y10) - float(y2), 4)
    if base.get("curve_10s30s") is None:
        y10, y30 = base.get("us_10y_yield"), base.get("us_30y_yield")
        if y10 is not None and y30 is not None:
            base["curve_10s30s"] = round(float(y30) - float(y10), 4)

    series_detail: dict[str, Any] = {}
    series_history: dict[str, Any] = {}
    for spec in FRED_TREASURY_SERIES:
        block = fred_series_block(
            spec.series_id,
            label=spec.label,
            obs_start=spec.obs_start,
            stale_after_days=STALE_FRED_DAYS,
            allow_live=allow_live,
        )
        series_detail[spec.key] = {
            "series_id": spec.series_id,
            "label": spec.label,
            "latest_value": block.get("latest_value"),
            "latest_date": block.get("latest_date"),
        }
        if block.get("history"):
            series_history[spec.key] = block.get("history")
        if base.get(spec.key) is None and block.get("latest_value") is not None:
            base[spec.key] = block.get("latest_value")

    base["series_detail"] = series_detail
    base["series_history"] = series_history
    return base


def _usd_block(*, allow_live: bool | None, cot_download: bool) -> dict[str, Any]:
    dxy = fred_series_block(
        FRED_USD_DXY.series_id,
        label=FRED_USD_DXY.label,
        obs_start=FRED_USD_DXY.obs_start,
        stale_after_days=STALE_FRED_DAYS,
        allow_live=allow_live,
    )
    cot = cot_block_from_cftc_code(
        COT_CFTC_USD_INDEX,
        label="ICE U.S. Dollar Index Futures",
        download=cot_download,
    )
    return {
        "dxy_price": dxy.get("latest_value"),
        "dxy_price_date": dxy.get("latest_date"),
        "dxy_source": dxy.get("source"),
        "dxy_series_id": FRED_USD_DXY.series_id,
        "dxy_freshness": dxy.get("freshness"),
        "dxy_history": dxy.get("history"),
        "dx_futures_price": None,
        "dx_futures_price_date": None,
        "dx_futures_source": None,
        "dx_futures_note": "ICE DX futures price not wired — DXY proxy via FRED broad USD index.",
        "cot": {
            "long": cot.get("long"),
            "short": cot.get("short"),
            "net": cot.get("net"),
            "weekly_net_change": cot.get("weekly_net_change"),
            "four_week_net_change": cot.get("four_week_net_change"),
            "open_interest": cot.get("open_interest"),
            "net_percentile_3y": cot.get("net_percentile_3y"),
            "long_percentile_3y": cot.get("long_percentile_3y"),
            "short_percentile_3y": cot.get("short_percentile_3y"),
            "oi_percentile_3y": cot.get("oi_percentile_3y"),
            "report_date": cot.get("report_date"),
            "source": cot.get("source"),
            "freshness": cot.get("freshness"),
            "error": cot.get("error"),
            "cftc_code": COT_CFTC_USD_INDEX,
        },
    }


def _bitcoin_block(*, cot_download: bool) -> dict[str, Any]:
    spot = price_block_from_store(BITCOIN_INSTRUMENT_ID, label="Bitcoin (BTCUSD)", stale_after_days=STALE_PRICE_DAYS)
    cot = cot_block_from_cftc_code(
        COT_CFTC_BITCOIN,
        label="CME Bitcoin Futures",
        download=cot_download,
    )
    return {
        "btcusd_price": spot.get("latest_price"),
        "btcusd_price_date": spot.get("latest_date"),
        "btcusd_source": spot.get("source"),
        "btcusd_freshness": spot.get("freshness"),
        "btcusd_history": spot.get("history"),
        "btc_futures_price": None,
        "btc_futures_price_date": None,
        "btc_futures_source": None,
        "btc_futures_note": "Bitcoin futures price feed not wired — spot via price_store.",
        "cot": {
            "long": cot.get("long"),
            "short": cot.get("short"),
            "net": cot.get("net"),
            "weekly_net_change": cot.get("weekly_net_change"),
            "four_week_net_change": cot.get("four_week_net_change"),
            "open_interest": cot.get("open_interest"),
            "net_percentile_3y": cot.get("net_percentile_3y"),
            "short_percentile_3y": cot.get("short_percentile_3y"),
            "oi_percentile_3y": cot.get("oi_percentile_3y"),
            "report_date": cot.get("report_date"),
            "source": cot.get("source"),
            "freshness": cot.get("freshness"),
            "error": cot.get("error"),
        },
    }


def _cross_assets_block() -> dict[str, Any]:
    out: dict[str, Any] = {}
    for spec in CROSS_ASSETS:
        price = price_block_from_store(spec.instrument_id, label=spec.label, stale_after_days=STALE_PRICE_DAYS)
        cot = cot_block_for_instrument(spec.instrument_id)
        out[spec.key] = {
            "instrument_id": spec.instrument_id,
            "label": spec.label,
            "latest_price": price.get("latest_price"),
            "latest_date": price.get("latest_date"),
            "source": price.get("source"),
            "freshness": price.get("freshness"),
            "history": price.get("history"),
            "cot": {
                "long": cot.get("long"),
                "short": cot.get("short"),
                "net": cot.get("net"),
                "weekly_net_change": cot.get("weekly_net_change"),
                "four_week_net_change": cot.get("four_week_net_change"),
                "open_interest": cot.get("open_interest"),
                "net_percentile_3y": cot.get("net_percentile_3y"),
                "short_percentile_3y": cot.get("short_percentile_3y"),
                "oi_percentile_3y": cot.get("oi_percentile_3y"),
                "report_date": cot.get("report_date"),
                "source": cot.get("source"),
                "freshness": cot.get("freshness"),
                "error": cot.get("error"),
            },
            "error": price.get("error"),
        }
    return out


def _source_health(payload: dict[str, Any]) -> dict[str, Any]:
    issues: list[dict[str, str]] = []

    def _check(path: str, label: str, freshness: dict[str, Any] | None) -> None:
        if not freshness:
            issues.append({"field": path, "label": label, "status": "missing", "detail": "No freshness metadata"})
            return
        st = freshness.get("status")
        if st == "missing":
            issues.append({"field": path, "label": label, "status": "missing", "detail": "Data unavailable"})
        elif st == "stale":
            issues.append(
                {
                    "field": path,
                    "label": label,
                    "status": "stale",
                    "detail": f"Last update {freshness.get('as_of')} ({freshness.get('age_days')}d ago)",
                }
            )

    usd = payload.get("usd") or {}
    _check("usd.dxy", "DXY proxy", usd.get("dxy_freshness"))
    _check("usd.cot", "USD Index futures COT", (usd.get("cot") or {}).get("freshness"))

    treas = payload.get("treasuries") or {}
    _check("treasuries", "US Treasuries", treas.get("freshness"))

    btc = payload.get("bitcoin") or {}
    _check("bitcoin.spot", "Bitcoin spot", btc.get("btcusd_freshness"))
    _check("bitcoin.cot", "Bitcoin futures COT", (btc.get("cot") or {}).get("freshness"))

    for key, block in (payload.get("cross_assets") or {}).items():
        _check(f"cross_assets.{key}", block.get("label") or key, block.get("freshness"))

    missing = [i for i in issues if i["status"] == "missing"]
    stale = [i for i in issues if i["status"] == "stale"]
    return {
        "generated_at": payload.get("generated_at"),
        "issue_count": len(issues),
        "missing_count": len(missing),
        "stale_count": len(stale),
        "issues": issues,
        "healthy": len(issues) == 0,
    }


def build_macro_hub_payload(*, allow_live: bool | None = None, cot_download: bool | None = None) -> dict[str, Any]:
    """Build the full Macro Hub JSON document."""
    if cot_download is None:
        cot_download = str(os.environ.get("HPTL_MACRO_HUB_COT_DOWNLOAD", "")).strip().lower() in {
            "1",
            "true",
            "yes",
        }

    usd = _usd_block(allow_live=allow_live, cot_download=cot_download)
    treasuries = _treasuries_block(allow_live=allow_live)
    bitcoin = _bitcoin_block(cot_download=cot_download)
    cross_assets = _cross_assets_block()

    as_of_date = _latest_as_of(
        usd.get("dxy_price_date"),
        (usd.get("cot") or {}).get("report_date"),
        treasuries.get("latest_date"),
        bitcoin.get("btcusd_price_date"),
        (bitcoin.get("cot") or {}).get("report_date"),
        *[a.get("latest_date") for a in cross_assets.values()],
    )

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "as_of_date": as_of_date,
        "usd": usd,
        "treasuries": treasuries,
        "bitcoin": bitcoin,
        "cross_assets": cross_assets,
        "correlation_prep": {
            "windows_days": [30, 90, 180],
            "engine_built": False,
            "note": "Daily close history stored per asset; rolling correlation engine not implemented yet.",
        },
    }
    payload["source_health"] = _source_health(payload)
    return payload
