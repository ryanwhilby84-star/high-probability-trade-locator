"""Audit-only index macro merge experiment (UMCSENT + DGS10).

NOT wired to the live valuation pillar or 5-Pillar Thesis Panel.

UMCSENT is consumer sentiment — not CAPE, earnings yield, or equity risk premium.
This module exists only to validate the structural FRED merge pipeline offline.

For live index valuation, continue Index Valuation V2 audit with Yale Shiller CAPE,
verified earnings yield, dividend yield, DGS10, and ERP before any live wiring.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import pandas as pd
import requests

from hptl.config import PROJECT_ROOT
from hptl.valuation.index_valuation_v2_audit import (
    composite_valuation_score,
    earnings_yield_from_cape,
    equity_risk_premium_pct,
    percentile_rank,
)

EXPERIMENTAL_LABEL = "Index Macro Context — Experimental"

FRED_OBS_URL = "https://api.stlouisfed.org/fred/series/observations"
# Structural proxy only — not CAPE. Used to prove monthly merge stability in shadow tests.
TRACKING_SERIES = "UMCSENT"
Y10_SERIES = "DGS10"
LOOKBACK_MONTHS = 36
MISSING = "."

_matrix_cache: pd.DataFrame | None = None


def _ensure_fred_api_key() -> bool:
    if os.environ.get("FRED_API_KEY", "").strip():
        return True
    for env_dir in (Path.cwd(), Path.cwd().parent, PROJECT_ROOT):
        env_path = env_dir / ".env"
        if not env_path.is_file():
            continue
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if stripped.startswith("FRED_API_KEY="):
                value = stripped.split("=", 1)[1].strip().strip('"').strip("'")
                if value:
                    os.environ["FRED_API_KEY"] = value
                break
        if os.environ.get("FRED_API_KEY", "").strip():
            return True
    return False


def _fetch_fred_series(session: requests.Session, series_id: str, observation_start: str) -> pd.DataFrame:
    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        return pd.DataFrame(columns=["date", "value"])

    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": observation_start,
        "sort_order": "asc",
    }

    try:
        response = session.get(f"{FRED_OBS_URL}?{urlencode(params)}", timeout=30)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError):
        return pd.DataFrame(columns=["date", "value"])

    if not isinstance(payload, dict) or payload.get("error_code"):
        return pd.DataFrame(columns=["date", "value"])

    rows: list[dict[str, object]] = []
    for obs in payload.get("observations") or []:
        raw = str(obs.get("value", "")).strip()
        if not raw or raw == MISSING:
            continue
        try:
            value = float(raw)
        except ValueError:
            continue
        rows.append({"date": pd.to_datetime(obs["date"]), "value": value})

    return pd.DataFrame(rows)


def _to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    monthly = df.copy()
    monthly["month"] = monthly["date"].dt.strftime("%Y-%m")
    return monthly.sort_values("date").groupby("month", as_index=False).last()[["month", "value"]]


def _build_tracking_matrix(tracking: pd.DataFrame, y10: pd.DataFrame) -> pd.DataFrame:
    tracking_monthly = _to_monthly(tracking).rename(columns={"value": "tracking_proxy"})
    y10_monthly = _to_monthly(y10).rename(columns={"value": "ten_year_yield_pct"})

    merged = (
        tracking_monthly.merge(y10_monthly, on="month", how="inner")
        .sort_values("month")
        .tail(LOOKBACK_MONTHS)
        .reset_index(drop=True)
    )

    merged["proxy_earnings_yield_pct"] = merged["tracking_proxy"].map(earnings_yield_from_cape)
    merged["proxy_erp_pct"] = merged.apply(
        lambda row: equity_risk_premium_pct(row["proxy_earnings_yield_pct"], row["ten_year_yield_pct"]),
        axis=1,
    )
    return merged


def _matrix_is_stable(df: pd.DataFrame) -> bool:
    if df.empty or len(df) < LOOKBACK_MONTHS:
        return False
    required = ["tracking_proxy", "ten_year_yield_pct", "proxy_earnings_yield_pct", "proxy_erp_pct"]
    return not df[required].isnull().any().any()


def _load_tracking_matrix() -> pd.DataFrame | None:
    global _matrix_cache
    if _matrix_cache is not None:
        return _matrix_cache

    if not _ensure_fred_api_key():
        return None

    observation_start = (date.today().replace(day=1) - timedelta(days=LOOKBACK_MONTHS * 31)).isoformat()
    with requests.Session() as session:
        tracking_raw = _fetch_fred_series(session, TRACKING_SERIES, observation_start)
        y10_raw = _fetch_fred_series(session, Y10_SERIES, observation_start)

    if tracking_raw.empty or y10_raw.empty:
        return None

    matrix = _build_tracking_matrix(tracking_raw, y10_raw)
    if not _matrix_is_stable(matrix):
        return None

    _matrix_cache = matrix
    return matrix


def build_index_macro_context_experiment(*, market: str = "S&P 500 / ES") -> dict[str, Any]:
    """Shadow-only structural merge check. Not index valuation — do not wire to pillars."""
    matrix = _load_tracking_matrix()
    if matrix is None:
        return {
            "label": EXPERIMENTAL_LABEL,
            "market": market,
            "experimental": True,
            "not_index_valuation": True,
            "data_source_status": "UNSTABLE",
            "tracking_series": TRACKING_SERIES,
            "ten_year_series": Y10_SERIES,
            "note": (
                "UMCSENT is consumer sentiment, not CAPE. Structural merge experiment only — "
                "not for Undervalued/Overvalued/Fair Value labels."
            ),
        }

    proxy_hist = matrix["tracking_proxy"].astype(float).tolist()
    erp_hist = [float(v) for v in matrix["proxy_erp_pct"].tolist() if v is not None and v == v]
    latest = matrix.iloc[-1]
    proxy_val = float(latest["tracking_proxy"])
    erp_val = float(latest["proxy_erp_pct"])

    proxy_pct = percentile_rank(proxy_val, proxy_hist)
    erp_pct = percentile_rank(erp_val, erp_hist)
    composite = composite_valuation_score(proxy_pct, erp_pct)

    return {
        "label": EXPERIMENTAL_LABEL,
        "market": market,
        "experimental": True,
        "not_index_valuation": True,
        "data_source_status": "STABLE",
        "tracking_series": TRACKING_SERIES,
        "ten_year_series": Y10_SERIES,
        "monthly_observations": len(matrix),
        "latest_month": str(latest["month"]),
        "tracking_proxy_value": proxy_val,
        "ten_year_yield_pct": float(latest["ten_year_yield_pct"]),
        "proxy_composite_pct": composite,
        "proxy_tracking_percentile": proxy_pct,
        "proxy_erp_percentile": erp_pct,
        "note": (
            "UMCSENT is consumer sentiment, not CAPE. Composite percentiles are merge-test "
            "outputs only — not trusted index valuation."
        ),
    }
