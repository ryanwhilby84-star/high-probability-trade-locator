"""Load CFTC TFF (fut_fin_txt) positioning for DXY and Treasury futures."""

from __future__ import annotations

import io
import logging
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from hptl.config import PROCESSED_DIR
from hptl.cot.contracts import FINANCIAL_FUTURES_ONLY_URL_TEMPLATE
from hptl.cot.positioning_percentiles import empirical_percentile_rank
from hptl.cot.tff_macro_contracts import (
    TFF_MACRO_CODE_TO_INSTRUMENT,
    WEEKS_HISTORY,
    WEEKS_PERCENTILE,
)

logger = logging.getLogger(__name__)

# Leveraged money (TFF institutional cohort).
_LONG_COLS = [
    "lev_money_positions_long_all",
    "leveraged_money_positions_long_all",
    "m_money_positions_long_all",
]
_SHORT_COLS = [
    "lev_money_positions_short_all",
    "leveraged_money_positions_short_all",
    "m_money_positions_short_all",
]
_OI_COLS = ["open_interest_all", "open_interest"]
_PCT_LONG_COLS = ["pct_of_oi_lev_money_long_all"]
_PCT_SHORT_COLS = ["pct_of_oi_lev_money_short_all"]


def _norm_code(raw: Any) -> str:
    s = str(raw or "").strip().upper().replace(".0", "")
    if s.isdigit():
        return s.zfill(6)
    return s


def _find_col(df: pd.DataFrame, *candidates: str) -> str | None:
    lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    for col in df.columns:
        cl = col.lower()
        for cand in candidates:
            if cand.lower() in cl:
                return col
    return None


def _resolve_cols(df: pd.DataFrame, candidates: list[str]) -> str | None:
    return _find_col(df, *candidates)


def _parse_dates(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series.astype(str).str.strip(), errors="coerce").dt.normalize()


def tff_macro_cache_path(year: int) -> Path:
    return PROCESSED_DIR / f"cot_tff_macro_{year}.csv"


def _fetch_zip(year: int, *, timeout: int = 120) -> bytes | None:
    url = FINANCIAL_FUTURES_ONLY_URL_TEMPLATE.format(year=year)
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.content
    except requests.RequestException as exc:
        logger.warning("TFF macro download failed %s: %s", year, exc)
        return None


def _extract_macro_subset(content: bytes) -> pd.DataFrame | None:
    zf = zipfile.ZipFile(io.BytesIO(content))
    inner = sorted(n for n in zf.namelist() if n.lower().endswith((".txt", ".csv")))
    if not inner:
        return None
    raw = pd.read_csv(zf.open(inner[0]), low_memory=False)
    code_col = _find_col(raw, "cftc_contract_market_code", "cftc_market_code")
    if code_col is None:
        return None
    codes = set(TFF_MACRO_CODE_TO_INSTRUMENT)
    keep = raw[code_col].map(_norm_code).isin(codes)
    sub = raw.loc[keep].copy()
    return sub if not sub.empty else None


def ensure_tff_macro_year_cache(year: int, *, force: bool = False, timeout: int = 120) -> Path | None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    cache = tff_macro_cache_path(year)
    if not force and str(os.environ.get("HPTL_SKIP_LIVE_FEEDS", "")).strip().lower() in {"1", "true", "yes"}:
        return cache if cache.exists() else None
    if cache.exists() and not force:
        return cache
    content = _fetch_zip(year, timeout=timeout)
    if content is None:
        return cache if cache.exists() else None
    subset = _extract_macro_subset(content)
    if subset is None or subset.empty:
        return cache if cache.exists() else None
    subset.to_csv(cache, index=False)
    return cache


def _week_row(
    *,
    report_date: str,
    long_v: float | None,
    short_v: float | None,
    oi: float | None,
    pct_long: float | None,
    pct_short: float | None,
    cftc_code: str,
    market_name: str,
    instrument_id: str,
) -> dict[str, Any]:
    net = (long_v - short_v) if long_v is not None and short_v is not None else None
    return {
        "date": report_date,
        "instrument_id": instrument_id,
        "cftc_code": cftc_code,
        "market_name": market_name,
        "long": long_v,
        "short": short_v,
        "net": net,
        "open_interest": oi,
        "pct_long": pct_long,
        "pct_short": pct_short,
        "one_week_net_change": None,
        "four_week_net_change": None,
        "net_percentile_13w": None,
        "source": "cftc_tff_fut_fin_txt",
        "trader_group": "leveraged_money",
    }


def _apply_changes_and_percentiles(weeks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for i, w in enumerate(weeks):
        row = dict(w)
        net = row.get("net")
        if i >= 1 and net is not None and weeks[i - 1].get("net") is not None:
            row["one_week_net_change"] = round(net - weeks[i - 1]["net"], 1)
        if i >= 4 and net is not None and weeks[i - 4].get("net") is not None:
            row["four_week_net_change"] = round(net - weeks[i - 4]["net"], 1)
        window = [x.get("net") for x in weeks[max(0, i - WEEKS_PERCENTILE + 1) : i + 1]]
        window = [x for x in window if x is not None]
        if net is not None and window:
            pct = empirical_percentile_rank(window, net)
            row["net_percentile_13w"] = round(float(pct), 1) if pct == pct else None
        out.append(row)
    return out


def load_tff_macro_weeks(
    *,
    start_year: int | None = None,
    end_year: int | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Weekly TFF leveraged-money positioning per macro instrument."""
    now_year = datetime.now(timezone.utc).year
    start = start_year or now_year - 2
    end = end_year or now_year
    by_inst: dict[str, list[dict[str, Any]]] = {iid: [] for iid in TFF_MACRO_CODE_TO_INSTRUMENT.values()}

    for year in range(start, end + 1):
        cache = ensure_tff_macro_year_cache(year)
        if cache is None or not cache.exists():
            continue
        df = pd.read_csv(cache, low_memory=False)
        if df.empty:
            continue
        code_col = _find_col(df, "cftc_contract_market_code", "cftc_market_code")
        date_col = _find_col(df, "report_date_as_yyyy_mm_dd", "cot_report_date", "report_date")
        market_col = _find_col(df, "market_and_exchange_names", "market")
        long_col = _resolve_cols(df, _LONG_COLS)
        short_col = _resolve_cols(df, _SHORT_COLS)
        oi_col = _resolve_cols(df, _OI_COLS)
        pct_l_col = _resolve_cols(df, _PCT_LONG_COLS)
        pct_s_col = _resolve_cols(df, _PCT_SHORT_COLS)
        if not all([code_col, date_col, long_col, short_col]):
            continue

        work = df.copy()
        work["_code"] = work[code_col].map(_norm_code)
        work["_date"] = _parse_dates(work[date_col])
        work = work.dropna(subset=["_date"])
        for code, iid in TFF_MACRO_CODE_TO_INSTRUMENT.items():
            sub = work.loc[work["_code"] == code].sort_values("_date")
            for _, r in sub.iterrows():
                rd = pd.Timestamp(r["_date"]).strftime("%Y-%m-%d")
                by_inst[iid].append(
                    _week_row(
                        report_date=rd,
                        long_v=_num(r.get(long_col)),
                        short_v=_num(r.get(short_col)),
                        oi=_num(r.get(oi_col)) if oi_col else None,
                        pct_long=_num(r.get(pct_l_col)) if pct_l_col else None,
                        pct_short=_num(r.get(pct_s_col)) if pct_s_col else None,
                        cftc_code=code,
                        market_name=str(r.get(market_col) or "") if market_col else "",
                        instrument_id=iid,
                    )
                )

    deduped: dict[str, list[dict[str, Any]]] = {}
    for iid, rows in by_inst.items():
        by_date: dict[str, dict[str, Any]] = {}
        for r in sorted(rows, key=lambda x: x["date"]):
            by_date[r["date"]] = r
        weeks = _apply_changes_and_percentiles(list(by_date.values()))
        deduped[iid] = weeks[-WEEKS_HISTORY:] if len(weeks) > WEEKS_HISTORY else weeks
    return deduped


def _num(v: Any) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        f = float(v)
        return f if pd.notna(f) else None
    except (TypeError, ValueError):
        return None


def latest_tff_macro_snapshot(weeks_by_inst: dict[str, list[dict[str, Any]]] | None = None) -> dict[str, Any]:
    weeks_by_inst = weeks_by_inst or load_tff_macro_weeks()
    instruments: dict[str, Any] = {}
    for iid, weeks in weeks_by_inst.items():
        if not weeks:
            instruments[iid] = {"instrument_id": iid, "available": False, "error": "no_tff_rows"}
            continue
        latest = weeks[-1]
        instruments[iid] = {
            "instrument_id": iid,
            "available": True,
            "latest": latest,
            "weeks": weeks,
            "report_date": latest.get("date"),
            "source": "cftc_tff_fut_fin_txt",
            "trader_group": "leveraged_money",
        }
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "cftc_tff_fut_fin_txt",
        "trader_group": "leveraged_money (TFF)",
        "instruments": instruments,
    }
