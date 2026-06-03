"""Multi-year CFTC Traders in Financial Futures (fut_fin_txt) equity index history."""
from __future__ import annotations

import io
import logging
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from hptl.confluence.build_decision_table import (
    _cftc_contract_code_str,
    _find_column,
    _parse_cot_report_dates,
    _resolve_position_columns,
)
from hptl.cot.contracts import FINANCIAL_INDEX_CODE_TO_TARGET
from hptl.config import PROCESSED_DIR
from hptl.cot.contracts import (
    CME_INDEX_MAPPINGS,
    FINANCIAL_FUTURES_ONLY_URL_TEMPLATE,
    GOOD_WORKBOOK_MARKET_ORDER,
)
from hptl.cot.parser import (
    clean_columns,
    deduplicate_market_weeks,
    filter_cme_index_history,
    filter_good_workbook_markets,
)

logger = logging.getLogger(__name__)

# First year with reliable fut_fin_txt equity index history in this pipeline.
FINANCIAL_INDEX_START_YEAR = 2020

# Workbook labels (subset of GOOD_WORKBOOK_MARKET_ORDER).
WORKBOOK_INDEX_MARKETS: tuple[str, ...] = ("NASDAQ", "S&P 500", "DOW")

# Dashboard / confluence labels.
DASHBOARD_INDEX_MARKETS: tuple[str, ...] = ("NASDAQ / NQ", "S&P 500 / ES", "Dow / YM")

WORKBOOK_TO_DASHBOARD: dict[str, str] = {
    "NASDAQ": "NASDAQ / NQ",
    "S&P 500": "S&P 500 / ES",
    "DOW": "Dow / YM",
}

CODE_TO_LABEL: dict[str, str] = {
    "209742": "NASDAQ",
    "13874A": "S&P 500",
    "124603": "Dow",
    "099741": "Euro FX",
    "096742": "GBP",
    "097741": "JPY",
    "092741": "CHF",
    "232741": "AUD",
    "090741": "CAD",
    "112741": "NZD",
}

# All dashboard labels sourced from fut_fin_txt (indices + FX).
DASHBOARD_FINANCIAL_MARKETS: tuple[str, ...] = tuple(
    dict.fromkeys(FINANCIAL_INDEX_CODE_TO_TARGET.values())
)


def financial_index_cache_path(year: int) -> Path:
    return PROCESSED_DIR / f"cot_financial_index_{year}.csv"


def _candidate_position_columns() -> tuple[list[str], list[str]]:
    long_cols = [
        "lev_money_positions_long_all",
        "leveraged_money_positions_long_all",
        "m_money_positions_long_all",
        "managed_money_positions_long_all",
        "money_manager_positions_long_all",
        "noncomm_positions_long_all",
        "noncommercial_positions_long_all",
    ]
    short_cols = [
        "lev_money_positions_short_all",
        "leveraged_money_positions_short_all",
        "m_money_positions_short_all",
        "managed_money_positions_short_all",
        "money_manager_positions_short_all",
        "noncomm_positions_short_all",
        "noncommercial_positions_short_all",
    ]
    return long_cols, short_cols


def _fetch_financial_zip_bytes(year: int, *, timeout: int = 120) -> bytes | None:
    url = FINANCIAL_FUTURES_ONLY_URL_TEMPLATE.format(year=year)
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Financial futures download failed for %s: %s", year, exc)
        return None
    return response.content


def _extract_index_subset_from_zip(content: bytes) -> pd.DataFrame | None:
    zf = zipfile.ZipFile(io.BytesIO(content))
    inner = sorted(n for n in zf.namelist() if n.lower().endswith((".txt", ".csv")))
    if not inner:
        return None
    raw = pd.read_csv(zf.open(inner[0]), low_memory=False)
    cleaned = clean_columns(raw)
    code_col = _find_column(cleaned, "cftc_contract_market_code", "cftc_market_code")
    if code_col is None:
        return None
    codes = cleaned[code_col].map(_cftc_contract_code_str)
    keep = codes.isin(set(FINANCIAL_INDEX_CODE_TO_TARGET))
    filtered = cleaned.loc[keep].copy()
    return filtered if not filtered.empty else None


def _latest_report_dates_by_code(df: pd.DataFrame) -> dict[str, str]:
    """Latest report date per locked CFTC contract code (YYYY-MM-DD)."""
    if df is None or df.empty:
        return {}
    code_col = _find_column(df, "cftc_contract_market_code", "cftc_market_code")
    date_col = _find_column(df, "report_date_as_yyyy_mm_dd", "cot_report_date", "report_date", "date")
    if code_col is None or date_col is None:
        return {}
    out: dict[str, str] = {}
    parsed = _parse_cot_report_dates(df[date_col], source_name="financial_index_subset")
    work = df.copy()
    work["_code"] = work[code_col].map(_cftc_contract_code_str)
    work["_date"] = parsed
    for code in FINANCIAL_INDEX_CODE_TO_TARGET:
        sub = work.loc[work["_code"] == code, "_date"].dropna()
        if sub.empty:
            continue
        out[code] = pd.Timestamp(sub.max()).strftime("%Y-%m-%d")
    return out


def _cache_is_stale(year: int, remote_latest: dict[str, str], cache_path: Path) -> bool:
    if not cache_path.exists():
        return True
    if os.environ.get("HPTL_FORCE_FIN_INDEX_CACHE", "").strip() in {"1", "true", "yes"}:
        return True
    cached = pd.read_csv(cache_path, low_memory=False)
    cache_latest = _latest_report_dates_by_code(cached)
    for code, label in CODE_TO_LABEL.items():
        remote_d = remote_latest.get(code)
        cache_d = cache_latest.get(code)
        if remote_d and (not cache_d or remote_d > cache_d):
            logger.info(
                "Financial index cache stale for %s %s: cache=%s remote=%s",
                year,
                label,
                cache_d,
                remote_d,
            )
            return True
    return False


def invalidate_financial_index_year_cache(year: int | None = None) -> list[Path]:
    """Remove cached fut_fin subset CSV(s). Returns deleted paths."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    removed: list[Path] = []
    if year is not None:
        paths = [financial_index_cache_path(year)]
    else:
        paths = sorted(PROCESSED_DIR.glob("cot_financial_index_*.csv"))
    for path in paths:
        if path.exists():
            path.unlink()
            removed.append(path)
            logger.info("Removed stale financial index cache %s", path.name)
    return removed


def ensure_financial_index_year_cache(year: int, *, timeout: int = 120, force: bool = False) -> Path | None:
    """Download and cache raw fut_fin rows for equity index contract codes.

    Current-year caches refresh when CFTC publishes a newer report week than the
    on-disk CSV (never reuse a stale cache for an in-progress annual file).
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = financial_index_cache_path(year)
    current_year = datetime.now(timezone.utc).year

    # Weekly COT job sets HPTL_SKIP_LIVE_FEEDS=1. Re-downloading the fut_fin zip for every
    # year (120s timeout each) to re-check cache staleness is what hangs the confluence
    # rebuild when CFTC archives are slow. Honor the flag: reuse the on-disk cache as-is.
    if not force and str(os.environ.get("HPTL_SKIP_LIVE_FEEDS", "")).strip().lower() in {"1", "true", "yes"}:
        if cache_path.exists():
            print_financial_index_stage_validation(year, "cleaned_csv_cached_skip_live", cache_path, None)
            return cache_path
        return None

    content = _fetch_financial_zip_bytes(year, timeout=timeout)
    if content is None:
        return cache_path if cache_path.exists() else None

    remote_subset = _extract_index_subset_from_zip(content)
    if remote_subset is None:
        return cache_path if cache_path.exists() else None

    remote_latest = _latest_report_dates_by_code(remote_subset)
    print_financial_index_stage_validation(year, "raw_zip", remote_subset, remote_latest)

    should_write = force or not cache_path.exists()
    if not should_write and year >= current_year - 1:
        should_write = _cache_is_stale(year, remote_latest, cache_path)
    elif not should_write and year < current_year - 1:
        should_write = False

    if should_write:
        remote_subset.to_csv(cache_path, index=False)
        logger.info("Wrote financial index cache %s (%s rows)", cache_path.name, len(remote_subset))
        print_financial_index_stage_validation(year, "cleaned_csv", cache_path, None)
    else:
        print_financial_index_stage_validation(year, "cleaned_csv_cached", cache_path, None)

    return cache_path if cache_path.exists() else None


def print_financial_index_stage_validation(
    year: int,
    stage: str,
    data: pd.DataFrame | Path,
    remote_latest: dict[str, str] | None,
) -> None:
    """Log earliest/latest report dates per index contract at a pipeline stage."""
    if isinstance(data, Path):
        if not data.exists():
            print(f"FIN_INDEX_STAGE year={year} stage={stage}: MISSING {data}")
            return
        frame = pd.read_csv(data, low_memory=False)
        label = data.name
    else:
        frame = data
        label = stage

    latest = _latest_report_dates_by_code(frame)
    print(f"FIN_INDEX_STAGE year={year} stage={label} rows={len(frame)}")
    date_col = _find_column(frame, "cot_report_date", "report_date_as_yyyy_mm_dd", "report_date", "date")
    code_col = _find_column(frame, "cftc_contract_market_code", "cftc_market_code")
    market_col = _find_column(frame, "market")
    for code, name in CODE_TO_LABEL.items():
        dates: list[str] = []
        dash_target = FINANCIAL_INDEX_CODE_TO_TARGET.get(code)
        if code_col is not None and date_col is not None:
            codes = frame[code_col].map(_cftc_contract_code_str)
            sub = frame.loc[codes == code]
            if not sub.empty:
                parsed = _parse_cot_report_dates(sub[date_col], source_name=label)
                dates = sorted(parsed.dropna().dt.strftime("%Y-%m-%d").unique().tolist())
        elif market_col is not None and date_col is not None and dash_target:
            sub = frame.loc[frame[market_col].astype(str) == dash_target]
            if not sub.empty:
                parsed = pd.to_datetime(sub[date_col], errors="coerce")
                dates = sorted(parsed.dropna().dt.strftime("%Y-%m-%d").unique().tolist())
        earliest = dates[0] if dates else "n/a"
        latest_d = latest.get(code) or (dates[-1] if dates else "n/a")
        remote_d = (remote_latest or {}).get(code, "n/a")
        print(f"  {name} ({code}): earliest={earliest} latest={latest_d} remote_latest={remote_d} n_weeks={len(dates)}")


def load_financial_index_decision_rows(
    start_year: int = FINANCIAL_INDEX_START_YEAR,
    end_year: int | None = None,
) -> pd.DataFrame:
    """Decision-table schema rows for NASDAQ / NQ, S&P 500 / ES, Dow / YM."""
    end = end_year if end_year is not None else datetime.now(timezone.utc).year
    long_cols, short_cols = _candidate_position_columns()
    frames: list[pd.DataFrame] = []

    for year in range(start_year, end + 1):
        cache_path = ensure_financial_index_year_cache(year)
        if cache_path is None:
            continue
        filtered = pd.read_csv(cache_path, low_memory=False)
        if filtered.empty:
            continue

        market_col = _find_column(filtered, "market_and_exchange_names")
        date_col = _find_column(filtered, "report_date_as_yyyy_mm_dd", "cot_report_date", "report_date", "date")
        long_col, short_col, source_family = _resolve_position_columns(filtered, long_cols, short_cols)
        code_col = _find_column(filtered, "cftc_contract_market_code", "cftc_market_code")
        if not all([market_col, date_col, long_col, short_col, code_col]):
            continue

        x = pd.DataFrame()
        code_series = filtered[code_col].map(_cftc_contract_code_str)
        x["market"] = code_series.map(FINANCIAL_INDEX_CODE_TO_TARGET)
        x["raw_cftc_market_name"] = filtered[market_col].astype(str).str.strip()
        x["cot_report_date"] = _parse_cot_report_dates(filtered[date_col], source_name=f"fut_fin_txt_{year}.csv")
        x["long_value"] = pd.to_numeric(filtered[long_col], errors="coerce")
        x["short_value"] = pd.to_numeric(filtered[short_col], errors="coerce")
        x["long_col_used"] = long_col
        x["short_col_used"] = short_col
        x["position_source_family"] = source_family
        x["missing_reason"] = pd.NA
        x = x.dropna(subset=["market", "cot_report_date"]).copy()
        x["net_value"] = x["long_value"] - x["short_value"]
        missing_positions = x["long_value"].isna() | x["short_value"].isna()
        x.loc[missing_positions, "net_value"] = pd.NA
        x.loc[missing_positions, "missing_reason"] = "missing long/short values in resolved source columns"
        zero_pair = x["long_value"].eq(0) & x["short_value"].eq(0)
        x.loc[zero_pair, "net_value"] = pd.NA
        x.loc[zero_pair, "missing_reason"] = "long and short are both 0 in source row (treated as invalid/stale)"
        x["quality_score"] = (
            x["long_value"].notna().astype(int) * 5
            + x["short_value"].notna().astype(int) * 5
            + (~zero_pair).astype(int) * 5
            + x["net_value"].notna().astype(int) * 5
        )
        frames.append(x)

    if not frames:
        return pd.DataFrame()

    cot = pd.concat(frames, ignore_index=True)
    cot = cot.sort_values(["market", "cot_report_date", "quality_score"], ascending=[True, True, False])
    cot = cot.drop_duplicates(["market", "cot_report_date"], keep="first")
    cot = cot.sort_values(["market", "cot_report_date"]).reset_index(drop=True)
    if not cot.empty:
        print_financial_index_stage_validation(end, "parsed_decision_df", cot, None)
    return cot


def load_financial_index_workbook_rows(
    start_year: int = FINANCIAL_INDEX_START_YEAR,
    end_year: int | None = None,
) -> pd.DataFrame:
    """Workbook dashboard schema (NASDAQ, S&P 500, DOW) with full weekly history."""
    end = end_year if end_year is not None else datetime.now(timezone.utc).year
    frames: list[pd.DataFrame] = []

    for year in range(start_year, end + 1):
        cache_path = ensure_financial_index_year_cache(year)
        if cache_path is None:
            continue
        filtered = pd.read_csv(cache_path, low_memory=False)
        if filtered.empty:
            continue
        cleaned = clean_columns(filtered)
        rows = filter_cme_index_history(cleaned)
        if not rows.empty:
            frames.append(rows)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined = filter_good_workbook_markets(combined)
    combined = deduplicate_market_weeks(combined)
    return combined.sort_values(["market_name", "report_date"]).reset_index(drop=True)


def _gap_weeks(dates: pd.Series, threshold_days: int = 10) -> list[str]:
    d = pd.to_datetime(dates, errors="coerce").dropna().sort_values().reset_index(drop=True)
    gaps: list[str] = []
    for i in range(1, len(d)):
        delta = (d.iloc[i] - d.iloc[i - 1]).days
        if delta > threshold_days:
            gaps.append(f"{d.iloc[i - 1].date()}→{d.iloc[i].date()} ({delta}d)")
    return gaps


def print_equity_index_coverage_report(
    *,
    workbook_rows: pd.DataFrame | None = None,
    decision_rows: pd.DataFrame | None = None,
    gap_threshold_days: int = 10,
) -> None:
    """Validation logging for equity index futures across workbook and dashboard schemas."""
    print("=" * 72)
    print("EQUITY INDEX COT COVERAGE (CFTC fut_fin_txt — actual data only)")
    print("=" * 72)
    print("Locked contract codes:")
    for code, target in FINANCIAL_INDEX_CODE_TO_TARGET.items():
        mapping = CME_INDEX_MAPPINGS.get(code)
        name = mapping.cftc_market_name if mapping else "?"
        print(f"  {code} -> {target} ({name})")

    if workbook_rows is not None and not workbook_rows.empty:
        print("-" * 72)
        print("Workbook schema (Trader_Report / Market_Blocks)")
        for wb_name in WORKBOOK_INDEX_MARKETS:
            sub = workbook_rows[workbook_rows["market_name"].astype(str).str.upper() == wb_name.upper()]
            if sub.empty:
                print(f"  {wb_name}: MISSING (0 rows)")
                continue
            dates = pd.to_datetime(sub["report_date"], errors="coerce").dropna()
            gaps = _gap_weeks(dates, gap_threshold_days)
            print(
                f"  {wb_name}: rows={len(sub)} earliest={dates.min().date()} latest={dates.max().date()} "
                f"gaps>{gap_threshold_days}d={len(gaps)}"
            )
            for g in gaps[:5]:
                print(f"    gap: {g}")

    if decision_rows is not None and not decision_rows.empty:
        print("-" * 72)
        print("Dashboard / confluence schema")
        for dash_name in DASHBOARD_FINANCIAL_MARKETS:
            sub = decision_rows[decision_rows["market"].astype(str) == dash_name]
            if sub.empty:
                print(f"  {dash_name}: MISSING (0 rows)")
                continue
            dates = pd.to_datetime(sub["cot_report_date"], errors="coerce").dropna()
            gaps = _gap_weeks(dates, gap_threshold_days)
            print(
                f"  {dash_name}: rows={len(sub)} earliest={dates.min().date()} latest={dates.max().date()} "
                f"gaps>{gap_threshold_days}d={len(gaps)}"
            )
            for g in gaps[:5]:
                print(f"    gap: {g}")
    print("=" * 72)
