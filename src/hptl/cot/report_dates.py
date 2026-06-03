"""Detect latest COT report dates from local exports and freshly downloaded CFTC files."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import pandas as pd

from hptl.config import Settings, get_settings
from hptl.confluence.build_decision_table import (
    TARGET_MARKETS,
    _merge_cot_from_cleaned_csvs,
    tracked_master_csv_path,
)
from hptl.cot.contracts import GOOD_WORKBOOK_MARKET_ORDER
from hptl.cot.downloader import download_financial_futures_only_history, download_latest_cot
from hptl.cot.parser import (
    align_index_history_to_date_range,
    cot_history_to_dashboard_rows,
    deduplicate_market_weeks,
    filter_cme_index_history,
    filter_good_workbook_markets,
    parse_cot_file,
)
from hptl.cot.update_log import log_kv, log_step

logger = logging.getLogger(__name__)

# Weekly tracked set (workbook canonical names — matches user-facing COT list).
WEEKLY_WORKBOOK_MARKETS: tuple[str, ...] = tuple(GOOD_WORKBOOK_MARKET_ORDER)


@dataclass(frozen=True)
class CftcProbeResult:
    latest_report_date: pd.Timestamp | None
    dashboard_rows: pd.DataFrame
    rows_fetched: int
    source_urls: tuple[str, ...]
    commodity_max_report_date: pd.Timestamp | None = None
    financial_max_report_date: pd.Timestamp | None = None
    commodity_raw_path: str | None = None
    financial_raw_path: str | None = None


def _max_report_date(rows: pd.DataFrame) -> pd.Timestamp | None:
    if rows.empty or "report_date" not in rows.columns:
        return None
    dates = pd.to_datetime(rows["report_date"], errors="coerce").dropna()
    if dates.empty:
        return None
    return pd.Timestamp(dates.max()).normalize()


def _normalize_report_ts(value: Any) -> pd.Timestamp | None:
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return None
    return pd.Timestamp(ts).tz_localize(None)


def get_latest_local_report_date() -> pd.Timestamp | None:
    """Max ``cot_report_date`` in tracked master or merged ``cot_cleaned_*.csv`` inputs."""
    master = tracked_master_csv_path()
    if master.exists():
        try:
            df = pd.read_csv(master, usecols=["cot_report_date"], low_memory=False)
            dates = pd.to_datetime(df["cot_report_date"], errors="coerce").dropna()
            if not dates.empty:
                return pd.Timestamp(dates.max()).normalize()
        except (OSError, ValueError, KeyError) as exc:
            logger.warning("Could not read tracked master for latest date: %s", exc)

    cot = _merge_cot_from_cleaned_csvs()
    if cot.empty or "cot_report_date" not in cot.columns:
        return None
    dates = pd.to_datetime(cot["cot_report_date"], errors="coerce").dropna()
    if dates.empty:
        return None
    return pd.Timestamp(dates.max()).normalize()


def tracked_market_week_keys(cot: pd.DataFrame | None = None) -> set[tuple[str, str]]:
    """``(market, YYYY-MM-DD)`` keys for dashboard TARGET_MARKETS."""
    frame = cot if cot is not None else _merge_cot_from_cleaned_csvs()
    if frame.empty:
        return set()
    sub = frame.loc[frame["market"].isin(TARGET_MARKETS)].copy()
    sub["cot_report_date"] = pd.to_datetime(sub["cot_report_date"], errors="coerce")
    sub = sub.dropna(subset=["market", "cot_report_date"])
    return {
        (str(row["market"]), pd.Timestamp(row["cot_report_date"]).strftime("%Y-%m-%d"))
        for _, row in sub.iterrows()
    }


def build_combined_dashboard_rows(
    settings: Settings | None = None,
    *,
    commodity_download=None,
    financial_download=None,
) -> tuple[pd.DataFrame, tuple[str, ...]]:
    """Parse CFTC commodity + financial ZIPs into deduped workbook dashboard rows."""
    settings = settings or get_settings()

    if commodity_download is None:
        commodity_download = download_latest_cot(settings)
    if financial_download is None:
        financial_download = download_financial_futures_only_history(settings, year=settings.cot_year)

    cot_df = parse_cot_file(commodity_download.raw_file_path)
    commodity_rows = cot_history_to_dashboard_rows(cot_df, source_report=settings.cot_report_type)
    commodity_rows = filter_good_workbook_markets(commodity_rows)

    financial_df = parse_cot_file(financial_download.raw_file_path)
    index_rows = filter_cme_index_history(financial_df)
    index_rows = align_index_history_to_date_range(index_rows, commodity_rows)

    combined = pd.concat([commodity_rows, index_rows], ignore_index=True, sort=False)
    combined = filter_good_workbook_markets(combined)
    combined = deduplicate_market_weeks(combined)

    urls = (commodity_download.source_url, financial_download.source_url)
    return combined, urls


def probe_cftc_latest_report_date(settings: Settings | None = None) -> CftcProbeResult:
    """Download CFTC files and return latest report date across tracked workbook markets."""
    settings = settings or get_settings()
    log_step("CFTC probe: downloading disaggregated commodities ZIP (this can take 30–120s)…")
    commodity_dl = download_latest_cot(settings)
    log_kv("commodity raw file", commodity_dl.raw_file_path)
    log_kv("commodity bytes", commodity_dl.bytes_downloaded)

    log_step("CFTC probe: downloading financial futures ZIP…")
    financial_dl = download_financial_futures_only_history(settings, year=settings.cot_year)
    log_kv("financial raw file", financial_dl.raw_file_path)
    log_kv("financial bytes", financial_dl.bytes_downloaded)

    log_step("CFTC probe: parsing ZIPs…")
    cot_df = parse_cot_file(commodity_dl.raw_file_path)
    commodity_rows = cot_history_to_dashboard_rows(cot_df, source_report=settings.cot_report_type)
    commodity_rows = filter_good_workbook_markets(commodity_rows)
    commodity_max = _max_report_date(commodity_rows)

    financial_df = parse_cot_file(financial_dl.raw_file_path)
    index_rows = filter_cme_index_history(financial_df)
    index_rows = align_index_history_to_date_range(index_rows, commodity_rows)
    financial_max = _max_report_date(index_rows)

    combined = pd.concat([commodity_rows, index_rows], ignore_index=True, sort=False)
    combined = filter_good_workbook_markets(combined)
    combined = deduplicate_market_weeks(combined)
    urls = (commodity_dl.source_url, financial_dl.source_url)
    latest = _max_report_date(combined)

    log_kv("parsed max report date (commodity)", commodity_max.strftime("%Y-%m-%d") if commodity_max is not None else None)
    log_kv("parsed max report date (financial)", financial_max.strftime("%Y-%m-%d") if financial_max is not None else None)
    log_kv("parsed max report date (combined)", latest.strftime("%Y-%m-%d") if latest is not None else None)

    return CftcProbeResult(
        latest_report_date=latest,
        dashboard_rows=combined,
        rows_fetched=len(combined),
        source_urls=urls,
        commodity_max_report_date=commodity_max,
        financial_max_report_date=financial_max,
        commodity_raw_path=str(commodity_dl.raw_file_path),
        financial_raw_path=str(financial_dl.raw_file_path),
    )


def workbook_markets_on_date(dashboard_rows: pd.DataFrame, report_date: pd.Timestamp) -> set[str]:
    if dashboard_rows.empty:
        return set()
    d = pd.to_datetime(dashboard_rows["report_date"], errors="coerce").dt.normalize()
    target = pd.Timestamp(report_date).normalize()
    mask = d == target
    return set(dashboard_rows.loc[mask, "market_name"].astype(str).str.strip())


def missing_workbook_markets(dashboard_rows: pd.DataFrame, report_date: pd.Timestamp) -> list[str]:
    present = workbook_markets_on_date(dashboard_rows, report_date)
    return [m for m in WEEKLY_WORKBOOK_MARKETS if m not in present]
