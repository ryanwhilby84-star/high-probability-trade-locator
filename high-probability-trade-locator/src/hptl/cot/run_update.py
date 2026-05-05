from __future__ import annotations

import logging

import pandas as pd

from hptl.config import get_settings
from hptl.cot.contracts import CME_INDEX_MAPPINGS
from hptl.cot.downloader import download_financial_futures_only_history, download_latest_cot
from hptl.cot.exporter import export_cot_workbook
from hptl.cot.parser import (
    align_index_history_to_date_range,
    cot_history_to_dashboard_rows,
    deduplicate_market_weeks,
    filter_cme_index_history,
    filter_good_workbook_markets,
    parse_cot_file,
)
from hptl.cot.summary import build_update_summary
from hptl.logging_setup import setup_logging

logger = logging.getLogger(__name__)


def _history_count_warnings(index_history_df: pd.DataFrame, minimum_rows: int = 5) -> list[str]:
    warnings: list[str] = []
    for code, mapping in CME_INDEX_MAPPINGS.items():
        rows_found = 0
        if not index_history_df.empty and "cftc_contract_market_code" in index_history_df.columns:
            rows_found = int((index_history_df["cftc_contract_market_code"].astype(str) == code).sum())
        if rows_found < minimum_rows:
            warnings.append(
                f"Fewer than {minimum_rows} historical rows found for {mapping.dashboard_name} "
                f"({mapping.cftc_market_name}, code {code}); found {rows_found}."
            )
    return warnings


def run() -> int:
    setup_logging()
    settings = get_settings()
    warnings: list[str] = []

    download = download_latest_cot(settings)
    cot_df = parse_cot_file(download.raw_file_path)
    warnings.extend(download.warnings)

    # Preserve the existing commodity/history pipeline from the configured COT
    # report, then append CME equity-index history from Financial Futures Only.
    # Treat the first/good workbook market set as the source of truth.
    # This keeps the commodity output unchanged and blocks unrelated rows
    # from broad CFTC files before NASDAQ/S&P are appended.
    commodity_dashboard_rows = cot_history_to_dashboard_rows(cot_df, source_report=settings.cot_report_type)
    commodity_dashboard_rows = filter_good_workbook_markets(commodity_dashboard_rows)

    financial_download = download_financial_futures_only_history(settings, year=settings.cot_year)
    financial_df = parse_cot_file(financial_download.raw_file_path)
    index_history_df = filter_cme_index_history(financial_df)
    index_history_df = align_index_history_to_date_range(index_history_df, commodity_dashboard_rows)
    warnings.extend(financial_download.warnings)
    warnings.extend(_history_count_warnings(index_history_df))

    combined_dashboard_rows = pd.concat([commodity_dashboard_rows, index_history_df], ignore_index=True, sort=False)
    combined_dashboard_rows = filter_good_workbook_markets(combined_dashboard_rows)
    combined_dashboard_rows = deduplicate_market_weeks(combined_dashboard_rows)

    export = export_cot_workbook(
        cot_df,
        settings,
        source_url=download.source_url,
        dashboard_df=combined_dashboard_rows,
        extra_sources=[financial_download.source_url],
        warnings=warnings,
    )
    summary = build_update_summary(
        download,
        export,
        settings.exports_dir,
        extra_sources=[financial_download.source_url],
        warnings=warnings,
    )

    print(summary.markdown)
    logger.info("COT update complete: %s", export.export_file_path)
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
