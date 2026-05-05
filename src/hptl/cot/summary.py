from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from hptl.cot.downloader import DownloadResult
from hptl.cot.exporter import ExportResult
from hptl.shared.file_utils import write_text


@dataclass(frozen=True)
class UpdateSummary:
    markdown: str
    summary_file_path: Path
    warnings: list[str]


def build_update_summary(
    download: DownloadResult,
    export: ExportResult,
    summary_dir: Path,
    extra_sources: list[str] | None = None,
    warnings: list[str] | None = None,
) -> UpdateSummary:
    all_warnings = list(warnings or download.warnings)
    markets_preview = export.markets[:20]
    markets_text = "\n".join(f"- {market}" for market in markets_preview) if markets_preview else "- No market names detected"
    warnings_text = "\n".join(f"- {warning}" for warning in all_warnings) if all_warnings else "- None"
    extra_sources_text = "\n".join(f"- {source}" for source in extra_sources or []) or "- None"

    markdown = f"""# COT Update Summary

## Sources

- Primary source used: {download.source_url}
- Additional sources:
{extra_sources_text}
- Raw primary file saved: {download.raw_file_path}
- Bytes downloaded from primary source: {download.bytes_downloaded}
- Downloaded at UTC: {download.downloaded_at_utc}

## Import

- Rows imported from primary report: {export.rows_exported}
- Processed CSV: {export.processed_csv_path}

## Markets included preview

{markets_text}

## Export

- Excel workbook: {export.export_file_path}
- Workbook tabs: Dashboard, Trader_Report, Market_Blocks, Raw_Data_Slim, Source_Notes

## Warnings/errors

{warnings_text}
"""
    summary_path = summary_dir / f"cot_update_summary_{export.export_file_path.stem.replace('cot_update_', '')}.md"
    write_text(summary_path, markdown)
    return UpdateSummary(markdown=markdown, summary_file_path=summary_path, warnings=all_warnings)
