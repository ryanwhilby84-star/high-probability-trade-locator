"""CFTC download validation and retry — Phase 2A."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from hptl.config import get_settings
from hptl.confluence.build_decision_table import TARGET_MARKETS
from hptl.cot.contracts import CME_INDEX_MAPPINGS, GOOD_WORKBOOK_DISPLAY_NAMES
from hptl.cot.cot_failures import log_cot_failure
from hptl.cot.downloader import DownloadResult, download_financial_futures_only_history, download_latest_cot
from hptl.cot.exporter import export_cot_workbook
from hptl.cot.parser import (
    align_index_history_to_date_range,
    cot_history_to_dashboard_rows,
    deduplicate_market_weeks,
    filter_cme_index_history,
    filter_good_workbook_markets,
    parse_cot_file,
)
from hptl.cot.report_dates import _max_report_date
from hptl.cot.summary import build_update_summary
from hptl.cot.update_log import log_kv, log_step
from hptl.cot.workbook_export import WorkbookExportPaths, _history_count_warnings
from hptl.logging_setup import setup_logging

MAX_DOWNLOAD_RETRIES = 3
RETRY_DELAY_SECONDS = 2.0


@dataclass
class DownloadValidationResult:
    ok: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    attempts: int = 0
    commodity_path: Path | None = None
    financial_path: Path | None = None
    max_report_date: str | None = None
    markets_on_latest_week: list[str] = field(default_factory=list)
    missing_markets: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "attempts": self.attempts,
            "commodity_path": str(self.commodity_path) if self.commodity_path else None,
            "financial_path": str(self.financial_path) if self.financial_path else None,
            "max_report_date": self.max_report_date,
            "markets_on_latest_week": list(self.markets_on_latest_week),
            "missing_markets": list(self.missing_markets),
        }


def _validate_raw_file(path: Path) -> list[str]:
    errors: list[str] = []
    if not path.exists():
        errors.append(f"download file missing: {path}")
        return errors
    size = path.stat().st_size
    if size <= 0:
        errors.append(f"download file empty: {path}")
    return errors


def _validate_parsed_frame(
    rows: pd.DataFrame,
    *,
    label: str,
    require_markets: bool = True,
) -> tuple[list[str], list[str], str | None, list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    if rows is None or rows.empty:
        errors.append(f"{label}: parse produced zero rows")
        return errors, warnings, None, [], list(TARGET_MARKETS)

    max_dt = _max_report_date(rows)
    max_iso = max_dt.strftime("%Y-%m-%d") if max_dt is not None else None
    if max_iso is None:
        errors.append(f"{label}: no report date in parsed rows")
        return errors, warnings, None, [], list(TARGET_MARKETS)

    latest = rows.copy()
    if "date" in latest.columns:
        latest["_d"] = pd.to_datetime(latest["date"], errors="coerce")
        latest = latest[latest["_d"] == max_dt]
    present = sorted(set(latest["market"].astype(str).tolist())) if "market" in latest.columns else []
    missing = sorted(set(TARGET_MARKETS) - set(present))
    if require_markets and missing:
        errors.append(f"{label}: missing markets on {max_iso}: {', '.join(missing[:8])}{'…' if len(missing) > 8 else ''}")

    req_cols = ("commercial_long", "noncommercial_long")
    for col in req_cols:
        if col not in rows.columns:
            errors.append(f"{label}: column {col} absent after parse")
        elif latest[col].isna().all():
            errors.append(f"{label}: {col} all null on latest week")

    return errors, warnings, max_iso, present, missing


def _download_with_retries(
    fn: Callable[[], DownloadResult],
    *,
    label: str,
) -> tuple[DownloadResult | None, list[dict[str, Any]]]:
    """Up to MAX_DOWNLOAD_RETRIES attempts; log each failure."""
    attempts: list[dict[str, Any]] = []
    last_exc: Exception | None = None
    for attempt in range(1, MAX_DOWNLOAD_RETRIES + 1):
        try:
            result = fn()
            file_errors = _validate_raw_file(result.raw_file_path)
            if file_errors:
                raise RuntimeError("; ".join(file_errors))
            attempts.append({"attempt": attempt, "status": "ok", "path": str(result.raw_file_path)})
            return result, attempts
        except Exception as exc:
            last_exc = exc
            msg = f"{type(exc).__name__}: {exc}"
            attempts.append({"attempt": attempt, "status": "failed", "error": msg})
            log_cot_failure(
                failure_type="download",
                source=label,
                error=msg,
                retry_result=f"attempt {attempt}/{MAX_DOWNLOAD_RETRIES}",
            )
            if attempt < MAX_DOWNLOAD_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)
    if last_exc is not None:
        log_cot_failure(
            failure_type="download",
            source=label,
            error=str(last_exc),
            retry_result=f"failed after {MAX_DOWNLOAD_RETRIES} attempts",
            detail={"attempts": attempts},
        )
    return None, attempts


def run_workbook_export_resilient() -> tuple[WorkbookExportPaths | None, DownloadValidationResult]:
    """Download (with retries), validate, parse, and export workbook + processed CSV."""
    setup_logging()
    settings = get_settings()
    validation = DownloadValidationResult(ok=False)
    warnings: list[str] = []

    commodity_dl, commodity_attempts = _download_with_retries(
        lambda: download_latest_cot(settings),
        label="CFTC commodity ZIP",
    )
    validation.attempts = max(validation.attempts, len(commodity_attempts))
    if commodity_dl is None:
        validation.errors.append("commodity download failed after retries")
        return None, validation

    validation.commodity_path = commodity_dl.raw_file_path
    try:
        cot_df = parse_cot_file(commodity_dl.raw_file_path)
    except Exception as exc:
        validation.errors.append(f"commodity parse failed: {exc}")
        log_cot_failure(failure_type="parse", source="commodity", error=str(exc))
        return None, validation

    commodity_dashboard_rows = cot_history_to_dashboard_rows(cot_df, source_report=settings.cot_report_type)
    commodity_dashboard_rows = filter_good_workbook_markets(commodity_dashboard_rows)
    errs, warns, max_iso, present, missing = _validate_parsed_frame(
        commodity_dashboard_rows, label="commodity", require_markets=False
    )
    validation.errors.extend(errs)
    validation.warnings.extend(warns)
    warnings.extend(commodity_dl.warnings)

    financial_dl, financial_attempts = _download_with_retries(
        lambda: download_financial_futures_only_history(settings, year=settings.cot_year),
        label="CFTC financial futures ZIP",
    )
    validation.attempts = max(validation.attempts, len(financial_attempts))
    if financial_dl is None:
        validation.errors.append("financial download failed after retries")
        return None, validation

    validation.financial_path = financial_dl.raw_file_path
    try:
        financial_df = parse_cot_file(financial_dl.raw_file_path)
    except Exception as exc:
        validation.errors.append(f"financial parse failed: {exc}")
        log_cot_failure(failure_type="parse", source="financial", error=str(exc))
        return None, validation

    index_history_df = filter_cme_index_history(financial_df)
    index_history_df = align_index_history_to_date_range(index_history_df, commodity_dashboard_rows)
    warnings.extend(financial_dl.warnings)
    warnings.extend(_history_count_warnings(index_history_df))

    combined = pd.concat([commodity_dashboard_rows, index_history_df], ignore_index=True, sort=False)
    combined = filter_good_workbook_markets(combined)
    combined = deduplicate_market_weeks(combined)

    errs, warns, max_iso, present, missing = _validate_parsed_frame(combined, label="combined")
    validation.errors.extend(errs)
    validation.warnings.extend(warns)
    validation.max_report_date = max_iso
    validation.markets_on_latest_week = present
    validation.missing_markets = missing

    if validation.errors:
        validation.ok = False
        log_cot_failure(
            failure_type="download_validation",
            source="workbook_export",
            error="; ".join(validation.errors),
            detail=validation.to_dict(),
        )
        return None, validation

    log_step("Workbook export: validation passed — writing exports")
    export = export_cot_workbook(
        cot_df,
        settings,
        source_url=commodity_dl.source_url,
        dashboard_df=combined,
        extra_sources=[financial_dl.source_url],
        warnings=warnings,
    )
    summary = build_update_summary(
        commodity_dl,
        export,
        settings.exports_dir,
        extra_sources=[financial_dl.source_url],
        warnings=warnings,
    )
    print(summary.markdown)
    validation.ok = True
    validation.warnings.extend(warnings)
    paths = WorkbookExportPaths(
        workbook_path=export.export_file_path,
        processed_csv_path=export.processed_csv_path,
    )
    return paths, validation
