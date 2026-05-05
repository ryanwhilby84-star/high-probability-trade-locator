from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import logging

import requests

from hptl.cot.contracts import (
    CME_FUTURES_ONLY_URL,
    FINANCIAL_FUTURES_ONLY_URL_TEMPLATE,
    LEGACY_FUTURES_ONLY_URL_TEMPLATE,
)

from hptl.config import Settings
from hptl.shared.file_utils import ensure_dir

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DownloadResult:
    source_url: str
    raw_file_path: Path
    bytes_downloaded: int
    downloaded_at_utc: str
    warnings: list[str]


def download_latest_cot(settings: Settings) -> DownloadResult:
    """Download the configured CFTC COT historical compressed ZIP file."""
    ensure_dir(settings.raw_dir)
    source_url = settings.cot_source_url
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"cot_{settings.cot_report_type}_{settings.cot_year}_{timestamp}.zip"
    raw_path = settings.raw_dir / filename
    warnings: list[str] = []

    logger.info("Downloading COT data from %s", source_url)
    try:
        response = requests.get(source_url, timeout=settings.request_timeout_seconds)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to download CFTC COT data from {source_url}: {exc}") from exc

    content_type = response.headers.get("content-type", "")
    if "zip" not in content_type.lower() and not source_url.lower().endswith(".zip"):
        warnings.append(f"Unexpected content type: {content_type or 'unknown'}")

    raw_path.write_bytes(response.content)
    logger.info("Saved raw COT file to %s", raw_path)

    return DownloadResult(
        source_url=source_url,
        raw_file_path=raw_path,
        bytes_downloaded=len(response.content),
        downloaded_at_utc=datetime.utcnow().isoformat(timespec="seconds") + "Z",
        warnings=warnings,
    )


def download_cme_futures_only(settings: Settings) -> DownloadResult:
    """Download the current CFTC CME Futures Only report HTML/text page."""
    ensure_dir(settings.raw_dir)
    source_url = CME_FUTURES_ONLY_URL
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    raw_path = settings.raw_dir / f"cot_cme_futures_only_{timestamp}.html"
    warnings: list[str] = []

    logger.info("Downloading CME Futures Only COT data from %s", source_url)
    try:
        response = requests.get(source_url, timeout=settings.request_timeout_seconds)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to download CFTC CME Futures Only data from {source_url}: {exc}") from exc

    content_type = response.headers.get("content-type", "")
    if "html" not in content_type.lower() and "text" not in content_type.lower():
        warnings.append(f"Unexpected CME Futures Only content type: {content_type or 'unknown'}")

    raw_path.write_bytes(response.content)
    logger.info("Saved raw CME Futures Only file to %s", raw_path)

    return DownloadResult(
        source_url=source_url,
        raw_file_path=raw_path,
        bytes_downloaded=len(response.content),
        downloaded_at_utc=datetime.utcnow().isoformat(timespec="seconds") + "Z",
        warnings=warnings,
    )



def download_financial_futures_only_history(settings: Settings, year: int | None = None) -> DownloadResult:
    """Download annual Traders in Financial Futures; Futures Only history.

    CME equity index futures such as E-mini Nasdaq 100 and E-mini S&P 500
    live in this historical compressed dataset. Use this for NASDAQ/S&P
    backfill instead of broad commodity/ICE datasets.
    """
    ensure_dir(settings.raw_dir)
    selected_year = year or settings.cot_year
    source_url = FINANCIAL_FUTURES_ONLY_URL_TEMPLATE.format(year=selected_year)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    raw_path = settings.raw_dir / f"cot_financial_futures_only_{selected_year}_{timestamp}.zip"
    warnings: list[str] = []

    logger.info("Downloading Financial Futures Only COT history from %s", source_url)
    try:
        response = requests.get(source_url, timeout=settings.request_timeout_seconds)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to download CFTC Financial Futures Only history from {source_url}: {exc}") from exc

    content_type = response.headers.get("content-type", "")
    if "zip" not in content_type.lower() and not source_url.lower().endswith(".zip"):
        warnings.append(f"Unexpected Financial Futures Only content type: {content_type or 'unknown'}")

    raw_path.write_bytes(response.content)
    logger.info("Saved raw Financial Futures Only history file to %s", raw_path)

    return DownloadResult(
        source_url=source_url,
        raw_file_path=raw_path,
        bytes_downloaded=len(response.content),
        downloaded_at_utc=datetime.utcnow().isoformat(timespec="seconds") + "Z",
        warnings=warnings,
    )


def download_legacy_futures_only_history(settings: Settings, year: int | None = None) -> DownloadResult:
    """Download the annual historical Legacy Futures Only COT ZIP.

    This dataset contains the classic noncommercial/commercial fields and is
    used for CME equity-index futures history such as NASDAQ Mini and
    E-mini S&P 500.
    """
    ensure_dir(settings.raw_dir)
    selected_year = year or settings.cot_year
    source_url = LEGACY_FUTURES_ONLY_URL_TEMPLATE.format(year=selected_year)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    raw_path = settings.raw_dir / f"cot_legacy_futures_only_{selected_year}_{timestamp}.zip"
    warnings: list[str] = []

    logger.info("Downloading Legacy Futures Only COT history from %s", source_url)
    try:
        response = requests.get(source_url, timeout=settings.request_timeout_seconds)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to download CFTC Legacy Futures Only history from {source_url}: {exc}") from exc

    content_type = response.headers.get("content-type", "")
    if "zip" not in content_type.lower() and not source_url.lower().endswith(".zip"):
        warnings.append(f"Unexpected Legacy Futures Only content type: {content_type or 'unknown'}")

    raw_path.write_bytes(response.content)
    logger.info("Saved raw Legacy Futures Only history file to %s", raw_path)

    return DownloadResult(
        source_url=source_url,
        raw_file_path=raw_path,
        bytes_downloaded=len(response.content),
        downloaded_at_utc=datetime.utcnow().isoformat(timespec="seconds") + "Z",
        warnings=warnings,
    )
