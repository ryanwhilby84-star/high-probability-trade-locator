from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import os

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
EXPORTS_DIR = DATA_DIR / "exports"

CFTC_URLS = {
    "disaggregated_futures_only": "https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip",
    "financial_futures_only": "https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip",
    "legacy_futures_only": "https://www.cftc.gov/files/dea/history/deacot{year}.zip",
}


@dataclass(frozen=True)
class Settings:
    cot_report_type: str
    cot_year: int
    request_timeout_seconds: int
    raw_dir: Path = RAW_DIR
    processed_dir: Path = PROCESSED_DIR
    exports_dir: Path = EXPORTS_DIR

    @property
    def cot_source_url(self) -> str:
        if self.cot_report_type not in CFTC_URLS:
            supported = ", ".join(sorted(CFTC_URLS))
            raise ValueError(f"Unsupported COT_REPORT_TYPE={self.cot_report_type!r}. Supported: {supported}")
        return CFTC_URLS[self.cot_report_type].format(year=self.cot_year)


def get_settings() -> Settings:
    year_value = os.getenv("COT_YEAR", "").strip()
    cot_year = int(year_value) if year_value else date.today().year

    return Settings(
        cot_report_type=os.getenv("COT_REPORT_TYPE", "disaggregated_futures_only").strip(),
        cot_year=cot_year,
        request_timeout_seconds=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30")),
    )
