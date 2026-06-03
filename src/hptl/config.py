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


def get_fred_api_key() -> str:
    """FRED Web Services API key (optional). Used by ``hptl.macro.fred_api`` / rates download."""
    return os.getenv("FRED_API_KEY", "").strip()


def get_finnhub_api_key() -> str:
    """Finnhub API key (optional). Calendar + filtered headlines in ``market_environment_feed``."""
    return os.getenv("FINNHUB_API_KEY", "").strip()


def get_openweather_api_key() -> str:
    """OpenWeather API key (optional). Used by ``hptl.intelligence.weather_adapter``."""
    return os.getenv("OPENWEATHER_API_KEY", "").strip()


def get_tradingview_webhook_secret() -> str:
    """Shared secret for TradingView / dashboard journal POSTs (logging only)."""
    return os.getenv("TRADINGVIEW_WEBHOOK_SECRET", "").strip()


def get_oanda_api_key() -> str:
    """OANDA v20 personal access token (Bearer). Required for live instrument audit."""
    return (os.getenv("OANDA_API_KEY") or os.getenv("OANDA_ACCESS_TOKEN") or "").strip()


def get_oanda_account_id() -> str:
    """OANDA account id (e.g. 101-004-...). If unset, first account from API is used."""
    return (os.getenv("OANDA_ACCOUNT_ID") or os.getenv("OANDA_ACCOUNT") or "").strip()


def get_oanda_api_host() -> str:
    """OANDA REST base URL. ``OANDA_ENVIRONMENT=live`` selects production; default practice."""
    explicit = os.getenv("OANDA_API_URL", "").strip().rstrip("/")
    if explicit:
        return explicit
    env = os.getenv("OANDA_ENVIRONMENT", "practice").strip().lower()
    if env in {"live", "production", "fxtrade"}:
        return "https://api-fxtrade.oanda.com"
    return "https://api-fxpractice.oanda.com"


def get_alpha_vantage_api_key() -> str:
    """Alpha Vantage API key (optional unless running price coverage audit)."""
    return (os.getenv("ALPHA_VANTAGE_API_KEY") or os.getenv("ALPHAVANTAGE_API_KEY") or "").strip()
