"""Price API configuration — OANDA + Alpha Vantage (keys never logged)."""

from __future__ import annotations

from dataclasses import dataclass
import os

from dotenv import load_dotenv

load_dotenv()


class PriceApiConfigError(RuntimeError):
    """Missing or invalid price API configuration."""


@dataclass(frozen=True)
class PriceApiConfig:
    oanda_configured: bool
    alpha_vantage_configured: bool

    @property
    def ready(self) -> bool:
        return self.oanda_configured and self.alpha_vantage_configured


def load_price_api_config() -> PriceApiConfig:
    """Read price API keys from environment (values are never returned in logs)."""
    from hptl.config import get_alpha_vantage_api_key, get_oanda_api_key

    return PriceApiConfig(
        oanda_configured=bool(get_oanda_api_key()),
        alpha_vantage_configured=bool(get_alpha_vantage_api_key()),
    )


def validate_price_api_keys(*, probe_live: bool = True) -> PriceApiConfig:
    """Validate both keys exist; optionally probe live APIs without printing secrets."""
    cfg = load_price_api_config()
    missing: list[str] = []
    if not cfg.oanda_configured:
        missing.append("OANDA_API_KEY")
    if not cfg.alpha_vantage_configured:
        missing.append("ALPHA_VANTAGE_API_KEY")
    if missing:
        raise PriceApiConfigError(f"Missing required environment variables: {', '.join(missing)}")

    if probe_live:
        from hptl.oanda.oanda_adapter import validate_oanda_connection
        from hptl.alpha_vantage.alpha_adapter import validate_alpha_vantage_connection

        validate_oanda_connection()
        validate_alpha_vantage_connection()

    return cfg
