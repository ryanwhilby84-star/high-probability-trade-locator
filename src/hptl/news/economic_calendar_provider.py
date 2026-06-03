"""Resolve economic calendar upstream from environment (no network)."""
from __future__ import annotations

import os
from typing import Any

from hptl.config import get_finnhub_api_key as _config_finnhub_key

SOURCE_NOT_CONFIGURED = "not configured"


def live_feeds_disabled() -> bool:
    return os.getenv("HPTL_SKIP_LIVE_FEEDS", "").strip().lower() in ("1", "true", "yes")


def trading_economics_api_key() -> str:
    return (os.getenv("TRADINGECONOMICS_API_KEY") or os.getenv("TRADING_ECONOMICS_API_KEY") or "").strip()


def finnhub_api_key() -> str:
    return _config_finnhub_key()


def resolve_economic_calendar_provider() -> str:
    """Return ``finnhub``, ``trading_economics``, ``both``, or ``none``. TE preferred when both keys set."""
    if live_feeds_disabled():
        return "none"
    explicit = os.getenv("ECONOMIC_CALENDAR_PROVIDER", "").strip().lower()
    if explicit in ("finnhub", "trading_economics", "both"):
        return explicit
    has_te = bool(trading_economics_api_key())
    has_fh = bool(finnhub_api_key())
    if has_te and has_fh:
        return "both"
    if has_te:
        return "trading_economics"
    if has_fh:
        return "finnhub"
    return "none"


def provider_api_keys_status() -> dict[str, Any]:
    prov = resolve_economic_calendar_provider()
    fh = bool(finnhub_api_key())
    te = bool(trading_economics_api_key())
    return {
        "provider": prov,
        "finnhub_api_key_detected": fh,
        "trading_economics_api_key_detected": te,
        "finnhub_key": "yes" if fh else "no",
        "trading_economics_key": "yes" if te else "no",
        "skip_live_feeds": live_feeds_disabled(),
    }
