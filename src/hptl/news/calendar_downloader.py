"""Fetch raw economic calendar payloads (Finnhub, Trading Economics).

FRED releases can be folded in later via `calendar_parser` when release series
IDs are mapped; this module focuses on HTTP calendar APIs.

Requires env vars (optional): FINNHUB_API_KEY, TRADINGECONOMICS_API_KEY
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import requests

from hptl.config import get_settings
from hptl.news.economic_calendar_provider import finnhub_api_key


@dataclass(frozen=True)
class RawCalendarBundle:
    """Unparsed payloads keyed by upstream name."""

    finnhub: list[dict[str, Any]] | None
    trading_economics: list[dict[str, Any]] | None
    fetched_at: datetime
    errors: tuple[str, ...]


def _iso_day(d: date) -> str:
    return d.isoformat()


def fetch_finnhub_economic_calendar(start: date, end: date) -> list[dict[str, Any]]:
    """GET Finnhub economic calendar (raw list under ``economicCalendar``)."""
    token = finnhub_api_key()
    if not token:
        return []
    settings = get_settings()
    url = "https://finnhub.io/api/v1/calendar/economic"
    params = {"from": _iso_day(start), "to": _iso_day(end), "token": token}
    r = requests.get(url, params=params, timeout=settings.request_timeout_seconds)
    r.raise_for_status()
    data = r.json()
    return list(data.get("economicCalendar") or [])


def fetch_trading_economics_calendar(start: date, end: date) -> list[dict[str, Any]]:
    """GET Trading Economics indicator calendar (JSON list).

    Documented pattern: ``https://api.tradingeconomics.com/calendar?d1=...&d2=...&c=...&format=json``
    """
    from hptl.news.economic_calendar_provider import trading_economics_api_key

    key = trading_economics_api_key()
    if not key:
        return []
    settings = get_settings()
    url = "https://api.tradingeconomics.com/calendar"
    params = {
        "d1": _iso_day(start),
        "d2": _iso_day(end),
        "format": "json",
        "c": key,
    }
    r = requests.get(url, params=params, timeout=settings.request_timeout_seconds)
    r.raise_for_status()
    body = r.json()
    if isinstance(body, list):
        return body
    if isinstance(body, dict) and "data" in body:
        return list(body["data"])
    return []


def download_calendar_window(start: date, end: date) -> RawCalendarBundle:
    """Download all configured sources; failures are captured in ``errors``."""
    errors: list[str] = []
    finnhub_rows: list[dict[str, Any]] | None = None
    te_rows: list[dict[str, Any]] | None = None
    try:
        finnhub_rows = fetch_finnhub_economic_calendar(start, end)
    except Exception as exc:
        errors.append(f"finnhub:{exc}")
        finnhub_rows = []
    try:
        te_rows = fetch_trading_economics_calendar(start, end)
    except Exception as exc:
        errors.append(f"trading_economics:{exc}")
        te_rows = []
    return RawCalendarBundle(
        finnhub=finnhub_rows,
        trading_economics=te_rows,
        fetched_at=datetime.now(timezone.utc),
        errors=tuple(errors),
    )
