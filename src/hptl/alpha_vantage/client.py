"""Alpha Vantage HTTP client — apikey never logged or written to audit output."""

from __future__ import annotations

import os
import re
from typing import Any
from urllib.parse import urlencode

import requests

from hptl.config import get_alpha_vantage_api_key, get_settings

ALPHA_VANTAGE_ROOT = "https://www.alphavantage.co/query"
_KEY_RE = re.compile(r"(apikey=)[^&\s]+", re.I)


class AlphaVantageApiError(RuntimeError):
    def __init__(self, message: str, *, function: str = "", note: str = "") -> None:
        super().__init__(message)
        self.function = function
        self.note = note


def _redact(text: str) -> str:
    return _KEY_RE.sub(r"\1***", text or "")


def _request_timeout() -> tuple[float, float]:
    """Bound Alpha Vantage connect/read time so one call cannot stall a refresh."""
    base = max(1.0, float(get_settings().request_timeout_seconds))
    read = max(1.0, float(os.getenv("ALPHA_VANTAGE_REQUEST_TIMEOUT_SECONDS", str(min(base, 12.0)))))
    connect = max(1.0, float(os.getenv("ALPHA_VANTAGE_CONNECT_TIMEOUT_SECONDS", str(min(read, 5.0)))))
    return connect, read


def _get(function: str, **params: str) -> dict[str, Any]:
    key = get_alpha_vantage_api_key()
    if not key:
        raise AlphaVantageApiError("ALPHA_VANTAGE_API_KEY not set", function=function)

    query = {"function": function, "apikey": key, **params}
    url = f"{ALPHA_VANTAGE_ROOT}?{urlencode(query)}"
    timeout = _request_timeout()
    try:
        response = requests.get(url, timeout=timeout)
    except requests.Timeout as exc:
        raise AlphaVantageApiError(
            f"Alpha Vantage request timed out ({function}; connect={timeout[0]}s read={timeout[1]}s)",
            function=function,
        ) from exc
    except requests.RequestException as exc:
        raise AlphaVantageApiError(
            f"Alpha Vantage request failed ({function}): {type(exc).__name__}",
            function=function,
        ) from exc

    if response.status_code >= 400:
        raise AlphaVantageApiError(
            f"Alpha Vantage HTTP {response.status_code} for {function}",
            function=function,
            note=_redact((response.text or "")[:500]),
        )
    try:
        doc = response.json()
    except ValueError as exc:
        raise AlphaVantageApiError(f"Alpha Vantage non-JSON for {function}", function=function) from exc

    if not isinstance(doc, dict):
        raise AlphaVantageApiError(f"Unexpected payload for {function}", function=function)
    if "Error Message" in doc:
        raise AlphaVantageApiError(
            f"Alpha Vantage error for {function}",
            function=function,
            note=str(doc.get("Error Message", ""))[:300],
        )
    if "Note" in doc and "Information" not in doc:
        raise AlphaVantageApiError(
            f"Alpha Vantage rate limit or quota for {function}",
            function=function,
            note=str(doc.get("Note", ""))[:300],
        )

    has_series = any("Time Series" in key_name for key_name in doc) or isinstance(doc.get("data"), list)
    if "Information" in doc and not has_series:
        info = str(doc.get("Information", ""))
        if "call frequency" in info.lower() or "thank you for using" in info.lower():
            raise AlphaVantageApiError(
                f"Alpha Vantage rate limit for {function}",
                function=function,
                note=info[:300],
            )
        raise AlphaVantageApiError(
            f"Alpha Vantage informational response for {function}",
            function=function,
            note=info[:300],
        )
    return doc


def probe_function(function: str, **params: str) -> dict[str, Any]:
    """Call one AV function; return doc plus safe metadata (no apikey)."""
    doc = _get(function, **params)
    return {
        "function": function,
        "params": params,
        "response_keys": sorted(doc.keys())[:20],
        "has_time_series": any("Time Series" in key_name or "data" in key_name.lower() for key_name in doc),
        "has_realtime": "Realtime" in str(doc.keys()),
    }
