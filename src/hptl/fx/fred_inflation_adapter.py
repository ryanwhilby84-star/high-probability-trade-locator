"""CPI YoY inflation fetch for G10 currencies (FX Institutional Macro V2).

Uses FRED OECD harmonized consumer-price inflation series (annual %). Results are
cached under ``data/cache/fx_rates/`` and honour ``HPTL_SKIP_LIVE_FEEDS=1``.
When FRED is unavailable, callers should carry forward prior config values.
"""

from __future__ import annotations

import json
from typing import Any

from hptl.config import get_fred_api_key
from hptl.fx.rate_adapter_base import FieldValue, fetch_text, offline_mode, to_float

# FRED inflation, consumer prices for the country (annual %).
CPI_FRED_SERIES: dict[str, str] = {
    "USD": "FPCPITOTLZGUSA",
    "EUR": "FPCPITOTLZGEMU",
    "GBP": "FPCPITOTLZGGBR",
    "JPY": "FPCPITOTLZGJPN",
    "CHF": "FPCPITOTLZGCHE",
    "AUD": "FPCPITOTLZGAUS",
    "NZD": "FPCPITOTLZGNZL",
    "CAD": "FPCPITOTLZGCAN",
}

FRED_OBS_URL = "https://api.stlouisfed.org/fred/series/observations"


def _latest_observation(series_id: str) -> FieldValue:
    cache_key = f"cpi_{series_id.lower()}"
    api_key = get_fred_api_key()

    if not api_key and not offline_mode():
        return FieldValue(error="FRED_API_KEY not set")

    try:
        if api_key and not offline_mode():
            url = (
                f"{FRED_OBS_URL}?series_id={series_id}&api_key={api_key}"
                f"&file_type=json&sort_order=desc&limit=5"
            )
            raw = fetch_text(url, cache_key=cache_key)
        else:
            from hptl.fx.rate_adapter_base import _read_cache  # noqa: PLC0415

            cached = _read_cache(cache_key, binary=False)
            if cached is None:
                return FieldValue(error=f"offline/no cache for {series_id}")
            raw = cached

        data = json.loads(raw)
        obs = data.get("observations") or []
        for row in obs:
            if not isinstance(row, dict):
                continue
            val = to_float(row.get("value"))
            if val is None:
                continue
            as_of = str(row.get("date") or "")[:10] or None
            return FieldValue(
                value=round(val, 3),
                as_of=as_of,
                source=f"FRED ({series_id})",
            )
        return FieldValue(error=f"no numeric observations for {series_id}")
    except Exception as exc:  # noqa: BLE001
        return FieldValue(error=f"{type(exc).__name__}: {exc}")


def fetch_all_cpi() -> dict[str, FieldValue]:
    """Return CPI YoY (annual %) for every supported G10 currency."""
    return {code: _latest_observation(sid) for code, sid in CPI_FRED_SERIES.items()}


def cpi_as_dict() -> dict[str, Any]:
    """Convenience dump for audits."""
    return {
        code: {
            "cpi_yoy": fv.value,
            "cpi_yoy_as_of": fv.as_of,
            "source": fv.source,
            "error": fv.error,
        }
        for code, fv in fetch_all_cpi().items()
    }
