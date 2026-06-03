"""Alpha Vantage adapter — category verification + coverage metadata."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from hptl.alpha_vantage.client import AlphaVantageApiError, probe_function
from hptl.alpha_vantage.mappings import CATEGORY_PROBES

# Seconds between probes to reduce rate-limit hits on free tier.
_PROBE_DELAY_SEC = 12.0


def validate_alpha_vantage_connection() -> None:
    """Minimal live ping (FX). Never logs the API key."""
    probe_function("CURRENCY_EXCHANGE_RATE", from_currency="EUR", to_currency="USD")


def fetch_alpha_vantage_coverage_metadata(*, delay_sec: float = _PROBE_DELAY_SEC) -> dict[str, Any]:
    """Verify commodities, FX, indices, crypto, rates endpoints; return probe evidence."""
    now_base = datetime.now(timezone.utc).isoformat()
    category_probes: list[dict[str, Any]] = []
    verified_functions: set[str] = set()
    category_timestamps: dict[str, str] = {}
    per_function_timestamps: dict[str, str] = {}

    for i, (cat, function, params) in enumerate(CATEGORY_PROBES):
        if i > 0 and delay_sec > 0:
            time.sleep(delay_sec)
        ts = datetime.now(timezone.utc).isoformat()
        row: dict[str, Any] = {
            "category": cat,
            "function": function,
            "params": params,
            "endpoint": f"https://www.alphavantage.co/query?function={function}",
            "last_successful_response": None,
            "coverage_status": "unsupported",
            "error": None,
        }
        try:
            meta = probe_function(function, **params)
            row["last_successful_response"] = ts
            row["coverage_status"] = "supported"
            row["response_keys"] = meta.get("response_keys")
            verified_functions.add(function)
            category_timestamps[cat] = ts
            per_function_timestamps[function] = ts
        except AlphaVantageApiError as exc:
            row["error"] = str(exc)[:200]
            if exc.note:
                row["error_note"] = exc.note[:200]
        category_probes.append(row)

    supported_categories = [p["category"] for p in category_probes if p["coverage_status"] == "supported"]

    return {
        "source": "alpha_vantage",
        "api_root": "https://www.alphavantage.co/query",
        "last_successful_response": now_base,
        "endpoint_summary": "category_probes",
        "supported_categories": supported_categories,
        "verified_functions": sorted(verified_functions),
        "category_probes": category_probes,
        "category_timestamps": category_timestamps,
        "per_function_timestamps": per_function_timestamps,
    }
