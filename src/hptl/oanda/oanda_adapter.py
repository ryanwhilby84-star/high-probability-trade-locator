"""OANDA adapter — instrument catalog and availability metadata."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from hptl.config import get_oanda_api_host
from hptl.oanda.oanda_client import (
    OandaApiError,
    fetch_account_instruments,
    instruments_by_name,
    instrument_names_set,
    list_accounts,
    resolve_account_id,
)


def validate_oanda_connection() -> str:
    """Ping OANDA (accounts list). Returns account id on success; never logs the API key."""
    accounts = list_accounts()
    if not accounts:
        raise OandaApiError("OANDA returned no accounts for this token.")
    return resolve_account_id()


def fetch_oanda_coverage_metadata(
    *,
    account_id: str | None = None,
) -> dict[str, Any]:
    """Pull all account instruments and normalized availability metadata."""
    aid = account_id or validate_oanda_connection()
    rows = fetch_account_instruments(aid)
    by_name = instruments_by_name(rows)
    now = datetime.now(timezone.utc).isoformat()

    instruments_out: list[dict[str, Any]] = []
    for name in sorted(by_name.keys()):
        meta = by_name[name]
        instruments_out.append(
            {
                "name": name,
                "display_name": meta.get("displayName"),
                "type": meta.get("type"),
                "pip_location": meta.get("pipLocation"),
                "display_precision": meta.get("displayPrecision"),
                "minimum_trade_size": meta.get("minimumTradeSize"),
                "margin_rate": meta.get("marginRate"),
                "available": True,
            }
        )

    return {
        "source": "oanda",
        "api_host": get_oanda_api_host(),
        "account_id": aid,
        "last_successful_response": now,
        "endpoint": "/v3/accounts/{accountId}/instruments",
        "instrument_count": len(instruments_out),
        "instrument_names": sorted(by_name.keys()),
        "instruments": instruments_out,
        "names_set": instrument_names_set(rows),
        "by_name": by_name,
    }
