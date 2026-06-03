"""TradingView webhook auth and payload handling (logging only)."""
from __future__ import annotations

import json
from typing import Any

from hptl.config import get_tradingview_webhook_secret
from hptl.journal.store import create_entry, upsert_entry


def _header_value(headers: dict[str, str], name: str) -> str:
    lower = {str(k).lower(): str(v) for k, v in headers.items()}
    return lower.get(name.lower(), "")


def verify_webhook_secret(headers: dict[str, str], query: dict[str, str] | None = None) -> bool:
    expected = get_tradingview_webhook_secret()
    if not expected:
        return False
    q = query or {}
    auth = _header_value(headers, "Authorization")
    if auth.lower().startswith("bearer "):
        auth = auth[7:].strip()
    candidates = [
        _header_value(headers, "X-TradingView-Webhook-Secret"),
        _header_value(headers, "X-Webhook-Secret"),
        auth,
        q.get("secret", ""),
    ]
    return any(c and c == expected for c in candidates)


def handle_webhook_body(body: bytes) -> dict[str, Any]:
    if not body:
        raise ValueError("Empty request body")
    try:
        data = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("Webhook payload must be a JSON object")
    if data.get("trade_id"):
        return upsert_entry(data, source="tradingview_webhook")
    return create_entry(data, source="tradingview_webhook")
