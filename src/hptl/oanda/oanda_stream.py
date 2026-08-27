"""OANDA v20 pricing stream (read-only, pricing-only).

Low-level helper that opens a persistent HTTP streaming connection to the OANDA
v20 pricing-stream endpoint and yields decoded messages (PRICE / HEARTBEAT) as
plain dicts. This module is intentionally *pricing only* — it never touches
order, position, trade or transaction endpoints.

    GET {stream_host}/v3/accounts/{account}/pricing/stream?instruments=A,B,C

The response is newline-delimited JSON. Each line is either::

    {"type": "PRICE", "instrument": "XAU_USD", "time": "...",
     "bids": [{"price": "..."}], "asks": [{"price": "..."}],
     "tradeable": true, "status": "tradeable"}

    {"type": "HEARTBEAT", "time": "..."}

The higher-level cache in :mod:`hptl.prices.current_price_stream` is responsible
for reconnection, heartbeat monitoring and the in-memory quote cache.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import requests

from hptl.config import get_oanda_api_key, get_oanda_stream_host
from hptl.oanda.oanda_client import OandaApiError

# OANDA imposes a per-connection instrument cap on the pricing stream. Keep well
# under it so a single account with many instruments is split across a few
# connections rather than rejected.
STREAM_MAX_INSTRUMENTS_PER_CONNECTION = 20


def _stream_headers() -> dict[str, str]:
    key = get_oanda_api_key()
    if not key:
        raise OandaApiError(
            "OANDA_API_KEY not set — add your personal access token to .env (see .env.example)."
        )
    return {
        "Authorization": f"Bearer {key}",
        "Accept-Datetime-Format": "RFC3339",
    }


def iter_pricing_stream(
    account_id: str,
    instruments: list[str],
    *,
    connect_timeout: float = 10.0,
    read_timeout: float = 15.0,
    session: requests.Session | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield decoded pricing-stream messages until the connection drops.

    Raises :class:`OandaApiError` on a non-200 response or a network error so the
    caller can apply reconnect/backoff. A silent connection triggers
    ``requests`` read-timeout (heartbeats arrive ~every 5s), which surfaces as an
    ``OandaApiError`` and lets the caller reconnect.
    """
    if not instruments:
        return
    host = get_oanda_stream_host().rstrip("/")
    url = f"{host}/v3/accounts/{account_id}/pricing/stream"
    params = {"instruments": ",".join(instruments)}
    owns_session = session is None
    sess = session or requests.Session()
    try:
        resp = sess.get(
            url,
            headers=_stream_headers(),
            params=params,
            stream=True,
            timeout=(connect_timeout, read_timeout),
        )
    except requests.RequestException as exc:
        if owns_session:
            sess.close()
        raise OandaApiError(f"OANDA stream connect failed: {type(exc).__name__}: {exc}") from exc

    if resp.status_code >= 400:
        body = (resp.text or "")[:1000]
        resp.close()
        if owns_session:
            sess.close()
        raise OandaApiError(
            f"OANDA stream HTTP {resp.status_code}",
            status_code=resp.status_code,
            body=body,
        )

    try:
        for raw in resp.iter_lines(decode_unicode=True):
            if not raw:
                continue
            try:
                yield json.loads(raw)
            except (ValueError, TypeError):
                continue
    except requests.RequestException as exc:
        raise OandaApiError(f"OANDA stream read failed: {type(exc).__name__}: {exc}") from exc
    finally:
        resp.close()
        if owns_session:
            sess.close()


def chunk_instruments(
    instruments: list[str],
    size: int = STREAM_MAX_INSTRUMENTS_PER_CONNECTION,
) -> list[list[str]]:
    """Split instruments into per-connection chunks (stable order, deduped)."""
    seen: set[str] = set()
    ordered: list[str] = []
    for name in instruments:
        if name and name not in seen:
            seen.add(name)
            ordered.append(name)
    size = max(1, size)
    return [ordered[i : i + size] for i in range(0, len(ordered), size)]
