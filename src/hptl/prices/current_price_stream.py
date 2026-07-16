"""Persistent OANDA pricing-stream cache.

Runs one or more background threads that hold persistent OANDA v20 pricing-stream
connections and keep an in-memory cache of the latest quote per instrument. The
cache implements the :data:`hptl.prices.current_price_service.QuoteSource`
contract (``Callable[[list[str]], dict[str, PriceSnapshot]]``) so it plugs
straight into :func:`set_quote_source` without changing any caller.

Features:
* many instruments split across several connections (OANDA per-connection cap)
* automatic reconnect with exponential backoff
* heartbeat monitoring via the stream read-timeout (OANDA sends heartbeats ~5s)
* last valid quote preserved across reconnects
* rich health snapshot for the service ``/health`` endpoint

Pricing only — no order/position/trade/transaction access.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any

from hptl.oanda.oanda_client import OandaApiError, resolve_account_id
from hptl.oanda.oanda_stream import (
    STREAM_MAX_INSTRUMENTS_PER_CONNECTION,
    chunk_instruments,
    iter_pricing_stream,
)
from hptl.prices.models import PriceSnapshot

log = logging.getLogger("hptl.current_price_stream")

_BACKOFF_BASE_SECONDS = 1.0
_BACKOFF_MAX_SECONDS = 30.0


def _float(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


class _Connection(threading.Thread):
    """One persistent stream connection for a chunk of instruments."""

    def __init__(self, cache: "OandaStreamCache", index: int, instruments: list[str]) -> None:
        super().__init__(name=f"oanda-stream-{index}", daemon=True)
        self._cache = cache
        self.index = index
        self.instruments = instruments
        self.connected = False
        self.reconnects = 0
        self.messages = 0
        self.last_message_at: float | None = None
        self.last_error: str | None = None

    def run(self) -> None:
        backoff = _BACKOFF_BASE_SECONDS
        while not self._cache.stop_event.is_set():
            try:
                stream = iter_pricing_stream(
                    self._cache.account_id,
                    self.instruments,
                    connect_timeout=self._cache.connect_timeout,
                    read_timeout=self._cache.read_timeout,
                )
                self.connected = True
                self.last_error = None
                log.info("stream[%d] connected (%d instruments)", self.index, len(self.instruments))
                for msg in stream:
                    if self._cache.stop_event.is_set():
                        break
                    self._handle(msg)
                    backoff = _BACKOFF_BASE_SECONDS
            except OandaApiError as exc:
                self.last_error = str(exc)
                log.warning("stream[%d] error: %s", self.index, exc)
            except Exception as exc:  # noqa: BLE001 - keep the worker alive
                self.last_error = f"{type(exc).__name__}: {exc}"
                log.warning("stream[%d] unexpected error: %s", self.index, exc)
            finally:
                self.connected = False

            if self._cache.stop_event.is_set():
                break
            self.reconnects += 1
            log.info("stream[%d] reconnecting in %.1fs (attempt %d)", self.index, backoff, self.reconnects)
            self._cache.stop_event.wait(backoff)
            backoff = min(backoff * 2.0, _BACKOFF_MAX_SECONDS)
        log.info("stream[%d] stopped", self.index)

    def _handle(self, msg: dict[str, Any]) -> None:
        self.last_message_at = time.monotonic()
        mtype = msg.get("type")
        if mtype == "HEARTBEAT":
            return
        if mtype != "PRICE":
            return
        symbol = str(msg.get("instrument") or "").strip()
        if not symbol:
            return
        bids = msg.get("bids") or []
        asks = msg.get("asks") or []
        bid = _float(bids[0].get("price")) if bids else None
        ask = _float(asks[0].get("price")) if asks else None
        mid = None
        if bid is not None and ask is not None:
            mid = (bid + ask) / 2.0
        elif bid is not None:
            mid = bid
        elif ask is not None:
            mid = ask
        if mid is None:
            return
        as_of = str(msg.get("time") or datetime.now(timezone.utc).isoformat())
        tradeable = bool(msg.get("tradeable", True))
        self.messages += 1
        self._cache.update_quote(
            symbol,
            {"mid": mid, "bid": bid, "ask": ask, "as_of": as_of},
            tradeable=tradeable,
            status=str(msg.get("status") or ("tradeable" if tradeable else "non-tradeable")),
        )


class OandaStreamCache:
    """In-memory live-quote cache backed by persistent OANDA pricing streams."""

    def __init__(
        self,
        symbols: list[str],
        *,
        account_id: str | None = None,
        chunk_size: int = STREAM_MAX_INSTRUMENTS_PER_CONNECTION,
        connect_timeout: float = 10.0,
        read_timeout: float = 15.0,
    ) -> None:
        self._symbols = [s for s in dict.fromkeys(symbols) if s]
        self._explicit_account_id = account_id
        self.account_id = account_id or ""
        self.chunk_size = chunk_size
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout

        self.stop_event = threading.Event()
        self._lock = threading.Lock()
        self._quotes: dict[str, PriceSnapshot] = {}
        self._meta: dict[str, dict[str, Any]] = {}
        self._connections: list[_Connection] = []
        self._started_at: float | None = None

    # -- lifecycle ----------------------------------------------------------- #

    def start(self) -> None:
        if self._connections:
            return
        if not self.account_id:
            self.account_id = self._explicit_account_id or resolve_account_id()
        self.stop_event.clear()
        self._started_at = time.monotonic()
        chunks = chunk_instruments(self._symbols, self.chunk_size)
        for i, chunk in enumerate(chunks):
            conn = _Connection(self, i, chunk)
            self._connections.append(conn)
            conn.start()
        log.info(
            "OandaStreamCache started: %d instruments across %d connection(s), account %s",
            len(self._symbols),
            len(chunks),
            self.account_id,
        )

    def stop(self, *, timeout: float = 5.0) -> None:
        self.stop_event.set()
        for conn in self._connections:
            conn.join(timeout=timeout)
        self._connections = []
        log.info("OandaStreamCache stopped")

    # -- quote access -------------------------------------------------------- #

    def update_quote(self, symbol: str, snap: PriceSnapshot, *, tradeable: bool, status: str) -> None:
        with self._lock:
            self._quotes[symbol] = snap
            self._meta[symbol] = {"tradeable": tradeable, "status": status}

    def get_snapshots(self, symbols: list[str]) -> dict[str, PriceSnapshot]:
        """QuoteSource contract: latest cached snapshot for each requested symbol."""
        with self._lock:
            return {s: dict(self._quotes[s]) for s in symbols if s in self._quotes}

    def as_quote_source(self):
        return self.get_snapshots

    # -- health -------------------------------------------------------------- #

    def quote_count(self) -> int:
        with self._lock:
            return len(self._quotes)

    def connected_count(self) -> int:
        return sum(1 for c in self._connections if c.connected)

    def any_connected(self) -> bool:
        return self.connected_count() > 0

    def last_message_age(self) -> float | None:
        ages = [
            time.monotonic() - c.last_message_at
            for c in self._connections
            if c.last_message_at is not None
        ]
        return min(ages) if ages else None

    def health(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "subscribed_instruments": len(self._symbols),
            "connections": len(self._connections),
            "connected": self.connected_count(),
            "any_connected": self.any_connected(),
            "cached_quotes": self.quote_count(),
            "total_reconnects": sum(c.reconnects for c in self._connections),
            "total_messages": sum(c.messages for c in self._connections),
            "last_message_age_seconds": self.last_message_age(),
            "uptime_seconds": (time.monotonic() - self._started_at) if self._started_at else None,
            "connection_detail": [
                {
                    "index": c.index,
                    "instruments": len(c.instruments),
                    "connected": c.connected,
                    "reconnects": c.reconnects,
                    "messages": c.messages,
                    "last_error": c.last_error,
                }
                for c in self._connections
            ],
        }
