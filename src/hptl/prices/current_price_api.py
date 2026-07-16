"""FastAPI Current Price Service (Phase 2).

Wires the persistent OANDA pricing-stream cache into the canonical
:mod:`hptl.prices.current_price_service` via :func:`set_quote_source`, then
exposes the current prices over HTTP and a WebSocket broadcast for the dashboard.

    OANDA pricing stream  ->  OandaStreamCache (in-memory)
                          ->  set_quote_source(...)  (canonical contract)
                          ->  current_price_service.get_current_prices(...)
                          ->  HTTP API  +  WebSocket broadcast

Pricing only — no order/position/trade/transaction endpoints.

Start (development)::

    python -m hptl.prices.current_price_api
    # or
    python scripts/run_current_price_service.py
    # or
    uvicorn hptl.prices.current_price_api:app --host 0.0.0.0 --port 8787
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from hptl.config import get_oanda_api_host, get_oanda_stream_host
from hptl.prices.current_price_service import (
    get_current_price,
    get_current_prices,
    load_discovery,
    load_instrument_mappings,
    mapping_source,
    set_quote_source,
)
from hptl.prices.current_price_stream import OandaStreamCache
from hptl.prices.live_weekly_candle import LiveWeeklyCandleTracker

log = logging.getLogger("hptl.current_price_api")

BROADCAST_INTERVAL_SECONDS = 1.0


def _streaming_symbols_and_account() -> tuple[list[str], str | None]:
    """Streamable OANDA symbols + account id from the canonical discovery mapping."""
    mappings = load_instrument_mappings()
    symbols = sorted(
        {
            m.provider_symbol
            for m in mappings.values()
            if m.is_mapped and m.supports_streaming and m.provider_symbol
        }
    )
    doc = load_discovery() or {}
    account_id = doc.get("oanda_account_id")
    return symbols, account_id


class ConnectionManager:
    """Tracks connected WebSocket clients and broadcasts JSON payloads."""

    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self._clients.add(ws)

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._lock:
            self._clients.discard(ws)

    def count(self) -> int:
        return len(self._clients)

    async def broadcast(self, payload: dict[str, Any]) -> None:
        async with self._lock:
            targets = list(self._clients)
        dead: list[WebSocket] = []
        for ws in targets:
            try:
                await ws.send_json(payload)
            except Exception:  # noqa: BLE001 - drop broken sockets
                dead.append(ws)
        if dead:
            async with self._lock:
                for ws in dead:
                    self._clients.discard(ws)


def _build_payload(app: FastAPI) -> dict[str, Any]:
    """Current prices + active weekly candles snapshot for HTTP/WebSocket."""
    tracker: LiveWeeklyCandleTracker = app.state.weekly
    cache: OandaStreamCache = app.state.cache

    prices = get_current_prices()
    price_out: dict[str, Any] = {}
    for key, cp in prices.items():
        price_out[key] = cp.to_dict()
        if cp.mid is not None and cp.status in ("LIVE", "STALE"):
            tracker.update(key, cp.mid, cp.timestamp)

    return {
        "type": "snapshot",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stream": {
            "any_connected": cache.any_connected(),
            "connected": cache.connected_count(),
            "connections": len(cache._connections),  # noqa: SLF001 - internal health
            "cached_quotes": cache.quote_count(),
            "subscribed_instruments": len(cache._symbols),  # noqa: SLF001
            "last_message_age_seconds": cache.last_message_age(),
        },
        "prices": price_out,
        "weekly_candles": tracker.snapshot(),
    }


async def _broadcaster(app: FastAPI) -> None:
    manager: ConnectionManager = app.state.manager
    while True:
        try:
            payload = await asyncio.to_thread(_build_payload, app)
            await manager.broadcast(payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - keep the loop alive
            log.warning("broadcaster error: %s", exc)
        await asyncio.sleep(BROADCAST_INTERVAL_SECONDS)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    symbols, account_id = _streaming_symbols_and_account()
    cache = OandaStreamCache(symbols, account_id=account_id)
    tracker = LiveWeeklyCandleTracker()
    tracker.load()
    manager = ConnectionManager()

    app.state.cache = cache
    app.state.weekly = tracker
    app.state.manager = manager

    log.info("=" * 68)
    log.info("HPTL Current Price Service starting")
    log.info("  OANDA REST host   : %s", get_oanda_api_host())
    log.info("  OANDA stream host : %s", get_oanda_stream_host())
    log.info("  Account           : %s", account_id or "(auto-resolve)")
    log.info("  Mapping source    : %s", mapping_source())
    log.info("  Subscribed        : %d instruments", len(symbols))
    log.info("=" * 68)

    cache.start()
    set_quote_source(cache.get_snapshots)
    broadcaster = asyncio.create_task(_broadcaster(app))

    try:
        yield
    finally:
        log.info("Current Price Service shutting down...")
        broadcaster.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await broadcaster
        set_quote_source(None)
        cache.stop()
        log.info("Shutdown complete")


def create_app() -> FastAPI:
    app = FastAPI(title="HPTL Current Price Service", version="2.0", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "http://localhost:4173",
            "http://127.0.0.1:4173",
        ],
        allow_credentials=False,
        allow_methods=["GET", "OPTIONS"],
        allow_headers=["*"],
    )

    @app.get("/health")
    def health() -> dict[str, Any]:
        cache: OandaStreamCache = app.state.cache
        h = cache.health()
        h["status"] = "ok" if cache.any_connected() else "degraded"
        h["clients"] = app.state.manager.count()
        h["generated_at"] = datetime.now(timezone.utc).isoformat()
        return h

    @app.get("/api/prices")
    def prices(keys: str | None = None) -> dict[str, Any]:
        selected = [k.strip() for k in keys.split(",") if k.strip()] if keys else None
        result = get_current_prices(selected)
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "prices": {k: cp.to_dict() for k, cp in result.items()},
        }

    @app.get("/api/prices/{key:path}")
    def price(key: str) -> JSONResponse:
        cp = get_current_price(key)
        if cp is None:
            return JSONResponse({"error": f"unknown instrument: {key}"}, status_code=404)
        return JSONResponse(cp.to_dict())

    @app.get("/api/weekly-candles")
    def weekly_candles() -> dict[str, Any]:
        tracker: LiveWeeklyCandleTracker = app.state.weekly
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "weekly_candles": tracker.snapshot(),
        }

    @app.get("/api/weekly-candle/{key:path}")
    def weekly_candle(key: str) -> JSONResponse:
        tracker: LiveWeeklyCandleTracker = app.state.weekly
        candle = tracker.get(key)
        if candle is None:
            return JSONResponse({"error": f"no active weekly candle for: {key}"}, status_code=404)
        return JSONResponse(candle)

    @app.websocket("/ws/prices")
    async def ws_prices(ws: WebSocket) -> None:
        manager: ConnectionManager = app.state.manager
        await manager.connect(ws)
        try:
            await ws.send_json(await asyncio.to_thread(_build_payload, app))
            while True:
                # We don't require client messages; this keeps the socket open
                # and detects disconnects. Broadcasts are pushed by _broadcaster.
                await ws.receive_text()
        except WebSocketDisconnect:
            pass
        except Exception:  # noqa: BLE001
            pass
        finally:
            await manager.disconnect(ws)

    return app


app = create_app()


def main() -> int:
    import uvicorn

    uvicorn.run(
        "hptl.prices.current_price_api:app",
        host="0.0.0.0",
        port=8787,
        log_level="info",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
