"""Phase 3 verification: live prices + weekly candles + WS + historical integrity."""

from __future__ import annotations

import asyncio
import hashlib
import json
import pathlib
import urllib.parse
import urllib.request

BASE = "http://127.0.0.1:8787"
KEYS = ["Gold", "Natural Gas / NG", "Crude Oil / CL", "West Texas Oil"]


def get(path: str):
    with urllib.request.urlopen(BASE + path, timeout=15) as resp:
        return json.loads(resp.read().decode())


def completed_hash(ohlc: dict, key: str):
    bars = (ohlc.get("instruments") or {}).get(key, {}).get("weekly_ohlc") or []
    payload = json.dumps(bars, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()[:16], len(bars), bars[-1] if bars else None


def exact(mid, precision):
    digits = int(precision) if precision is not None else 3
    return f"{mid:,.{digits}f}"


async def ws_test():
    try:
        import websockets
    except ImportError:
        import subprocess
        import sys

        subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets", "-q"])
        import websockets

    async with websockets.connect("ws://127.0.0.1:8787/ws/prices") as ws:
        msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
        print(
            "WS",
            msg["type"],
            "nprices",
            len(msg["prices"]),
            "ncandles",
            len(msg["weekly_candles"]),
            "conn",
            msg["stream"]["connected"],
        )
        for k in ["Gold", "Natural Gas / NG", "Crude Oil / CL"]:
            px = msg["prices"].get(k, {})
            print(
                "  WS",
                k,
                "mid",
                px.get("mid"),
                "status",
                px.get("status"),
                "candle",
                msg["weekly_candles"].get(k),
            )


def main() -> int:
    h = get("/health")
    print("HEALTH", h["status"], "connected", h["connected"], "cached", h["cached_quotes"])

    ohlc = json.loads(
        pathlib.Path("data/processed/workstation_ohlc_latest.json").read_text(encoding="utf-8")
    )
    hashes_before = {}
    for k in KEYS:
        hashes_before[k] = completed_hash(ohlc, k)
        print(
            "HIST",
            k,
            "n=",
            hashes_before[k][1],
            "last",
            hashes_before[k][2],
            "hash",
            hashes_before[k][0],
        )

    for key in KEYS:
        p = get("/api/prices/" + urllib.parse.quote(key, safe=""))
        mid = p.get("mid")
        print(
            "PRICE",
            key,
            "->",
            p.get("provider_symbol"),
            "mid",
            mid,
            "status",
            p.get("status"),
            "prec",
            p.get("price_precision"),
            "age",
            round(p.get("age_seconds") or -1, 2),
        )
        if mid is not None:
            s = exact(mid, p.get("price_precision"))
            assert "K" not in s and "k" not in s, s
            print("  exact", s)

    for key in ["Gold", "Natural Gas / NG", "Crude Oil / CL"]:
        try:
            c = get("/api/weekly-candle/" + urllib.parse.quote(key, safe=""))
            print("WEEKLY", key, c)
        except Exception as exc:  # noqa: BLE001
            print("WEEKLY", key, "ERR", exc)

    asyncio.run(ws_test())

    ohlc2 = json.loads(
        pathlib.Path("data/processed/workstation_ohlc_latest.json").read_text(encoding="utf-8")
    )
    for k in KEYS:
        h2 = completed_hash(ohlc2, k)
        assert h2[0] == hashes_before[k][0], (k, hashes_before[k], h2)
    print("HISTORICAL_INTEGRITY PASS — completed weekly bars unchanged on disk")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
