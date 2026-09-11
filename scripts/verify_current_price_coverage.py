"""System-wide Current Price Service coverage validation.

The static discovery JSON records OANDA account coverage, but Current Price
Service can resolve additional instruments through alternate providers. This
auditor therefore treats the backend's resolved provider/provider_symbol as the
authoritative runtime mapping and uses the JSON only as the instrument roster.

Run:
  python scripts/verify_current_price_coverage.py --base-url http://127.0.0.1:8787
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from hptl.config import PROJECT_ROOT  # noqa: E402

MAPPING_PATH = PROJECT_ROOT / "data" / "config" / "current_price_instruments.json"
AUDIT_PATH = PROJECT_ROOT / "data" / "audits" / "current_price_coverage_audit.json"


def _get_json(url: str, timeout: float = 30.0) -> dict[str, Any]:
    req = Request(url, headers={"Accept": "application/json", "Cache-Control": "no-store"})
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _ws_probe(ws_url: str, timeout: float = 6.0) -> dict[str, Any]:
    try:
        import websocket  # type: ignore
    except ImportError:
        return {"ok": False, "error": "websocket-client not installed", "frames": 0}
    frames = 0
    keys: set[str] = set()
    error = None
    try:
        ws = websocket.create_connection(ws_url, timeout=timeout)
        deadline = time.time() + timeout
        while time.time() < deadline and frames < 3:
            payload = json.loads(ws.recv())
            frames += 1
            keys.update((payload.get("prices") or {}).keys())
        ws.close()
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
    return {"ok": frames > 0 and error is None, "error": error, "frames": frames, "price_keys_seen": len(keys)}


def _load_roster() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    doc = json.loads(MAPPING_PATH.read_text(encoding="utf-8"))
    instruments = doc.get("instruments") or {}
    if isinstance(instruments, dict):
        rows: list[dict[str, Any]] = []
        for key, raw in instruments.items():
            row = dict(raw) if isinstance(raw, dict) else {}
            row.setdefault("internal_key", key)
            rows.append(row)
        return doc, rows
    return doc, list(instruments)


def _classify(quote: dict[str, Any] | None, candle: dict[str, Any] | None) -> str:
    if quote is None:
        return "FRONTEND RESOLUTION ERROR"
    if not quote.get("provider") or not quote.get("provider_symbol"):
        return "MAPPING ERROR"
    status = str(quote.get("status") or "UNAVAILABLE").upper()
    current = quote.get("current_price")
    if current is None and quote.get("mid") is None:
        return "UNAVAILABLE" if status != "FALLBACK" else "FALLBACK"
    if status == "LIVE":
        return "PASS"
    if status in {"IDLE", "STALE", "FALLBACK", "UNAVAILABLE"}:
        return status
    return status


def _reason(row: dict[str, Any]) -> str:
    c = row["classification"]
    if c == "MAPPING ERROR":
        return f"no runtime provider mapping for {row['internal_key']!r}"
    if c == "FRONTEND RESOLUTION ERROR":
        return f"backend omitted {row['internal_key']!r}"
    if c == "UNAVAILABLE":
        return f"{row['provider']}:{row['provider_symbol']} mapped but returned no usable price"
    if c == "FALLBACK":
        return f"using {row.get('fallback_source') or 'trusted fallback close'}"
    if c == "IDLE":
        return f"valid provider quote idle age={row.get('age_seconds')}s"
    if c == "STALE":
        return f"provider quote stale age={row.get('age_seconds')}s"
    return c


def main() -> int:
    parser = argparse.ArgumentParser(description="All-instrument current price coverage audit")
    parser.add_argument("--base-url", default="http://127.0.0.1:8787")
    parser.add_argument("--ws-url", default="ws://127.0.0.1:8787/ws/prices")
    args = parser.parse_args()
    base = args.base_url.rstrip("/")

    mapping_doc, roster = _load_roster()
    try:
        health = _get_json(f"{base}/health")
        prices_doc = _get_json(f"{base}/api/prices")
        weekly_doc = _get_json(f"{base}/api/weekly-candles")
    except Exception as exc:  # noqa: BLE001
        print(f"BACKEND OFFLINE: {exc}")
        return 1

    prices = prices_doc.get("prices") or {}
    weekly = weekly_doc.get("weekly_candles") or {}
    rows: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    failed: list[dict[str, Any]] = []

    for item in roster:
        key = item.get("internal_key") or item.get("key") or item.get("id")
        if not key:
            continue
        quote = prices.get(key)
        candle = weekly.get(key)
        classification = _classify(quote, candle)
        row = {
            "internal_key": key,
            "display_name": item.get("display_name") or key,
            "provider": (quote or {}).get("provider"),
            "provider_symbol": (quote or {}).get("provider_symbol"),
            "config_provider": item.get("provider"),
            "config_provider_symbol": item.get("provider_symbol") or item.get("oanda_symbol"),
            "runtime_mapped": bool((quote or {}).get("provider") and (quote or {}).get("provider_symbol")),
            "bid": (quote or {}).get("bid"),
            "ask": (quote or {}).get("ask"),
            "mid": (quote or {}).get("mid"),
            "current_price": (quote or {}).get("current_price"),
            "timestamp": (quote or {}).get("timestamp"),
            "age_seconds": (quote or {}).get("age_seconds"),
            "backend_status": (quote or {}).get("status"),
            "fallback_source": (quote or {}).get("fallback_source"),
            "active_weekly_candle_present": candle is not None,
            "classification": classification,
        }
        rows.append(row)
        counts[classification] += 1
        if classification not in {"PASS", "IDLE"}:
            failed.append({"internal_key": key, "classification": classification, "reason": _reason(row)})

    audit = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "base_url": base,
        "health": health,
        "mapping_path": str(MAPPING_PATH.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "mapping_source": "runtime_current_price_service",
        "config_generated_from": mapping_doc.get("generated_from"),
        "totals": {
            "config_entries": len(rows),
            "runtime_mapped": sum(1 for r in rows if r["runtime_mapped"]),
            "PASS": counts.get("PASS", 0),
            "IDLE": counts.get("IDLE", 0),
            "STALE": counts.get("STALE", 0),
            "FALLBACK": counts.get("FALLBACK", 0),
            "UNAVAILABLE": counts.get("UNAVAILABLE", 0),
            "MAPPING ERROR": counts.get("MAPPING ERROR", 0),
            "FRONTEND RESOLUTION ERROR": counts.get("FRONTEND RESOLUTION ERROR", 0),
        },
        "websocket": _ws_probe(args.ws_url),
        "failed": failed,
        "instruments": rows,
    }
    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_PATH.write_text(json.dumps(audit, indent=2), encoding="utf-8")

    print("=" * 78)
    print("CURRENT PRICE COVERAGE")
    print("=" * 78)
    print(f"Runtime mapped: {audit['totals']['runtime_mapped']}/{audit['totals']['config_entries']}")
    print("  " + "  ".join(f"{k}={counts.get(k, 0)}" for k in ("PASS", "IDLE", "STALE", "FALLBACK", "UNAVAILABLE", "MAPPING ERROR", "FRONTEND RESOLUTION ERROR")))
    if failed:
        print(f"\nNon-pass ({len(failed)}):")
        for failure in failed:
            print(f"  [{failure['classification']}] {failure['internal_key']}: {failure['reason']}")
    else:
        print("\nAll instruments are PASS or IDLE; no coverage failures.")
    print(f"\nWrote {AUDIT_PATH}")

    hard = counts.get("MAPPING ERROR", 0) or counts.get("FRONTEND RESOLUTION ERROR", 0)
    return 1 if hard else 0


if __name__ == "__main__":
    raise SystemExit(main())
