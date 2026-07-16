"""System-wide Current Price Service coverage validation.

Iterates every entry in data/config/current_price_instruments.json and classifies
each instrument against the live Current Price Service (HTTP + WebSocket).

Run (backend must be on :8787):
  python scripts/verify_current_price_coverage.py
  python scripts/verify_current_price_coverage.py --base-url http://127.0.0.1:8787

Writes data/audits/current_price_coverage_audit.json
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
from urllib.error import URLError
from urllib.request import Request, urlopen

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from hptl.config import PROJECT_ROOT  # noqa: E402

MAPPING_PATH = PROJECT_ROOT / "data" / "config" / "current_price_instruments.json"
AUDIT_PATH = PROJECT_ROOT / "data" / "audits" / "current_price_coverage_audit.json"

REPRESENTATIVE = [
    "Gold",
    "Natural Gas / NG",
    "WTI Crude Oil / CL",
    "Silver",
    "Copper",
    "EUR/USD",
    "USD/JPY",
    "S&P 500 / ES",
    "Bitcoin",
]


def _get_json(url: str, timeout: float = 20.0) -> dict[str, Any]:
    req = Request(url, headers={"Accept": "application/json", "Cache-Control": "no-store"})
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _ws_probe(ws_url: str, timeout: float = 8.0) -> dict[str, Any]:
    """Receive at least one frame from the prices WebSocket."""
    try:
        import websocket  # type: ignore
    except ImportError:
        # Fallback: stdlib-free probe via http snapshot only
        return {
            "ok": False,
            "error": "websocket-client not installed; skipped WS frame probe",
            "frames": 0,
            "price_keys_seen": 0,
        }

    frames = 0
    keys_seen: set[str] = set()
    first_type = None
    err = None
    try:
        ws = websocket.create_connection(ws_url, timeout=timeout)
        deadline = time.time() + timeout
        while time.time() < deadline and frames < 3:
            raw = ws.recv()
            frames += 1
            payload = json.loads(raw)
            if first_type is None:
                first_type = payload.get("type")
            prices = payload.get("prices") or {}
            keys_seen.update(prices.keys())
        ws.close()
    except Exception as exc:  # noqa: BLE001
        err = str(exc)

    return {
        "ok": frames > 0 and err is None,
        "error": err,
        "frames": frames,
        "first_frame_type": first_type,
        "price_keys_seen": len(keys_seen),
    }


def _load_mapping() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    doc = json.loads(MAPPING_PATH.read_text(encoding="utf-8"))
    instruments = doc.get("instruments") or []
    if isinstance(instruments, dict):
        rows = []
        for key, row in instruments.items():
            item = dict(row) if isinstance(row, dict) else {}
            item.setdefault("internal_key", key)
            rows.append(item)
        return doc, rows
    return doc, list(instruments)


def _classify(
    *,
    mapped: bool,
    subscribed: bool,
    quote: dict[str, Any] | None,
    candle: dict[str, Any] | None,
    backend_reachable: bool,
    frontend_key_ok: bool,
) -> str:
    if not backend_reachable:
        return "UNAVAILABLE"
    if not mapped:
        return "MAPPING ERROR"
    if mapped and not subscribed:
        # Mapped but provider symbol missing / not in stream subscription set
        status = (quote or {}).get("status")
        if status == "FALLBACK":
            return "FALLBACK"
        if status == "UNAVAILABLE" or quote is None:
            return "SUBSCRIPTION ERROR"
        if status == "STALE":
            return "STALE"
        if status == "LIVE":
            return "PASS"
        return "SUBSCRIPTION ERROR"
    if not frontend_key_ok:
        return "FRONTEND RESOLUTION ERROR"
    if quote is None:
        return "UNAVAILABLE"

    status = str(quote.get("status") or "UNAVAILABLE").upper()
    mid = quote.get("mid")
    if mid is None and quote.get("current_price") is None:
        if status == "FALLBACK":
            return "FALLBACK"
        return "UNAVAILABLE"

    if candle is None and status in ("LIVE", "STALE"):
        # Active candle expected when we have a live/stale quote
        return "ACTIVE CANDLE ERROR"

    if status == "LIVE":
        return "PASS"
    if status == "STALE":
        return "STALE"
    if status == "FALLBACK":
        return "FALLBACK"
    if status == "UNAVAILABLE":
        return "UNAVAILABLE"
    return status


def _match_representative(key: str, display: str | None) -> str | None:
    blob = f"{key} {(display or '')}".lower()
    aliases = {
        "Gold": ["gold"],
        "Natural Gas / NG": ["natural gas", "ng"],
        "WTI Crude Oil / CL": ["wti", "crude oil", "cl"],
        "Silver": ["silver"],
        "Copper": ["copper"],
        "EUR/USD": ["eur/usd", "eurusd"],
        "USD/JPY": ["usd/jpy", "usdjpy"],
        "S&P 500 / ES": ["s&p 500", "spx", "es"],
        "Bitcoin": ["bitcoin", "btc"],
        "Corn": ["corn"],
        "Soybeans": ["soybean"],
        "Wheat": ["wheat"],
    }
    for label, needles in aliases.items():
        if any(n in blob for n in needles):
            return label
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="All-instrument current price coverage audit.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8787")
    parser.add_argument("--ws-url", default="ws://127.0.0.1:8787/ws/prices")
    args = parser.parse_args()
    base = args.base_url.rstrip("/")

    mapping_doc, instruments = _load_mapping()
    health: dict[str, Any] | None = None
    prices_doc: dict[str, Any] | None = None
    weekly_doc: dict[str, Any] | None = None
    backend_reachable = True
    backend_error = None

    try:
        health = _get_json(f"{base}/health")
        prices_doc = _get_json(f"{base}/api/prices")
        weekly_doc = _get_json(f"{base}/api/weekly-candles")
    except URLError as exc:
        backend_reachable = False
        backend_error = str(exc)
    except Exception as exc:  # noqa: BLE001
        backend_reachable = False
        backend_error = str(exc)

    prices = (prices_doc or {}).get("prices") or {}
    weekly = (weekly_doc or {}).get("weekly_candles") or {}
    subscribed_count = int((health or {}).get("subscribed_instruments") or 0)

    rows: list[dict[str, Any]] = []
    class_counts: Counter[str] = Counter()
    failed: list[dict[str, Any]] = []
    representative_hits: dict[str, dict[str, Any]] = {}

    for item in instruments:
        key = item.get("internal_key") or item.get("key") or item.get("id")
        display = item.get("display_name") or item.get("name") or key
        provider_symbol = item.get("provider_symbol") or item.get("oanda_symbol")
        mapping_status = item.get("mapping_status") or item.get("status") or (
            "mapped" if provider_symbol else "unmapped"
        )
        is_mapped = bool(provider_symbol) and str(mapping_status).lower() not in (
            "unmapped",
            "missing",
            "error",
        )
        # Some configs use is_mapped / mapped bools
        if "is_mapped" in item:
            is_mapped = bool(item["is_mapped"])
        if "mapped" in item and isinstance(item["mapped"], bool):
            is_mapped = item["mapped"]

        quote = prices.get(key) if key else None
        candle = weekly.get(key) if key else None

        # Frontend key resolution: internal_key must match API key exactly
        frontend_key_ok = bool(key) and (key in prices or not backend_reachable)

        subscribed = False
        if quote is not None:
            subscribed = quote.get("provider_symbol") is not None and quote.get("status") in (
                "LIVE",
                "STALE",
                "FALLBACK",
                "UNAVAILABLE",
            )
            # Stronger: subscribed if backend has a quote row with provider symbol matching map
            if provider_symbol and quote.get("provider_symbol") == provider_symbol:
                subscribed = True
            elif provider_symbol and quote.get("provider_symbol"):
                subscribed = True
        if health and is_mapped and provider_symbol:
            # If health reports stream up and quote exists for key, treat as subscribed
            if quote is not None and quote.get("provider_symbol"):
                subscribed = True

        classification = _classify(
            mapped=is_mapped,
            subscribed=subscribed,
            quote=quote,
            candle=candle,
            backend_reachable=backend_reachable,
            frontend_key_ok=frontend_key_ok if is_mapped else True,
        )

        # Prefer backend status labels when mapped+subscribed and healthy
        if classification == "PASS" and quote and str(quote.get("status")).upper() != "LIVE":
            classification = str(quote.get("status")).upper()

        age = quote.get("age_seconds") if quote else None
        mid = quote.get("mid") if quote else None
        bid = quote.get("bid") if quote else None
        ask = quote.get("ask") if quote else None
        ts = quote.get("timestamp") if quote else None
        backend_status = quote.get("status") if quote else None

        displayed_source = "current_price_service"
        if not backend_reachable:
            displayed_source = "BACKEND OFFLINE"
        elif classification in ("FALLBACK",):
            displayed_source = quote.get("fallback_source") or "FALLBACK" if quote else "FALLBACK"
        elif classification == "UNAVAILABLE":
            displayed_source = "UNAVAILABLE"
        elif classification == "MAPPING ERROR":
            displayed_source = "UNMAPPED"
        elif classification == "SUBSCRIPTION ERROR":
            displayed_source = "NOT_SUBSCRIBED"

        row = {
            "internal_key": key,
            "display_name": display,
            "provider_symbol": provider_symbol,
            "mapping_status": mapping_status,
            "is_mapped": is_mapped,
            "subscribed": subscribed,
            "backend_quote_present": quote is not None,
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "timestamp": ts,
            "age_seconds": age,
            "backend_status": backend_status,
            "active_weekly_candle_present": candle is not None,
            "active_candle_week_key": (candle or {}).get("date") if candle else None,
            "active_candle_close": (candle or {}).get("close") if candle else None,
            "frontend_key_resolution": "ok" if frontend_key_ok else "MISSING_API_KEY",
            "final_displayed_current_price_source": displayed_source,
            "classification": classification,
        }
        rows.append(row)
        class_counts[classification] += 1

        if classification not in ("PASS", "STALE", "FALLBACK"):
            failed.append(
                {
                    "internal_key": key,
                    "classification": classification,
                    "reason": _failure_reason(row),
                }
            )
        elif classification == "FALLBACK" and is_mapped:
            failed.append(
                {
                    "internal_key": key,
                    "classification": classification,
                    "reason": _failure_reason(row),
                }
            )

        rep = _match_representative(str(key or ""), display)
        if rep and rep not in representative_hits:
            representative_hits[rep] = row

    # Agricultural commodity representative (first agri PASS/STALE)
    for row in rows:
        key = str(row.get("internal_key") or "")
        if any(a in key.lower() for a in ("corn", "soy", "wheat", "cattle", "hogs", "cotton", "sugar", "coffee")):
            if "agri" not in representative_hits:
                representative_hits["Agricultural"] = row
                break

    ws_result = _ws_probe(args.ws_url) if backend_reachable else {"ok": False, "error": "backend down", "frames": 0}

    # Count subscribed as mapped instruments with provider symbol present in stream quotes
    total_mapped = sum(1 for r in rows if r["is_mapped"])
    total_subscribed = sum(1 for r in rows if r["is_mapped"] and r["subscribed"])

    audit = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "base_url": base,
        "backend_reachable": backend_reachable,
        "backend_error": backend_error,
        "health": health,
        "mapping_path": str(MAPPING_PATH.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "mapping_source": mapping_doc.get("source") or mapping_doc.get("generated_by"),
        "totals": {
            "mapped_instruments": total_mapped,
            "config_entries": len(rows),
            "subscribed": total_subscribed,
            "health_subscribed": subscribed_count,
            "PASS": class_counts.get("PASS", 0),
            "STALE": class_counts.get("STALE", 0),
            "FALLBACK": class_counts.get("FALLBACK", 0),
            "UNAVAILABLE": class_counts.get("UNAVAILABLE", 0),
            "MAPPING ERROR": class_counts.get("MAPPING ERROR", 0),
            "SUBSCRIPTION ERROR": class_counts.get("SUBSCRIPTION ERROR", 0),
            "FRONTEND RESOLUTION ERROR": class_counts.get("FRONTEND RESOLUTION ERROR", 0),
            "ACTIVE CANDLE ERROR": class_counts.get("ACTIVE CANDLE ERROR", 0),
            "errors": sum(
                class_counts[k]
                for k in (
                    "MAPPING ERROR",
                    "SUBSCRIPTION ERROR",
                    "FRONTEND RESOLUTION ERROR",
                    "ACTIVE CANDLE ERROR",
                )
            ),
        },
        "websocket": ws_result,
        "representative": representative_hits,
        "failed": failed,
        "instruments": rows,
    }

    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_PATH.write_text(json.dumps(audit, indent=2), encoding="utf-8")

    _print_summary(audit)
    # Hard fail only if backend down or frontend resolution / mapping errors on mapped set
    hard = (
        not backend_reachable
        or class_counts.get("FRONTEND RESOLUTION ERROR", 0) > 0
        or class_counts.get("ACTIVE CANDLE ERROR", 0) > 0
        or class_counts.get("SUBSCRIPTION ERROR", 0) > 0
    )
    return 1 if hard else 0


def _failure_reason(row: dict[str, Any]) -> str:
    c = row["classification"]
    if c == "MAPPING ERROR":
        return f"no provider symbol for {row['internal_key']!r}"
    if c == "SUBSCRIPTION ERROR":
        return (
            f"mapped symbol {row['provider_symbol']!r} not producing stream quote "
            f"(backend_status={row['backend_status']}, quote_present={row['backend_quote_present']})"
        )
    if c == "FRONTEND RESOLUTION ERROR":
        return f"internal key {row['internal_key']!r} missing from /api/prices"
    if c == "ACTIVE CANDLE ERROR":
        return f"quote status={row['backend_status']} but no active weekly candle"
    if c == "FALLBACK":
        return f"FALLBACK mid={row['mid']} source={row['final_displayed_current_price_source']}"
    if c == "UNAVAILABLE":
        return "no usable quote"
    if c == "STALE":
        return f"STALE age_seconds={row['age_seconds']}"
    return c


def _print_summary(audit: dict[str, Any]) -> None:
    t = audit["totals"]
    print("=" * 72)
    print("CURRENT PRICE COVERAGE AUDIT")
    print("=" * 72)
    print(f"backend_reachable : {audit['backend_reachable']}")
    if audit.get("backend_error"):
        print(f"backend_error     : {audit['backend_error']}")
    print(f"config entries    : {t['config_entries']}")
    print(f"mapped            : {t['mapped_instruments']}")
    print(f"subscribed        : {t['subscribed']} (health={t['health_subscribed']})")
    print(
        f"PASS={t['PASS']}  STALE={t['STALE']}  FALLBACK={t['FALLBACK']}  "
        f"UNAVAILABLE={t['UNAVAILABLE']}  ERRORS={t['errors']}"
    )
    print(
        f"  MAPPING ERROR={t['MAPPING ERROR']}  SUBSCRIPTION ERROR={t['SUBSCRIPTION ERROR']}  "
        f"FRONTEND={t['FRONTEND RESOLUTION ERROR']}  ACTIVE CANDLE={t['ACTIVE CANDLE ERROR']}"
    )
    ws = audit.get("websocket") or {}
    print(f"websocket         : ok={ws.get('ok')} frames={ws.get('frames')} keys={ws.get('price_keys_seen')} err={ws.get('error')}")
    print("-" * 72)
    print("Representative:")
    for label, row in (audit.get("representative") or {}).items():
        print(
            f"  {label:22} {row.get('classification'):12} mid={row.get('mid')} "
            f"status={row.get('backend_status')} age={row.get('age_seconds')} "
            f"candle={row.get('active_candle_week_key')}"
        )
    failed = audit.get("failed") or []
    if failed:
        print("-" * 72)
        print(f"Failed ({len(failed)}):")
        for f in failed[:40]:
            print(f"  [{f['classification']}] {f['internal_key']}: {f['reason']}")
        if len(failed) > 40:
            print(f"  ... +{len(failed) - 40} more")
    print("=" * 72)
    print(f"Wrote {AUDIT_PATH}")


if __name__ == "__main__":
    raise SystemExit(main())
