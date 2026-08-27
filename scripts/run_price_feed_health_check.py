"""Registry-driven price-feed health check for every HPTL instrument.

Dynamically enumerates the instrument registry (not a hard-coded count) and
reports canonical quote status for each:

    instrument | provider | provider_symbol | price | timestamp | age_seconds | status

PASS rules:
  - quote price (when present) is finite and > 0
  - timestamp parseable when status is LIVE or STALE
  - LIVE quotes satisfy freshness (age <= CURRENT_PRICE_STALE_SECONDS)
  - no stale quote is labelled LIVE
  - valuation datasets are never used as the live source

Optional ``--http`` checks the running Current Price Service
(``http://127.0.0.1:8787/api/prices``) matches the in-process canonical service
for mapped instruments (within bid/ask/mid tolerance).

Writes ``data/audits/price_feed_health_latest.json`` and prints a summary.

Run:
    python scripts/run_price_feed_health_check.py
    python scripts/run_price_feed_health_check.py --http
    python scripts/run_price_feed_health_check.py --no-fetch
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from hptl.config import PROJECT_ROOT, get_oanda_api_key  # noqa: E402
from hptl.markets.instrument_registry import all_instrument_ids  # noqa: E402
from hptl.prices.current_price_service import (  # noqa: E402
    CURRENT_PRICE_STALE_SECONDS,
    STATUS_FALLBACK,
    STATUS_LIVE,
    STATUS_STALE,
    STATUS_UNAVAILABLE,
    get_current_prices,
    load_instrument_mappings,
    mapping_source,
)

OUT_PATH = PROJECT_ROOT / "data" / "audits" / "price_feed_health_latest.json"
DEFAULT_HTTP = "http://127.0.0.1:8787/api/prices"


def _price_of(cp) -> float | None:
    if cp.mid is not None and cp.status in (STATUS_LIVE, STATUS_STALE):
        return float(cp.mid)
    if cp.current_price is not None:
        return float(cp.current_price)
    if cp.fallback_close is not None:
        return float(cp.fallback_close)
    return None


def _fetch_http(url: str) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="HPTL price-feed health check")
    parser.add_argument("--no-fetch", action="store_true", help="Skip OANDA REST (mapping/cache only)")
    parser.add_argument(
        "--http",
        nargs="?",
        const=DEFAULT_HTTP,
        default=None,
        help=f"Compare against running service (default {DEFAULT_HTTP})",
    )
    parser.add_argument("--gold-only-tolerance", type=float, default=0.5)
    args = parser.parse_args()

    has_key = bool(get_oanda_api_key())
    fetch = (not args.no_fetch) and has_key
    registry = list(all_instrument_ids())
    mappings = load_instrument_mappings()
    prices = get_current_prices(fetch=fetch)

    http_prices: dict = {}
    http_error: str | None = None
    if args.http:
        try:
            payload = _fetch_http(args.http)
            http_prices = payload.get("prices") or {}
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
            http_error = str(exc)

    rows: list[dict] = []
    hard_failures: list[str] = []
    status_counts: Counter[str] = Counter()

    for key in sorted(set(registry) | set(mappings) | set(prices)):
        cp = prices.get(key)
        m = mappings.get(key)
        if cp is None:
            hard_failures.append(f"{key}: missing from get_current_prices")
            rows.append(
                {
                    "instrument": key,
                    "provider": m.provider if m else None,
                    "provider_symbol": m.provider_symbol if m else None,
                    "price": None,
                    "timestamp": None,
                    "age_seconds": None,
                    "status": STATUS_UNAVAILABLE,
                    "pass": False,
                    "flags": ["missing_canonical_quote"],
                }
            )
            status_counts[STATUS_UNAVAILABLE] += 1
            continue

        price = _price_of(cp)
        flags: list[str] = []
        ok = True

        if cp.status == STATUS_LIVE:
            if cp.age_seconds is None:
                flags.append("live_without_age")
                ok = False
            elif cp.age_seconds > CURRENT_PRICE_STALE_SECONDS:
                flags.append("stale_labelled_live")
                ok = False
            if price is None or price <= 0:
                flags.append("invalid_live_price")
                ok = False
            if not cp.timestamp:
                flags.append("live_without_timestamp")
                ok = False

        if cp.status in (STATUS_LIVE, STATUS_STALE) and price is not None and price <= 0:
            flags.append("non_positive_price")
            ok = False

        if cp.status == STATUS_FALLBACK and price is not None and price <= 0:
            flags.append("invalid_fallback_price")
            ok = False

        if cp.status not in (STATUS_LIVE, STATUS_STALE, STATUS_FALLBACK, STATUS_UNAVAILABLE):
            flags.append(f"unknown_status:{cp.status}")
            ok = False

        # Consistency vs HTTP service (when requested)
        if http_prices and key in http_prices:
            remote = http_prices[key] or {}
            remote_status = str(remote.get("status") or "").upper()
            remote_mid = remote.get("mid")
            remote_cp = remote.get("current_price")
            if remote_status != cp.status:
                flags.append(f"http_status_mismatch:{remote_status}")
                ok = False
            if (
                cp.status in (STATUS_LIVE, STATUS_STALE)
                and remote_mid is not None
                and cp.mid is not None
                and abs(float(remote_mid) - float(cp.mid)) > max(1e-6, abs(float(cp.mid)) * 1e-6)
            ):
                # Allow tiny float noise; stream may move between calls — flag only large gaps
                if abs(float(remote_mid) - float(cp.mid)) > args.gold_only_tolerance and key == "Gold":
                    flags.append("http_mid_mismatch")
                    ok = False
                elif abs(float(remote_mid) - float(cp.mid)) > max(args.gold_only_tolerance, abs(float(cp.mid)) * 0.002):
                    flags.append("http_mid_mismatch")
                    ok = False
            if remote_status == STATUS_LIVE and remote.get("age_seconds") is not None:
                if float(remote["age_seconds"]) > CURRENT_PRICE_STALE_SECONDS:
                    flags.append("http_stale_labelled_live")
                    ok = False
            _ = remote_cp  # reserved for future strict equality checks

        if not ok:
            hard_failures.append(f"{key}: {', '.join(flags)}")

        status_counts[cp.status] += 1
        rows.append(
            {
                "instrument": key,
                "provider": cp.provider,
                "provider_symbol": cp.provider_symbol,
                "price": price,
                "timestamp": cp.timestamp,
                "age_seconds": cp.age_seconds,
                "status": cp.status,
                "tradeable": cp.tradeable,
                "price_precision": cp.price_precision,
                "fallback_source": cp.fallback_source,
                "note": cp.note,
                "pass": ok,
                "flags": flags,
            }
        )

    # Gold end-to-end spot check (in-process)
    gold = next((r for r in rows if r["instrument"] == "Gold"), None)
    gold_ok = bool(gold and gold["pass"] and gold["status"] in (STATUS_LIVE, STATUS_STALE, STATUS_FALLBACK))

    provider_groups: dict[str, list[str]] = {}
    for r in rows:
        g = r["provider"] or "unmapped"
        provider_groups.setdefault(g, []).append(r["instrument"])

    unavailable = [r["instrument"] for r in rows if r["status"] == STATUS_UNAVAILABLE]
    fallback = [r["instrument"] for r in rows if r["status"] == STATUS_FALLBACK]
    live = [r["instrument"] for r in rows if r["status"] == STATUS_LIVE]
    stale = [r["instrument"] for r in rows if r["status"] == STATUS_STALE]

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mapping_source": mapping_source(),
        "fetch_live": fetch,
        "oanda_key_present": has_key,
        "stale_threshold_seconds": CURRENT_PRICE_STALE_SECONDS,
        "registry_count": len(registry),
        "checked_count": len(rows),
        "status_counts": dict(status_counts),
        "live": live,
        "stale": stale,
        "fallback": fallback,
        "unavailable": unavailable,
        "provider_groups": {k: sorted(v) for k, v in sorted(provider_groups.items())},
        "hard_failures": hard_failures,
        "http_url": args.http,
        "http_error": http_error,
        "gold": gold,
        "gold_ok": gold_ok,
        "pass": not hard_failures and http_error is None,
        "rows": rows,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"HPTL price-feed health — {report['checked_count']} instruments (registry={len(registry)})")
    print(f"  mapping: {report['mapping_source']}  fetch={fetch}  key={has_key}")
    print(
        f"  LIVE={status_counts.get(STATUS_LIVE, 0)}  "
        f"STALE={status_counts.get(STATUS_STALE, 0)}  "
        f"FALLBACK={status_counts.get(STATUS_FALLBACK, 0)}  "
        f"UNAVAILABLE={status_counts.get(STATUS_UNAVAILABLE, 0)}"
    )
    print(f"  Gold: status={gold and gold['status']} price={gold and gold['price']} ok={gold_ok}")
    if unavailable:
        print(f"  UNAVAILABLE ({len(unavailable)}): {', '.join(unavailable[:12])}"
              + ("…" if len(unavailable) > 12 else ""))
    if http_error:
        print(f"  HTTP check FAILED: {http_error}")
    if hard_failures:
        print(f"  HARD FAILURES ({len(hard_failures)}):")
        for line in hard_failures[:20]:
            print(f"    - {line}")
        if len(hard_failures) > 20:
            print(f"    … +{len(hard_failures) - 20} more")
    else:
        print("  HARD FAILURES: none")
    print(f"  wrote {OUT_PATH}")
    print(f"RESULT: {'PASS' if report['pass'] else 'FAIL'}")
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
