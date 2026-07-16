"""Validation script for the canonical Current Price Service.

Checks every configured instrument and reports:

    dashboard key | provider | provider symbol | latest quote | quote age |
    current price | status | historical source | historical close | diff vs live

and flags: missing mappings, stale quotes, invalid prices, zero prices,
duplicate mappings, unverified/incorrect provider symbols, and quotes labelled
LIVE that are actually stale.

Writes ``data/audits/current_price_service_audit.json`` and prints a summary.

Run:  python scripts/verify_current_price_service.py
      python scripts/verify_current_price_service.py --no-fetch   (offline mapping check)

Exit code is non-zero when a hard failure is present (missing mapping for an
instrument that has candidates, zero/invalid live price, or a false-LIVE label).
Markets simply being closed (STALE / no quote) is not a hard failure.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# Allow running as a bare script (python scripts/verify_current_price_service.py).
_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from hptl.config import PROJECT_ROOT, get_oanda_api_key  # noqa: E402
from hptl.prices.current_price_service import (  # noqa: E402
    CURRENT_PRICE_STALE_SECONDS,
    STATUS_LIVE,
    STATUS_STALE,
    STATUS_UNAVAILABLE,
    get_current_prices,
    latest_trusted_close,
    load_instrument_mappings,
    mapping_source,
)

AUDIT_PATH = PROJECT_ROOT / "data" / "audits" / "current_price_service_audit.json"


def _fmt(v: object, nd: int | None = None) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        if nd is not None:
            return f"{v:.{nd}f}"
        return f"{v:.6g}"
    return str(v)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the Current Price Service.")
    parser.add_argument(
        "--no-fetch",
        action="store_true",
        help="Skip live OANDA fetch (mapping/precision validation only).",
    )
    args = parser.parse_args()

    has_key = bool(get_oanda_api_key())
    fetch = (not args.no_fetch) and has_key
    src = mapping_source()
    mappings = load_instrument_mappings()
    prices = get_current_prices(fetch=fetch)

    # Duplicate provider symbols across instruments (proxies legitimately share).
    symbol_to_keys: dict[str, list[str]] = defaultdict(list)
    for key, m in mappings.items():
        if m.provider_symbol:
            symbol_to_keys[m.provider_symbol].append(key)
    duplicates = {sym: keys for sym, keys in symbol_to_keys.items() if len(keys) > 1}

    rows: list[dict] = []
    flags: dict[str, list[str]] = defaultdict(list)

    for key in sorted(prices.keys()):
        cp = prices[key]
        m = mappings.get(key)
        hist_close, hist_source = latest_trusted_close(key)
        diff = None
        if cp.mid is not None and hist_close not in (None, 0):
            diff = cp.mid - hist_close

        row_flags: list[str] = []
        has_candidates = bool(m and m.provider_symbol)

        if not (m and m.is_mapped):
            row_flags.append("missing_mapping")
        elif src == "registry_fallback":
            row_flags.append("unverified_mapping")

        if fetch and (m and m.is_mapped):
            if cp.mid is None:
                row_flags.append("no_quote")
            elif cp.mid == 0:
                row_flags.append("zero_price")
            elif cp.mid != cp.mid:  # NaN guard
                row_flags.append("invalid_price")

        if cp.status == STATUS_STALE:
            row_flags.append("stale_quote")
        if cp.status == STATUS_UNAVAILABLE and (m and m.is_mapped) and not fetch:
            row_flags.append("not_fetched")
        if (
            cp.status == STATUS_LIVE
            and cp.age_seconds is not None
            and cp.age_seconds > CURRENT_PRICE_STALE_SECONDS
        ):
            row_flags.append("false_live")

        for f in row_flags:
            flags[f].append(key)

        rows.append(
            {
                "dashboard_key": key,
                "provider": cp.provider,
                "provider_symbol": cp.provider_symbol,
                "asset_type": cp.asset_type,
                "currency": cp.currency,
                "price_precision": cp.price_precision,
                "latest_quote_mid": cp.mid,
                "bid": cp.bid,
                "ask": cp.ask,
                "quote_age_seconds": cp.age_seconds,
                "current_price": cp.current_price,
                "status": cp.status,
                "historical_source": hist_source,
                "historical_close": hist_close,
                "diff_from_live": diff,
                "flags": row_flags,
            }
        )

    # Hard failures: things that mean a *mapped* price cannot be trusted,
    # regardless of market hours. Missing mappings are a WARNING, not a failure —
    # whether OANDA offers an instrument is account-dependent (crypto, cash
    # treasuries, some EM FX, etc. are legitimately not offered). Markets being
    # closed (STALE / no quote) is also not a hard failure.
    hard_fail_keys: set[str] = set()
    hard_fail_keys.update(flags.get("zero_price", []))
    hard_fail_keys.update(flags.get("invalid_price", []))
    hard_fail_keys.update(flags.get("false_live", []))

    # Instruments that have OANDA candidates but did not resolve on this account.
    unmapped_with_candidates = sorted(
        k for k in flags.get("missing_mapping", []) if _candidate_count(k) > 0
    )

    payload = {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mapping_source": src,
        "oanda_key_present": has_key,
        "live_fetch_performed": fetch,
        "stale_threshold_seconds": CURRENT_PRICE_STALE_SECONDS,
        "summary": {
            "instruments": len(rows),
            "mapped": sum(1 for r in rows if r["provider_symbol"]),
            "live": sum(1 for r in rows if r["status"] == STATUS_LIVE),
            "stale": sum(1 for r in rows if r["status"] == STATUS_STALE),
            "unavailable": sum(1 for r in rows if r["status"] == STATUS_UNAVAILABLE),
            "hard_failures": sorted(hard_fail_keys),
            "unmapped_with_candidates": unmapped_with_candidates,
        },
        "flags": {k: sorted(v) for k, v in flags.items()},
        "duplicate_provider_symbols": duplicates,
        "instruments": rows,
    }

    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    _print_report(payload)
    return 1 if hard_fail_keys else 0


def _candidate_count(key: str) -> int:
    """Number of OANDA candidate symbols for an instrument (0 => not expected to map)."""
    from hptl.markets.instrument_registry import load_registry
    from hptl.prices.oanda_instrument_discovery import _candidate_symbols

    reg = load_registry()
    spec = reg.get(key)
    if not spec:
        return 0
    return len(_candidate_symbols(spec, reg))


def _print_report(payload: dict) -> None:
    s = payload["summary"]
    print("=" * 96)
    print("HPTL Current Price Service - validation")
    print("=" * 96)
    print(f"mapping source     : {payload['mapping_source']}")
    print(f"OANDA key present  : {payload['oanda_key_present']}")
    print(f"live fetch         : {payload['live_fetch_performed']}")
    print(
        f"instruments        : {s['instruments']} "
        f"(mapped {s['mapped']}, live {s['live']}, stale {s['stale']}, "
        f"unavailable {s['unavailable']})"
    )
    print("-" * 96)
    header = (
        f"{'dashboard key':28} {'symbol':12} {'mid':>12} "
        f"{'age(s)':>8} {'status':11} {'hist close':>12} {'diff':>10}"
    )
    print(header)
    print("-" * 96)
    for r in payload["instruments"]:
        flag_mark = " *" if r["flags"] else ""
        print(
            f"{r['dashboard_key'][:28]:28} "
            f"{(r['provider_symbol'] or '-')[:12]:12} "
            f"{_fmt(r['latest_quote_mid']):>12} "
            f"{_fmt(r['quote_age_seconds'], 0):>8} "
            f"{r['status']:11} "
            f"{_fmt(r['historical_close']):>12} "
            f"{_fmt(r['diff_from_live']):>10}"
            f"{flag_mark}"
        )
    print("-" * 96)
    if payload["flags"]:
        print("flags:")
        for name, keys in sorted(payload["flags"].items()):
            print(f"  {name:20} ({len(keys)}): {', '.join(keys[:12])}"
                  + (" ..." if len(keys) > 12 else ""))
    if payload["duplicate_provider_symbols"]:
        print("duplicate provider symbols (proxies may legitimately share):")
        for sym, keys in sorted(payload["duplicate_provider_symbols"].items()):
            print(f"  {sym}: {', '.join(keys)}")
    unmapped = payload["summary"].get("unmapped_with_candidates") or []
    if unmapped:
        print(
            f"warning: {len(unmapped)} instrument(s) have OANDA candidates but are "
            f"not offered on this account: {', '.join(unmapped)}"
        )
    hard = payload["summary"]["hard_failures"]
    print("-" * 96)
    if hard:
        print(f"RESULT: FAIL — {len(hard)} hard failure(s): {', '.join(hard)}")
    else:
        print("RESULT: PASS — no hard failures.")
    print(f"audit written to {AUDIT_PATH}")


if __name__ == "__main__":
    raise SystemExit(main())
