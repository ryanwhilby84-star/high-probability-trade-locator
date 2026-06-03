"""Fetch live OANDA instruments and write HTPL coverage audit JSON."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from hptl.config import get_oanda_api_host, get_oanda_api_key
from hptl.oanda.oanda_client import OandaApiError, resolve_account_id
from hptl.oanda.oanda_coverage_audit import AUDIT_JSON_PATH, build_oanda_coverage_audit, write_oanda_coverage_audit


def main() -> None:
    parser = argparse.ArgumentParser(description="OANDA v20 instrument coverage audit vs HTPL registry")
    parser.add_argument(
        "--out",
        type=str,
        default=str(AUDIT_JSON_PATH),
        help="Output JSON path (also mirrored to web-dashboard/public/data/)",
    )
    parser.add_argument("--account-id", type=str, default="", help="Override OANDA_ACCOUNT_ID")
    args = parser.parse_args()

    if not get_oanda_api_key():
        print(
            "ERROR: Set OANDA_API_KEY in .env (personal access token from OANDA v20 settings).",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        aid = args.account_id.strip() or resolve_account_id()
        payload = build_oanda_coverage_audit(account_id=aid)
        path = write_oanda_coverage_audit(payload, path=Path(args.out))
    except OandaApiError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        if exc.body:
            print(exc.body[:500], file=sys.stderr)
        sys.exit(1)

    s = payload["summary"]
    print(f"OANDA host: {get_oanda_api_host()}")
    print(f"Account: {payload['oanda_account_id']}")
    print(f"OANDA instruments on account: {payload['oanda_instruments_on_account']}")
    print(f"HTPL tradeable: {payload['htpl_tradeable_instruments']}")
    print(f"Supported: {s['supported_count']}  Unsupported: {s['unsupported_count']}")
    print(f"Written: {path}")
    print(f"Dashboard: web-dashboard/public/data/oanda_coverage_audit.json")

    print("\n--- SUPPORTED (sample) ---")
    for row in payload["supported"][:15]:
        print(f"  * {row['friendly_name']}  ({row['resolved_oanda_symbol']})")
    if len(payload["supported"]) > 15:
        print(f"  ... +{len(payload['supported']) - 15} more")

    print("\n--- UNSUPPORTED ---")
    for row in payload["unsupported"]:
        print(f"  * {row['friendly_name']}  [{row.get('unsupported_reason')}]")


if __name__ == "__main__":
    main()
