"""Run live OANDA + Alpha Vantage price coverage audit."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from hptl.price_config import PriceApiConfigError, validate_price_api_keys
from hptl.prices.price_coverage_audit import AUDIT_JSON_PATH, build_price_coverage_audit, write_price_coverage_audit


def main() -> None:
    parser = argparse.ArgumentParser(description="HTPL price coverage audit (OANDA + Alpha Vantage)")
    parser.add_argument("--out", type=Path, default=AUDIT_JSON_PATH)
    parser.add_argument(
        "--av-delay",
        type=float,
        default=12.0,
        help="Seconds between Alpha Vantage category probes (rate-limit friendly)",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip startup key validation (not recommended)",
    )
    args = parser.parse_args()

    try:
        if not args.skip_validation:
            validate_price_api_keys(probe_live=True)
        payload = build_price_coverage_audit(av_probe_delay_sec=args.av_delay)
        path = write_price_coverage_audit(payload, path=args.out)
    except (PriceApiConfigError, RuntimeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    s = payload["summary"]
    print("Price coverage audit complete")
    print(f"  OANDA supported:        {s['oanda_supported_count']}")
    print(f"  Alpha Vantage supported: {s['alpha_supported_count']}")
    print(f"  Both:                   {s['supported_by_both_count']}")
    print(f"  Unsupported:            {s['unsupported_count']}")
    print(f"Written: {path}")
    print("Dashboard: #/price-coverage")


if __name__ == "__main__":
    main()
