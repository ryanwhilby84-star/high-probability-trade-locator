"""CLI: ingest official currency rates -> data/config/fx_currency_rates.json.

    python -m hptl.fx.run_fx_currency_rates

Set ``HPTL_SKIP_LIVE_FEEDS=1`` to run cache-only (no network).
"""

from __future__ import annotations

import argparse

from hptl.fx.ingest_currency_rates import ingest


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest official FX currency rates.")
    parser.add_argument("--dry-run", action="store_true", help="fetch + report but do not write the config")
    parser.add_argument("--quiet", action="store_true", help="suppress per-currency progress output")
    args = parser.parse_args()

    config = ingest(write=not args.dry_run, verbose=not args.quiet)
    statuses = {c: b["status"] for c, b in config["currencies"].items()}
    n_pass = sum(1 for s in statuses.values() if s == "PASS")
    print(f"\nSummary: {n_pass}/{len(statuses)} PASS — {statuses}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
