"""Refresh canonical price store for all coverage-supported instruments."""

from __future__ import annotations

import argparse
import sys

from hptl.price_config import PriceApiConfigError, validate_price_api_keys
from hptl.prices.cot_fail_backfill import FRED_COT_FAIL_SERIES
from hptl.prices.coverage import load_price_coverage, supported_instrument_ids
from hptl.prices.price_store import (
    load_instrument_record,
    load_instrument_record_internal,
    merge_fetched_into_production,
    write_instrument_record,
    write_price_store_merged,
)
from hptl.prices.unified_adapter import UnifiedPriceAdapter


def refresh_instrument_record(
    instrument_id: str,
    fetched: dict,
    *,
    fetched_via: str,
) -> dict:
    """Merge a live fetch into the stored production record (or return fetch on empty store)."""
    existing = load_instrument_record_internal(instrument_id)
    has_incoming = bool(fetched.get("daily") or fetched.get("weekly"))

    if fetched_via == "fred" and instrument_id in FRED_COT_FAIL_SERIES and has_incoming:
        existing = None

    if fetched.get("error") and not has_incoming:
        if existing:
            rec = load_instrument_record(instrument_id)
            if rec is not None:
                rec["error"] = fetched.get("error")
            return rec or fetched
        return fetched

    if not existing and not has_incoming:
        return fetched

    merged, meta = merge_fetched_into_production(existing, fetched, fetched_via=fetched_via)
    write_instrument_record(
        merged,
        fetched_via=meta.get("fetched_via"),
        historical_via=meta.get("historical_via"),
    )
    return merged


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Refresh HTPL canonical price store (OANDA + Alpha Vantage)")
    parser.add_argument("--limit", type=int, default=0, help="Max instruments (0 = all supported)")
    parser.add_argument("--instrument", type=str, default="", help="Refresh single instrument id")
    parser.add_argument("--skip-validation", action="store_true")
    args = parser.parse_args(argv)

    probe_warnings: list[str] = []
    if not args.skip_validation:
        # Missing API keys are fatal (nothing can fetch). Live-source probes are
        # advisory only: a rate-limited or unavailable source (e.g. an Alpha
        # Vantage informational/quota response) must NOT abort the refresh, so
        # OANDA/FRED instruments still fetch and last-known-good data is kept.
        try:
            validate_price_api_keys(probe_live=False)
        except (PriceApiConfigError, RuntimeError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1

        from hptl.oanda.oanda_adapter import validate_oanda_connection
        from hptl.alpha_vantage.alpha_adapter import validate_alpha_vantage_connection

        for name, probe in (
            ("OANDA", validate_oanda_connection),
            ("Alpha Vantage", validate_alpha_vantage_connection),
        ):
            try:
                probe()
            except Exception as exc:  # noqa: BLE001 — advisory probe, never fatal
                probe_warnings.append(f"{name} live probe: {exc}")
                print(f"WARNING (price source probe, non-fatal): {name}: {exc}", file=sys.stderr)

    coverage = load_price_coverage()
    ids = [args.instrument.strip()] if args.instrument.strip() else supported_instrument_ids(coverage)
    if args.limit > 0:
        ids = ids[: args.limit]

    adapter = UnifiedPriceAdapter(coverage)
    records: dict = {}

    def progress(i: int, total: int, iid: str, rec: dict) -> None:
        status = "ok" if (rec.get("daily") or rec.get("weekly")) and not rec.get("error") else "fail"
        daily_n = len(rec.get("daily") or [])
        print(f"[{i}/{total}] {iid}: {status} daily_bars={daily_n}", flush=True)

    for iid in ids:
        fetched = adapter.fetch(iid)
        src = str(fetched.get("_fetched_via") or adapter.source_for(iid) or "none")
        rec = refresh_instrument_record(iid, fetched, fetched_via=src)
        records[iid] = rec
        progress(len(records), len(ids), iid, rec)

    path = write_price_store_merged(
        records,
        coverage_generated_at=coverage.get("generated_at"),
    )
    s = records
    ok = sum(1 for r in s.values() if r.get("daily") and not r.get("error"))
    print(f"\nStored {len(records)} refreshed instruments ({ok} with daily bars)")
    print(f"Canonical: {path}")
    print(f"Dashboard: web-dashboard/public/data/prices_latest.json")

    if probe_warnings:
        print(f"\nPrice source probe warnings ({len(probe_warnings)}, non-fatal):")
        for w in probe_warnings:
            print(f"  - {w}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
