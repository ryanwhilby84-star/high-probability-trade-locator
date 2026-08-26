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
    parser.add_argument(
        "--strict-health",
        action="store_true",
        help="Return non-zero when refreshed canonical prices fail freshness/integrity checks",
    )
    args = parser.parse_args(argv)

    probe_warnings: list[str] = []
    if not args.skip_validation:
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
        has_history = bool(rec.get("daily") or rec.get("weekly"))
        status = "ok" if has_history and not rec.get("error") else ("kept" if has_history else "fail")
        daily_n = len(rec.get("daily") or [])
        last_bar = ((rec.get("daily") or [{}])[-1] or {}).get("date") if rec.get("daily") else None
        suffix = f" last={last_bar}" if last_bar else ""
        if rec.get("error"):
            suffix += f" error={str(rec.get('error'))[:100]}"
        print(f"[{i}/{total}] {iid}: {status} daily_bars={daily_n}{suffix}", flush=True)

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
    ok = sum(1 for r in records.values() if r.get("daily") and not r.get("error"))
    kept = sum(1 for r in records.values() if r.get("daily") and r.get("error"))
    failed = sum(1 for r in records.values() if not r.get("daily"))
    print(f"\nStored {len(records)} refreshed instruments ({ok} clean, {kept} kept-with-error, {failed} no daily history)")
    print(f"Canonical: {path}")
    print("Dashboard: web-dashboard/public/data/prices_latest.json")

    if probe_warnings:
        print(f"\nPrice source probe warnings ({len(probe_warnings)}, non-fatal):")
        for warning in probe_warnings:
            print(f"  - {warning}")

    # A fetch returning bars is not enough. Verify the resulting canonical store
    # for freshness, gaps, duplicates and OHLC sanity every time we refresh.
    from hptl.prices.price_health import write_health_audit

    health = write_health_audit(ids)
    hs = health["summary"]
    print(f"\nPrice health: PASS={hs['pass']} WARN={hs['warn']} FAIL={hs['fail']} TOTAL={hs['total']}")
    for row in health["rows"]:
        if row["status"] != "PASS":
            detail = "; ".join(row["issues"] + row["warnings"])
            print(f"  {row['status']:4} {row['instrument']}: {detail}")

    if args.strict_health and hs["fail"]:
        print("ERROR: strict price health gate failed", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
