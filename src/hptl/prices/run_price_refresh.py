"""Refresh canonical price store for all coverage-supported instruments."""

from __future__ import annotations

import argparse
import sys

from hptl.price_config import PriceApiConfigError, validate_price_api_keys
from hptl.prices.cot_fail_backfill import FRED_COT_FAIL_SERIES
from hptl.prices.coverage import load_price_coverage, supported_instrument_ids
from hptl.prices.price_store import (
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
    """Merge a live fetch into the stored production record.

    Last-known-good bars are retained on provider failure, but the failed fetch
    itself is persisted as an explicit error so stale data cannot masquerade as
    a successful refresh.
    """
    existing = load_instrument_record_internal(instrument_id)
    has_incoming = bool(fetched.get("daily") or fetched.get("weekly"))

    if fetched_via == "fred" and instrument_id in FRED_COT_FAIL_SERIES and has_incoming:
        existing = None

    if not existing and not has_incoming:
        # Nothing useful exists yet, but still persist the failure record so the
        # operational audit can explain why this instrument has no history.
        write_instrument_record(fetched, fetched_via=fetched_via)
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
        "--require-healthy",
        action="store_true",
        help="Exit non-zero when the post-refresh price health audit has failures",
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
            except Exception as exc:
                probe_warnings.append(f"{name} live probe: {exc}")
                print(f"WARNING (price source probe, non-fatal): {name}: {exc}", file=sys.stderr)

    coverage = load_price_coverage()
    ids = [args.instrument.strip()] if args.instrument.strip() else supported_instrument_ids(coverage)
    if args.limit > 0:
        ids = ids[: args.limit]

    adapter = UnifiedPriceAdapter(coverage)
    records: dict = {}

    def progress(i: int, total: int, iid: str, rec: dict) -> None:
        daily = rec.get("daily") or []
        status = "ok" if daily and not rec.get("error") else "fail"
        last_bar = daily[-1].get("date") if daily else "—"
        src = rec.get("_fetched_via") or adapter.source_for(iid) or "none"
        err = f" error={rec.get('error')}" if rec.get("error") else ""
        print(
            f"[{i}/{total}] {iid}: {status} source={src} daily_bars={len(daily)} last_bar={last_bar}{err}",
            flush=True,
        )

    for iid in ids:
        fetched = adapter.fetch(iid)
        src = str(fetched.get("_fetched_via") or adapter.source_for(iid) or "none")
        rec = refresh_instrument_record(iid, fetched, fetched_via=src)
        rec["_fetched_via"] = src
        records[iid] = rec
        progress(len(records), len(ids), iid, rec)

    path = write_price_store_merged(records, coverage_generated_at=coverage.get("generated_at"))
    ok = sum(1 for r in records.values() if r.get("daily") and not r.get("error"))
    print(f"\nStored {len(records)} refreshed instruments ({ok} healthy fetches with daily bars)")
    print(f"Canonical: {path}")
    print("Dashboard: web-dashboard/public/data/prices_latest.json")

    if probe_warnings:
        print(f"\nPrice source probe warnings ({len(probe_warnings)}, non-fatal):")
        for w in probe_warnings:
            print(f"  - {w}")

    from hptl.prices.price_health import write_health_audit

    health_ids = ids if args.instrument.strip() or args.limit > 0 else None
    health = write_health_audit(health_ids)
    summary = health["summary"]
    print(
        f"\nPost-refresh health: PASS={summary['pass']} WARN={summary['warn']} "
        f"FAIL={summary['fail']} TOTAL={summary['total']}"
    )
    for row in health["rows"]:
        if row["status"] != "PASS":
            detail = "; ".join(row["issues"] + row["warnings"])
            print(f"  {row['status']:4} {row['instrument']}: {detail}")

    if args.require_healthy and summary["fail"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
