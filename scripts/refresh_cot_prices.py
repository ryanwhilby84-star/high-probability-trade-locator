#!/usr/bin/env python3
"""Refresh prices + workstation OHLC for COT-mapped instruments only."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.markets.instrument_registry import cot_mapped_ids
from hptl.prices.cot_fail_backfill import OANDA_COT_FAIL_PAIRS, run_oanda_backfill
from hptl.prices.coverage import load_price_coverage
from hptl.prices.price_store import write_price_store_merged
from hptl.prices.promote_price_backfill import promote_staging_backfill
from hptl.prices.run_price_refresh import refresh_instrument_record
from hptl.prices.unified_adapter import UnifiedPriceAdapter
from hptl.prices.workstation_index_ohlc_history import backfill_workstation_index
from hptl.prices.workstation_ohlc_export import write_workstation_ohlc_exports


def main() -> int:
    ids = cot_mapped_ids()
    coverage = load_price_coverage()
    adapter = UnifiedPriceAdapter(coverage)
    records: dict = {}

    for i, iid in enumerate(ids, 1):
        fetched = adapter.fetch(iid)
        src = str(fetched.get("_fetched_via") or adapter.source_for(iid) or "none")
        rec = refresh_instrument_record(iid, fetched, fetched_via=src)
        records[iid] = rec
        daily = rec.get("daily") or []
        last = daily[-1] if daily else {}
        print(
            f"[{i}/{len(ids)}] {iid}: daily={len(daily)} "
            f"last={last.get('date')} close={last.get('close')} src={src}",
            flush=True,
        )

    print("\n--- OANDA commodity backfill ---")
    run_oanda_backfill(years=3)
    promotion = promote_staging_backfill([p[2] for p in OANDA_COT_FAIL_PAIRS])
    print(f"Promoted {promotion.get('count', 0)} staging records")

    print("\n--- Index workstation OHLC ---")
    for idx in ("NASDAQ / NQ", "S&P 500 / ES", "Dow / YM"):
        result = backfill_workstation_index(idx, refresh=True)
        print(f"{idx}: {result.get('daily_rows')} rows, last={result.get('last_date')}")

    path = write_price_store_merged(records, coverage_generated_at=coverage.get("generated_at"))
    print(f"\nPrice store: {path}")

    ohlc_path = write_workstation_ohlc_exports()
    print(f"Workstation OHLC: {ohlc_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
