#!/usr/bin/env python3
"""Repair Phase-2 universe price failures under the closed feature gate.

1. Promote OANDA NAS100/SPX500/US30 into the COT price store (replace QQQ/SPY/DIA)
2. Backfill Coffee/Cocoa/Cotton via Yahoo continuous futures (replace stale FRED months)
3. Refresh all OANDA-backed LEGACY instruments to clear lag failures
4. Rebuild cot_3y_series
5. Rerun universe integrity audit
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main() -> int:
    from hptl.cot.cot_3y_series_export import run as run_cot_3y
    from hptl.markets.instrument_registry import LEGACY_COT_MARKETS, load_registry
    from hptl.markets.universe_integrity_audit import write_report
    from hptl.prices.coverage import load_price_coverage
    from hptl.prices.promote_index_oanda_prices import promote_all_index_oanda
    from hptl.prices.run_price_refresh import refresh_instrument_record
    from hptl.prices.softs_futures_backfill import promote_all_softs_futures
    from hptl.prices.unified_adapter import UnifiedPriceAdapter
    from hptl.prices.price_store import write_price_store_merged

    repair_log: dict[str, object] = {"steps": []}

    print("=== 1) Promote index OANDA histories ===")
    index_rows = promote_all_index_oanda(refresh_cache=True)
    repair_log["indices"] = index_rows
    for row in index_rows:
        print(
            f"  {row['instrument_id']}: {row['previous_source']}@{row['previous_latest_date']} "
            f"-> {row['corrected_source']}@{row['corrected_latest_date']} "
            f"(+{row['missing_rows_backfilled']} days, {row['weekly_bars_rebuilt']} weekly)"
        )

    print("=== 2) Softs Yahoo futures backfill ===")
    soft_rows = promote_all_softs_futures()
    repair_log["softs"] = soft_rows
    for row in soft_rows:
        print(
            f"  {row['instrument_id']}: {row['previous_source']}@{row['previous_latest_date']} "
            f"-> {row['corrected_source']}@{row['corrected_latest_date']} "
            f"(+{row['missing_rows_backfilled']} days, {row['weekly_bars_rebuilt']} weekly)"
        )

    print("=== 3) Refresh remaining OANDA LEGACY instruments ===")
    reg = load_registry()
    skip = {"NASDAQ / NQ", "S&P 500 / ES", "Dow / YM", "Coffee", "Cocoa", "Cotton"}
    oanda_ids = [
        m
        for m in LEGACY_COT_MARKETS
        if m not in skip and reg.get(m) and reg[m].oanda_symbol
    ]
    coverage = load_price_coverage()
    adapter = UnifiedPriceAdapter(coverage)
    refreshed: dict = {}
    errors: dict[str, str] = {}
    for iid in oanda_ids:
        fetched = adapter.fetch(iid)
        src = str(fetched.get("_fetched_via") or "oanda")
        rec = refresh_instrument_record(iid, fetched, fetched_via=src)
        refreshed[iid] = rec
        if rec.get("error") and not (rec.get("daily") or []):
            errors[iid] = str(rec.get("error"))
        print(
            f"  [{iid}] daily={len(rec.get('daily') or [])} "
            f"as_of={(rec.get('price') or {}).get('as_of')} err={rec.get('error')}"
        )
    if refreshed:
        write_price_store_merged(refreshed, coverage_generated_at=coverage.get("generated_at"))
    repair_log["oanda_refresh"] = {
        "instruments": oanda_ids,
        "ok": sum(1 for r in refreshed.values() if r and (r.get("daily") or []) and not r.get("error")),
        "errors": errors,
    }
    print(f"  refreshed {repair_log['oanda_refresh']['ok']}/{len(oanda_ids)}")
    for k, err in errors.items():
        print(f"  ERROR {k}: {err}")

    print("=== 4) Rebuild cot_3y_series ===")
    run_cot_3y()
    repair_log["cot_3y"] = "rebuilt"

    print("=== 5) Universe integrity audit ===")
    report = write_report()
    summary = report["summary"]
    repair_log["audit_summary"] = summary
    out = ROOT / "data" / "audits" / "phase2_price_repair_log.json"
    out.write_text(json.dumps(repair_log, indent=2), encoding="utf-8")
    print(
        f"Gate: {'OPEN' if summary.get('gate_open') else 'CLOSED'} | "
        f"PASS={summary['passed']} WARN={summary['warnings']} FAIL={summary['failed']}"
    )
    if summary["manual_review_required"]:
        print("Manual review:", ", ".join(summary["manual_review_required"]))
    print(f"Wrote {out}")
    print(f"Wrote data/audits/universe_integrity_audit.md")
    return 0 if summary.get("gate_open") and summary["warnings"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
