#!/usr/bin/env python3
"""Phase 4E — ingest Gold drivers from data/manual/metals/ and optionally export."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.data_sources.metals_manual_gold_ingest import run_phase4e_gold_manual_ingest
from hptl.valuation.metals_institutional_drivers import build_driver_bundle
from hptl.valuation.metals_institutional_fair_value_v1 import compute_metals_institutional_valuation


def _gold_status_table(ingest_doc: dict) -> None:
    gold = compute_metals_institutional_valuation(market="Gold")
    bundle = build_driver_bundle("Gold")
    reg = gold.get("regression") or {}

    driver_rows = [
        ("real_yield_10y", "real_yield"),
        ("dxy_broad", "log_dxy"),
        ("central_bank_net_purchases", "cb_net_purchases"),
        ("etf_holdings_or_flows", "etf_holdings"),
    ]
    ingest_by_id = {d["driver_id"]: d for d in ingest_doc.get("drivers", [])}

    print("| Driver | Status | Latest date | Obs count | Cache written? |")
    print("| --- | --- | --- | ---: | --- |")
    for label, feat in driver_rows:
        if feat in bundle.missing_required or label in bundle.missing_required:
            status = "Missing"
        elif feat in bundle.stale:
            status = "Stale"
        elif feat in bundle.features or (label == "etf_holdings_or_flows" and "etf_flows" in bundle.features):
            status = "Present"
        else:
            status = "Missing"
        lin = bundle.lineage.get(feat, {})
        latest = lin.get("source_date") or "—"
        obs = "—"
        cache_written = "—"
        if label == "central_bank_net_purchases":
            ing = ingest_by_id.get("cb_net_purchases", {})
            obs = str(ing.get("observation_count") or "—")
            cache_written = "Yes" if ing.get("cache_written") else "No"
            if ing.get("latest_date"):
                latest = ing["latest_date"]
        elif label == "etf_holdings_or_flows":
            ing = ingest_by_id.get("gold_etf_holdings", {})
            obs = str(ing.get("observation_count") or "—")
            cache_written = "Yes" if ing.get("cache_written") else "No"
            if ing.get("latest_date"):
                latest = ing["latest_date"]
        elif status == "Present":
            obs = str(bundle.n)
            cache_written = "n/a (FRED)"
        print(f"| {label} | {status} | {latest} | {obs} | {cache_written} |")

    r2 = reg.get("r_squared")
    r2_s = f"{r2:.4f}" if r2 is not None else "—"
    dev = gold.get("deviation_pct")
    dev_s = f"{dev:+.2f}%" if dev is not None else "—"
    publish = "Yes" if gold.get("publish") else "No"
    blocker = gold.get("blocker_reason") or "—"
    print()
    print(f"Model R²: {r2_s}")
    print(f"Valuation %: {dev_s}")
    print(f"Publish?: {publish}")
    print(f"Blocker if no: {blocker}")


def main() -> int:
    ingest_doc = run_phase4e_gold_manual_ingest()
    audit_path = ROOT / "data" / "audits" / "phase4e_gold_manual_ingest.json"
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(json.dumps(ingest_doc, indent=2), encoding="utf-8")
    print(f"Wrote {audit_path}")

    if not ingest_doc.get("ready_for_model"):
        print()
        print("GOLD WITHHELD — manual CB purchase and ETF holdings files required.")
        print()
        print("Required manual files (one format each):")
        print("  data/manual/metals/gold_cb_purchases.csv | .xlsx | .json")
        print("  data/manual/metals/gold_etf_holdings.csv | .xlsx | .json")
        print()
        for d in ingest_doc.get("drivers", []):
            if d.get("status") == "missing":
                print(f"  MISSING: {d.get('blocker_reason')}")
        _gold_status_table(ingest_doc)
        return 1

    subprocess.run([sys.executable, str(ROOT / "scripts" / "phase4b_metals_export.py")], check=True)
    subprocess.run([sys.executable, str(ROOT / "scripts" / "phase4b_metals_report.py")], check=True)
    print()
    _gold_status_table(ingest_doc)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
