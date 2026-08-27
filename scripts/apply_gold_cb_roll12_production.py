#!/usr/bin/env python3
"""Promote rolling-12M CB driver, rebuild exports, write diagnostics, verify."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.data_sources.cb_gold_purchases_ingest import ingest_cb_gold_purchases
from hptl.data_sources.metals_driver_ingest import ingest_gold_etf_holdings
from hptl.valuation.export import write_valuation_exports
from hptl.valuation.gold_cb_driver_comparison import run_cb_driver_comparison, write_comparison_artifacts
from hptl.valuation.gold_production_cb_driver import (
    build_production_decision,
    write_production_decision_artifacts,
)
from hptl.valuation.metals_institutional_fair_value_v1 import compute_metals_institutional_valuation
from hptl.valuation.valuation_export_verify import print_verification_report


def main() -> int:
    print("=== Gold production CB driver (rolling 12m) ===\n")

    print("1. Rebuild WGC CB purchases cache...")
    cb = ingest_cb_gold_purchases(write_status=True)
    print(f"   status={cb.status} obs={cb.observation_count} latest={cb.latest_date}")
    if cb.status != "ok":
        print(f"   BLOCKED: {cb.blocker_reason}")
        return 1

    print("\n2. Refresh Gold ETF holdings...")
    etf = ingest_gold_etf_holdings()
    print(f"   status={etf.status} obs={etf.observation_count}")

    print("\n3. Run level vs roll12 comparison...")
    comparison = run_cb_driver_comparison()
    cmp_json, cmp_md = write_comparison_artifacts(comparison)
    print(f"   Wrote {cmp_json}")

    print("\n4. Fit production Gold model (cb_roll12)...")
    gold = compute_metals_institutional_valuation(market="Gold")
    decision = build_production_decision(comparison=comparison, gold_result=gold)
    dec_json, dec_md = write_production_decision_artifacts(decision)
    print(f"   Wrote {dec_json}")
    print(f"   Wrote {dec_md}")
    print(f"   Outcome: {decision['decision']['outcome']}")
    print(f"   Publish: {gold.get('publish')}")
    print(f"   Status: {gold.get('model_status')}")
    print(f"   Reason: {gold.get('valuation_reason') or gold.get('blocker_reason')}")

    reg = gold.get("regression") or {}
    feats = reg.get("features") or {}
    print(f"   R2: {reg.get('r_squared')}  cb_roll12 beta: {feats.get('cb_roll12')}")

    print("\n5. Write valuation / dashboard exports...")
    paths = write_valuation_exports(verbose=True)

    print("\n6. Verify exports...")
    pub = paths.get("public")
    if pub and Path(pub).exists():
        doc = json.loads(Path(pub).read_text(encoding="utf-8"))
        g = (doc.get("instruments") or {}).get("Gold") or {}
        print(f"   Gold export publish={g.get('publish')} deviation_pct={g.get('deviation_pct')}")
        print(f"   engine_deviation_pct={g.get('engine_deviation_pct')}")
        print(f"   model_status={g.get('model_status')}")
        if g.get("publish") and g.get("deviation_pct") is None:
            print("   ERROR: publish=true but deviation_pct null")
            return 1
        if not g.get("publish") and g.get("deviation_pct") is not None:
            print("   ERROR: publish=false but deviation_pct exposed")
            return 1

    rc = print_verification_report()
    return 0 if rc == 0 else rc


if __name__ == "__main__":
    raise SystemExit(main())
