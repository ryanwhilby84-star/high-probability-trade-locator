#!/usr/bin/env python3
"""Rebuild WGC CB driver, run Gold model research variants, export diagnostics."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.data_sources.cb_gold_purchases_ingest import ingest_cb_gold_purchases
from hptl.data_sources.metals_driver_ingest import ingest_gold_etf_holdings
from hptl.valuation.export import PUBLIC_OUT, _sanitize_withheld_export_block
from hptl.valuation.gold_model_research import run_gold_model_research, write_research_artifacts
from hptl.valuation.ive_adapter import attach_ive_to_export_block
from hptl.valuation.metals_institutional_fair_value_v1 import compute_metals_institutional_valuation
from hptl.valuation.export import METALS_PILLAR_ENGINE


def _export_gold_block(gold: dict) -> Path:
    generated_at = datetime.now(timezone.utc).isoformat()
    enriched = attach_ive_to_export_block(dict(gold), "Gold", generated_at=generated_at)
    block = _sanitize_withheld_export_block(enriched)

    path = PUBLIC_OUT if PUBLIC_OUT.is_absolute() else ROOT / PUBLIC_OUT
    doc = {}
    if path.exists():
        doc = json.loads(path.read_text(encoding="utf-8"))
    instruments = dict(doc.get("instruments") or {})
    instruments["Gold"] = block
    doc["instruments"] = instruments
    doc["generated_at"] = generated_at
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def main() -> int:
    print("=== Gold valuation research pipeline ===\n")

    print("1. Ingest WGC central-bank gold purchases (Monthly worksheet)...")
    cb = ingest_cb_gold_purchases(write_status=True)
    print(f"   status={cb.status} obs={cb.observation_count} latest={cb.latest_date}")
    print(f"   source={cb.source_id}")
    if cb.status != "ok":
        print(f"   BLOCKED: {cb.blocker_reason}")
        return 1

    print("\n2. Refresh Gold ETF holdings cache...")
    etf = ingest_gold_etf_holdings()
    print(f"   status={etf.status} obs={etf.observation_count} latest={etf.latest_date}")

    print("\n3. Run feature-engineering research variants...")
    report = run_gold_model_research()
    json_path, md_path = write_research_artifacts(report)
    print(f"   Wrote {json_path}")
    print(f"   Wrote {md_path}")
    print(f"   Variants fitted: {report['variants_fitted']}/{report['variants_tested']}")
    print(f"   Publishable: {report['variants_publishable']}")
    print(f"   Recommendation: {report['recommendation']}")

    print("\n4. Re-run production Gold institutional model (baseline spec)...")
    gold = compute_metals_institutional_valuation(market="Gold")
    gold["valuation_pillar"] = METALS_PILLAR_ENGINE
    export_path = _export_gold_block(gold)
    print(f"   Wrote {export_path}")

    publish = gold.get("publish")
    status = gold.get("model_status")
    dev = gold.get("deviation_pct")
    reason = gold.get("valuation_reason") or gold.get("blocker_reason") or "—"
    reg = gold.get("regression") or {}
    print(f"\n=== Production Gold model ===")
    print(f"   Status: {status}")
    print(f"   Publish: {'Yes' if publish else 'No (withhold)'}")
    print(f"   Deviation: {dev:+.2f}%" if dev is not None else "   Deviation: —")
    print(f"   R²: {reg.get('r_squared')}")
    print(f"   Reason: {reason}")

    print("\n=== Variant summary ===")
    print(f"{'Variant':<28} {'Adj R²':>8} {'CB β':>10} {'CB p':>8} {'CB corr':>8} {'Publish':>10}")
    print("-" * 78)
    for v in report.get("variants") or []:
        cb_name = v.get("cb_feature", "")
        cb_row = next((c for c in v.get("coefficients", []) if c.get("feature") == cb_name), {})
        corr = (v.get("correlations_with_log_price") or {}).get(cb_name)
        adj = v.get("adj_r_squared")
        print(
            f"{v.get('variant_id', ''):<28} "
            f"{adj if adj is not None else '—':>8} "
            f"{cb_row.get('beta', '—'):>10} "
            f"{cb_row.get('p_value', '—'):>8} "
            f"{corr if corr is not None else '—':>8} "
            f"{v.get('publish_decision', '—'):>10}"
        )

    return 0 if publish else 1


if __name__ == "__main__":
    raise SystemExit(main())
