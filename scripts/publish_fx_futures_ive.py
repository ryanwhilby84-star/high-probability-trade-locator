#!/usr/bin/env python3
"""Phase 1G — refresh data, export futures IVE, print per-symbol blockers."""
from __future__ import annotations

import json
import subprocess
import sys

from hptl.config import DATA_DIR, PROJECT_ROOT
from hptl.valuation.currency_futures_ive_v1 import (
    FUTURES_REGISTRY,
    build_currency_futures_ive_export,
    classify_blockers,
    compute_futures_instrument,
    write_currency_futures_ive_export,
)

FX_FUTURES_IDS = [spec.instrument_id for spec in FUTURES_REGISTRY.values()]


def _run(cmd: list[str]) -> None:
    print(f"\n>> {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=False)


def main() -> int:
    _run([sys.executable, "-m", "hptl.fx.ingest_currency_rates"])
    for iid in FX_FUTURES_IDS:
        _run([sys.executable, "scripts/run_price_refresh.py", "--instrument", iid, "--skip-validation"])

    doc = build_currency_futures_ive_export()
    paths = write_currency_futures_ive_export()

    blockers_path = DATA_DIR / "audits" / "fx_futures_publish_blockers.json"
    blockers_path.parent.mkdir(parents=True, exist_ok=True)

    report: dict = {
        "phase": "1G FX Futures Publish",
        "generated_at": doc["generated_at"],
        "symbols": {},
    }

    print("\n=== FX Futures Valuation Status ===")
    for sym, spec in FUTURES_REGISTRY.items():
        block = doc["by_symbol"][sym]
        report["symbols"][sym] = {
            "instrument": spec.instrument_id,
            "model": block.get("model_name"),
            "model_status": block.get("model_status"),
            "wired": block.get("wired"),
            "valuation_pct": block.get("valuation_pct"),
            "valuation_label": block.get("valuation_label"),
            "blocker_codes": block.get("blocker_codes"),
            "blocker_reason": block.get("blocker_reason"),
        }
        if block.get("wired"):
            print(
                f"{sym}: VALIDATED  {block.get('valuation_pct'):+.2f}%  "
                f"{block.get('valuation_label')}  FV={block.get('fair_value')}  spot={block.get('current_price')}"
            )
        else:
            print(f"{sym}: {block.get('blocker_reason')}")

    blockers_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nWrote {paths['public_json']}")
    print(f"Wrote {blockers_path}")
    validated = sum(1 for s in report["symbols"].values() if s["wired"])
    print(f"\nValidated: {validated}/{len(FUTURES_REGISTRY)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
