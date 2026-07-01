#!/usr/bin/env python3
"""Close Gold institutional valuation research cycle — archive and final report."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.util.console_safe import configure_stdout_utf8, safe_print as print

configure_stdout_utf8()

from hptl.valuation.gold_institutional_research_cycle import close_gold_institutional_research_cycle


def main() -> int:
    print("=== Close Gold institutional valuation research cycle ===\n")
    closure = close_gold_institutional_research_cycle()
    if closure.get("status") != "closed":
        print(f"ERROR: {closure.get('error')}")
        return 1

    prod = closure.get("production_model") or {}
    be = closure.get("breakeven_experiment") or {}
    arch = closure.get("archive") or {}

    print(f"Cycle: {closure.get('cycle_id')}")
    print(f"Closed: {closure.get('closed_at')}")
    print(f"\nProduction model changed: {prod.get('changed_in_cycle')}")
    print(f"Gold publishable: {prod.get('publishable')}")
    print(f"Gates weakened: {closure.get('validation_gates', {}).get('weakened')}")
    print(f"\nBreakeven experiment: {be.get('result')}")
    print(f"  real_yield sign restored: {be.get('real_yield_sign_restored')}")
    print(f"  breakeven significant: {be.get('breakeven_significant')}")
  print(f"  adj R2 delta: {be.get('adj_r_squared_delta')}")
    print(f"\nArchived: {arch.get('files_archived')} files -> {arch.get('directory')}")
    print(f"\nFinal report: data/processed/gold_institutional_research_cycle_FINAL.md")
    print(f"Audit: data/audits/gold_institutional_research_cycle_audit.json")
    print(f"\nNext phase: {closure.get('next_phase')}")
    print("Gold valuation improvements paused.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
