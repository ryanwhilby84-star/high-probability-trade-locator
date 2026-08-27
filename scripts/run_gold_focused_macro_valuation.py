"""CLI: Gold focused macro valuation (research only).

Usage:
  python scripts/run_gold_focused_macro_valuation.py
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.gold_focused_macro_valuation import (  # noqa: E402
    run_gold_focused_macro_valuation,
    write_outputs,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Gold focused macro valuation (research only).")
    parser.add_argument("--start", default="2003-01-01")
    args = parser.parse_args(argv)

    t0 = time.perf_counter()
    payload = run_gold_focused_macro_valuation(start=args.start)
    paths = write_outputs(payload)
    elapsed = round(time.perf_counter() - t0, 2)

    print("Gold Focused Macro Valuation (research only)")
    print(f"  ok={payload.get('ok')} runtime_sec={elapsed}")
    panel = payload.get("panel") or {}
    print(f"  panel={panel.get('n_weeks')} {panel.get('start')}..{panel.get('end')}")
    v = payload.get("verdict") or {}
    print(f"  verdict={v.get('verdict')} best={v.get('best_model')}")
    for row in payload.get("ranking") or []:
        print(
            f"    #{row.get('rank')} {row.get('id')}: score={row.get('usefulness_score')} "
            f"signs={(row.get('signs') or {}).get('ok')} "
            f"spread13={(row.get('spread_13w') or {}).get('spread_pp')} "
            f"tip_dev={row.get('tip_deviation_pct')} fv={row.get('tip_fair_value')}"
        )
    narrative = (v.get("narrative") or "").replace("\u2212", "-")
    print(f"  narrative={narrative}")
    for label, path in paths.items():
        print(f"  wrote {label}: {path}")
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
