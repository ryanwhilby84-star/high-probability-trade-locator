#!/usr/bin/env python3
"""Phase 4B — compare old metals_real_yield_v1 export vs new metal-specific models."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

METALS = ("Gold", "Silver", "Copper / HG", "Platinum", "Palladium")
OLD_EXPORT = ROOT / "data" / "valuation_latest.json"


def _old_pct(doc: dict, market: str) -> str:
    block = (doc.get("instruments") or {}).get(market) or {}
    pct = block.get("engine_deviation_pct")
    if pct is None:
        pct = block.get("deviation_pct")
    model = block.get("model_id") or block.get("model_name") or "—"
    if pct is None:
        return "—", model
    return f"{pct:+.2f}%", model


def main() -> None:
    from hptl.valuation.metals_institutional_fair_value_v1 import compute_metals_institutional_valuation

    old_doc: dict = {}
    if OLD_EXPORT.exists():
        old_doc = json.loads(OLD_EXPORT.read_text(encoding="utf-8"))

    rows: list[tuple[str, str, str, str, str, str, str]] = []
    for market in METALS:
        old_pct, old_model = _old_pct(old_doc, market)
        new = compute_metals_institutional_valuation(market=market)
        reg = new.get("regression") or {}
        r2 = reg.get("r_squared")
        r2_s = f"{r2:.4f}" if r2 is not None else "—"
        new_pct = new.get("deviation_pct")
        new_pct_s = f"{new_pct:+.2f}%" if new_pct is not None else "—"
        publish = "Yes" if new.get("publish") else "No"
        blocker = new.get("blocker_reason") or new.get("withheld_reason") or "—"
        if not new.get("publish"):
            new_pct_s = "WITHHELD"
        rows.append(
            (
                market,
                old_pct,
                new_pct_s,
                str(new.get("model_id") or "—"),
                r2_s,
                publish,
                blocker[:120],
            )
        )

    print("| Metal | Old valuation % | New valuation % | Model | R² | Publish? | Blocker reason |")
    print("| --- | ---: | ---: | --- | ---: | --- | --- |")
    for r in rows:
        print(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} | {r[5]} | {r[6]} |")

    audit = {
        "phase": "4B",
        "metals": [
            {
                "market": r[0],
                "old_valuation_pct": r[1],
                "new_valuation_pct": r[2],
                "model": r[3],
                "r_squared": r[4],
                "publish": r[5] == "Yes",
                "blocker_reason": r[6],
            }
            for r in rows
        ],
    }
    out = ROOT / "data" / "audits" / "phase4b_metals_replacement.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(audit, indent=2), encoding="utf-8")
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
