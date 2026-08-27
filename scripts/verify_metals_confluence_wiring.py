"""Verify metals valuation wiring in valuation_latest + confluence export."""
from __future__ import annotations

import json
import sys
from pathlib import Path

METALS = ["Gold", "Silver", "Copper / HG", "Platinum", "Palladium"]
EXPECTED = {
    "Gold": (50.99, "Overvalued"),
    "Silver": (97.29, "Overvalued"),
    "Copper / HG": (40.06, "Overvalued"),
    "Platinum": (36.14, "Overvalued"),
    "Palladium": (5.85, "Overvalued"),
}


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    failures: list[str] = []

    val = load(root / "data" / "valuation_latest.json")
    summary = val.get("metals_valuation_summary") or {}
    inst = val.get("instruments") or val.get("markets") or {}

    print("=== 1. valuation_latest.json ===")
    print(f"metals_valuation_summary: {summary}")
    if summary.get("wired_count") != 5:
        failures.append("valuation_latest metals wired_count != 5")
    for m in METALS:
        block = inst.get(m) or {}
        wired = block.get("wired")
        dev = block.get("deviation_pct")
        bias = block.get("valuation_bias")
        model = block.get("model_id")
        print(f"  {m}: wired={wired} dev={dev} bias={bias} model={model}")
        if not wired:
            failures.append(f"{m} not wired in valuation_latest")

    conf_path = root / "web-dashboard" / "public" / "data" / "confluence_history_latest.json"
    conf = load(conf_path)
    print("\n=== 2. confluence_history_latest.json ===")
    print(f"generated_at: {conf.get('generated_at')}")

    records = conf.get("records") or []
    latest_date = max(str(r.get("date") or "") for r in records if r.get("date"))
    print(f"latest_week: {latest_date}")

    print("\n=== 3. Latest-week metals confluence rows ===")
    for m in METALS:
        row = next(
            (r for r in records if r.get("market") == m and str(r.get("date")) == latest_date),
            None,
        )
        if not row:
            failures.append(f"{m} missing from latest confluence week")
            print(f"  {m}: MISSING")
            continue
        vw = row.get("valuation_wired")
        dev = row.get("deviation_pct")
        bias = row.get("valuation_bias")
        model = row.get("valuation_model_id")
        print(f"  {m}: valuation_wired={vw} dev={dev} bias={bias} model={model}")
        if vw is not True:
            failures.append(f"{m} latest row valuation_wired={vw}")
        exp_dev, exp_bias = EXPECTED[m]
        if dev is None or abs(float(dev) - exp_dev) >= 0.05:
            failures.append(f"{m} deviation {dev} != {exp_dev}")
        if exp_bias.lower() not in str(bias or "").lower():
            failures.append(f"{m} bias {bias} != {exp_bias}")

    false_count = sum(
        1 for r in records if r.get("market") in METALS and r.get("valuation_wired") is False
    )
    true_count = sum(
        1 for r in records if r.get("market") in METALS and r.get("valuation_wired") is True
    )
    total = sum(1 for r in records if r.get("market") in METALS)
    print(f"\n=== 4. All metals rows ===")
    print(f"total={total} wired_true={true_count} wired_false={false_count}")
    if false_count:
        failures.append(f"{false_count} metals rows still have valuation_wired=false")

    print("\n=== 5. Radar display simulation ===")
    for m in METALS:
        row = next(
            (r for r in records if r.get("market") == m and str(r.get("date")) == latest_date),
            {},
        )
        block = inst.get(m) or {}
        merged_wired = block.get("wired") if block.get("wired") is not None else row.get("valuation_wired")
        merged_bias = block.get("valuation_bias") or row.get("valuation_bias")
        merged_dev = block.get("deviation_pct") if block.get("wired") else row.get("deviation_pct")
        if block.get("wired"):
            merged_dev = block.get("deviation_pct")
            merged_bias = block.get("valuation_bias")
            merged_wired = block.get("wired")
        na = not (
            merged_wired is True
            and merged_bias
            and str(merged_bias).upper() != "UNAVAILABLE"
        )
        exp_dev, exp_bias = EXPECTED[m]
        ok = not na and merged_dev is not None and abs(float(merged_dev) - exp_dev) < 0.05
        status = "Metals N/A" if na else f"+{float(merged_dev):.2f}% {merged_bias}"
        flag = "OK" if ok else "FAIL"
        print(f"  [{flag}] {m}: {status}")
        if na or not ok:
            failures.append(f"radar display fail for {m}: {status}")

    print("\n=== 6. Instrument workstation valuation block ===")
    for m in METALS:
        block = inst.get(m) or {}
        has_context = bool(block.get("wired") and block.get("valuation_bias") and block.get("deviation_pct") is not None)
        print(f"  {m}: workstation_context={'yes' if has_context else 'no'}")
        if not has_context:
            failures.append(f"{m} missing workstation valuation context")

    if failures:
        print("\nFAILURES:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nAll checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
