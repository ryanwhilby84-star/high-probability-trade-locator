"""Scan seasonality export for extreme indexed values and dump diagnostics."""
from __future__ import annotations

import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SEA_PATH = ROOT / "web-dashboard/public/data/seasonality_latest.json"
THRESHOLD = 500


def is_num(v):
    return isinstance(v, (int, float)) and v == v


def top_values(vals_with_keys, n=20):
    items = [(k, v) for k, v in vals_with_keys if is_num(v)]
    items.sort(key=lambda x: abs(x[1]), reverse=True)
    return items[:n]


def audit_block(market: str, block: dict) -> dict | None:
    if not block:
        return None
    all_indexed: list[tuple[str, float]] = []

    def add(field: str, v):
        if is_num(v):
            all_indexed.append((field, float(v)))

    for i, r in enumerate(block.get("chart_series") or []):
        if not isinstance(r, dict):
            continue
        w = r.get("week")
        for k in ("actual", "seasonal_10y", "seasonal_5y", "seasonal_3y", "proj_10y", "proj_5y", "proj_3y"):
            add(f"chart_series[w{w}].{k}", r.get(k))

    for r in block.get("current_path") or []:
        if isinstance(r, dict):
            add(f"current_path[w{r.get('week')}].index", r.get("index"))

    for r in block.get("forward_projection") or []:
        if isinstance(r, dict):
            w = r.get("week")
            for k in ("anchor", "proj_10y", "proj_5y", "proj_3y"):
                add(f"forward_projection[w{w}].{k}", r.get(k))

    for hp in block.get("hist_year_paths") or []:
        yr = hp.get("year")
        for p in hp.get("points") or []:
            if isinstance(p, dict):
                add(f"hist_year_paths[{yr}][w{p.get('week')}].index", p.get("index"))

    if not all_indexed:
        return None

    vals = [v for _, v in all_indexed]
    mx = max(vals)
    if mx <= THRESHOLD and min(vals) >= 0:
        return None

    return {
        "market": market,
        "trust_grade": block.get("trust_grade"),
        "canonical_source": block.get("canonical_source"),
        "canonical_symbol": block.get("canonical_symbol"),
        "years_of_history": block.get("years_of_history"),
        "current_week": block.get("current_week"),
        "current_year": block.get("current_year"),
        "latest_price": block.get("latest_price"),
        "min": min(vals),
        "max": mx,
        "median": statistics.median(vals),
        "top20": top_values(all_indexed, 20),
    }


def main():
    doc = json.loads(SEA_PATH.read_text(encoding="utf-8"))
    inst = doc.get("instruments") or {}
    hits = []
    for m, b in inst.items():
        r = audit_block(m, b)
        if r:
            hits.append(r)
    hits.sort(key=lambda x: x["max"], reverse=True)

    print(f"seasonality_latest generated_at: {doc.get('generated_at')}")
    print(f"Instruments with indexed value > {THRESHOLD} or min < 0: {len(hits)}\n")
    for h in hits[:15]:
        print("=" * 72)
        print(h["market"])
        print(
            f"  trust={h['trust_grade']} source={h['canonical_source']} symbol={h['canonical_symbol']}"
        )
        print(f"  history_years={h['years_of_history']} week={h['current_week']} year={h['current_year']}")
        print(f"  min={h['min']:.2f} max={h['max']:.2f} median={h['median']:.2f}")
        print("  top values:")
        for k, v in h["top20"][:10]:
            print(f"    {v:,.2f}  {k}")

    out = ROOT / "data/audits/seasonality_spike_scan.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"hits": hits}, indent=2), encoding="utf-8")
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
