"""Live recompute seasonality blocks and find indexed spikes at each pipeline stage."""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.seasonality.seasonality_engine import (  # noqa: E402
    avg_path,
    build_hist_year_paths,
    normalized_year_path,
    project_forward,
    year_week_closes,
)
from hptl.seasonality.seasonality_price_bars import weekly_closes_for_instrument  # noqa: E402
from hptl.seasonality.seasonality_trust import attach_trust_metadata  # noqa: E402
from hptl.seasonality.seasonality_engine import compute_seasonality_price_block  # noqa: E402

RADAR = [
    "NASDAQ / NQ", "S&P 500 / ES", "Dow / YM", "Euro FX / 6E", "British Pound / 6B",
    "Japanese Yen / 6J", "Swiss Franc / 6S", "Australian Dollar / 6A", "Canadian Dollar / 6C",
    "NZ Dollar / 6N", "Gold", "Silver", "Copper / HG", "Platinum", "Palladium",
    "Crude Oil / CL", "Natural Gas / NG", "Coffee", "Cocoa", "Corn", "Wheat",
    "Soybeans", "Sugar", "Bitcoin", "US Dollar Index / DX",
]


def stats(vals):
    if not vals:
        return None
    return {
        "min": min(vals),
        "max": max(vals),
        "median": statistics.median(vals),
        "count": len(vals),
    }


def audit_instrument(market: str) -> dict | None:
    bars, bar_source, tl = weekly_closes_for_instrument(market)
    if not bars:
        return {"market": market, "error": "no bars"}

    raw_closes = [c for _, c in bars]
    yw = year_week_closes(bars)

    # Stage: per-year normalized paths
    year_max = {}
    tiny_bases = []
    for y, wc in yw.items():
        base_w = 1 if 1 in wc else min(wc.keys()) if wc else None
        base = wc.get(base_w) if base_w else None
        path = normalized_year_path(wc)
        if path:
            year_max[y] = max(path.values())
        if base is not None and base < 1.0:
            tiny_bases.append({"year": y, "base_week": base_w, "base_close": base, "path_max": max(path.values()) if path else None})

    block = compute_seasonality_price_block(
        market,
        bars,
        price_store_key=market,
        bar_source=bar_source,
    )
    block = attach_trust_metadata(block, bars)

    indexed_vals = []
    for r in block.get("chart_series") or []:
        for k in ("actual", "seasonal_10y", "seasonal_5y", "seasonal_3y", "proj_10y", "proj_5y", "proj_3y"):
            v = r.get(k)
            if isinstance(v, (int, float)):
                indexed_vals.append((k, v, r.get("week")))

    close_vals = [r.get("close") for r in block.get("chart_series") or [] if isinstance(r.get("close"), (int, float))]

    mx_idx = max((v for _, v, _ in indexed_vals), default=0)
    if mx_idx < 500 and max(raw_closes, default=0) < 50000:
        return None

    top = sorted(indexed_vals, key=lambda x: abs(x[1]), reverse=True)[:20]

    return {
        "market": market,
        "symbol": block.get("canonical_symbol"),
        "source": block.get("canonical_source"),
        "bar_source": bar_source,
        "earliest": bars[0][0],
        "latest": bars[-1][0],
        "trust_grade": block.get("trust_grade"),
        "raw_close_stats": stats(raw_closes),
        "chart_close_stats": stats(close_vals),
        "indexed_stats": stats([v for _, v, _ in indexed_vals]),
        "year_path_max": dict(sorted(year_max.items())[-5:]),
        "tiny_bases": tiny_bases[:10],
        "top20_indexed": [{"field": f, "week": w, "value": v} for f, v, w in top],
        "anchor_index": block.get("latest_price", {}).get("index"),
    }


def main():
    hits = []
    for m in RADAR:
        r = audit_instrument(m)
        if r:
            hits.append(r)
    hits.sort(key=lambda x: (x.get("indexed_stats") or {}).get("max", 0), reverse=True)

    print(f"Live audit: {len(hits)} instruments with anomalies\n")
    for h in hits:
        print("=" * 72)
        print(h["market"])
        print(f"  symbol={h.get('symbol')} source={h.get('source')} bar_source={h.get('bar_source')}")
        print(f"  dates={h.get('earliest')} -> {h.get('latest')} trust={h.get('trust_grade')}")
        print(f"  raw_close: {h.get('raw_close_stats')}")
        print(f"  indexed: {h.get('indexed_stats')}")
        if h.get("tiny_bases"):
            print(f"  tiny_bases: {h['tiny_bases'][:3]}")
        print("  top indexed:")
        for t in h.get("top20_indexed", [])[:8]:
            print(f"    {t['value']:,.2f}  {t['field']} w{t['week']}")

    out = ROOT / "data/audits/seasonality_live_spike_audit.json"
    out.write_text(json.dumps({"hits": hits}, indent=2), encoding="utf-8")
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
