"""Verify the COT workstation graph data path is consistent end-to-end.

The workstation renders its COT lines from ``/data/cot_3y_series_latest.json``.
That file is published to three physical locations that must agree exactly:

  1. data/processed/cot_3y_series_latest.json                (pipeline source)
  2. web-dashboard/public/data/cot_3y_series_latest.json     (vite dev serves this)
  3. web-dashboard/dist/data/cot_3y_series_latest.json       (vite preview/build)

The frontend loads the file with cache-busting (``?v=<ts>`` + ``cache: no-store``),
so a fresh page load always fetches whichever copy the running server serves. This
check enforces that all three copies carry the same latest report date AND the same
last-three plotted values for a target market, so the workstation cannot silently
render an older week than the processed source.

Usage:
    python scripts/verify_cot_graph_data_path.py
    python scripts/verify_cot_graph_data_path.py --market "Gold"

Exit code 0 = PASS (all copies agree). Exit code 1 = FAIL (a divergence was found).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

COPIES = {
    "processed": ROOT / "data" / "processed" / "cot_3y_series_latest.json",
    "public": ROOT / "web-dashboard" / "public" / "data" / "cot_3y_series_latest.json",
    "dist": ROOT / "web-dashboard" / "dist" / "data" / "cot_3y_series_latest.json",
}

# Fields the workstation actually plots (see rowsToLinePoints in CotWorkstation.jsx).
PLOT_FIELDS = ("institutional_net", "commercial_net", "retail_net")


def _norm(s: str) -> str:
    return " ".join(str(s or "").lower().split())


def resolve_block(doc: dict, market: str):
    """Mirror of web-dashboard marketBlockResolve.resolveMarketBlock (subset)."""
    markets = (doc or {}).get("markets") or {}
    if market in markets:
        return market, markets[market]
    target = _norm(market)
    for key, block in markets.items():
        if _norm(key) == target:
            return key, block
    compact = target.replace("/", "").replace(" ", "")
    for key, block in markets.items():
        if _norm(key).replace("/", "").replace(" ", "") == compact:
            return key, block
        sym = block.get("display_symbol") if isinstance(block, dict) else None
        if sym and _norm(sym).replace("/", "").replace(" ", "") == compact:
            return key, block
    return None, None


def last_rows(block: dict, n: int = 3):
    series = (block or {}).get("series") or (block or {}).get("rows") or []
    tail = series[-n:]
    out = []
    for r in tail:
        out.append(
            {
                "date": r.get("date") or r.get("label"),
                **{f: r.get(f) for f in PLOT_FIELDS},
            }
        )
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--market", default="Crude Oil / CL", help="Market key to verify")
    ap.add_argument("--rows", type=int, default=3, help="How many trailing rows to compare")
    args = ap.parse_args()

    loaded = {}
    for name, path in COPIES.items():
        if not path.exists():
            print(f"FAIL: missing copy [{name}] {path}")
            return 1
        try:
            loaded[name] = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL: cannot parse [{name}] {path}: {exc}")
            return 1

    latest_dates = {}
    tails = {}
    matched_keys = {}
    for name, doc in loaded.items():
        key, block = resolve_block(doc, args.market)
        if not block:
            print(f"FAIL: market {args.market!r} not found in [{name}] copy")
            return 1
        matched_keys[name] = key
        latest_dates[name] = block.get("latest_date")
        tails[name] = last_rows(block, args.rows)

    print(f"Market: {args.market!r}")
    for name in COPIES:
        print(f"  [{name:9}] generated_at={loaded[name].get('generated_at')} "
              f"matched_key={matched_keys[name]!r} latest_date={latest_dates[name]}")

    print(f"\nLast {args.rows} plotted rows ({', '.join(PLOT_FIELDS)}):")
    for name in COPIES:
        print(f"  [{name}]")
        for row in tails[name]:
            vals = " ".join(f"{f}={row[f]}" for f in PLOT_FIELDS)
            print(f"      {row['date']}  {vals}")

    ref = "processed"
    failures = []
    for name in ("public", "dist"):
        if latest_dates[name] != latest_dates[ref]:
            failures.append(
                f"latest_date mismatch: {ref}={latest_dates[ref]} vs {name}={latest_dates[name]}"
            )
        if tails[name] != tails[ref]:
            failures.append(f"trailing {args.rows} rows differ between {ref} and {name}")

    print()
    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        print("\nRESULT: FAIL — published COT graph copies diverge; workstation may render stale data.")
        return 1

    print(f"RESULT: PASS — processed, public, and dist agree "
          f"(latest_date={latest_dates[ref]}, last {args.rows} rows identical).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
