"""Normalize licensed/public long-history daily CSVs into a research-only store.

Usage:
  python scripts/ingest_long_history_csv.py --market "Japanese Yen / 6J" --input path.csv --source cme_datamine --kind native_futures

The script never overwrites production price data. It writes normalized CSV plus
metadata under data/research_long_history/<slug>/ and rejects ambiguous columns.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from datetime import date
from pathlib import Path

OUT_ROOT = Path("data/research_long_history")
DATE_NAMES = ("date", "trade_date", "trading_date", "timestamp")
CLOSE_NAMES = ("settle", "settlement", "close", "last")


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def pick(fieldnames, candidates):
    lookup = {x.lower().strip(): x for x in fieldnames or []}
    for name in candidates:
        if name in lookup:
            return lookup[name]
    return None


def parse_date(raw: str) -> str:
    s = str(raw).strip()[:10]
    return date.fromisoformat(s).isoformat()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--market", required=True)
    p.add_argument("--input", required=True)
    p.add_argument("--source", required=True)
    p.add_argument("--kind", required=True, choices=("native_futures", "spot_proxy", "index_proxy", "predecessor_proxy"))
    p.add_argument("--date-column")
    p.add_argument("--close-column")
    args = p.parse_args()

    src = Path(args.input)
    if not src.exists():
        raise SystemExit(f"input not found: {src}")

    with src.open("rb") as fh:
        digest = hashlib.sha256(fh.read()).hexdigest()

    with src.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        date_col = args.date_column or pick(reader.fieldnames, DATE_NAMES)
        close_col = args.close_column or pick(reader.fieldnames, CLOSE_NAMES)
        if not date_col or not close_col:
            raise SystemExit(f"Could not identify date/close columns. fields={reader.fieldnames}")
        by_date = {}
        rejected = 0
        for row in reader:
            try:
                d = parse_date(row[date_col])
                px = float(str(row[close_col]).replace(",", "").strip())
                if px <= 0:
                    raise ValueError
                by_date[d] = px
            except Exception:
                rejected += 1

    if not by_date:
        raise SystemExit("No usable daily rows")

    out_dir = OUT_ROOT / slugify(args.market)
    out_dir.mkdir(parents=True, exist_ok=True)
    normalized = out_dir / "daily.csv"
    with normalized.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(("date", "close"))
        for d in sorted(by_date):
            writer.writerow((d, f"{by_date[d]:.10g}"))

    years = sorted({int(d[:4]) for d in by_date})
    meta = {
        "market": args.market,
        "source": args.source,
        "series_kind": args.kind,
        "production_safe": False,
        "research_only": True,
        "input_sha256": digest,
        "rows": len(by_date),
        "rejected_rows": rejected,
        "first_date": min(by_date),
        "last_date": max(by_date),
        "calendar_years_present": len(years),
        "normalized_file": str(normalized).replace("\\", "/"),
    }
    (out_dir / "metadata.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(json.dumps(meta, indent=2))

if __name__ == "__main__":
    main()
