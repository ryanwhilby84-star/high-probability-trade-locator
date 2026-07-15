"""Verify crop workstation OHLC is current and consistent across publish copies.

The workstation candles for Corn / Sugar / Wheat / Soybeans come from
``workstation_ohlc_latest.json`` (built by hptl.prices.workstation_ohlc_export).
That file is published to three physical locations that must agree, and the
running dashboard fetches the *public* copy with cache-busting:

  1. data/processed/cot ... /workstation_ohlc_latest.json          (pipeline source)
  2. web-dashboard/public/data/workstation_ohlc_latest.json         (vite dev serves)
  3. web-dashboard/dist/data/workstation_ohlc_latest.json           (vite preview/build)

This check FAILS (exit 1) unless, for every crop:
  - the market exists in all three copies,
  - ``ohlc_last_date`` is identical across processed / public / dist, and
  - the latest completed OHLC is not stale (age <= MAX_AGE_DAYS).

It never marks stale data as fresh: if a provider was rate-limited and the store
kept last-known-good bars, the age check fails loudly.

Usage:
    python scripts/verify_crop_ohlc_freshness.py
    python scripts/verify_crop_ohlc_freshness.py --max-age-days 10
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

COPIES = {
    "processed": ROOT / "data" / "processed" / "workstation_ohlc_latest.json",
    "public": ROOT / "web-dashboard" / "public" / "data" / "workstation_ohlc_latest.json",
    "dist": ROOT / "web-dashboard" / "dist" / "data" / "workstation_ohlc_latest.json",
}

CROPS = ["Corn", "Sugar", "Wheat", "Soybeans"]

# Matches MAX_COMPLETED_OHLC_AGE_DAYS in workstation_ohlc_export.py.
DEFAULT_MAX_AGE_DAYS = 10


def _age_days(date_str: str | None) -> int | None:
    if not date_str:
        return None
    try:
        d = datetime.fromisoformat(str(date_str)[:10]).date()
    except ValueError:
        return None
    return max(0, (datetime.now(timezone.utc).date() - d).days)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--max-age-days", type=int, default=DEFAULT_MAX_AGE_DAYS)
    args = ap.parse_args()

    loaded: dict[str, dict] = {}
    for name, path in COPIES.items():
        if not path.exists():
            print(f"FAIL: missing copy [{name}] {path}")
            return 1
        try:
            loaded[name] = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL: cannot parse [{name}] {path}: {exc}")
            return 1

    failures: list[str] = []
    print(f"Max allowed OHLC age: {args.max_age_days} days\n")

    for crop in CROPS:
        blocks: dict[str, dict] = {}
        for name, doc in loaded.items():
            block = (doc.get("instruments") or {}).get(crop)
            if block is None:
                failures.append(f"{crop}: missing in [{name}] copy")
                continue
            blocks[name] = block

        if len(blocks) != len(COPIES):
            print(f"{crop}: INCOMPLETE — present in {sorted(blocks)}\n")
            continue

        dates = {name: blk.get("ohlc_last_date") for name, blk in blocks.items()}
        ref = blocks["processed"]
        provider = ref.get("canonical_source") or ref.get("price_source")
        symbol = ref.get("canonical_symbol")
        rows = ref.get("ohlc_rows")
        status = (ref.get("price_quality") or {}).get("status")
        age = _age_days(dates["processed"])

        print(f"{crop}")
        print(f"  provider={provider}  symbol={symbol}  weekly_bars={rows}  status={status}")
        for name in COPIES:
            print(f"    [{name:9}] ohlc_last_date={dates[name]}")

        if len(set(dates.values())) != 1:
            failures.append(
                f"{crop}: ohlc_last_date differ across copies -> {dates}"
            )
        if age is None:
            failures.append(f"{crop}: no parseable ohlc_last_date ({dates['processed']})")
        elif age > args.max_age_days:
            failures.append(f"{crop}: STALE — latest OHLC {dates['processed']} is {age} days old")
        print()

    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        print("\nRESULT: FAIL — crop workstation OHLC is stale or inconsistent.")
        return 1

    print("RESULT: PASS — Corn/Sugar/Wheat/Soybeans OHLC current and identical "
          "across processed/public/dist.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
