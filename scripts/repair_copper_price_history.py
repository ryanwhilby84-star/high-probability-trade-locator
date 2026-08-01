#!/usr/bin/env python3
"""Repair Copper / HG canonical price history.

Root cause: store mixed OANDA XCU_USD $/lb daily bars with Alpha Vantage
COPPER USD/tonne (and HG-scaled) month-start points → 200+ discontinuities.

Fix: replace canonical store with Yahoo HG=F continuous futures (single scale,
pre-stitched continuous). Does not weaken seasonality integrity thresholds.

Usage:
  python scripts/repair_copper_price_history.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.prices.softs_futures_backfill import promote_soft_futures  # noqa: E402
from hptl.seasonality_workstation.indexed_seasonality import (  # noqa: E402
    load_daily_closes_for_seasonality,
)
from hptl.seasonality_workstation.integrity import audit_daily_series  # noqa: E402

OUT = ROOT / "data" / "audits" / "copper_price_repair.json"


def main() -> int:
    before_daily, before_meta = load_daily_closes_for_seasonality("Copper / HG")
    before = audit_daily_series(
        "Copper / HG", before_daily, source=before_meta.get("source")
    )

    promo = promote_soft_futures("Copper / HG")

    after_daily, after_meta = load_daily_closes_for_seasonality("Copper / HG")
    after = audit_daily_series(
        "Copper / HG", after_daily, source=after_meta.get("source")
    )

    # Scale sanity: COMEX HG cents/lb typically 50–600 in recent decades (Yahoo HG=F)
    closes = [c for _, c in after_daily]
    scale_ok = bool(closes) and max(closes) < 2000 and min(closes) > 0.1

    doc = {
        "instrument": "Copper / HG",
        "root_cause": (
            "Mixed-unit store: OANDA XCU_USD $/lb daily interleaved with "
            "Alpha Vantage COPPER tonne / HG-chart month-start points."
        ),
        "fix": "Replace canonical history with Yahoo HG=F continuous futures.",
        "before_integrity": {
            "status": before.get("status"),
            "issues": before.get("issues"),
            "discontinuity_count": before.get("discontinuity_count"),
            "gap_count": before.get("gap_count"),
            "thin_years": before.get("thin_years"),
            "first_date": before.get("first_date"),
            "last_date": before.get("last_date"),
            "bar_count": before.get("bar_count"),
        },
        "promotion": promo,
        "after_integrity": {
            "status": after.get("status"),
            "issues": after.get("issues"),
            "warnings": after.get("warnings"),
            "discontinuity_count": after.get("discontinuity_count"),
            "gap_count": after.get("gap_count"),
            "thin_years": after.get("thin_years"),
            "usable_year_count": after.get("usable_year_count"),
            "first_date": after.get("first_date"),
            "last_date": after.get("last_date"),
            "bar_count": after.get("bar_count"),
            "source": after_meta.get("source"),
        },
        "scale_sanity_ok": scale_ok,
        "close_min": min(closes) if closes else None,
        "close_max": max(closes) if closes else None,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    print(json.dumps(doc, indent=2))
    if after.get("status") != "PASS" or not scale_ok:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
