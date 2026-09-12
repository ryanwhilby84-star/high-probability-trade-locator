#!/usr/bin/env python
"""Export annual high/low timing analysis for the dashboard seasonality universe."""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.markets.instrument_registry import LEGACY_COT_MARKETS
from hptl.seasonality_workstation.annual_turning_points import build_annual_turning_points
from hptl.seasonality_workstation.integrity import audit_daily_series_for_lookback
from hptl.seasonality_workstation.returns import load_daily_closes


def main() -> int:
    rows = []
    for instrument_id in LEGACY_COT_MARKETS:
        daily, source, error = load_daily_closes(instrument_id)
        if error or not daily:
            rows.append({"instrument_id": instrument_id, "status": "unavailable", "error": error or "no_daily_history"})
            continue

        integrity = audit_daily_series_for_lookback(
            instrument_id,
            daily,
            source=source,
            lookback_years=15,
            asof=daily[-1][0],
        )
        if integrity.get("status") != "PASS":
            rows.append({
                "instrument_id": instrument_id,
                "status": "integrity_failed",
                "integrity": integrity,
            })
            continue

        result = build_annual_turning_points(daily, lookback_years=15, asof=daily[-1][0])
        rows.append({
            "instrument_id": instrument_id,
            "status": "ok" if result.get("available") else "unavailable",
            "source": source,
            "turning_points": result,
        })

    payload = {
        "status": "ok",
        "engine": "annual_turning_points_v1",
        "asof": date.today().isoformat(),
        "instrument_count": len(LEGACY_COT_MARKETS),
        "available_instruments": sum(1 for r in rows if r.get("status") == "ok"),
        "instrument_results": rows,
    }
    out = ROOT / "web-dashboard" / "public" / "data" / "annual_turning_points_latest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps({"status": "ok", "output": str(out), "available_instruments": payload["available_instruments"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
