#!/usr/bin/env python
"""Build/export the cross-market Seasonal Edge Scanner payload."""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

from hptl.seasonality_workstation.seasonal_edge_scanner import build_seasonal_edge_scan


def main() -> int:
    payload = build_seasonal_edge_scan(asof=date.today())
    out = Path("web-dashboard/public/data/seasonal_edge_scan_latest.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps({
        "status": payload.get("status"),
        "output": str(out),
        "instrument_count": payload.get("instrument_count"),
        "available_instruments": payload.get("available_instruments"),
        "edge_count": payload.get("edge_count"),
        "alert_count": payload.get("alert_count"),
    }))
    return 0 if payload.get("status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
