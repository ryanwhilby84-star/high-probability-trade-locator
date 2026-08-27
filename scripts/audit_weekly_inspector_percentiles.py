"""Audit weekly_inspector percentiles across the full COT universe."""

from __future__ import annotations

import json
from pathlib import Path

from hptl.config import PROJECT_ROOT
from hptl.cot.positioning_research_export import run_positioning_research_export
from hptl.cot.weekly_inspector_export import (
    expand_compact_market,
    run_weekly_inspector_export,
)

PUBLIC_WI = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_weekly_inspector_latest.json"
AUDIT = PROJECT_ROOT / "data" / "audits" / "weekly_inspector_percentile_audit.json"


def main() -> int:
    print("Rebuilding research (lean) + compact weekly_inspector…")
    run_positioning_research_export(markets=None)
    payload = run_weekly_inspector_export(markets=None)

    markets = payload.get("markets") or {}
    total_instruments = 0
    available_instruments = 0
    total_weeks = 0
    weeks_with_net = 0
    weeks_with_percentile = 0
    unexpected_missing = []
    genuine_gaps = []
    failing_markets = []
    sample_latest = {}

    for mid, compact in sorted(markets.items()):
        total_instruments += 1
        block = expand_compact_market(compact)
        if not compact.get("available"):
            failing_markets.append({"market": mid, "reason": "unavailable"})
            continue
        available_instruments += 1
        weeks = block.get("weeks") or []
        total_weeks += len(weeks)
        market_unexpected = 0
        for w in weeks:
            for g in ("commercial", "noncommercial", "nonreportable"):
                pack = w.get(g) or {}
                net = pack.get("net")
                pct = pack.get("percentile")
                if net is None:
                    genuine_gaps.append({"market": mid, "date": w.get("date"), "group": g})
                else:
                    weeks_with_net += 1
                    if pct is None:
                        market_unexpected += 1
                        unexpected_missing.append(
                            {"market": mid, "date": w.get("date"), "group": g, "net": net}
                        )
                    else:
                        weeks_with_percentile += 1
        if weeks:
            last = weeks[-1]
            sample_latest[mid] = {
                "date": last.get("date"),
                "commercial_percentile": (last.get("commercial") or {}).get("percentile"),
                "noncommercial_percentile": (last.get("noncommercial") or {}).get(
                    "percentile"
                ),
                "state": (last.get("commercial") or {}).get("state_label"),
            }
        if market_unexpected:
            failing_markets.append(
                {"market": mid, "reason": f"unexpected_missing_percentiles={market_unexpected}"}
            )

    size = PUBLIC_WI.stat().st_size if PUBLIC_WI.is_file() else 0
    report = {
        "total_instruments_checked": total_instruments,
        "available_instruments": available_instruments,
        "total_cot_weeks_checked": total_weeks,
        "participant_weeks_with_net": weeks_with_net,
        "participant_weeks_with_percentile": weeks_with_percentile,
        "genuine_source_gaps_net_missing": len(genuine_gaps),
        "unexpected_missing_percentile_when_net_exists": len(unexpected_missing),
        "markets_still_failing": failing_markets[:50],
        "unexpected_missing_sample": unexpected_missing[:30],
        "genuine_gaps_sample": genuine_gaps[:20],
        "latest_week_sample": sample_latest,
        "compact_export_bytes": size,
        "public_path": str(PUBLIC_WI),
        "point_in_time_tests": "tests/test_weekly_inspector_flow.py",
        "pass": len(unexpected_missing) == 0
        and available_instruments > 0
        and weeks_with_percentile > 0,
    }
    AUDIT.parent.mkdir(parents=True, exist_ok=True)
    AUDIT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({k: report[k] for k in report if k != "latest_week_sample"}, indent=2))
    print(f"Wrote {AUDIT}")
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
