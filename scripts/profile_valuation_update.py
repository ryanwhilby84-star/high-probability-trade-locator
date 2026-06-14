#!/usr/bin/env python3
"""Profile run_valuation_update stage timings (read-only instrumentation).

Usage:
    python scripts/profile_valuation_update.py              # offline (default)
    python scripts/profile_valuation_update.py --online     # include live cache refresh
"""
from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

OUT_JSON = ROOT / "data" / "audits" / "valuation_update_profile.json"
OUT_MD = ROOT / "data" / "audits" / "valuation_update_profile.md"


@dataclass
class Stage:
    name: str
    duration_s: float = 0.0
    records_processed: int | str = 0
    notes: str = ""


@dataclass
class ProfileReport:
    generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    offline_mode: bool = False
    total_s: float = 0.0
    stages: list[Stage] = field(default_factory=list)

    def add(self, name: str, t0: float, *, records: int | str = 0, notes: str = "") -> None:
        self.stages.append(
            Stage(name=name, duration_s=round(time.monotonic() - t0, 3), records_processed=records, notes=notes)
        )

    def slowest(self) -> Stage | None:
        return max(self.stages, key=lambda s: s.duration_s) if self.stages else None


def _run_stage(report: ProfileReport, name: str, fn: Callable[[], Any], *, records: int | str = 0, notes: str = "") -> Any:
    t0 = time.monotonic()
    result = fn()
    report.add(name, t0, records=records, notes=notes)
    print(f"{report.stages[-1].duration_s:8.3f}s  {name}", flush=True)
    return result


def profile_run_valuation_update(*, offline: bool = True) -> ProfileReport:
    if offline:
        os.environ["HPTL_SKIP_LIVE_FEEDS"] = "1"
    else:
        os.environ.pop("HPTL_SKIP_LIVE_FEEDS", None)

    from hptl.confluence.build_decision_table import TARGET_MARKETS
    from hptl.fx.fx_macro_history import currency_histories, ensure_fx_macro_caches
    from hptl.location.export import build_location_latest, write_location_exports
    from hptl.valuation.export import build_valuation_latest, write_valuation_exports
    from hptl.valuation.fx_carry_real_yield_v3 import FX_V3_PAIRS, build_all_fx_v3_pairs, compute_fx_pair_v3
    from hptl.valuation.fx_v3_audit import run_fx_v3_audit, write_fx_v3_audit_artifacts

    report = ProfileReport(offline_mode=offline)
    t_all = time.monotonic()
    n = len(TARGET_MARKETS)

    loc = _run_stage(report, "build location_latest", build_location_latest, records=n)
    _run_stage(
        report,
        "export location_latest.json (+ dashboard copies)",
        lambda: write_location_exports(loc),
        records=n,
    )

    refresh_notes = "offline: no network" if offline else "BIS/MoF/BoE/CAD fetches when online"
    _run_stage(report, "refresh caches (ensure_fx_macro_caches)", ensure_fx_macro_caches, records=0, notes=refresh_notes)

    hist = _run_stage(report, "load macro history (currency_histories)", currency_histories, records=8, notes="G10 legs")

    t0 = time.monotonic()
    compute_fx_pair_v3("EUR/USD")
    report.add("V3 single pair (currency_histories per call)", t0, records=1, notes="compute_fx_pair_v3 default")
    print(f"{report.stages[-1].duration_s:8.3f}s  {report.stages[-1].name}", flush=True)

    t0 = time.monotonic()
    compute_fx_pair_v3("EUR/USD", histories=hist)
    report.add("V3 single pair (shared histories)", t0, records=1, notes="histories injected")
    print(f"{report.stages[-1].duration_s:8.3f}s  {report.stages[-1].name}", flush=True)

    fx_report = _run_stage(
        report,
        "run FX V3 audit (write_pillar_exports path)",
        lambda: run_fx_v3_audit(refresh_caches=not offline),
        records=len(FX_V3_PAIRS),
        notes="default refresh_caches=True when online",
    )
    _run_stage(
        report,
        "export FX V3 audit files (+ dashboard copies)",
        lambda: write_fx_v3_audit_artifacts(fx_report),
        records=len(fx_report.get("rows") or []),
    )

    report.add("run V2 valuation (fx_institutional_valuation)", time.monotonic(), records=0, notes="NOT INVOKED")

    val = _run_stage(report, "build valuation_latest.json", build_valuation_latest, records=n)
    _run_stage(
        report,
        "export valuation_latest.json (+ dashboard copies)",
        lambda: write_valuation_exports(val),
        records=n,
    )

    _run_stage(report, "duplicate: build location_latest (main pass)", build_location_latest, records=n)
    _run_stage(report, "duplicate: build valuation_latest (main pass)", build_valuation_latest, records=n)

    report.total_s = round(time.monotonic() - t_all, 3)
    return report


def _to_dict(report: ProfileReport) -> dict[str, Any]:
    slow = report.slowest()
    return {
        "generated_at": report.generated_at,
        "offline_mode": report.offline_mode,
        "total_s": report.total_s,
        "slowest_stage": slow.name if slow else None,
        "slowest_duration_s": slow.duration_s if slow else None,
        "stages": [
            {
                "stage": s.name,
                "duration_s": s.duration_s,
                "records_processed": s.records_processed,
                "notes": s.notes,
            }
            for s in report.stages
        ],
    }


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Profile run_valuation_update timings.")
    ap.add_argument("--online", action="store_true", help="Allow live cache refresh (slower).")
    args = ap.parse_args()

    report = profile_run_valuation_update(offline=not args.online)
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(_to_dict(report), indent=2), encoding="utf-8")

    slow = report.slowest()
    print(f"Total: {report.total_s}s | Slowest: {slow.name if slow else 'n/a'} ({slow.duration_s if slow else 0}s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
