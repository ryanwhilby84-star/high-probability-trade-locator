"""Universe coverage gate for COT positioning research.

FAILS if the research artifact is a single-instrument or partial subset of
the cot3y series universe (the supported COT workstation chart universe).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.config import PROCESSED_DIR, PROJECT_ROOT

COT3Y_CANDIDATES = (
    PROCESSED_DIR / "cot_3y_series_latest.json",
    PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_3y_series_latest.json",
)
RESEARCH_CANDIDATES = (
    PROCESSED_DIR / "cot_positioning_research_latest.json",
    PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_positioning_research_latest.json",
    PROJECT_ROOT / "data" / "cot_positioning_research_latest.json",
)


def _load_first(paths: tuple[Path, ...]) -> tuple[Path, dict]:
    for p in paths:
        if p.is_file():
            return p, json.loads(p.read_text(encoding="utf-8"))
    raise FileNotFoundError(f"none of {[str(p) for p in paths]}")


def verify() -> dict:
    cot3y_path, cot3y = _load_first(COT3Y_CANDIDATES)
    research_path, research = _load_first(RESEARCH_CANDIDATES)

    source_markets = set((cot3y.get("markets") or {}).keys())
    research_markets = set((research.get("markets") or {}).keys())
    available = {
        k
        for k, v in (research.get("markets") or {}).items()
        if isinstance(v, dict) and v.get("available")
    }
    missing = sorted(source_markets - research_markets)
    extra = sorted(research_markets - source_markets)

    # Every source market must appear in the research doc (available or explicit unavailable).
    coverage_ok = not missing and len(source_markets) > 1
    # Must not be a single-instrument artifact.
    not_singleton = len(research_markets) > 1
    # Production exports must declare full-universe scope.
    scope_ok = research.get("scope") == "full_cot3y_universe"
    # At least half the universe must compute (price/COT gaps allowed as unavailable).
    avail_ratio = (len(available) / len(source_markets)) if source_markets else 0.0
    availability_ok = avail_ratio >= 0.5

    asset_class_probes = [
        "Gold",
        "Crude Oil / CL",
        "Natural Gas / NG",
        "Euro FX / 6E",
        "S&P 500 / ES",
        "Corn",
        "US Dollar Index / DX",
    ]
    present_probes = [m for m in asset_class_probes if m in research_markets]
    available_probes = [m for m in present_probes if m in available]

    passed = coverage_ok and not_singleton and scope_ok and availability_ok

    return {
        "passed": passed,
        "cot3y_path": str(cot3y_path),
        "research_path": str(research_path),
        "source_market_count": len(source_markets),
        "research_market_count": len(research_markets),
        "available_count": len(available),
        "available_ratio": round(avail_ratio, 3),
        "missing_from_research": missing,
        "extra_in_research": extra,
        "scope": research.get("scope"),
        "scope_ok": scope_ok,
        "coverage_ok": coverage_ok,
        "not_singleton": not_singleton,
        "availability_ok": availability_ok,
        "asset_class_probes_present": present_probes,
        "asset_class_probes_available": available_probes,
        "summary": research.get("summary"),
    }


def main() -> int:
    report = verify()
    print(json.dumps(report, indent=2))
    print("PASS" if report["passed"] else "FAIL")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
