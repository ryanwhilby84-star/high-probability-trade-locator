#!/usr/bin/env python3
"""One-command final local validation for the DAILY seasonality rebuild."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "web-dashboard"
BUILD_CHECK = DASHBOARD / ".seasonality-build-check"


def run(label: str, command: list[str], *, cwd: Path = ROOT) -> None:
    print(f"\n=== {label} ===")
    print("$ " + " ".join(command))
    completed = subprocess.run(command, cwd=cwd)
    if completed.returncode != 0:
        raise SystemExit(f"\nFAILED: {label} (exit {completed.returncode})")


def main() -> int:
    py = sys.executable
    npm = "npm.cmd" if os.name == "nt" else "npm"

    run("Finalize production sources", [py, "scripts/finalize_seasonality_production_ui.py"])
    run("Compile seasonality sources", [py, "-m", "compileall", "-q", "src/hptl/seasonality_workstation", "src/hptl/seasonality/seasonality_foundation_rebuild.py", "scripts/audit_production_seasonality.py"])
    run("Python production seasonality tests", [py, "-m", "pytest", "-q", "tests/test_production_seasonality_roadmap.py", "tests/test_seasonality_production_validation.py"])
    run("Seasonality workstation JS tests", ["node", "--test", "src/seasonality_workstation/roadmapView.test.js", "src/seasonality_workstation/weeklyRoadmapContract.test.js"], cwd=DASHBOARD)

    if BUILD_CHECK.exists():
        shutil.rmtree(BUILD_CHECK)
    try:
        run("Dashboard production build", [npm, "run", "build", "--", "--outDir", ".seasonality-build-check", "--emptyOutDir"], cwd=DASHBOARD)
    finally:
        if BUILD_CHECK.exists():
            shutil.rmtree(BUILD_CHECK)

    run("Soybeans DAILY production audit", [py, "scripts/audit_production_seasonality.py", "--instrument", "Soybeans", "--asof", "2026-08-24", "--lookback", "15Y"])

    print("\n" + "=" * 72)
    print("SEASONALITY DAILY REBUILD VALIDATION: PASS")
    print("Robust daily-return plotted path, no smoothing, UI and build all passed.")
    print("The reliability gate may legitimately report NO RELIABLE SEASONAL EDGE;")
    print("that is a model conclusion, not a validation failure.")
    print("=" * 72)
    subprocess.run(["git", "status", "--short"], cwd=ROOT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
