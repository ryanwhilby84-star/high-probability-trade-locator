#!/usr/bin/env python3
"""One-command final local validation for the seasonality rebuild.

This is intentionally the last step before staging/committing local changes.
It applies the small UI source finalizer, compiles the touched Python modules,
runs focused Python + JS tests, builds the dashboard, and audits the Soybeans
2026-08-24 15Y reference case against the exact production model.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "web-dashboard"


def run(label: str, command: list[str], *, cwd: Path = ROOT) -> None:
    print(f"\n=== {label} ===")
    print("$ " + " ".join(command))
    completed = subprocess.run(command, cwd=cwd)
    if completed.returncode != 0:
        raise SystemExit(f"\nFAILED: {label} (exit {completed.returncode})")


def main() -> int:
    py = sys.executable

    run("Finalize production UI", [py, "scripts/finalize_seasonality_production_ui.py"])
    run(
        "Compile seasonality sources",
        [
            py,
            "-m",
            "compileall",
            "-q",
            "src/hptl/seasonality_workstation",
            "src/hptl/seasonality/seasonality_foundation_rebuild.py",
            "scripts/audit_production_seasonality.py",
        ],
    )
    run(
        "Python production seasonality tests",
        [
            py,
            "-m",
            "pytest",
            "-q",
            "tests/test_production_seasonality_roadmap.py",
            "tests/test_seasonality_production_validation.py",
        ],
    )
    run(
        "Seasonality workstation JS tests",
        [
            "node",
            "--test",
            "src/seasonality_workstation/roadmapView.test.js",
            "src/seasonality_workstation/weeklyRoadmapContract.test.js",
        ],
        cwd=DASHBOARD,
    )
    run("Dashboard production build", ["npm", "run", "build"], cwd=DASHBOARD)
    run(
        "Soybeans reference production audit",
        [
            py,
            "scripts/audit_production_seasonality.py",
            "--instrument",
            "Soybeans",
            "--asof",
            "2026-08-24",
            "--lookback",
            "15Y",
        ],
    )

    print("\n" + "=" * 72)
    print("SEASONALITY REBUILD VALIDATION: PASS")
    print("Robust weekly-return production path, OOS validation, UI and build all passed.")
    print("The audit may legitimately report NO RELIABLE SEASONAL EDGE; that is a valid")
    print("model conclusion, not a validation failure, provided the audit itself says PASS.")
    print("=" * 72)

    # Informational only: show exactly what the user is about to commit.
    subprocess.run(["git", "status", "--short"], cwd=ROOT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
