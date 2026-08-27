"""Weekly Roadmap reaches the frontend API contract without stdout pollution."""
from __future__ import annotations

import io
import json
import os
import subprocess
import sys
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_cli_stdout_is_pure_json_with_weekly_roadmap() -> None:
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    proc = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "build_seasonality_workstation_payload.py"),
            "NASDAQ / NQ",
            "15Y",
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    # stdout must parse as a single JSON object (no log prefix)
    payload = json.loads(proc.stdout.strip())
    assert payload.get("status") == "ok"
    wr = payload.get("weekly_roadmap")
    assert wr is not None, "canonical key weekly_roadmap missing from API payload"
    assert wr.get("available") is True
    assert len(wr.get("weekly_points") or []) == 52
    assert wr.get("quality_status") in {"valid", "warning"}
    # warnings must not strip points
    if wr.get("quality_status") == "warning":
        assert wr.get("available") is True
        assert len(wr["weekly_points"]) == 52


def test_engine_dev_log_goes_to_stderr_not_stdout() -> None:
    from hptl.seasonality_workstation.engine import build_seasonality_research

    out = io.StringIO()
    err = io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        research = build_seasonality_research("NASDAQ / NQ", lookback="15Y")
    assert research.get("status") == "ok"
    assert "weekly_roadmap" in research
    assert out.getvalue().strip() == ""
    assert "weekly_roadmap" in err.getvalue()


def test_warning_payload_still_has_points_for_chart() -> None:
    from hptl.seasonality_workstation.payload import build_seasonality_workstation_payload

    payload = build_seasonality_workstation_payload("NASDAQ / NQ", lookback="15Y")
    wr = payload["weekly_roadmap"]
    assert wr["available"] is True
    assert len(wr["weekly_points"]) == 52
    # point schema for frontend
    p0 = wr["weekly_points"][0]
    for key in ("week", "average_return", "cumulative_return", "sample_count", "quality_flag"):
        assert key in p0


def test_copper_roadmaps_available_after_price_repair() -> None:
    """Copper / HG must use repaired Yahoo HG=F history (not mixed-unit FAIL)."""
    from hptl.seasonality_workstation.payload import build_seasonality_workstation_payload

    payload = build_seasonality_workstation_payload("Copper / HG", lookback="15Y")
    assert payload.get("status") == "ok"
    wr = payload.get("weekly_roadmap") or {}
    assert wr.get("available") is True
    assert len(wr.get("weekly_points") or []) == 52
    assert wr.get("quality_status") in {"valid", "warning"}
    assert (payload.get("integrity") or {}).get("status") == "PASS"