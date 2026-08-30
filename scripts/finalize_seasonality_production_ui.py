#!/usr/bin/env python3
"""One-shot idempotent source finalizer for the production seasonality UI.

The GitHub connector can safely add the backend production adapter directly,
but the two React workstation files are large generated/hand-edited sources.
This script applies the deliberately tiny presentation-only edits locally:

* Roadmap defaults to the unsmoothed source.
* Recharts uses straight line segments instead of monotone spline interpolation.
* Two pre-existing typos in the seasonality foundation utility are repaired so
  compile/test discovery cannot be broken by an unrelated syntax error.

Run from the repository root. The script refuses to silently succeed if an
expected source pattern is absent and verifies every target after writing.
"""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _replace(path: Path, old: str, new: str, *, minimum: int = 1, maximum: int | None = None) -> int:
    text = path.read_text(encoding="utf-8")
    count = text.count(old)
    max_allowed = minimum if maximum is None else maximum
    if count < minimum or count > max_allowed:
        raise RuntimeError(
            f"{path.relative_to(ROOT)}: expected {minimum}..{max_allowed} occurrences of {old!r}, found {count}"
        )
    path.write_text(text.replace(old, new), encoding="utf-8")
    return count


def _replace_if_present(path: Path, old: str, new: str) -> int:
    text = path.read_text(encoding="utf-8")
    count = text.count(old)
    if count:
        path.write_text(text.replace(old, new), encoding="utf-8")
    return count


def main() -> int:
    workstation = ROOT / "web-dashboard" / "src" / "seasonality_workstation" / "SeasonalityWorkstation.jsx"
    charts = ROOT / "web-dashboard" / "src" / "seasonality_workstation" / "SeasonalityCharts.jsx"
    foundation = ROOT / "src" / "hptl" / "seasonality" / "seasonality_foundation_rebuild.py"

    changes: list[str] = []

    # Idempotent: only replace the old defaults if they still exist.
    if _replace_if_present(
        workstation,
        "const [roadmapSmoothed, setRoadmapSmoothed] = React.useState(true)",
        "const [roadmapSmoothed, setRoadmapSmoothed] = React.useState(false)",
    ):
        changes.append("Roadmap default -> unsmoothed")
    if _replace_if_present(
        workstation,
        "setRoadmapSmoothed(true)\n  }, [payload?.report_date, payload?.instrument_id, payload?.selected_lookback])",
        "setRoadmapSmoothed(false)\n  }, [payload?.report_date, payload?.instrument_id, payload?.selected_lookback])",
    ):
        changes.append("Payload refresh -> unsmoothed")

    # Straight segments preserve the actual observation-to-observation shape.
    chart_text = charts.read_text(encoding="utf-8")
    monotone_count = chart_text.count('type="monotone"')
    if monotone_count:
        charts.write_text(chart_text.replace('type="monotone"', 'type="linear"'), encoding="utf-8")
        changes.append(f"Recharts monotone -> linear ({monotone_count} lines)")

    # Repair known pre-existing parse typos in the foundation utility.
    if _replace_if_present(
        foundation,
        '"confidence_before": b.get("confidence_level else"),',
        '"confidence_before": b.get("confidence_level"),',
    ):
        changes.append("Foundation confidence typo repaired")
    if _replace_if_present(
        foundation,
        '        farkdown f"- Generated: {payload[\'generated_at\']}",',
        '        f"- Generated: {payload[\'generated_at\']}",',
    ):
        changes.append("Foundation markdown typo repaired")

    # Verify the intended final state, whether edits happened now or earlier.
    wt = workstation.read_text(encoding="utf-8")
    ct = charts.read_text(encoding="utf-8")
    ft = foundation.read_text(encoding="utf-8")
    checks = {
        "roadmap_defaults_raw": "React.useState(false)" in wt and "setRoadmapSmoothed(false)" in wt,
        "no_monotone_chart_interpolation": 'type="monotone"' not in ct,
        "linear_chart_segments_present": 'type="linear"' in ct,
        "foundation_confidence_typo_absent": 'confidence_level else' not in ft,
        "foundation_farkdown_typo_absent": "farkdown f" not in ft,
    }
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise RuntimeError("Finalization verification failed: " + ", ".join(failed))

    print("Seasonality production UI finalization: PASS")
    if changes:
        for change in changes:
            print(f"  changed: {change}")
    else:
        print("  no changes needed (already finalized)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
