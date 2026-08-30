#!/usr/bin/env python3
"""Idempotent UI finalizer for the production DAILY seasonality roadmap."""
from __future__ import annotations
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


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

    if _replace_if_present(workstation, "const [roadmapSmoothed, setRoadmapSmoothed] = React.useState(true)", "const [roadmapSmoothed, setRoadmapSmoothed] = React.useState(false)"):
        changes.append("Roadmap default -> unsmoothed")
    if _replace_if_present(workstation, "setRoadmapSmoothed(true)\n  }, [payload?.report_date, payload?.instrument_id, payload?.selected_lookback])", "setRoadmapSmoothed(false)\n  }, [payload?.report_date, payload?.instrument_id, payload?.selected_lookback])"):
        changes.append("Payload refresh -> unsmoothed")
    if _replace_if_present(workstation, "'Unsmoothed weekly'", "'Unsmoothed daily'"):
        changes.append("Roadmap metadata -> unsmoothed daily")

    old_active_smooth = """  const activeSmooth =
    seasonalView === 'roadmap'
      ? roadmapSmoothed
        ? 'SMA(5)'
        : 'Unsmoothed'
      : seasonalView === 'freeze_index'
        ? `SMA(${method.smooth ?? 5})`
        : 'n/a'"""
    new_active_smooth = """  const activeSmooth =
    seasonalView === 'roadmap'
      ? 'Unsmoothed daily'
      : seasonalView === 'freeze_index'
        ? `SMA(${method.smooth ?? 5})`
        : 'n/a'"""
    if _replace_if_present(workstation, old_active_smooth, new_active_smooth):
        changes.append("Roadmap metadata -> unsmoothed daily")

    old_toggle = """          {seasonalView === 'roadmap' ? (
            <div className=\"sws-lookbacks\" role=\"group\" aria-label=\"Roadmap smooth\">
              <button type=\"button\" className={`sws-btn${roadmapSmoothed ? ' is-active' : ''}`} onClick={() => setRoadmapSmoothed(true)}>SMA(5)</button>
              <button type=\"button\" className={`sws-btn${!roadmapSmoothed ? ' is-active' : ''}`} onClick={() => setRoadmapSmoothed(false)}>Unsmoothed</button>
            </div>
          ) : null}
"""
    _replace_if_present(workstation, old_toggle, "")

    chart_text = charts.read_text(encoding="utf-8")
    if 'type="monotone"' in chart_text:
        count = chart_text.count('type="monotone"')
        charts.write_text(chart_text.replace('type="monotone"', 'type="linear"'), encoding="utf-8")
        changes.append(f"Recharts monotone -> linear ({count})")

    _replace_if_present(foundation, '"confidence_before": b.get("confidence_level else"),', '"confidence_before": b.get("confidence_level"),')
    _replace_if_present(foundation, '        farkdown f"- Generated: {payload[\'generated_at\']}",', '        f"- Generated: {payload[\'generated_at\']}",')

    wt = workstation.read_text(encoding="utf-8")
    ct = charts.read_text(encoding="utf-8")
    checks = {
        "roadmap_defaults_raw": "React.useState(false)" in wt and "setRoadmapSmoothed(false)" in wt,
        "roadmap_metadata_daily": "'Unsmoothed daily'" in wt,
        "no_monotone_chart_interpolation": 'type="monotone"' not in ct,
        "linear_chart_segments_present": 'type="linear"' in ct,
    }
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise RuntimeError("Finalization verification failed: " + ", ".join(failed))
    print("Seasonality DAILY production UI finalization: PASS")
    for change in changes:
        print(f"  changed: {change}")
    if not changes:
        print("  no changes needed (already finalized)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
