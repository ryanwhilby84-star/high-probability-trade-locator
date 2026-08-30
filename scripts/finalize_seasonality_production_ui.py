#!/usr/bin/env python3
"""One-shot idempotent finalizer for the production seasonality rebuild.

Run from the repository root after pulling the production seasonality backend.
It applies the deliberately small source edits that are awkward to make safely
through whole-file GitHub writes on the large workstation sources.

Final state:
* robust Roadmap defaults to unsmoothed weekly observations
* obsolete SMA(5) production toggle is removed
* Recharts uses straight segments, not monotone spline interpolation
* reliability verdict/score is visible beside horizon statistics
* the current-week plotted point keeps the real as-of date on partial weeks
* two pre-existing foundation utility parse typos are repaired
"""
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
    roadmap_py = ROOT / "src" / "hptl" / "seasonality_workstation" / "production_roadmap.py"
    foundation = ROOT / "src" / "hptl" / "seasonality" / "seasonality_foundation_rebuild.py"

    changes: list[str] = []

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
      ? 'Unsmoothed weekly'
      : seasonalView === 'freeze_index'
        ? `SMA(${method.smooth ?? 5})`
        : 'n/a'"""
    if _replace_if_present(workstation, old_active_smooth, new_active_smooth):
        changes.append("Roadmap metadata -> unsmoothed weekly")

    old_toggle = """          {seasonalView === 'roadmap' ? (
            <div className=\"sws-lookbacks\" role=\"group\" aria-label=\"Roadmap smooth\">
              <button
                type=\"button\"
                className={`sws-btn${roadmapSmoothed ? ' is-active' : ''}`}
                onClick={() => setRoadmapSmoothed(true)}
              >
                SMA(5)
              </button>
              <button
                type=\"button\"
                className={`sws-btn${!roadmapSmoothed ? ' is-active' : ''}`}
                onClick={() => setRoadmapSmoothed(false)}
              >
                Unsmoothed
              </button>
            </div>
          ) : null}
"""
    if _replace_if_present(workstation, old_toggle, ""):
        changes.append("Removed obsolete production SMA(5) toggle")

    reliability_anchor = """      <div className=\"sws-stat\">
        <span>Anchor price</span>
        <strong>
          {roadmap?.asof_price != null || roadmap?.anchor_price != null
            ? Number(roadmap.asof_price ?? roadmap.anchor_price).toFixed(3)
            : '—'}
        </strong>
      </div>
"""
    reliability_block = reliability_anchor + """      <div className=\"sws-stat sws-stat-block\">
        <span>Reliability</span>
        <strong>{roadmap?.reliability?.verdict || '—'}</strong>
        <strong>
          score {roadmap?.reliability?.score ?? '—'} · {roadmap?.reliability?.label || '—'}
        </strong>
        {roadmap?.reliability?.reasons?.length ? (
          <span className=\"sws-muted\">{roadmap.reliability.reasons.join(' · ')}</span>
        ) : null}
      </div>
"""
    wt_now = workstation.read_text(encoding="utf-8")
    if "roadmap?.reliability?.verdict" not in wt_now:
        if reliability_anchor not in wt_now:
            raise RuntimeError("Could not locate Roadmap anchor-price block for reliability UI insertion")
        workstation.write_text(wt_now.replace(reliability_anchor, reliability_block, 1), encoding="utf-8")
        changes.append("Reliability verdict/score added to Roadmap panel")

    chart_text = charts.read_text(encoding="utf-8")
    monotone_count = chart_text.count('type="monotone"')
    if monotone_count:
        charts.write_text(chart_text.replace('type="monotone"', 'type="linear"'), encoding="utf-8")
        changes.append(f"Recharts monotone -> linear ({monotone_count} lines)")

    old_date = '                "date": _week_date(iso_year, week),'
    new_date = '                "date": (str(anchor.get("date"))[:10] if week == anchor_week and anchor.get("date") else _week_date(iso_year, week)),'
    if _replace_if_present(roadmap_py, old_date, new_date):
        changes.append("Current-week point keeps exact as-of date")

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

    wt = workstation.read_text(encoding="utf-8")
    ct = charts.read_text(encoding="utf-8")
    rt = roadmap_py.read_text(encoding="utf-8")
    ft = foundation.read_text(encoding="utf-8")
    checks = {
        "roadmap_defaults_raw": "React.useState(false)" in wt and "setRoadmapSmoothed(false)" in wt,
        "production_sma_toggle_removed": 'aria-label="Roadmap smooth"' not in wt,
        "roadmap_metadata_unsmoothed": "'Unsmoothed weekly'" in wt,
        "reliability_visible": "roadmap?.reliability?.verdict" in wt,
        "no_monotone_chart_interpolation": 'type="monotone"' not in ct,
        "linear_chart_segments_present": 'type="linear"' in ct,
        "exact_asof_date_on_current_week": 'week == anchor_week and anchor.get("date")' in rt,
        "foundation_confidence_typo_absent": 'confidence_level else' not in ft,
        "foundation_farkdown_typo_absent": "farkdown f" not in ft,
    }
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise RuntimeError("Finalization verification failed: " + ", ".join(failed))

    print("Seasonality production finalization: PASS")
    if changes:
        for change in changes:
            print(f"  changed: {change}")
    else:
        print("  no changes needed (already finalized)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
