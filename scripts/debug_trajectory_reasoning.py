#!/usr/bin/env python3
"""Structured debug for trajectory-aware Weekly Analysis classifications."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.cot.trajectory_reasoning import (  # noqa: E402
    PROSE_ENABLED,
    build_market_trajectory_analysis,
)
from hptl.cot.weekly_inspector_export import expand_compact_market  # noqa: E402

WI_PATH = ROOT / "web-dashboard" / "public" / "data" / "cot_weekly_inspector_latest.json"
OHLC_PATH = ROOT / "web-dashboard" / "public" / "data" / "workstation_ohlc_latest.json"
OUT_JSON = ROOT / "data" / "audits" / "trajectory_reasoning_debug.json"
OUT_MD = ROOT / "data" / "audits" / "trajectory_reasoning_debug.md"

PROBES = [
    {"instrument": "Copper / HG", "report_date": None, "label": "Copper latest"},
    {"instrument": "Soybeans", "report_date": None, "label": "Soybeans latest"},
    {"instrument": "Natural Gas / NG", "report_date": None, "label": "Natural Gas latest"},
    {"instrument": "Crude Oil / CL", "report_date": None, "label": "Crude Oil latest"},
    {
        "instrument": "NZ Dollar / 6N",
        "report_date": "2017-06-06",
        "label": "NZ Dollar historical extreme/rotation example",
    },
]


def _load() -> tuple[dict, dict]:
    wi = json.loads(WI_PATH.read_text(encoding="utf-8"))
    ohlc = json.loads(OHLC_PATH.read_text(encoding="utf-8"))
    return wi, ohlc


def _compact(analysis: dict) -> dict:
    p = analysis.get("participants") or {}
    c = p.get("commercial") or {}
    nc = p.get("non_commercial") or {}
    nr = p.get("non_reportable") or {}
    price = analysis.get("price_trajectory") or {}
    rf = analysis.get("rotation_factor") or {}
    return {
        "label": analysis.get("_label"),
        "instrument_id": analysis.get("instrument_id"),
        "report_date": analysis.get("report_date"),
        "prose_enabled": analysis.get("prose_enabled"),
        "commercial": {
            "percentile": c.get("percentile"),
            "percentile_1w_ago": c.get("percentile_1w_ago"),
            "percentile_4w_ago": c.get("percentile_4w_ago"),
            "percentile_12w_ago": c.get("percentile_12w_ago"),
            "latest_bullish_extreme": c.get("latest_bullish_extreme"),
            "latest_bearish_extreme": c.get("latest_bearish_extreme"),
            "weeks_since_bullish_extreme": c.get("weeks_since_bullish_extreme"),
            "weeks_since_bearish_extreme": c.get("weeks_since_bearish_extreme"),
            "percentile_distance_from_bullish_extreme": c.get(
                "percentile_distance_from_bullish_extreme"
            ),
            "percentile_distance_from_bearish_extreme": c.get(
                "percentile_distance_from_bearish_extreme"
            ),
            "consecutive_weeks_current_direction": c.get("consecutive_weeks_current_direction"),
            "velocity_1w_percentile_pts": c.get("velocity_1w_percentile_pts"),
            "acceleration": c.get("acceleration"),
            "phase": c.get("phase"),
            "classification": c.get("classification"),
        },
        "non_commercial": {
            "percentile": nc.get("percentile"),
            "percentile_4w_ago": nc.get("percentile_4w_ago"),
            "percentile_12w_ago": nc.get("percentile_12w_ago"),
            "consecutive_weeks_current_direction": nc.get("consecutive_weeks_current_direction"),
            "classification": nc.get("classification"),
        },
        "non_reportable": {
            "percentile": nr.get("percentile"),
            "classification": nr.get("classification"),
        },
        "price_trajectory": {
            "classification": price.get("classification"),
            "return_1w_pct": price.get("return_1w_pct"),
            "return_4w_pct": price.get("return_4w_pct"),
            "return_12w_pct": price.get("return_12w_pct"),
            "near_12w_high": price.get("near_12w_high"),
            "near_12w_low": price.get("near_12w_low"),
            "structure_break_up": price.get("structure_break_up"),
            "structure_break_down": price.get("structure_break_down"),
            "price_state": price.get("price_state"),
        },
        "positioning_price_relationship": analysis.get("positioning_price_relationship"),
        "cross_group": {
            "classification": (analysis.get("cross_group") or {}).get("classification"),
            "trajectories_oppose": (analysis.get("cross_group") or {}).get("trajectories_oppose"),
            "opposition_widening": (analysis.get("cross_group") or {}).get("opposition_widening"),
            "opposition_narrowing": (analysis.get("cross_group") or {}).get("opposition_narrowing"),
            "crowded": (analysis.get("cross_group") or {}).get("crowded"),
            "leading_group": (analysis.get("cross_group") or {}).get("leading_group"),
        },
        "rotation_factor": {
            "rotation_factor": rf.get("rotation_factor"),
            "classification": rf.get("classification"),
            "components": rf.get("components"),
            "component_notes": rf.get("component_notes"),
            "guards": rf.get("guards"),
        },
        "workflow": analysis.get("workflow"),
        "dominant_story": analysis.get("dominant_story"),
        "confirmation": analysis.get("confirmation"),
        "invalidation": analysis.get("invalidation"),
        "next_development": analysis.get("next_development"),
        "rules_fired": analysis.get("rules_fired"),
    }


def _md(rows: list[dict]) -> str:
    lines = [
        "# Trajectory Reasoning Debug",
        "",
        f"PROSE_ENABLED: **{PROSE_ENABLED}**",
        "",
    ]
    for r in rows:
        lines.append(f"## {r['label']}")
        lines.append("")
        lines.append(f"- Instrument: `{r['instrument_id']}`")
        lines.append(f"- Report date: `{r['report_date']}`")
        c = r["commercial"]
        nc = r["non_commercial"]
        lines.append(
            f"- Commercial: pct `{c.get('percentile')}` | "
            f"4W ago `{c.get('percentile_4w_ago')}` | 12W ago `{c.get('percentile_12w_ago')}` | "
            f"**{c.get('classification')}** | streak `{c.get('consecutive_weeks_current_direction')}`"
        )
        if c.get("latest_bullish_extreme"):
            lines.append(
                f"- Latest C bullish extreme: `{c['latest_bullish_extreme']}` "
                f"(weeks since `{c.get('weeks_since_bullish_extreme')}`, "
                f"Δpct `{c.get('percentile_distance_from_bullish_extreme')}`)"
            )
        if c.get("latest_bearish_extreme"):
            lines.append(
                f"- Latest C bearish extreme: `{c['latest_bearish_extreme']}` "
                f"(weeks since `{c.get('weeks_since_bearish_extreme')}`, "
                f"Δpct `{c.get('percentile_distance_from_bearish_extreme')}`)"
            )
        lines.append(
            f"- Non-commercial: pct `{nc.get('percentile')}` | "
            f"**{nc.get('classification')}** | streak `{nc.get('consecutive_weeks_current_direction')}`"
        )
        px = r["price_trajectory"]
        lines.append(
            f"- Price: **{px.get('classification')}** | "
            f"1W `{px.get('return_1w_pct')}` | 4W `{px.get('return_4w_pct')}` | "
            f"near_high `{px.get('near_12w_high')}` | near_low `{px.get('near_12w_low')}`"
        )
        pp = r.get("positioning_price_relationship") or {}
        lines.append(f"- Positioning–price: **{pp.get('classification')}**")
        cg = r["cross_group"]
        lines.append(f"- Cross-group: **{cg.get('classification')}** | crowded `{cg.get('crowded')}`")
        rf = r["rotation_factor"]
        lines.append(
            f"- Rotation Factor: **{rf.get('rotation_factor')}** → `{rf.get('classification')}`"
        )
        lines.append(f"- Components: `{json.dumps(rf.get('components'), sort_keys=True)}`")
        lines.append(f"- Component notes: `{json.dumps(rf.get('component_notes'), sort_keys=True)}`")
        ds = r.get("dominant_story") or {}
        wf = r.get("workflow") or ds
        lines.append(
            f"- Workflow: **{wf.get('workflow_stage') or ds.get('workflow_stage')}** | "
            f"structural `{wf.get('structural_state') or ds.get('structural_state')}`"
        )
        lines.append(
            f"- Dominant story: **{ds.get('dominant_story')}** | phase `{ds.get('phase')}`"
        )
        lines.append(f"- Confirmation: {r.get('confirmation')}")
        lines.append(f"- Invalidation: {r.get('invalidation')}")
        lines.append(f"- Next development: {r.get('next_development')}")
        lines.append(f"- Rules fired ({len(r.get('rules_fired') or [])}):")
        for rule in r.get("rules_fired") or []:
            lines.append(f"  - `{rule}`")
        lines.append("")
    lines.append("## Status")
    lines.append("")
    lines.append(f"PROSE_ENABLED: {PROSE_ENABLED}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    wi, ohlc_doc = _load()
    markets = wi.get("markets") or {}
    instruments = ohlc_doc.get("instruments") or {}
    full: list[dict] = []
    compact: list[dict] = []

    for probe in PROBES:
        mid = probe["instrument"]
        block = markets.get(mid)
        if not block:
            compact.append(
                {
                    "label": probe["label"],
                    "instrument_id": mid,
                    "available": False,
                    "reason": "missing_inspector_market",
                }
            )
            continue
        weeks = expand_compact_market(block).get("weeks") or []
        bars = (instruments.get(mid) or {}).get("weekly_ohlc") or []
        analysis = build_market_trajectory_analysis(
            mid,
            weeks=weeks,
            weekly_ohlc=bars,
            report_date=probe.get("report_date"),
        )
        analysis["_label"] = probe["label"]
        full.append(analysis)
        compact.append(_compact(analysis))
        wf = analysis.get("workflow") or {}
        print(
            f"{probe['label']}: date={analysis.get('report_date')} "
            f"C={analysis['participants']['commercial']['classification']} "
            f"NC={analysis['participants']['non_commercial']['classification']} "
            f"PX={analysis['price_trajectory'].get('classification')} "
            f"PP={analysis['positioning_price_relationship'].get('classification')} "
            f"RF={analysis['rotation_factor']['rotation_factor']} "
            f"({analysis['rotation_factor']['classification']}) "
            f"stage={wf.get('workflow_stage')}/{wf.get('structural_state')} "
            f"story={analysis['dominant_story']['dominant_story']}"
        )

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(
        json.dumps({"prose_enabled": PROSE_ENABLED, "probes": full, "summary": compact}, indent=2),
        encoding="utf-8",
    )
    OUT_MD.write_text(_md(compact), encoding="utf-8")
    print(f"\nWrote {OUT_MD}")
    print(f"PROSE_ENABLED: {PROSE_ENABLED}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
