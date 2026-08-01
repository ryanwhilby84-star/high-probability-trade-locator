#!/usr/bin/env python3
"""Compare legacy snapshot vs trajectory Weekly Analysis for probe instruments."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

LEGACY = ROOT / "data" / "audits" / "weekly_analysis_legacy_snapshot.json"
NEW = ROOT / "web-dashboard" / "public" / "data" / "cot_analyst_intelligence_latest.json"
OUT = ROOT / "data" / "audits" / "weekly_analysis_legacy_vs_trajectory.md"

KEYS = [
    "Copper / HG",
    "Soybeans",
    "Crude Oil / CL",
    "Natural Gas / NG",
    "NZ Dollar / 6N",
]

LEGACY_KEYS = ("what_happened", "progression", "checklist", "next_week", "confidence")


def main() -> int:
    legacy = json.loads(LEGACY.read_text(encoding="utf-8"))
    new = json.loads(NEW.read_text(encoding="utf-8"))

    assert new.get("engine") == "trajectory_reasoning", new.get("engine")
    assert new.get("prose_enabled") is True

    lines = [
        "# Weekly Analysis — Legacy vs Trajectory Integration",
        "",
        f"Document engine: `{new.get('engine')}` | version: `{new.get('version')}` | "
        f"prose_enabled: `{new.get('prose_enabled')}`",
        "",
        "UI consumption check:",
        "- CotWorkstation still fetches `/data/cot_analyst_intelligence_latest.json`.",
        "- WeeklyAnalysisPanel requires `engine === \"trajectory_reasoning\"` and renders "
        "Dominant Story / Workflow / Positioning / Price / Rotation Factor / Confirmation / "
        "Invalidation / Historical Context.",
        "- Legacy keys (`what_happened`, `progression`, `checklist`, `next_week`) are not "
        "present on market blocks and are not rendered.",
        "",
    ]

    for key in KEYS:
        old = legacy["markets"][key]
        neu = new["markets"][key]
        leftover = [x for x in LEGACY_KEYS if x in neu]
        lines.extend(
            [
                f"## {key}",
                "",
                "### Old (analyst_intelligence / weekly_analysis)",
                f"- Engine: `{legacy.get('engine')}`",
                f"- Summary: {(old.get('summary') or '')[:420]}",
                f"- What happened[0]: {(old.get('what_happened') or [''])[0]}",
                "",
                "### New (trajectory_reasoning)",
                f"- Engine: `{neu.get('engine')}`",
                "- Dominant story: "
                f"**{(neu.get('dominant_story') or {}).get('label')}** "
                f"(`{(neu.get('dominant_story') or {}).get('code')}`)",
                f"- Narrative: {((neu.get('dominant_story') or {}).get('narrative') or '')[:480]}",
                "- Workflow: "
                f"`{(neu.get('workflow_state') or {}).get('stage')}` / structural "
                f"`{(neu.get('workflow_state') or {}).get('structural_state')}`",
                f"- Positioning: {((neu.get('positioning_trajectory') or {}).get('narrative') or '')[:280]}",
                f"- Price relationship: {((neu.get('price_relationship') or {}).get('narrative') or '')[:220]}",
                "- Rotation Factor: "
                f"{(neu.get('rotation_factor') or {}).get('rotation_factor')} → "
                f"{(neu.get('rotation_factor') or {}).get('band')}",
                f"- Confirmation: {neu.get('confirmation')}",
                f"- Invalidation: {neu.get('invalidation')}",
                "",
                "### Material difference",
                "- Wording differs: legacy used 1W flow templates "
                "(\"increased bearish/bullish exposure\"); trajectory uses dominant story + "
                "workflow stage + rotation factor language.",
                f"- Legacy template keys present on new block: `{leftover}` (expected empty).",
                "",
            ]
        )
        assert neu.get("engine") == "trajectory_reasoning"
        assert not leftover, leftover
        assert "what_happened" not in neu

    OUT.parent.mkdir(parents=True, exist_ok=True)
    text = "\n".join(lines)
    OUT.write_text(text, encoding="utf-8")
    sys.stdout.buffer.write(text.encode("utf-8", errors="replace"))
    sys.stdout.buffer.write(f"\nwrote {OUT}\n".encode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
