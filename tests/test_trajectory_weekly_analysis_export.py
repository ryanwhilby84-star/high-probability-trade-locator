"""Weekly Analysis export is driven by trajectory_reasoning only."""

from __future__ import annotations

from datetime import date, timedelta

from hptl.cot.trajectory_reasoning import (
    ENGINE,
    PROSE_ENABLED,
    build_market_trajectory_analysis,
    build_trajectory_weekly_analysis,
    to_weekly_analysis_ui_block,
)


def _week(date_s: str, c_pct: float, nc_pct: float):
    return {
        "date": date_s,
        "commercial": {"net": 0.0, "percentile": c_pct},
        "noncommercial": {"net": 0.0, "percentile": nc_pct},
        "nonreportable": {"net": 0.0, "percentile": 50.0},
        "cross": {"comm_nc_spread": c_pct - nc_pct, "comm_nc_spread_change_4w": 0.0},
    }


def test_prose_enabled_and_engine_constant():
    assert PROSE_ENABLED is True
    assert ENGINE == "trajectory_reasoning"


def test_ui_block_has_trajectory_sections_not_legacy_template_keys():
    d0 = date(2026, 1, 6)
    weeks = []
    for i in range(13):
        c = max(2.0, 8.0 - i * 0.4)
        nc = min(98.0, 90.0 + i * 0.5)
        weeks.append(_week((d0 + timedelta(weeks=i)).isoformat(), c, nc))
    closes = [
        {"date": (d0 + timedelta(weeks=i)).isoformat(), "close": 100.0 + i * 1.2}
        for i in range(13)
    ]
    analysis = build_market_trajectory_analysis("CopperLike", weeks=weeks, weekly_ohlc=closes)
    block = to_weekly_analysis_ui_block(analysis)
    assert block["engine"] == "trajectory_reasoning"
    assert block["prose_enabled"] is True
    assert "dominant_story" in block
    assert "workflow_state" in block
    assert "positioning_trajectory" in block
    assert "price_relationship" in block
    assert "rotation_factor" in block
    assert "confirmation" in block
    assert "invalidation" in block
    assert "historical_context" in block
    # Must not emit legacy template section payloads as the primary narrative shape
    assert "what_happened" not in block
    assert "progression" not in block
    assert "checklist" not in block
    assert "Mature opposition" in block["dominant_story"]["narrative"] or block[
        "dominant_story"
    ]["code"] == "MATURE_OPPOSITION_ROTATION_WATCH"


def test_document_builder_marks_engine():
    d0 = date(2026, 1, 6)
    weeks = [_week((d0 + timedelta(weeks=i)).isoformat(), 95.0 - i, 5.0 + i) for i in range(8)]
    doc = build_trajectory_weekly_analysis(
        weekly_inspector={
            "markets": {
                "Demo": {
                    "available": True,
                    "weeks": weeks,
                }
            }
        },
        workstation_ohlc={"instruments": {}},
        positioning_research={"markets": {}},
    )
    assert doc["engine"] == "trajectory_reasoning"
    assert doc["prose_enabled"] is True
    assert doc["markets"]["Demo"]["engine"] == "trajectory_reasoning"
