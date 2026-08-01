"""Weekly Analysis — deterministic directional narrative over existing packs."""

from __future__ import annotations

from hptl.cot.analyst_intelligence import (
    PROGRESSION,
    build_market_analyst_intelligence,
    progression_state,
)


def _group(
    *,
    percentile: float,
    weekly_change: float = 0.0,
    percentile_change_1w: float = 0.0,
    temperature: str = "neutral",
    state_label: str = "Neutral",
    is_extreme: bool = False,
) -> dict:
    return {
        "net": 1000.0,
        "weekly_change": weekly_change,
        "four_week_change": weekly_change * 2,
        "twelve_week_change": weekly_change * 3,
        "percentile": percentile,
        "percentile_change_1w": percentile_change_1w,
        "percentile_change_4w": percentile_change_1w * 2,
        "percentile_change_12w": percentile_change_1w * 3,
        "temperature": temperature,
        "state_label": state_label,
        "is_extreme": is_extreme,
        "direction": "increasing" if weekly_change > 0 else "decreasing",
        "direction_arrow": "▲" if weekly_change > 0 else "▼",
    }


def test_progression_ladder_order():
    assert PROGRESSION[0] == "Neutral"
    assert PROGRESSION[-1] == "Confirmed Rotation"


def test_approaching_extreme_not_auto_excited():
    state = progression_state(_group(percentile=17.0), has_rotation=False)
    assert state == "Approaching Extreme"


def test_extreme_without_rotation_is_not_confirmed():
    state = progression_state(
        _group(percentile=3.0, is_extreme=True, temperature="deepening_extreme"),
        has_rotation=False,
    )
    assert state == "Setup Developing"


def test_rotation_marker_lifts_to_confirmed_when_cooling():
    state = progression_state(
        _group(percentile=9.0, is_extreme=True, temperature="recovering"),
        has_rotation=True,
    )
    assert state == "Confirmed Rotation"


def test_never_hedges_buying_selling_slash():
    weeks = []
    for i, (pct, ch) in enumerate(
        [
            (28.0, -1200),
            (24.0, -1800),
            (20.0, -2200),
            (17.0, -5000),
        ]
    ):
        weeks.append(
            {
                "date": f"2026-06-{10 + i * 7:02d}",
                "commercial": _group(
                    percentile=pct,
                    weekly_change=ch,
                    percentile_change_1w=-4,
                    temperature="heating",
                    state_label="Deeper into extreme",
                ),
                "noncommercial": _group(
                    percentile=70 + i,
                    weekly_change=800,
                    percentile_change_1w=2,
                ),
                "nonreportable": _group(percentile=40.0, weekly_change=50),
                "cross": {"relationship": "opposed", "flow": "opposition_widening"},
            }
        )
    weeks[-1]["commercial"]["is_extreme"] = False
    weeks[-1]["noncommercial"] = _group(
        percentile=79.0,
        weekly_change=2000,
        percentile_change_1w=3,
        temperature="elevated_stable",
        state_label="Elevated / stable",
    )
    weeks[-1]["cross"] = {
        "relationship": "opposed",
        "flow": "opposition_widening",
        "commercial_percentile": 17.0,
        "noncommercial_percentile": 79.0,
    }

    out = build_market_analyst_intelligence(
        "Gold",
        inspector_block={"available": True, "weeks": weeks},
        research_block={
            "available": True,
            "source_week": weeks[-1]["date"],
            "markers": [],
            "current_analogues": {
                "independent_case_count": 14,
                "sample_quality": "MODERATE CONFIDENCE",
                "directional_tendency": "mixed",
                "outcomes_by_horizon": {
                    "12": {
                        "n": 14,
                        "higher_count": 4,
                        "lower_count": 8,
                        "horizon_weeks": 12,
                        "median_return_pct": -1.2,
                    }
                },
            },
        },
    )
    blob = " ".join(
        [
            out.get("summary") or "",
            " ".join(out.get("what_happened") or []),
            " ".join(out.get("interpretation") or []),
            " ".join(out.get("missing_evidence") or []),
            " ".join((out.get("next_week") or {}).get("confirmations_needed") or []),
        ]
    ).lower()

    assert out["available"] is True
    assert out["title"] == "Weekly Analysis"
    assert "buying/selling" not in blob
    assert "narrowing or widening" not in blob
    assert "increasing/decreasing" not in blob
    assert "bullish/bearish" not in blob
    assert "four consecutive weeks" in blob or "4 consecutive weeks" in blob
    assert "fourth consecutive" in blob or "opposition widened" in blob
    assert out["progression"]["commercial"]["state"] == "Approaching Extreme"
    assert "Commercial rotation" in out["missing_evidence"]
    assert out["next_week"]["confirmations_needed"]
    assert any("selling stops" in x.lower() or "positive commercial" in x.lower() for x in out["next_week"]["confirmations_needed"])
    assert any(c["id"] == "rotation_evidence" and c["stars"] == 1 for c in out["confidence"])
    assert "not a forecast" in (out["historical_context"]["outcomes_note"] or "").lower()


def test_opposition_narrowing_is_specific():
    weeks = [
        {
            "date": "2026-07-07",
            "commercial": _group(percentile=12.0, weekly_change=-1000),
            "noncommercial": _group(percentile=80.0, weekly_change=500),
            "nonreportable": _group(percentile=40.0),
            "cross": {"relationship": "strong_opposition", "flow": "opposition_widening"},
        },
        {
            "date": "2026-07-14",
            "commercial": _group(percentile=14.0, weekly_change=2000),
            "noncommercial": _group(percentile=78.0, weekly_change=-400),
            "nonreportable": _group(percentile=41.0),
            "cross": {"relationship": "strong_opposition", "flow": "opposition_narrowing"},
        },
        {
            "date": "2026-07-21",
            "commercial": _group(percentile=16.0, weekly_change=1500, percentile_change_1w=2),
            "noncommercial": _group(percentile=76.0, weekly_change=-300),
            "nonreportable": _group(percentile=42.0),
            "cross": {"relationship": "strong_opposition", "flow": "opposition_narrowing"},
        },
    ]
    out = build_market_analyst_intelligence(
        "Copper / HG",
        inspector_block={"available": True, "weeks": weeks},
        research_block={"available": True, "source_week": "2026-07-21", "markers": []},
    )
    joined = " ".join(out["what_happened"]).lower()
    assert "narrowed" in joined
    assert "buying/selling" not in joined
    assert "second consecutive" in joined or "narrowed this week" in joined or "narrowed for" in joined
