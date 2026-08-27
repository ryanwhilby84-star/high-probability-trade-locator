"""Positioning story score layer tests."""

from __future__ import annotations

from hptl.fx.positioning_story_score import (
    STORY_EARLY_BULLISH,
    STORY_MIXED,
    build_positioning_story,
    build_positioning_story_table_rows,
    compute_currency_positioning_story,
)


def _weeks(nets: list[float], *, group: str = "commercials") -> dict:
    return {
        "groups": {
            "commercials": {
                "weeks": [
                    {
                        "report_date": f"2024-01-{i + 1:02d}",
                        "net": v if group == "commercials" else 0.0,
                    }
                    for i, v in enumerate(nets)
                ]
            },
            "noncommercials": {
                "weeks": [
                    {
                        "report_date": f"2024-01-{i + 1:02d}",
                        "net": v if group == "noncommercials" else 0.0,
                    }
                    for i, v in enumerate(nets)
                ]
            },
        }
    }


def _dual_weeks(commercial_nets: list[float], noncommercial_nets: list[float]) -> dict:
    n = max(len(commercial_nets), len(noncommercial_nets))
    comm = commercial_nets + [commercial_nets[-1]] * (n - len(commercial_nets))
    non = noncommercial_nets + [noncommercial_nets[-1]] * (n - len(noncommercial_nets))
    return {
        "groups": {
            "commercials": {
                "weeks": [
                    {"report_date": f"2024-01-{i + 1:02d}", "net": v}
                    for i, v in enumerate(comm)
                ]
            },
            "noncommercials": {
                "weeks": [
                    {"report_date": f"2024-01-{i + 1:02d}", "net": v}
                    for i, v in enumerate(non)
                ]
            },
        }
    }


def test_bullish_rotation_detects_opposite_group_changes():
    # Commercial net rising from deep negative; non-commercial net falling from high positive.
    comm = [-120_000.0 + float(i) * 3_500.0 for i in range(20)]
    non = [90_000.0 - float(i) * 2_800.0 for i in range(20)]
    row = compute_currency_positioning_story(
        _dual_weeks(comm, non),
        currency="AUD",
        cot_market="Australian Dollar / 6A",
        invert_cot=False,
    )
    assert row["available"] is True
    assert row["commercial_change_score"] > 0
    assert row["noncommercial_change_score"] < 0
    assert row["commercial_noncommercial_rotation_score"] > 20
    assert row["story_state"] in {STORY_EARLY_BULLISH, "Commercial accumulation", "Non-commercial capitulation"}


def test_story_table_sorted_by_abs_score():
    doc = {
        "currencies": {
            "AUD": {"story_score": 72, "available": True},
            "CHF": {"story_score": -18, "available": True},
            "EUR": {"story_score": 5, "available": True},
        }
    }
    rows = build_positioning_story_table_rows(doc)
    assert rows[0]["currency"] == "AUD"
    assert rows[1]["currency"] == "CHF"


def test_build_positioning_story_all_fx_currencies():
    legacy = {
        "instruments": {
            "Australian Dollar / 6A": _dual_weeks([float(i) for i in range(20)], [20.0 - i for i in range(20)]),
            "US Dollar Index / DX": _dual_weeks([float(i) for i in range(20)], [10.0 - i for i in range(20)]),
        }
    }
    doc = build_positioning_story(legacy, calendar_week="2026-01-01")
    assert doc["research_only"] is True
    assert doc["no_trade_signals"] is True
    assert len(doc["currencies"]) == 8
    aud = doc["currencies"]["AUD"]
    assert aud["available"] is True
    assert aud["story_state"] != STORY_MIXED or aud["story_score"] is not None
    assert "buy" not in aud["explanation"].lower()
    assert "sell" not in aud["explanation"].lower()
