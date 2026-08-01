"""Phase 2 turning-point validation tests."""

from __future__ import annotations

from datetime import date, timedelta

from hptl.cot.intelligence_phase2_turning_points import (
    TP_COOLDOWN_WEEKS,
    TP_CONFIRM_WEEKS,
    _cluster_independent,
    detect_turning_points_raw,
    score_positioning_followthrough,
)
from hptl.cot.positioning_research_engine import (
    GROUP_COMMERCIAL,
    MIN_HISTORY,
    build_group_state_series,
)


def _series(n: int = MIN_HISTORY + 160):
    start = date(2016, 1, 5)
    rows = []
    price = 100.0
    for i in range(n):
        phase = (i % 50) / 50.0
        c = -50_000 + phase * 100_000
        nr = 25_000 - phase * 60_000
        nc = -c * 0.55
        price *= 1.0 + ((i % 9) - 4) * 0.0015
        rows.append(
            {
                "date": (start + timedelta(weeks=i)).isoformat(),
                "commercial_net": c,
                "institutional_net": nc,
                "retail_net": nr,
                "price": price,
            }
        )
    return rows


def test_definition_d_timestamps_at_confirmation_not_peak():
    series = _series()
    states = build_group_state_series(series, GROUP_COMMERCIAL)
    raw = detect_turning_points_raw("Test", states, GROUP_COMMERCIAL)
    d_events = [e for e in raw if e["definition"] == "D"]
    assert d_events, "expected some D candidates in oscillating fixture"
    for e in d_events:
        assert e["confirm_index"] == e["onset_index"] + TP_CONFIRM_WEEKS
        assert e["confirm_date"] != e["onset_date"] or TP_CONFIRM_WEEKS == 0
        assert e["detail"]["timestamp_rule"] == "confirmation_week_only"


def test_independence_cooldown_collapses_clusters():
    events = []
    for i in range(0, 40, 2):
        events.append(
            {
                "market": "X",
                "group": "commercial",
                "definition": "B",
                "direction": "bullish",
                "onset_index": i,
                "confirm_index": i,
                "confirm_date": f"2020-01-{i+1:02d}",
                "onset_date": f"2020-01-{i+1:02d}",
            }
        )
    kept = _cluster_independent(events)
    assert len(kept) < len(events)
    for a, b in zip(kept, kept[1:]):
        assert b["confirm_index"] - a["confirm_index"] >= TP_COOLDOWN_WEEKS


def test_positioning_followthrough_keys_present():
    series = _series()
    states = build_group_state_series(series, GROUP_COMMERCIAL)
    raw = detect_turning_points_raw("Test", states, GROUP_COMMERCIAL)
    assert raw
    e = raw[0]
    # pick an event with room for 8w forward
    e = next(x for x in raw if x["confirm_index"] + 8 < len(states))
    score = score_positioning_followthrough(e, states)
    assert "pct_aligned_4w" in score
    assert "false_turn" in score
    assert "whipsaw" in score
    assert "persistent_reversal" in score
