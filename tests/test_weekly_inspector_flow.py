"""Weekly Inspector flow — expanding percentiles, PIT safety, direction labels."""

from __future__ import annotations

from hptl.cot.positioning_research_engine import (
    GROUP_COMMERCIAL,
    build_group_state_series,
)
from hptl.cot.weekly_inspector_flow import (
    PCT_CHG_MILD,
    PCT_CHG_STRONG,
    build_weekly_inspector_series,
    classify_direction,
    classify_temperature,
    pack_group_week,
)


def _series(n: int = 120) -> list[dict]:
    from datetime import date, timedelta

    start = date(2018, 1, 2)
    rows = []
    for i in range(n):
        rows.append(
            {
                "date": (start + timedelta(weeks=i)).isoformat(),
                "commercial_net": float(i * 100 - 2000),
                "institutional_net": float(2000 - i * 80),
                "retail_net": float(i * 10 - 200),
                "price": 1.0 + i * 0.01,
            }
        )
    return rows


def test_percentile_present_when_net_exists():
    series = _series(80)
    payload = build_weekly_inspector_series(series)
    assert payload["available"] is True
    weeks = payload["weeks"]
    assert len(weeks) == 80
    for w in weeks:
        c = w["commercial"]
        if c["net"] is not None:
            assert c["percentile"] is not None, w["date"]
            assert c["percentile_observation_count"] >= 1


def test_first_week_percentile_not_unavailable():
    series = _series(10)
    payload = build_weekly_inspector_series(series)
    first = payload["weeks"][0]["commercial"]
    assert first["net"] is not None
    assert first["percentile"] == 50.0  # single-observation convention
    assert first["percentile_observation_count"] == 1


def test_no_lookahead_percentile_unchanged_when_future_removed():
    series = _series(100)
    mid = 40
    full = build_weekly_inspector_series(series)
    trunc = build_weekly_inspector_series(series[: mid + 1])
    assert (
        full["weeks"][mid]["commercial"]["percentile"]
        == trunc["weeks"][mid]["commercial"]["percentile"]
    )
    assert (
        full["weeks"][mid]["noncommercial"]["percentile"]
        == trunc["weeks"][mid]["noncommercial"]["percentile"]
    )
    assert (
        full["weeks"][mid]["cross"]["comm_nc_spread"]
        == trunc["weeks"][mid]["cross"]["comm_nc_spread"]
    )


def test_future_spike_does_not_change_past_percentile():
    series = _series(90)
    mid = 50
    before = build_weekly_inspector_series(series)["weeks"][mid]["commercial"]["percentile"]
    series[-1]["commercial_net"] = 10_000_000
    after = build_weekly_inspector_series(series)["weeks"][mid]["commercial"]["percentile"]
    assert before == after


def test_direction_thresholds():
    assert classify_direction(PCT_CHG_STRONG) == "strongly_increasing"
    assert classify_direction(PCT_CHG_MILD) == "increasing"
    assert classify_direction(0.0) == "stable"
    assert classify_direction(-PCT_CHG_MILD) == "decreasing"
    assert classify_direction(-PCT_CHG_STRONG) == "strongly_decreasing"


def test_temperature_heating_vs_cooling_at_high_percentile():
    heat, label = classify_temperature(92.0, 8.0, 15.0)
    assert heat == "heating_rapidly"
    assert "Deeper into extreme" in label
    cool, clabel = classify_temperature(92.0, -8.0, -12.0)
    assert cool == "cooling_from_extreme"
    assert "Cooling from extreme" in clabel


def test_compact_export_roundtrip_preserves_percentiles():
    from hptl.cot.weekly_inspector_export import (
        compact_market_weeks,
        expand_compact_market,
    )

    series = _series(80)
    full = build_weekly_inspector_series(series)
    compact = compact_market_weeks(full)
    expanded = expand_compact_market(compact)
    assert expanded["week_count"] == 80
    last = expanded["weeks"][-1]
    assert last["commercial"]["percentile"] == full["weeks"][-1]["commercial"]["percentile"]
    assert last["noncommercial"]["percentile"] is not None
    assert last["cross"]["comm_nc_spread"] == full["weeks"][-1]["cross"]["comm_nc_spread"]


def test_group_state_series_first_week_has_percentile():
    series = _series(5)
    states = build_group_state_series(series, GROUP_COMMERCIAL)
    assert states[0]["percentiles"]["long_history"] == 50.0
    packed = pack_group_week(states[0], [r["commercial_net"] for r in series])
    assert packed["percentile"] == 50.0
