from __future__ import annotations

from datetime import date, timedelta

from hptl.cot.positioning_research_engine import (
    GROUP_COMMERCIAL,
    build_group_state_series,
)
from hptl.cot.weekly_inspector_flow import (
    MEASURE,
    build_weekly_inspector_series,
)


def _series() -> list[dict[str, float | str]]:
    start = date(2022, 1, 4)
    rows: list[dict[str, float | str]] = []

    # Old history is deliberately far above the recent 3Y window so expanding
    # and rolling-3Y ranks materially disagree near the end of the series.
    for i in range(44):
        rows.append(
            {
                "date": str(start + timedelta(days=7 * i)),
                "commercial_net": 1000.0 + i,
                "institutional_net": -(1000.0 + i),
                "retail_net": 0.0,
            }
        )

    recent = list(range(154)) + [154, 120]
    for j, value in enumerate(recent, start=44):
        rows.append(
            {
                "date": str(start + timedelta(days=7 * j)),
                "commercial_net": float(value),
                "institutional_net": -float(value),
                "retail_net": float(value) / 10.0,
            }
        )
    return rows


def test_weekly_inspector_uses_point_in_time_rolling_3y_percentile() -> None:
    series = _series()
    states = build_group_state_series(series, GROUP_COMMERCIAL)
    inspector = build_weekly_inspector_series(series)

    assert inspector["measure"] == MEASURE == "net_positioning_3y_rolling_percentile"

    previous = inspector["weeks"][-2]["commercial"]
    current = inspector["weeks"][-1]["commercial"]
    previous_state = states[-2]
    current_state = states[-1]

    assert previous["percentile"] == previous_state["percentiles"]["3y"]
    assert current["percentile"] == current_state["percentiles"]["3y"]
    assert current["percentile"] != current_state["percentiles"]["long_history"]

    expected_change = round(
        current_state["percentiles"]["3y"] - previous_state["percentiles"]["3y"], 2
    )
    assert current["percentile_change_1w"] == expected_change
    assert current["percentile"] != previous["percentile"]
