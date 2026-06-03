"""Hard COT row integrity validation tests."""

from __future__ import annotations

import pandas as pd

from hptl.cot.data_integrity import (
    MIN_REPORTED_POSITIONS,
    frame_integrity_summary,
    validate_cot_frame,
    validate_row,
)


def test_normal_liquid_row_is_valid():
    res = validate_row(long_value=93934, short_value=11095, net_value=82839)
    assert res.valid
    assert res.reasons == []


def test_zero_short_in_liquid_market_rejected():
    res = validate_row(long_value=25, short_value=0)
    assert not res.valid
    assert "zero_side_in_liquid_market" in res.reasons
    assert "reported_positions_below_market_threshold" in res.reasons


def test_tiny_placeholder_rows_rejected():
    for lv, sv in [(25, 0), (40, 0), (0, 59), (32, 0)]:
        res = validate_row(long_value=lv, short_value=sv)
        assert not res.valid, f"{lv}/{sv} should be invalid"


def test_non_numeric_rejected():
    res = validate_row(long_value="N/A", short_value=None)
    assert not res.valid
    assert "non_numeric_positions" in res.reasons


def test_net_inconsistent_with_long_minus_short_rejected():
    # long-short=10000 but net claims 1 → bad join / column mismatch
    res = validate_row(long_value=60000, short_value=50000, net_value=1)
    assert not res.valid
    assert "net_inconsistent_with_long_minus_short" in res.reasons


def test_wow_collapse_flagged():
    res = validate_row(long_value=2000, short_value=2000, prev_long=80000, prev_short=2000)
    assert not res.valid
    assert "long_collapse_wow_unexplained" in res.reasons


def test_validate_frame_quarantines_aud_placeholder_rows():
    df = pd.DataFrame(
        [
            {"market": "Australian Dollar / 6A", "cot_report_date": "2023-01-03", "long_value": 0, "short_value": 40, "net_value": -40},
            {"market": "Australian Dollar / 6A", "cot_report_date": "2026-05-19", "long_value": 45000, "short_value": 30000, "net_value": 15000},
            {"market": "Gold", "cot_report_date": "2026-05-19", "long_value": 93934, "short_value": 11095, "net_value": 82839},
        ]
    )
    out = validate_cot_frame(df)
    bad = out[~out["cot_valid"].astype(bool)]
    good = out[out["cot_valid"].astype(bool)]
    assert len(bad) == 1
    assert bad.iloc[0]["long_value"] == 0
    assert len(good) == 2

    summary = frame_integrity_summary(out)
    assert summary["invalid_rows"] == 1
    assert summary["valid_rows"] == 2
    assert summary["by_market"]["Gold"]["valid_rows"] == 1
