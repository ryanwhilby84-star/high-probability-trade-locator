"""Tests for hptl.validation numeric safety layer."""
from __future__ import annotations

import math
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.validation import (
    ValidationReport,
    safe_float,
    safe_gt,
    safe_gte,
    safe_int,
    safe_is_negative,
    safe_is_positive,
    safe_lt,
    safe_lte,
    safe_numeric,
    validate_field,
    validate_fields,
)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("123", 123.0),
        (123, 123.0),
        (123.4, 123.4),
        (None, None),
        ("N/A", None),
        ("n/a", None),
        ("", None),
        (pd.NA, None),
        (float("nan"), None),
        (float("inf"), None),
        ("not-a-number", None),
        ("  45.5  ", 45.5),
    ],
)
def test_safe_numeric(raw, expected) -> None:
    result = safe_numeric(raw)
    if expected is None:
        assert result is None
    else:
        assert result == pytest.approx(expected)


def test_safe_int_whole_numbers() -> None:
    assert safe_int("42") == 42
    assert safe_int(42.0) == 42
    assert safe_int(42.7) == 42
    assert safe_int(None) is None


def test_safe_comparisons_never_raise() -> None:
    assert safe_gt(None, 0) is False
    assert safe_gt("100", 0) is True
    assert safe_gt(pd.NA, 0) is False
    assert safe_gt("N/A", 0) is False
    assert safe_lt(None, 0) is False
    assert safe_lte("3", 3) is True
    assert safe_gte("3.1", 3) is True


def test_safe_sign_helpers() -> None:
    assert safe_is_positive("5") is True
    assert safe_is_negative("-1") is True
    assert safe_is_positive(None) is False
    assert safe_is_negative("N/A") is False


def test_validate_field_reports_invalid() -> None:
    item = validate_field("weekly_change", "N/A")
    assert item.valid is False
    assert item.reason == "non-numeric"
    assert item.parsed is None


def test_validate_fields_summary() -> None:
    report = validate_fields(
        {
            "net_value": 302002,
            "weekly_change": -56100,
            "four_week_change": "N/A",
        },
        debug=False,
    )
    assert report.total_count == 3
    assert report.valid_count == 2
    assert report.invalid_count == 1
    assert report.invalid_fields[0].field == "four_week_change"
    summary = report.summary_text()
    assert "Fields Checked: 3" in summary
    assert "four_week_change (non-numeric)" in summary


def test_confidence_multiplier() -> None:
    report = ValidationReport(
        fields=[
            validate_field("weekly_change", "N/A"),
            validate_field("net_value", 100),
        ]
    )
    mult = report.confidence_multiplier(critical=["weekly_change"], optional=["four_week_change"])
    assert mult == pytest.approx(0.90)
