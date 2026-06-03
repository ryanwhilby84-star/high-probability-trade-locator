"""Numeric safety, safe comparisons, and validation reporting for scoring pipelines.

All helpers are fault-tolerant: they never raise on bad inputs and return conservative
defaults (None / False) when comparison or coercion is impossible.
"""
from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Debug mode
# ---------------------------------------------------------------------------

DEBUG: bool = os.environ.get("HPTL_VALIDATION_DEBUG", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

_INVALID_STRINGS = frozenset(
    {
        "",
        "n/a",
        "na",
        "none",
        "null",
        "-",
        "--",
        "nan",
        "#n/a",
        "#na",
        "undefined",
    }
)

_MISSING_CONFIDENCE_PENALTY = 0.90
_OPTIONAL_MISSING_CONFIDENCE_PENALTY = 0.95


# ---------------------------------------------------------------------------
# Numeric coercion
# ---------------------------------------------------------------------------


def _is_missing_scalar(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        stripped = value.strip().lower()
        if stripped in _INVALID_STRINGS:
            return True
    return False


def safe_numeric(value: Any) -> float | None:
    """Coerce any scalar to a finite float, or return None."""
    if _is_missing_scalar(value):
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        v = float(value)
        return v if math.isfinite(v) else None
    try:
        import numpy as np

        if isinstance(value, (np.integer, np.floating)):
            v = float(value)
            return v if math.isfinite(v) else None
    except Exception:
        pass
    if isinstance(value, str):
        stripped = value.strip().replace(",", "")
        if stripped in _INVALID_STRINGS:
            return None
        try:
            v = float(stripped)
            return v if math.isfinite(v) else None
        except (TypeError, ValueError):
            return None
    try:
        v = float(value)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def safe_float(value: Any) -> float | None:
    """Alias for :func:`safe_numeric`."""
    return safe_numeric(value)


def safe_int(value: Any) -> int | None:
    """Coerce to int when the value is a finite whole number."""
    num = safe_numeric(value)
    if num is None:
        return None
    try:
        return int(num)
    except (TypeError, ValueError, OverflowError):
        return None


# ---------------------------------------------------------------------------
# Safe comparisons
# ---------------------------------------------------------------------------


def _pair(value: Any, other: Any) -> tuple[float | None, float | None]:
    return safe_numeric(value), safe_numeric(other)


def safe_gt(a: Any, b: Any) -> bool:
    left, right = _pair(a, b)
    if left is None or right is None:
        return False
    return left > right


def safe_lt(a: Any, b: Any) -> bool:
    left, right = _pair(a, b)
    if left is None or right is None:
        return False
    return left < right


def safe_gte(a: Any, b: Any) -> bool:
    left, right = _pair(a, b)
    if left is None or right is None:
        return False
    return left >= right


def safe_lte(a: Any, b: Any) -> bool:
    left, right = _pair(a, b)
    if left is None or right is None:
        return False
    return left <= right


def safe_eq(a: Any, b: Any) -> bool:
    left, right = _pair(a, b)
    if left is None or right is None:
        return False
    return left == right


def safe_ne(a: Any, b: Any) -> bool:
    left, right = _pair(a, b)
    if left is None or right is None:
        return False
    return left != right


def safe_is_positive(value: Any) -> bool:
    return safe_gt(value, 0)


def safe_is_negative(value: Any) -> bool:
    return safe_lt(value, 0)


def safe_is_zero(value: Any) -> bool:
    num = safe_numeric(value)
    if num is None:
        return False
    return num == 0.0


def safe_sign(value: Any) -> int | None:
    """Return -1, 0, or 1; None when value is not numeric."""
    num = safe_numeric(value)
    if num is None:
        return None
    if num > 0:
        return 1
    if num < 0:
        return -1
    return 0


def safe_abs(value: Any) -> float | None:
    num = safe_numeric(value)
    if num is None:
        return None
    return abs(num)


# ---------------------------------------------------------------------------
# Validation reporting
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FieldValidation:
    field: str
    value: Any
    valid: bool
    reason: str | None = None
    parsed: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "field": self.field,
            "value": self.value,
            "valid": self.valid,
            "reason": self.reason,
            "parsed": self.parsed,
        }


@dataclass
class ValidationReport:
    fields: list[FieldValidation] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def valid_count(self) -> int:
        return sum(1 for f in self.fields if f.valid)

    @property
    def invalid_count(self) -> int:
        return sum(1 for f in self.fields if not f.valid)

    @property
    def total_count(self) -> int:
        return len(self.fields)

    @property
    def invalid_fields(self) -> list[FieldValidation]:
        return [f for f in self.fields if not f.valid]

    def confidence_multiplier(self, *, critical: Iterable[str], optional: Iterable[str] | None = None) -> float:
        """Apply multiplicative penalties for missing critical / optional fields."""
        invalid_names = {f.field for f in self.invalid_fields}
        multiplier = 1.0
        for name in critical:
            if name in invalid_names:
                multiplier *= _MISSING_CONFIDENCE_PENALTY
        for name in optional or ():
            if name in invalid_names:
                multiplier *= _OPTIONAL_MISSING_CONFIDENCE_PENALTY
        return multiplier

    def summary_text(self) -> str:
        lines = [
            "Validation Summary",
            "------------------",
            f"Fields Checked: {self.total_count}",
            f"Valid: {self.valid_count}",
            f"Invalid: {self.invalid_count}",
        ]
        if self.invalid_fields:
            lines.append("")
            lines.append("Invalid Fields:")
            for item in self.invalid_fields:
                reason = item.reason or "invalid"
                lines.append(f"- {item.field} ({reason})")
        if self.warnings:
            lines.append("")
            lines.append("Warnings:")
            for warning in self.warnings:
                lines.append(f"- {warning}")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        return {
            "fields_checked": self.total_count,
            "valid": self.valid_count,
            "invalid": self.invalid_count,
            "fields": [f.to_dict() for f in self.fields],
            "warnings": list(self.warnings),
        }


def _invalid_reason(value: Any) -> str:
    if value is None:
        return "missing"
    if isinstance(value, str) and value.strip().lower() in _INVALID_STRINGS:
        return "non-numeric"
    try:
        if pd.isna(value):
            return "missing"
    except (TypeError, ValueError):
        pass
    return "non-numeric"


def validate_field(name: str, value: Any) -> FieldValidation:
    parsed = safe_numeric(value)
    if parsed is None:
        return FieldValidation(
            field=name,
            value=value,
            valid=False,
            reason=_invalid_reason(value),
            parsed=None,
        )
    return FieldValidation(field=name, value=value, valid=True, reason=None, parsed=parsed)


def validate_fields(values: Mapping[str, Any], *, debug: bool | None = None) -> ValidationReport:
    """Validate a mapping of metric names to raw values."""
    report = ValidationReport()
    show_debug = DEBUG if debug is None else debug
    for name, raw in values.items():
        item = validate_field(name, raw)
        report.fields.append(item)
        if show_debug:
            status = "VALID" if item.valid else "INVALID"
            print(f"Field: {name}")
            print(f"Raw Value: {raw!r}")
            print(f"Parsed Value: {item.parsed}")
            print(f"Status: {status}")
            if item.reason:
                print(f"Reason: {item.reason}")
            print("")
        if not item.valid:
            logger.warning("validation: field %s invalid (%s)", name, item.reason)
    return report


def coerce_series_numeric(series: pd.Series) -> pd.Series:
    """Return a float series with invalid values coerced to NaN."""
    return pd.to_numeric(series, errors="coerce")


# ---------------------------------------------------------------------------
# Scoring component status
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ScoringComponentStatus:
    name: str
    raw_value: Any
    parsed_value: float | None
    score: float | str
    weight: float
    contribution: float | None
    status: str  # OK | UNKNOWN | DEFAULT

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "raw_value": self.raw_value,
            "parsed_value": self.parsed_value,
            "score": self.score,
            "weight": self.weight,
            "contribution": self.contribution,
            "status": self.status,
        }
