"""JSON-safe serialisation helpers for COT / workstation payloads."""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any


class JsonUnsafeError(ValueError):
    """Payload contains non-JSON-safe values."""


def sanitize_for_json(value: Any, *, path: str = "root") -> Any:
    """Recursively convert to JSON-safe natives; raise on NaN/Inf."""
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, int) and not isinstance(value, bool):
        return int(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise JsonUnsafeError(f"{path}: non-finite float {value!r}")
        return float(value)
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return sanitize_for_json(item(), path=path)
        except Exception as exc:  # noqa: BLE001
            raise JsonUnsafeError(f"{path}: unsanitisable scalar {type(value)}") from exc
    if isinstance(value, dict):
        return {str(k): sanitize_for_json(v, path=f"{path}.{k}") for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize_for_json(v, path=f"{path}[{i}]") for i, v in enumerate(value)]
    raise JsonUnsafeError(f"{path}: non-serialisable type {type(value).__name__}")
