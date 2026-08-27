"""Macro Intelligence service — API / CLI payload builder."""

from __future__ import annotations

import json
from typing import Any

from hptl.macro_intelligence.engine import analyse_macro_intelligence
from hptl.macro_intelligence.models import ENGINE_VERSION


def build_macro_intelligence_payload(
    *,
    instrument_id: str,
) -> dict[str, Any]:
    result = analyse_macro_intelligence(instrument_id)
    payload = result.to_dict()
    payload["engine"] = ENGINE_VERSION
    return payload


def build_macro_intelligence_payload_from_json(
    raw: str | bytes | dict[str, Any],
) -> dict[str, Any]:
    if isinstance(raw, dict):
        body = raw
    else:
        body = json.loads(raw)
    instrument_id = str(
        body.get("instrument_id") or body.get("instrument") or body.get("market") or ""
    ).strip()
    return build_macro_intelligence_payload(instrument_id=instrument_id)
