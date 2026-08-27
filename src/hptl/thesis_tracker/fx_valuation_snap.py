"""FX Institutional Macro V2 overrides for thesis scoring snaps."""

from __future__ import annotations

import re
from typing import Any

from hptl.fx.fx_valuation import value_condition_from_bias

_FX_MAJOR_RE = re.compile(r"/ 6[A-Z0-9]", re.I)
_FX_CROSS_RE = re.compile(r"^[A-Z]{3}/[A-Z]{3}$")

V2_MODEL = "FX Institutional Macro V2"


def is_fx_market(row: dict[str, Any] | None) -> bool:
    if not row:
        return False
    inst = row.get("instrument_meta") if isinstance(row.get("instrument_meta"), dict) else {}
    if str(inst.get("asset_class") or "").lower() == "fx":
        return True
    if row.get("fx_valuation_model_type") or row.get("fx_valuation_bias"):
        return True
    market = str(row.get("market") or "").strip()
    if not market:
        return False
    if _FX_CROSS_RE.match(market):
        return True
    if _FX_MAJOR_RE.search(market):
        return True
    return False


def has_fx_valuation(row: dict[str, Any] | None) -> bool:
    if not row:
        return False
    fx = row.get("fx_valuation") if isinstance(row.get("fx_valuation"), dict) else {}
    if fx.get("pair"):
        return True
    bias = str(row.get("fx_valuation_bias") or "").strip()
    if not bias or bias.upper() == "UNAVAILABLE":
        return False
    return bool(row.get("fx_valuation_model_type") or row.get("fx_valuation_score") is not None)


def fx_value_condition_from_row(row: dict[str, Any]) -> str:
    fx = row.get("fx_valuation") if isinstance(row.get("fx_valuation"), dict) else {}
    if fx.get("value_condition"):
        return str(fx["value_condition"])
    if row.get("fx_valuation_condition"):
        return str(row["fx_valuation_condition"])
    bias = row.get("fx_valuation_bias") or fx.get("valuation_bias") or fx.get("bias")
    return value_condition_from_bias(str(bias) if bias else None)


def apply_fx_valuation_to_snap(row: dict[str, Any] | None, snap: dict[str, Any]) -> dict[str, Any]:
    if not is_fx_market(row):
        return snap
    base = dict(snap)
    if not has_fx_valuation(row):
        return {
            **base,
            "valuation_bias": "UNAVAILABLE",
            "valuation_score": None,
            "valuation_reason": "FX institutional valuation unavailable for this instrument.",
            "valuation_wired": False,
            "valuation_source": None,
            "valuation_condition": None,
            "valuation_grade": None,
            "valuation_model_status": None,
            "valuation_is_fx": True,
        }

    fx = row.get("fx_valuation") if isinstance(row.get("fx_valuation"), dict) else {}
    bias = row.get("fx_valuation_bias") or fx.get("valuation_bias") or fx.get("bias") or base.get("valuation_bias")
    score = row.get("fx_valuation_score")
    if score is None:
        score = fx.get("valuation_score")
    if score is None:
        score = base.get("valuation_score")
    grade = row.get("valuation_grade") or fx.get("valuation_grade")
    model_status = row.get("valuation_model_status") or fx.get("model_status")
    condition = fx_value_condition_from_row(row)
    model = row.get("fx_valuation_model_type") or fx.get("valuation_model_type") or V2_MODEL
    bias_s = str(bias or "").strip().upper()
    wired = bool(bias_s and bias_s not in {"UNAVAILABLE", "PENDING", ""})

    return {
        **base,
        "valuation_bias": bias,
        "valuation_score": score,
        "valuation_reason": fx.get("explanation") or row.get("fx_valuation_explanation") or base.get("valuation_reason"),
        "valuation_wired": wired,
        "valuation_source": model,
        "valuation_condition": condition,
        "valuation_grade": grade,
        "valuation_model_status": model_status,
        "valuation_model_type": model,
        "valuation_gap_pct": fx.get("valuation_gap_pct") or row.get("fx_valuation_gap_pct"),
        "valuation_fair_value": fx.get("fair_value_estimate") or row.get("fx_fair_value_estimate"),
        "valuation_is_fx": True,
    }
