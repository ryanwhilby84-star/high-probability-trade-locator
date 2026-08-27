"""Flat split-score contract for FX setup ranking JSON ↔ dashboard."""

from __future__ import annotations

from typing import Any

from hptl.setup_ranking.layers import LayerResult

SPLIT_SCORE_KEYS = (
    "macro_score",
    "valuation_edge_score",
    "trade_readiness_score",
    "action_label",
    "macro_explanation",
    "valuation_explanation",
    "readiness_explanation",
)


def _layer_explanation(layer: LayerResult) -> str:
    parts = [layer.summary.strip()]
    if layer.detail and layer.detail.strip() not in parts[0]:
        parts.append(layer.detail.strip())
    return " ".join(p for p in parts if p)


def split_score_fields(
    *,
    macro: LayerResult,
    valuation: LayerResult,
    readiness: LayerResult,
    action_label: str,
) -> dict[str, Any]:
    """Canonical flat fields every opportunity row must expose."""
    return {
        "macro_score": round(float(macro.score), 1),
        "valuation_edge_score": round(float(valuation.score), 1),
        "trade_readiness_score": round(float(readiness.score), 1),
        "action_label": action_label,
        "macro_explanation": _layer_explanation(macro),
        "valuation_explanation": _layer_explanation(valuation),
        "readiness_explanation": _layer_explanation(readiness),
        "macro_grade": macro.grade,
        "valuation_edge_grade": valuation.grade,
        "trade_readiness_grade": readiness.grade,
    }


def row_has_split_scores(row: dict[str, Any] | None) -> bool:
    if not row:
        return False
    try:
        for key in ("macro_score", "valuation_edge_score", "trade_readiness_score"):
            v = row.get(key)
            if v is None:
                return False
            float(v)
        action = row.get("action_label")
        return isinstance(action, str) and bool(action.strip())
    except (TypeError, ValueError):
        return False


def enrich_row_from_nested(row: dict[str, Any]) -> dict[str, Any]:
    """Backfill flat contract from nested layer objects (legacy export compat)."""
    if row_has_split_scores(row):
        return row
    macro = row.get("macro_bias") or {}
    val = row.get("valuation_edge") or {}
    ready = row.get("trade_readiness") or {}
    if macro.get("score") is None:
        return row
    flat = {
        "macro_score": round(float(macro["score"]), 1),
        "valuation_edge_score": round(float(val.get("score") or 0), 1),
        "trade_readiness_score": round(float(ready.get("score") or 0), 1),
        "action_label": row.get("action_label") or "",
        "macro_explanation": row.get("macro_explanation") or macro.get("detail") or macro.get("summary") or "",
        "valuation_explanation": row.get("valuation_explanation") or val.get("detail") or val.get("summary") or "",
        "readiness_explanation": row.get("readiness_explanation") or ready.get("summary") or ready.get("detail") or "",
        "macro_grade": macro.get("grade"),
        "valuation_edge_grade": val.get("grade"),
        "trade_readiness_grade": ready.get("grade"),
    }
    return {**row, **flat}


def audit_split_scores(opportunities: list[dict[str, Any]], *, generated_at: str | None = None) -> dict[str, Any]:
    rows = list(opportunities or [])
    enriched = [enrich_row_from_nested(r) for r in rows]
    split_n = sum(1 for r in enriched if row_has_split_scores(r))
    return {
        "rows": len(rows),
        "split_score_rows": split_n,
        "missing_split_score_rows": len(rows) - split_n,
        "source_file": "fx_setup_ranking_latest.json",
        "generated_at": generated_at,
        "contract_keys": list(SPLIT_SCORE_KEYS),
    }
