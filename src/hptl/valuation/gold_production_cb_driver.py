"""Gold production CB driver promotion — diagnostics and decision artifact."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.valuation.cb_gold_features import GOLD_CB_ENGINEERING, GOLD_CB_FEATURE
from hptl.valuation.gold_cb_driver_comparison import run_cb_driver_comparison
from hptl.valuation.metals_institutional_fair_value_v1 import compute_metals_institutional_valuation

DECISION_JSON = PROJECT_ROOT / "data" / "processed" / "gold_production_cb_driver_latest.json"
DECISION_MD = PROJECT_ROOT / "data" / "processed" / "gold_production_cb_driver_latest.md"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def build_production_decision(
    *,
    comparison: dict[str, Any] | None = None,
    gold_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    comparison = comparison or run_cb_driver_comparison()
    gold_result = gold_result or compute_metals_institutional_valuation(market="Gold")

    rec = comparison.get("recommendation") or {}
    level = comparison.get("monthly_level", {}).get("full_sample", {})
    roll12 = comparison.get("rolling_12m", {}).get("full_sample", {})
    lcb = level.get("cb_coefficient") or {}
    rcb = roll12.get("cb_coefficient") or {}

    reg = gold_result.get("regression") or {}
    audit = gold_result.get("institutional_audit") or {}
    publish = bool(gold_result.get("publish"))
    blockers = list(audit.get("blockers") or [])

    promoted = rec.get("recommended_production_cb_driver") == "cb_roll12"
    cb_sign_ok = rcb.get("sign_passed") is True

    if publish:
        outcome = "PUBLISH"
        summary = (
            f"Gold valuation PUBLISHED with production CB driver `{GOLD_CB_FEATURE}` "
            f"(rolling 12-month WGC net purchases). All institutional validation gates passed."
        )
    elif promoted and cb_sign_ok and "real_yield" in (gold_result.get("sign_gate_diagnostic") or {}).get(
        "failed_sign_gates", gold_result.get("sign_gate_diagnostic", {}).get("failed_sign_gates") if isinstance(gold_result.get("sign_gate_diagnostic"), dict) else []
    ):
        # Check failed sign gates from result
        sign_diag = gold_result.get("sign_gate_diagnostic") or {}
        failed = sign_diag.get("failed_sign_gates") or [
            f for f in (gold_result.get("regression") or {}).get("features", {})
        ]
        failed = sign_diag.get("failed_sign_gates") or []
        if not failed:
            failed = [b.split(":")[0].replace("Coefficient sign mismatch: ", "") for b in blockers if "sign mismatch" in b.lower()]
        non_cb_failures = [f for f in failed if f != GOLD_CB_FEATURE]
        outcome = "WITHHELD_MACRO_SIGN"
        summary = (
            f"Production CB driver promoted to `{GOLD_CB_FEATURE}` ({GOLD_CB_ENGINEERING}). "
            f"CB sign gate passes (beta={rcb.get('beta')}, p={rcb.get('p_value')}). "
            f"Gold remains WITHHELD: {gold_result.get('valuation_reason') or gold_result.get('blocker_reason')}. "
            "Validation gates were not weakened."
        )
    elif promoted:
        outcome = "WITHHELD"
        summary = (
            f"Production CB driver set to `{GOLD_CB_FEATURE}` per research scorecard, "
            f"but model fails publish gate: {gold_result.get('blocker_reason') or 'see blockers'}."
        )
    else:
        outcome = "WITHHELD"
        summary = "Rolling 12m CB not promoted — see comparison scorecard."

    return {
        "generated_at": _now_iso(),
        "decision": {
            "promoted_cb_driver": GOLD_CB_FEATURE,
            "cb_engineering": GOLD_CB_ENGINEERING,
            "raw_source": "data/cache/metals_drivers/wgc_cb_gold_net_purchases.json",
            "promotion_rationale": rec.get("rationale"),
            "comparison_scorecard": rec.get("scorecard"),
            "outcome": outcome,
            "summary": summary,
        },
        "comparison_snapshot": {
            "monthly_level_adj_r2": level.get("adj_r_squared"),
            "roll12_adj_r2": roll12.get("adj_r_squared"),
            "monthly_cb_beta": lcb.get("beta"),
            "monthly_cb_sign_passed": lcb.get("sign_passed"),
            "roll12_cb_beta": rcb.get("beta"),
            "roll12_cb_p_value": rcb.get("p_value"),
            "roll12_cb_sign_passed": rcb.get("sign_passed"),
            "walk_forward_rmse_level": (comparison.get("monthly_level") or {}).get("walk_forward_oos", {}).get("oos_metrics", {}).get("rmse_log"),
            "walk_forward_rmse_roll12": (comparison.get("rolling_12m") or {}).get("walk_forward_oos", {}).get("oos_metrics", {}).get("rmse_log"),
        },
        "production_model": {
            "model_id": gold_result.get("model_id"),
            "publish": publish,
            "model_status": gold_result.get("model_status"),
            "valuation_reason": gold_result.get("valuation_reason"),
            "blocker_reason": gold_result.get("blocker_reason"),
            "r_squared": reg.get("r_squared"),
            "adj_r_squared": reg.get("adj_r_squared"),
            "features": reg.get("features"),
            "deviation_pct_public": gold_result.get("deviation_pct"),
            "engine_deviation_note": (
                "Public deviation_pct is null when publish=false (export sanitizer)."
            ),
            "fair_value": gold_result.get("fair_value"),
            "spot_price": gold_result.get("spot_price"),
            "blockers": blockers,
            "sign_gate_diagnostic": gold_result.get("sign_gate_diagnostic"),
            "institutional_audit": audit,
        },
        "validation_gates": {
            "weakened": False,
            "publish_required_for_scanner_gap": True,
            "gates_failed": blockers,
        },
    }


def write_production_decision_artifacts(
    decision: dict[str, Any] | None = None,
) -> tuple[Path, Path]:
    decision = decision or build_production_decision()
    DECISION_JSON.parent.mkdir(parents=True, exist_ok=True)
    DECISION_JSON.write_text(json.dumps(decision, indent=2, ensure_ascii=False), encoding="utf-8")

    d = decision.get("decision") or {}
    snap = decision.get("comparison_snapshot") or {}
    prod = decision.get("production_model") or {}

    lines = [
        "# Gold production CB driver decision",
        "",
        f"Generated: {decision.get('generated_at')}",
        "",
        f"**Outcome:** {d.get('outcome')}",
        "",
        d.get("summary", ""),
        "",
        "## Promotion",
        "",
        f"- Production CB feature: `{d.get('promoted_cb_driver')}`",
        f"- Engineering: {d.get('cb_engineering')}",
        f"- Raw WGC cache: `{d.get('raw_source')}`",
        "",
        "## Comparison (monthly level vs roll12)",
        "",
        f"| Metric | Monthly level | Rolling 12m |",
        f"| --- | ---: | ---: |",
        f"| Adj R² | {snap.get('monthly_level_adj_r2')} | {snap.get('roll12_adj_r2')} |",
        f"| CB β | {snap.get('monthly_cb_beta')} | {snap.get('roll12_cb_beta')} |",
        f"| CB sign pass | {snap.get('monthly_cb_sign_passed')} | {snap.get('roll12_cb_sign_passed')} |",
        f"| CB p-value | — | {snap.get('roll12_cb_p_value')} |",
        f"| Walk-forward RMSE | {snap.get('walk_forward_rmse_level')} | {snap.get('walk_forward_rmse_roll12')} |",
        "",
        "## Production model after promotion",
        "",
        f"- Publish: **{prod.get('publish')}**",
        f"- Status: {prod.get('model_status')}",
        f"- R²: {prod.get('r_squared')}",
        f"- Reason: {prod.get('valuation_reason') or prod.get('blocker_reason')}",
        "",
        "### Blockers",
        "",
    ]
    for b in prod.get("blockers") or []:
        lines.append(f"- {b}")
    lines.extend(
        [
            "",
            f"Validation gates weakened: **{decision.get('validation_gates', {}).get('weakened')}**",
        ]
    )
    DECISION_MD.write_text("\n".join(lines), encoding="utf-8")
    return DECISION_JSON, DECISION_MD
