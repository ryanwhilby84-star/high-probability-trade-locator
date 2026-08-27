#!/usr/bin/env python3
"""Trace FX V3 valuation path for priority dashboard rows."""
from __future__ import annotations

import dataclasses
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
os.environ.setdefault("HPTL_SKIP_LIVE_FEEDS", "1")

PRIORITY = [
    ("Japanese Yen / 6J", "USD/JPY"),
    ("NZ Dollar / 6N", "NZD/USD"),
    ("Swiss Franc / 6S", "USD/CHF"),
    ("British Pound / 6B", "GBP/USD"),
    ("US Dollar Index / DX", None),
]

COT_MARKET_TO_PAIR = {
    "Euro FX / 6E": "EUR/USD",
    "British Pound / 6B": "GBP/USD",
    "Australian Dollar / 6A": "AUD/USD",
    "NZ Dollar / 6N": "NZD/USD",
    "Japanese Yen / 6J": "USD/JPY",
    "Canadian Dollar / 6C": "USD/CAD",
    "Swiss Franc / 6S": "USD/CHF",
}

FX_V3_LIVE_PAIRS_JS = {
    "EUR/USD",
    "AUD/USD",
    "USD/CAD",
    "EUR/GBP",
    "EUR/AUD",
}


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _dashboard_reason(pair_id: str | None, v3_block: dict | None, foundation: dict | None) -> tuple[str, str]:
    if not pair_id:
        return "NOT_SUPPORTED — no FX pair mapping for DX", "NOT_SUPPORTED"
    if not v3_block:
        return "EXPORT_MISSING — pair absent from fx_valuation_v3_latest.json", "EXPORT_MISSING"
    fp = (foundation or {}).get("pairs", {}).get(pair_id) or {}
    foundation_pass = fp.get("overall_status") == "PASS"
    in_live = pair_id in FX_V3_LIVE_PAIRS_JS or (pair_id == "USD/CHF" and foundation_pass)
    if not in_live:
        return f"DASHBOARD_KEY_MISMATCH — {pair_id} outside JS FX_V3_LIVE_PAIRS", "DASHBOARD_KEY_MISMATCH"
    if not foundation_pass:
        return f"MODEL_GATE_FAIL — foundation audit not PASS for {pair_id}", "MODEL_GATE_FAIL"
    if v3_block.get("audit_status") != "PASS":
        r2 = v3_block.get("regression", {}).get("r_squared") if isinstance(v3_block.get("regression"), dict) else None
        if r2 is not None and r2 < 0.08:
            return f"MODEL_GATE_FAIL — R²={r2} below threshold", "MODEL_GATE_FAIL"
        missing = v3_block.get("missing_inputs") or []
        if missing:
            return f"PARSER_DATA — missing: {', '.join(missing[:5])}", "PARSER_DATA"
        return f"MODEL_GATE_FAIL — audit_status={v3_block.get('audit_status')}", "MODEL_GATE_FAIL"
    if not v3_block.get("wired"):
        return "DASHBOARD_KEY_MISMATCH — audit PASS but wired=false in export", "DASHBOARD_KEY_MISMATCH"
    return "OK — should display valuation %", "none"


def main() -> int:
    from hptl.valuation.fx_carry_real_yield_v3 import (
        build_all_fx_v3_pairs,
        compute_fx_pair_v3,
        is_live_scope_pair,
    )
    from hptl.valuation.fx_v3_audit import _load_foundation_pairs, run_fx_v3_audit

    print("Running compute_fx_pair_v3 (offline)...")
    all_pairs = build_all_fx_v3_pairs()
    foundation = _load_foundation_pairs()
    print("Running run_fx_v3_audit (offline)...")
    audit = run_fx_v3_audit(refresh_caches=False)

    v3_public = _read_json(ROOT / "web-dashboard/public/data/fx_valuation_v3_latest.json")
    val_latest = _read_json(ROOT / "data/valuation_latest.json")
    val_instruments = val_latest.get("instruments") or {}

    rows = []
    for display_name, expected_pair in PRIORITY:
        instrument_id = display_name
        compute_result = None
        if expected_pair:
            compute_result = compute_fx_pair_v3(expected_pair)
            cr_dict = compute_result.as_dict()
        else:
            cr_dict = None

        build_key_found = expected_pair in (all_pairs.get("pairs") or {}) if expected_pair else False
        audit_pair = (audit.get("pairs") or {}).get(expected_pair or "") if expected_pair else None
        audit_status = audit_pair.get("audit_status") if audit_pair else None
        audit_wired = audit_pair.get("wired") if audit_pair else None

        v3_market = (audit.get("markets") or {}).get(display_name) or (v3_public.get("markets") or {}).get(display_name)
        v3_pair_block = (v3_public.get("pairs") or {}).get(expected_pair or "") if expected_pair else None

        val_key_found = display_name in val_instruments
        val_wired = (val_instruments.get(display_name) or {}).get("wired") if val_key_found else None

        dashboard_pair = COT_MARKET_TO_PAIR.get(display_name)
        reason, category = _dashboard_reason(dashboard_pair, v3_pair_block or audit_pair, {"pairs": foundation})

        row = {
            "display_name": display_name,
            "instrument_id": instrument_id,
            "expected_v3_pair": expected_pair,
            "compute_result_status": compute_result.audit_status if compute_result else "NOT_SUPPORTED",
            "compute_result_value": compute_result.deviation_pct if compute_result else None,
            "compute_valuation_state": compute_result.valuation_state if compute_result else None,
            "compute_missing_inputs": compute_result.missing_inputs if compute_result else [],
            "compute_r_squared": (compute_result.regression or {}).get("r_squared") if compute_result else None,
            "build_all_fx_v3_pairs_key_found": build_key_found,
            "audit_status": audit_status,
            "audit_wired": audit_wired,
            "audit_live_scope": is_live_scope_pair(expected_pair, foundation_pass=(foundation.get(expected_pair or {}) or {}).get("overall_status") == "PASS") if expected_pair else False,
            "valuation_latest_key_found": val_key_found,
            "valuation_latest_wired": val_wired,
            "dashboard_lookup_key": dashboard_pair,
            "v3_public_market_found": v3_market is not None,
            "v3_public_wired": (v3_market or {}).get("wired") if v3_market else None,
            "reason_dashboard_shows_unavailable": reason,
            "repair_category": category,
        }
        rows.append(row)
        print(json.dumps(row, indent=2))

    out = ROOT / "data/audits/fx_dashboard_valuation_coverage_trace.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"rows": rows}, indent=2), encoding="utf-8")
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
