"""Audit gate for fx_carry_real_yield_v3 — must PASS before dashboard wiring."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import DATA_DIR, PROJECT_ROOT
from hptl.fx.fx_macro_history import currency_histories, ensure_fx_macro_caches
from hptl.fx.fx_spot_history import get_daily_spot_series
from hptl.valuation.fx_carry_real_yield_v3 import (
    FX_V3_LIVE_PAIRS,
    FX_V3_PAIRS,
    MIN_R_SQUARED,
    MIN_WEEKLY_OBS,
    MODEL_ID,
    PAIR_BY_COT_MARKET,
    VALUATION_PHASE,
    _align_daily_panel,
    apply_live_wiring_gate,
    build_all_fx_v3_pairs,
    compute_fx_market_v3,
    compute_fx_pair_v3,
    is_live_scope_pair,
)

AUDIT_JSON = DATA_DIR / "audits" / "fx_valuation_v3_audit.json"
AUDIT_MD = DATA_DIR / "audits" / "fx_valuation_v3_audit.md"
LIVE_AUDIT_MD = DATA_DIR / "audits" / "fx_valuation_v3_live_audit.md"
FOUNDATION_JSON = DATA_DIR / "audits" / "fx_valuation_data_foundation_audit.json"
PUBLIC_JSON = PROJECT_ROOT / "web-dashboard/public/data/fx_valuation_v3_latest.json"
PUBLIC_AUDIT = PROJECT_ROOT / "web-dashboard/public/data/fx_valuation_v3_audit.json"
PUBLIC_FOUNDATION = PROJECT_ROOT / "web-dashboard/public/data/fx_valuation_data_foundation_audit.json"


def _load_foundation_pairs() -> dict[str, Any]:
    path = FOUNDATION_JSON if FOUNDATION_JSON.exists() else PUBLIC_FOUNDATION
    if not path.exists():
        return {}
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return doc.get("pairs") or {}


def _foundation_pass(pair_id: str, foundation_pairs: dict[str, Any]) -> bool:
    block = foundation_pairs.get(pair_id) or {}
    return block.get("overall_status") == "PASS"


def _gate_pair_block(pair_id: str, block: dict[str, Any], foundation_pairs: dict[str, Any]) -> dict[str, Any]:
    return apply_live_wiring_gate(
        block,
        pair_id=pair_id,
        foundation_pass=_foundation_pass(pair_id, foundation_pairs),
    )


def _policy_aligned_days(pair_id: str, histories: dict[str, Any]) -> int:
    from hptl.fx.fx_valuation import resolve_pair_currencies
    from hptl.fx.currency_rates import get_currency_rate
    from hptl.valuation.fx_carry_real_yield_v3 import _value_as_of

    resolved = resolve_pair_currencies(pair_id)
    if not resolved:
        return 0
    base, quote, _ = resolved
    spot, _ = get_daily_spot_series(pair_id)
    base_pol = dict((histories.get(base) or {}).get("policy") or {})
    quote_pol = dict((histories.get(quote) or {}).get("policy") or {})
    br = get_currency_rate(base)
    qr = get_currency_rate(quote)
    if br.policy_rate is not None and br.policy_rate_as_of:
        base_pol[str(br.policy_rate_as_of)[:10]] = float(br.policy_rate)
    if qr.policy_rate is not None and qr.policy_rate_as_of:
        quote_pol[str(qr.policy_rate_as_of)[:10]] = float(qr.policy_rate)
    n = 0
    for pt in spot:
        d = pt["date"]
        if _value_as_of(base_pol, d) is not None and _value_as_of(quote_pol, d) is not None:
            n += 1
    return n


def _audit_row(pair_id: str, block: dict[str, Any], histories: dict[str, Any]) -> dict[str, Any]:
    missing = block.get("missing_inputs") or []
    reg = block.get("regression") or {}
    spot_series, spot_meta = get_daily_spot_series(pair_id)
    panel = _align_daily_panel(
        pair_id,
        block.get("base") or "",
        block.get("quote") or "",
        histories,
    )
    y2_intersect = len(
        set((histories.get(block.get("base") or "") or {}).get("y2") or {})
        & set((histories.get(block.get("quote") or "") or {}).get("y2") or {})
    )
    return {
        "instrument": pair_id,
        "pair": pair_id,
        "spot_obs": len(spot_series),
        "yield_obs": y2_intersect,
        "policy_obs": _policy_aligned_days(pair_id, histories),
        "aligned_obs": len(panel),
        "r_squared": reg.get("r_squared"),
        "spot": block.get("spot_price"),
        "fair_value": block.get("fair_value"),
        "deviation_pct": block.get("deviation_pct"),
        "state": block.get("valuation_state"),
        "confidence": block.get("confidence"),
        "missing_inputs": missing,
        "audit_status": block.get("audit_status"),
        "wired": block.get("wired"),
        "live_scope": block.get("live_scope"),
        "foundation_status": block.get("foundation_status"),
        "spot_meta": spot_meta,
        "regression_n": reg.get("n"),
    }


def _live_audit_table(rows: list[dict[str, Any]]) -> str:
    lines = [
        "| Pair | Spot | Fair Value | Deviation % | Confidence | Audit Status | PASS/FAIL |",
        "|---|---:|---:|---:|---|---|---|",
    ]
    for r in rows:
        spot = f"{r['spot']:.5f}" if r.get("spot") is not None else "—"
        fv = f"{r['fair_value']:.5f}" if r.get("fair_value") is not None else "—"
        dev = f"{r['deviation_pct']:+.2f}" if r.get("deviation_pct") is not None else "—"
        conf = r.get("confidence") or "None"
        status = "PASS" if r.get("wired") else "FAIL"
        lines.append(
            f"| {r['pair']} | {spot} | {fv} | {dev} | {conf} | {r.get('audit_status')} | **{status}** |"
        )
    return "\n".join(lines)


def run_fx_v3_audit(*, refresh_caches: bool = True) -> dict[str, Any]:
    if refresh_caches:
        ensure_fx_macro_caches()
    histories = currency_histories()
    foundation_pairs = _load_foundation_pairs()
    payload = build_all_fx_v3_pairs()

    gated_pairs: dict[str, Any] = {}
    for pid in FX_V3_PAIRS:
        gated_pairs[pid] = _gate_pair_block(pid, payload["pairs"][pid], foundation_pairs)

    rows = [_audit_row(pid, gated_pairs[pid], histories) for pid in FX_V3_PAIRS]
    live_rows = [r for r in rows if is_live_scope_pair(r["pair"], foundation_pass=_foundation_pass(r["pair"], foundation_pairs))]

    markets: dict[str, Any] = {}
    for market, pair in PAIR_BY_COT_MARKET.items():
        raw = compute_fx_market_v3(market)
        gated = _gate_pair_block(pair, gated_pairs.get(pair) or raw, foundation_pairs)
        gated["market"] = market
        markets[market] = gated

    wired_live = [r["pair"] for r in live_rows if r.get("wired")]
    report = {
        "model_id": MODEL_ID,
        "valuation_phase": VALUATION_PHASE,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "audit_gate": "fx_carry_real_yield_v3",
        "live_scope_pairs": list(FX_V3_LIVE_PAIRS),
        "gates": {
            "min_aligned_obs": MIN_WEEKLY_OBS,
            "min_r_squared": MIN_R_SQUARED,
            "foundation_required": True,
            "live_scope_only": True,
        },
        "summary": {
            **payload["summary"],
            "live_wired": len(wired_live),
            "live_scope_count": len(live_rows),
        },
        "rows": rows,
        "live_rows": live_rows,
        "detail_table": rows,
        "pairs": gated_pairs,
        "markets": markets,
        "dashboard_eligible_markets": [
            m for m, b in markets.items() if b.get("wired") is True and b.get("audit_status") == "PASS"
        ],
        "live_wired_pairs": wired_live,
    }
    return report


def _markdown_table(rows: list[dict[str, Any]]) -> str:
    lines = [
        "| Pair | Spot obs | Yield obs | Policy obs | Aligned obs | R² | Fair Value | Deviation % | State | Confidence | PASS/FAIL |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|---|---|",
    ]
    for r in rows:
        r2 = f"{r['r_squared']:.4f}" if r.get("r_squared") is not None else "—"
        fv = f"{r['fair_value']:.5f}" if r.get("fair_value") is not None else "—"
        dev = f"{r['deviation_pct']:+.2f}" if r.get("deviation_pct") is not None else "—"
        wired = "PASS" if r.get("wired") else "FAIL"
        lines.append(
            f"| {r['pair']} | {r.get('spot_obs', '—')} | {r.get('yield_obs', '—')} | {r.get('policy_obs', '—')} | "
            f"{r.get('aligned_obs', '—')} | {r2} | {fv} | {dev} | {r.get('state')} | {r.get('confidence')} | **{wired}** |"
        )
    return "\n".join(lines)


def write_fx_v3_audit_artifacts(report: dict[str, Any] | None = None) -> dict[str, Path]:
    report = report or run_fx_v3_audit()
    AUDIT_JSON.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    PUBLIC_AUDIT.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_AUDIT.write_text(json.dumps(report, indent=2), encoding="utf-8")

    md = [
        "# FX Valuation V3.0 Audit — fx_carry_real_yield_v3",
        "",
        f"- Generated: {report.get('generated_at')}",
        f"- Live wired: {report['summary'].get('live_wired', 0)} / {report['summary'].get('live_scope_count', 0)} live-scope pairs",
        f"- Model pass (all pairs): {report['summary']['audit_pass']} / {report['summary']['total_pairs']}",
        "",
        "## Live scope audit (dashboard + thesis)",
        "",
        _live_audit_table(report.get("live_rows") or []),
        "",
        "## All pairs (diagnostic)",
        "",
        _markdown_table(report["rows"]),
        "",
        "## Dashboard-eligible COT markets",
        "",
    ]
    eligible = report.get("dashboard_eligible_markets") or []
    md.append(", ".join(eligible) if eligible else "None — no pairs passed live wiring gate.")
    AUDIT_MD.write_text("\n".join(md), encoding="utf-8")

    live_md = [
        "# FX Valuation V3.0 — Live wiring audit",
        "",
        _live_audit_table(report.get("live_rows") or []),
        "",
        f"Live scope: {', '.join(report.get('live_scope_pairs') or [])}",
        "",
        "USD/CHF included only when foundation audit PASS.",
    ]
    LIVE_AUDIT_MD.write_text("\n".join(live_md), encoding="utf-8")

    public_payload = {
        "model_id": MODEL_ID,
        "valuation_phase": VALUATION_PHASE,
        "generated_at": report["generated_at"],
        "live_scope_pairs": report.get("live_scope_pairs"),
        "summary": report["summary"],
        "pairs": report["pairs"],
        "markets": report.get("markets"),
        "live_wired_pairs": report.get("live_wired_pairs") or [],
        "audit_pass_pairs": list(report.get("live_wired_pairs") or []),
    }
    PUBLIC_JSON.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_JSON.write_text(json.dumps(public_payload, indent=2), encoding="utf-8")

    return {
        "audit_json": AUDIT_JSON,
        "audit_md": AUDIT_MD,
        "live_audit_md": LIVE_AUDIT_MD,
        "public_json": PUBLIC_JSON,
        "public_audit": PUBLIC_AUDIT,
    }


def load_v3_audit_cache() -> dict[str, Any]:
    path = PUBLIC_AUDIT if PUBLIC_AUDIT.exists() else AUDIT_JSON
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
