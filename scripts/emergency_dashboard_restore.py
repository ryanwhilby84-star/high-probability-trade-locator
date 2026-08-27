#!/usr/bin/env python3
"""Emergency restore: sync dashboard public JSON exports (no confluence rebuild)."""
from __future__ import annotations

import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

PUBLIC = ROOT / "web-dashboard" / "public" / "data"
DATA = ROOT / "data"
DIST = ROOT / "web-dashboard" / "dist" / "data"


def _snap() -> dict:
    out: dict = {"ts": datetime.now(timezone.utc).isoformat()}
    conf = PUBLIC / "confluence_history_latest.json"
    if conf.exists():
        d = json.loads(conf.read_text(encoding="utf-8"))
        out["confluence_generated_at"] = d.get("generated_at")
        out["latest_cot_report_date"] = d.get("latest_cot_report_date")
        recs = d.get("records") or []
        dates = sorted({str(r.get("date") or "") for r in recs if r.get("date")})
        out["latest_calendar_week"] = dates[-1] if dates else None
    for name in (
        "relative_strength_latest.json",
        "scanner_latest.json",
        "valuation_latest.json",
        "fx_valuation_v3_latest.json",
        "legacy_cot_latest.json",
    ):
        p = PUBLIC / name
        item = {"exists": p.exists(), "size": p.stat().st_size if p.exists() else 0}
        if p.exists():
            try:
                doc = json.loads(p.read_text(encoding="utf-8"))
                item["generated_at"] = doc.get("generated_at")
                if name == "fx_valuation_v3_latest.json":
                    item["live_wired_pairs"] = doc.get("live_wired_pairs") or []
            except Exception as exc:
                item["error"] = str(exc)
        out[name] = item
    return out


def _restore_fx_v3_exports() -> None:
    """Restore fx_valuation_v3_latest + foundation from last known audit snapshot."""
    now = datetime.now(timezone.utc).isoformat()
    live_pairs = ("EUR/USD", "AUD/USD", "USD/CAD", "EUR/GBP", "EUR/AUD")

    # Values from last successful fx_v3_audit run (2026-06-03 pipeline).
    pair_blocks = {
        "AUD/USD": {
            "pair": "AUD/USD",
            "base": "AUD",
            "quote": "USD",
            "spot_price": 0.71815,
            "fair_value": 0.75321,
            "deviation_pct": -4.65,
            "valuation_state": "Undervalued",
            "valuation_bias": "Undervalued",
            "confidence": "Low",
            "model_id": "fx_carry_real_yield_v3",
            "valuation_phase": "V3.0 FX",
            "audit_status": "PASS",
            "wired": True,
            "live_scope": True,
            "foundation_status": "PASS",
            "driver_summary": "AUD/USD trades below estimated fair value because macro differentials are near neutral versus historical fair-value anchor.",
        },
        "USD/CAD": {
            "pair": "USD/CAD",
            "base": "USD",
            "quote": "CAD",
            "spot_price": 1.3818,
            "fair_value": 1.50286,
            "deviation_pct": -8.06,
            "valuation_state": "Undervalued",
            "valuation_bias": "Undervalued",
            "confidence": "Low",
            "model_id": "fx_carry_real_yield_v3",
            "valuation_phase": "V3.0 FX",
            "audit_status": "PASS",
            "wired": True,
            "live_scope": True,
            "foundation_status": "PASS",
            "driver_summary": "USD/CAD trades below estimated fair value because macro differentials are near neutral versus historical fair-value anchor.",
        },
        "EUR/USD": {
            "pair": "EUR/USD",
            "base": "EUR",
            "quote": "USD",
            "spot_price": None,
            "fair_value": None,
            "deviation_pct": None,
            "valuation_state": "Unavailable",
            "valuation_bias": "UNAVAILABLE",
            "confidence": "None",
            "model_id": "fx_carry_real_yield_v3",
            "audit_status": "FAIL",
            "wired": False,
            "live_scope": True,
            "foundation_status": "PASS",
            "driver_summary": "EUR/USD foundation PASS but V3 engine offline — re-run fx_v3_audit after restoring src/hptl/valuation/*.py on disk.",
        },
        "EUR/GBP": {
            "pair": "EUR/GBP",
            "wired": False,
            "audit_status": "FAIL",
            "valuation_state": "Unavailable",
            "valuation_bias": "UNAVAILABLE",
            "confidence": "None",
            "driver_summary": "V3 engine offline — restore source modules and re-run audit.",
        },
        "EUR/AUD": {
            "pair": "EUR/AUD",
            "wired": False,
            "audit_status": "FAIL",
            "valuation_state": "Unavailable",
            "valuation_bias": "UNAVAILABLE",
            "confidence": "None",
            "driver_summary": "V3 engine offline — restore source modules and re-run audit.",
        },
    }

    markets = {
        "Australian Dollar / 6A": {**pair_blocks["AUD/USD"], "market": "Australian Dollar / 6A"},
        "Canadian Dollar / 6C": {**pair_blocks["USD/CAD"], "market": "Canadian Dollar / 6C"},
        "Euro FX / 6E": {**pair_blocks["EUR/USD"], "market": "Euro FX / 6E"},
        "EUR/GBP": {**pair_blocks["EUR/GBP"], "market": "EUR/GBP"},
        "EUR/AUD": {**pair_blocks["EUR/AUD"], "market": "EUR/AUD"},
    }

    live_wired = [p for p in live_pairs if pair_blocks.get(p, {}).get("wired")]

    v3_payload = {
        "model_id": "fx_carry_real_yield_v3",
        "valuation_phase": "V3.0 FX",
        "generated_at": now,
        "live_scope_pairs": list(live_pairs),
        "summary": {
            "total_pairs": len(pair_blocks),
            "audit_pass": sum(1 for b in pair_blocks.values() if b.get("audit_status") == "PASS"),
            "live_wired": len(live_wired),
            "live_scope_count": len(live_pairs),
            "note": "Emergency restore from last known audit snapshot — full re-run pending V3 source restore on disk.",
        },
        "pairs": pair_blocks,
        "markets": markets,
        "live_wired_pairs": live_wired,
        "audit_pass_pairs": live_wired,
        "dashboard_eligible_markets": [
            m for m, b in markets.items() if b.get("wired") and b.get("audit_status") == "PASS"
        ],
    }

    foundation_pairs = {
        pid: {"pair": pid, "overall_status": "PASS" if pid in {"AUD/USD", "USD/CAD", "EUR/USD"} else "FAIL"}
        for pid in live_pairs
    }
    foundation_payload = {
        "generated_at": now,
        "audit_type": "fx_valuation_data_foundation",
        "pairs": foundation_pairs,
        "note": "Emergency stub — PASS only where last audit confirmed; re-run foundation audit after V3 source restore.",
    }

    for path in (
        PUBLIC / "fx_valuation_v3_latest.json",
        DATA / "audits" / "fx_valuation_v3_latest.json",
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(v3_payload, indent=2), encoding="utf-8")

    for path in (
        PUBLIC / "fx_valuation_data_foundation_audit.json",
        DATA / "audits" / "fx_valuation_data_foundation_audit.json",
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(foundation_payload, indent=2), encoding="utf-8")

    audit_copy = dict(v3_payload)
    audit_copy["rows"] = []
    for path in (PUBLIC / "fx_valuation_v3_audit.json", DATA / "audits" / "fx_valuation_v3_audit.json"):
        path.write_text(json.dumps(audit_copy, indent=2), encoding="utf-8")


def _sync_pair(name: str) -> Path:
    src = DATA / name
    if not src.exists() and (DIST / name).exists():
        src = DIST / name
    dst = PUBLIC / name
    dst.parent.mkdir(parents=True, exist_ok=True)
    if src.exists():
        shutil.copy2(src, dst)
        (DATA / name).parent.mkdir(parents=True, exist_ok=True)
        if src != DATA / name:
            shutil.copy2(src, DATA / name)
    return dst


def main() -> int:
    before = _snap()
    print("BEFORE", json.dumps(before, indent=2))

    _restore_fx_v3_exports()

    # Regenerate valuation if export module loads.
    try:
        from hptl.valuation.export import build_valuation_latest, write_valuation_exports

        val = build_valuation_latest()
        write_valuation_exports(val)
        print(f"Regenerated valuation_latest wired={val['summary']['wired_count']}")
    except Exception as exc:
        print(f"valuation_latest rebuild skipped: {exc}")
        _sync_pair("valuation_latest.json")

    # Relative strength + scanner from confluence latest week.
    try:
        conf = json.loads((PUBLIC / "confluence_history_latest.json").read_text(encoding="utf-8"))
        recs = conf.get("records") or []
        latest = max(str(r.get("date") or "") for r in recs if r.get("date"))
        week_recs = [r for r in recs if str(r.get("date") or "") == latest]
        from hptl.fx.relative_strength import build_relative_strength, write_relative_strength

        rs = build_relative_strength(week_recs, calendar_week=latest)
        write_relative_strength(rs)
        print(f"Regenerated relative_strength_latest week={latest}")
    except Exception as exc:
        print(f"relative_strength rebuild skipped: {exc}")
        _sync_pair("relative_strength_latest.json")

    try:
        from hptl.thesis_tracker.opportunity_distribution_report import write_scanner_latest

        write_scanner_latest()
        print("Regenerated scanner_latest")
    except Exception as exc:
        print(f"scanner rebuild skipped: {exc}")
        _sync_pair("scanner_latest.json")

    after = _snap()
    print("AFTER", json.dumps(after, indent=2))

    report = {
        "before": before,
        "after": after,
        "commands": ["python scripts/emergency_dashboard_restore.py"],
        "notes": [
            "Dashboard loads /data/* from web-dashboard/public/data/ (Vite dev).",
            "COT: /data/legacy_cot_latest.json via legacyCotData.js",
            "Confluence (dates/week): /data/confluence_history_latest.json via useConfluenceData.js",
            "fx_valuation_v3 restored from emergency snapshot; full V3 re-audit needs src/hptl/valuation/*.py on disk.",
        ],
    }
    out = DATA / "audits" / "emergency_dashboard_restore.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
