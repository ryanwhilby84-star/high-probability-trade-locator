#!/usr/bin/env python3
"""Tier 1 FX Repair Board — seven G10 majors only."""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

TIER1_PAIRS = (
    "EUR/USD",
    "GBP/USD",
    "USD/JPY",
    "USD/CHF",
    "AUD/USD",
    "NZD/USD",
    "USD/CAD",
)

PAIR_TO_MARKET = {
    "EUR/USD": "Euro FX / 6E",
    "GBP/USD": "British Pound / 6B",
    "USD/JPY": "Japanese Yen / 6J",
    "USD/CHF": "Swiss Franc / 6S",
    "AUD/USD": "Australian Dollar / 6A",
    "NZD/USD": "NZ Dollar / 6N",
    "USD/CAD": "Canadian Dollar / 6C",
}

OUT_JSON = ROOT / "data" / "audits" / "tier1_fx_repair_board.json"
OUT_MD = ROOT / "data" / "audits" / "tier1_fx_repair_board.md"

MIN_OBS = 52


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _leg_status(meta: dict[str, Any] | None, *, min_obs: int = MIN_OBS) -> str:
    if not meta:
        return "MISSING"
    st = meta.get("audit_status") or meta.get("status")
    if st == "PASS":
        return "PASS"
    count = int(meta.get("observation_count") or 0)
    if count >= min_obs:
        return "PASS"
    if count > 0:
        return f"SHALLOW ({count})"
    return "FAIL"


def _bis_status(ref: str) -> tuple[str, int]:
    from hptl.fx.rate_adapter_base import CACHE_DIR

    hist = CACHE_DIR / f"bis_cbpol_{ref.lower()}_history.txt"
    shallow = CACHE_DIR / f"bis_cbpol_{ref.lower()}.txt"
    best = 0
    source = "MISSING"
    if hist.exists():
        raw = hist.read_text(encoding="utf-8", errors="replace")
        n = sum(1 for line in raw.splitlines() if line.strip() and "TIME_PERIOD" not in line and line[0].isdigit())
        if n == 0:
            import csv, io

            r = csv.DictReader(io.StringIO(raw))
            n = sum(1 for row in r if (row.get("TIME_PERIOD") or "").strip())
        best = max(best, n)
        source = "history"
    if shallow.exists():
        raw = shallow.read_text(encoding="utf-8", errors="replace")
        n = sum(1 for line in raw.splitlines() if line.strip() and line[0].isdigit())
        if n > best:
            best = n
            source = "shallow"
    if best >= MIN_OBS:
        return f"PASS ({best} obs, {source})", best
    if best > 0:
        return f"SHALLOW ({best} obs, {source})", best
    return "MISSING", 0


def _effort_score(row: dict[str, Any]) -> int:
    """Lower = easier fix."""
    if row["status"] == "LIVE":
        return 0
    blocker = row.get("blocker") or ""
    if "EXPORT" in blocker or "dashboard" in blocker.lower():
        return 1
    if "SHALLOW" in blocker or "PARSER" in blocker:
        return 2
    if "foundation" in blocker.lower() and "R²" not in blocker:
        return 3
    if "R²" in blocker or "regression" in blocker.lower():
        return 5
    return 4


def _estimate_effort(score: int, status: str) -> str:
    if status == "LIVE":
        return "None — wired"
    if score <= 1:
        return "Low (<1h export/wiring)"
    if score == 2:
        return "Medium (cache refresh + re-audit)"
    if score == 3:
        return "Medium–High (macro leg repair)"
    return "High (model gate / new data source)"


def build_board(*, offline: bool = True) -> dict[str, Any]:
    if offline:
        os.environ["HPTL_SKIP_LIVE_FEEDS"] = "1"

    foundation = _read_json(ROOT / "data" / "audits" / "fx_valuation_data_foundation_audit.json")
    v3_public = _read_json(ROOT / "web-dashboard/public/data/fx_valuation_v3_latest.json")
    v3_audit = _read_json(ROOT / "data" / "audits" / "fx_valuation_v3_audit.json")
    validation = _read_json(ROOT / "data" / "audits" / "instrument_validation_coverage.json")

    val_by_market = {
        r.get("instrument_id"): r
        for r in (validation.get("instruments") or [])
        if r.get("instrument_id")
    }

    from hptl.fx.currency_rates import get_currency_rate
    from hptl.fx.fx_macro_history import load_bis_policy_history
    from hptl.fx.fx_spot_history import get_daily_spot_series
    from hptl.valuation.fx_carry_real_yield_v3 import compute_fx_pair_v3

    g10 = {r["currency"]: r for r in (foundation.get("g10_currency_table") or []) if r.get("currency")}

    rows: list[dict[str, Any]] = []
    for pair in TIER1_PAIRS:
        market = PAIR_TO_MARKET[pair]
        base, quote = pair.split("/", 1)

        fpair = (foundation.get("pairs") or {}).get(pair) or {}
        v3_pair = (v3_public.get("pairs") or {}).get(pair) or (v3_audit.get("pairs") or {}).get(pair) or {}
        v3_market = (v3_public.get("markets") or {}).get(market) or {}

        compute = compute_fx_pair_v3(pair)
        spot_series, spot_meta = get_daily_spot_series(pair)
        spot_f = fpair.get("spot_history") or {}
        pol_f = fpair.get("policy_history") or {}
        yld_f = fpair.get("yield_history") or {}

        br = get_currency_rate(base)
        qr = get_currency_rate(quote)

        # BIS for non-USD legs that use BIS policy
        bis_legs: list[str] = []
        bis_status_parts: list[str] = []
        for ccy, ref in (("JPY", "jp"), ("NZD", "nz"), ("CHF", "ch")):
            if ccy in (base, quote):
                st, n = _bis_status(ref)
                bis_legs.append(f"{ccy}:{st}")
                loaded = len(load_bis_policy_history(ref))
                bis_status_parts.append(f"{ccy} cache={loaded}")

        if base == "EUR" or quote == "EUR":
            bis_status_parts.append("EUR: ECB (not BIS)")
        if "USD" in (base, quote):
            bis_status_parts.append("USD: FRED (not BIS)")

        bis_summary = "; ".join(bis_status_parts) if bis_status_parts else "N/A (no BIS leg)"

        g10_base = (g10.get(base) or {}).get("detail") or {}
        g10_quote = (g10.get(quote) or {}).get("detail") or {}

        depth = {
            "spot_daily": len(spot_series),
            "aligned_panel": (v3_audit.get("rows") or [{}])[0],  # placeholder
            "foundation_spot": spot_f.get("observation_count"),
            "policy_align_days": pol_f.get("aligned_days"),
            "yield_intersect": yld_f.get("intersection_days"),
        }
        # aligned from v3 audit rows
        for ar in v3_audit.get("rows") or []:
            if ar.get("pair") == pair:
                depth["aligned_panel"] = ar.get("aligned_obs")
                depth["r_squared"] = ar.get("r_squared")
                break

        val_row = val_by_market.get(market) or {}
        validation_result = val_row.get("overall_status") or val_row.get("validation_status") or "UNKNOWN"

        wired = v3_market.get("wired") or v3_pair.get("wired")
        audit_st = v3_pair.get("audit_status") or compute.audit_status
        foundation_st = fpair.get("overall_status") or "UNKNOWN"

        if wired and audit_st == "PASS":
            status = "LIVE"
            blocker = "None"
            fix = "None"
        elif audit_st == "FAIL" and (depth.get("r_squared") or 0) < 0.08:
            status = "BLOCKED"
            blocker = f"MODEL_GATE_FAIL R²={depth.get('r_squared')}"
            fix = "Do not weaken gate; improve yield/policy depth or accept unavailable"
        elif not v3_public.get("pairs", {}).get(pair):
            status = "BROKEN"
            blocker = "EXPORT_MISSING fx_valuation_v3_latest.json"
            fix = "Run fx_v3_audit + copy to public/data"
        elif foundation_st == "FAIL":
            status = "BLOCKED"
            blocker = fpair.get("root_cause") or "Foundation FAIL"
            fix = "Repair failing macro leg(s); re-run foundation audit"
        elif not wired:
            status = "PARTIAL"
            blocker = "DASHBOARD_KEY_MISMATCH or wiring gate"
            fix = "Confirm FX_V3_LIVE_PAIRS + foundation PASS + re-export"
        else:
            status = "PARTIAL"
            blocker = compute.driver_summary[:120] if compute.driver_summary else "V3 compute FAIL"
            fix = "Inspect missing_inputs in V3 audit row"

        detail = {
            "pair": pair,
            "market": market,
            "valuation_status": f"{audit_st} wired={wired} dev={v3_pair.get('deviation_pct') or compute.deviation_pct}",
            "price_history_status": _leg_status(spot_f) if spot_f else f"PASS ({len(spot_series)} daily, {spot_meta.get('source', '?')})",
            "policy_rate_status": pol_f.get("summary") or f"foundation {foundation_st}",
            "yield_2y_status": yld_f.get("summary") or "see g10 table",
            "cpi_status": (
                f"base CPI {'PASS' if br.cpi_yoy is not None else 'FAIL'} / "
                f"quote CPI {'PASS' if qr.cpi_yoy is not None else 'FAIL'}"
            ),
            "bis_status": bis_summary,
            "data_depth": (
                f"spot={depth.get('spot_daily')} aligned={depth.get('aligned_panel')} "
                f"r²={depth.get('r_squared')} pol_align={depth.get('policy_align_days')}"
            ),
            "validation_result": validation_result,
            "status": status,
            "blocker": blocker,
            "fix_required": fix,
        }
        detail["_effort_score"] = _effort_score(detail)
        detail["estimated_effort"] = _estimate_effort(detail["_effort_score"], status)
        rows.append(detail)

    rows.sort(key=lambda r: (r["_effort_score"], r["pair"]))
    for r in rows:
        r.pop("_effort_score", None)

    return {
        "audit_type": "tier1_fx_repair_board",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tier": 1,
        "pairs": list(TIER1_PAIRS),
        "mode": "offline" if offline else "online",
        "rows": rows,
    }


def _markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Tier 1 FX Repair Board",
        "",
        f"- Generated: {report.get('generated_at')}",
        f"- Scope: {', '.join(report.get('pairs') or [])}",
        f"- Sorted: easiest fix first",
        "",
        "## Summary",
        "",
        "| PAIR | STATUS | BLOCKER | FIX_REQUIRED | ESTIMATED_EFFORT |",
        "|---|---|---|---|---|",
    ]
    for r in report.get("rows") or []:
        lines.append(
            f"| {r['pair']} | {r['status']} | {r['blocker']} | {r['fix_required']} | {r['estimated_effort']} |"
        )

    lines.extend(["", "## Detail per pair", ""])
    for r in report.get("rows") or []:
        lines.extend(
            [
                f"### {r['pair']} ({r['market']})",
                "",
                f"- **Valuation:** {r['valuation_status']}",
                f"- **Price history:** {r['price_history_status']}",
                f"- **Policy rate:** {r['policy_rate_status']}",
                f"- **2Y yield:** {r['yield_2y_status']}",
                f"- **CPI:** {r['cpi_status']}",
                f"- **BIS:** {r['bis_status']}",
                f"- **Data depth:** {r['data_depth']}",
                f"- **Validation:** {r['validation_result']}",
                "",
            ]
        )
    return "\n".join(lines)


def main() -> int:
    report = build_board(offline=True)
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    OUT_MD.write_text(_markdown(report), encoding="utf-8")
    print(f"Wrote {OUT_JSON}")
    print(f"Wrote {OUT_MD}")
    for r in report["rows"]:
        print(f"{r['pair']}\t{r['status']}\t{r['estimated_effort']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
