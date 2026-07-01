"""Generate valuation completion audit for all RADAR_ELIGIBLE instruments."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

RADAR = [
    "NASDAQ / NQ",
    "S&P 500 / ES",
    "Dow / YM",
    "Euro FX / 6E",
    "British Pound / 6B",
    "Japanese Yen / 6J",
    "Swiss Franc / 6S",
    "Australian Dollar / 6A",
    "Canadian Dollar / 6C",
    "NZ Dollar / 6N",
    "Gold",
    "Silver",
    "Copper / HG",
    "Platinum",
    "Palladium",
    "Crude Oil / CL",
    "Natural Gas / NG",
    "Coffee",
    "Cocoa",
    "Corn",
    "Wheat",
    "Soybeans",
    "Sugar",
    "Bitcoin",
    "US Dollar Index / DX",
    "US 2-Year Treasury Yield",
    "US 10-Year Treasury Yield",
    "US 30-Year Treasury Yield",
    "2s10s Yield Curve",
    "10-Year Real Yield",
]

PRIORITY: dict[str, int] = {
    "British Pound / 6B": 1,
    "US Dollar Index / DX": 1,
    "US 2-Year Treasury Yield": 1,
    "US 10-Year Treasury Yield": 1,
    "US 30-Year Treasury Yield": 1,
    "10-Year Real Yield": 1,
    "2s10s Yield Curve": 1,
    "Crude Oil / CL": 2,
    "Natural Gas / NG": 2,
    "S&P 500 / ES": 3,
    "NASDAQ / NQ": 3,
    "Dow / YM": 3,
    "Bitcoin": 4,
    "Coffee": 5,
    "Cocoa": 5,
}

ASSET_CLASS = {
    "NASDAQ / NQ": "indices",
    "S&P 500 / ES": "indices",
    "Dow / YM": "indices",
    "Euro FX / 6E": "fx",
    "British Pound / 6B": "fx",
    "Japanese Yen / 6J": "fx",
    "Swiss Franc / 6S": "fx",
    "Australian Dollar / 6A": "fx",
    "Canadian Dollar / 6C": "fx",
    "NZ Dollar / 6N": "fx",
    "US Dollar Index / DX": "fx",
    "Gold": "metals",
    "Silver": "metals",
    "Copper / HG": "metals",
    "Platinum": "metals",
    "Palladium": "metals",
    "Crude Oil / CL": "energy",
    "Natural Gas / NG": "energy",
    "Coffee": "softs",
    "Cocoa": "softs",
    "Corn": "grains",
    "Wheat": "grains",
    "Soybeans": "grains",
    "Sugar": "grains",
    "Bitcoin": "crypto",
    "US 2-Year Treasury Yield": "rates",
    "US 10-Year Treasury Yield": "rates",
    "US 30-Year Treasury Yield": "rates",
    "2s10s Yield Curve": "rates",
    "10-Year Real Yield": "rates",
}

MODEL_SPEC: dict[str, dict[str, str]] = {
    "fx": {
        "model_id": "fx_carry_real_yield_v3",
        "methodology": "log(spot) ~ policy/2Y/real-yield/CPI diffs + DXY + UST regime",
        "inputs": "Spot, policy rate, 2Y/10Y, CPI YoY, USD legs, DXY percentile, Treasury 2s10s",
        "trust": "Low if R²≥0.08 & foundation PASS; else unpublished",
    },
    "metals": {
        "model_id": "metals_real_yield_v1",
        "methodology": "log(price) ~ DFII10 real yield + log(DXY); percentile composite",
        "inputs": "Canonical spot, FRED DFII10, DTWEXBGS/DXY",
        "trust": "A: n≥156 & R²≥0.15; B: n≥52 & R²≥0.08; C otherwise",
    },
    "grains": {
        "model_id": "agri_stu_percentile_v1",
        "methodology": "USDA PSD stocks-to-use percentile vs price regression",
        "inputs": "USDA WASDE/PSD ending stocks & use, spot, DXY context",
        "trust": "Medium if PSD aligned n≥24; Low if sparse",
    },
    "softs": {
        "model_id": "softs_balance_sheet_v3",
        "methodology": "Origin balance sheet S/U + DXY (planned)",
        "inputs": "ICE spot, origin production/grind stats (ICCO/CONAB/USDA softs)",
        "trust": "TBD after data wiring",
    },
    "energy": {
        "model_id": "energy_inventory_dxy_v3",
        "methodology": "EIA inventory vs 5Y norm + term structure + DXY (planned)",
        "inputs": "WTI/HH spot, EIA weekly stocks, DXY",
        "trust": "TBD after EIA feed",
    },
    "indices": {
        "model_id": "indices_erp_cape_v3",
        "methodology": "CAPE / earnings yield − 10Y = ERP; percentile bands (planned)",
        "inputs": "Shiller CAPE, div yield, DGS10, earnings yield",
        "trust": "TBD — audit blocked without CAPE source",
    },
    "crypto": {
        "model_id": "crypto_liquidity_risk_v3",
        "methodology": "Macro liquidity + real yields + DXY; optional MVRV overlay (planned)",
        "inputs": "BTC spot, DGS10/DFII10, DXY, on-chain MVRV (optional)",
        "trust": "TBD",
    },
    "rates": {
        "model_id": "rates_curve_fair_value_v1",
        "methodology": "Policy anchor + inflation breakeven + term premium fair yield (planned)",
        "inputs": "DGS2/DGS10/DGS30, DFII10, Fed funds, ACM term premium",
        "trust": "TBD — macro series cached; model not built",
    },
}

BLOCKERS: dict[str, str] = {
    "British Pound / 6B": "GBP/USD V3 audit FAIL: R²=0.025 < 0.08 gate; BoE policy history ~85d only",
    "US Dollar Index / DX": "Not in FX_V3_PAIRS; needs usd_broad_fair_value_v1 basket model",
    "US 2-Year Treasury Yield": "No rates asset-class router in engine.py",
    "US 10-Year Treasury Yield": "No rates asset-class router in engine.py",
    "US 30-Year Treasury Yield": "No rates asset-class router in engine.py",
    "2s10s Yield Curve": "Derived from 2Y/10Y fair values — blocked until rates V1",
    "10-Year Real Yield": "Derived from DFII10 fair value vs breakeven — blocked until rates V1",
    "Crude Oil / CL": "energy_inventory_dxy_v3 not implemented; EIA inventory feed missing",
    "Natural Gas / NG": "Same as CL; needs EIA working-gas storage series",
    "S&P 500 / ES": "indices_erp_cape_v3 audit-only; CAPE/Yale + FRED inputs unavailable in audit",
    "NASDAQ / NQ": "No free NASDAQ CAPE/earnings series; ES-relative ERP fallback possible",
    "Dow / YM": "Same as indices; div-yield proxy vs ES ERP",
    "Bitcoin": "crypto_liquidity_risk_v3 not implemented",
    "Coffee": "softs_balance_sheet_v3 not implemented; no USDA softs PSD on disk",
    "Cocoa": "softs_balance_sheet_v3 not implemented; ICCO/grind stats not wired",
}

RADAR_UI = {
    "fx": "ValuationCell → fxValuationV3Display (v3Doc); wired when audit PASS",
    "metals": "ValuationCell → metalsValuationDisplay (valuationDoc)",
    "grains": "ValuationCell → agriValuationDisplay (valuationDoc)",
    "softs": "ValuationCell → location fallback until softs model",
    "energy": "ValuationCell → location fallback",
    "indices": "ValuationCell → location fallback",
    "crypto": "ValuationCell → location fallback",
    "rates": "ValuationCell → location fallback",
}


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def radar_display_status(market: str, block: dict[str, Any], ac: str) -> str:
    wired = block.get("wired") is True
    dev = block.get("deviation_pct")
    bias = block.get("valuation_bias") or block.get("valuation_state")
    if ac == "fx" and wired and dev is not None:
        return f"Live — {dev:+.2f}% {bias}"
    if ac in {"metals", "grains"} and wired and dev is not None:
        return f"Live — {dev:+.2f}% {bias}"
    if ac == "fx" and not wired:
        return "FX N/A (audit fail or out of scope)"
    if ac == "metals":
        return "Metals N/A"
    if ac == "grains":
        return "Agri N/A"
    return "Location fallback (no valuation cell)"


def confluence_status(row: dict[str, Any] | None, ac: str) -> str:
    if not row:
        return "missing row"
    vw = row.get("valuation_wired")
    fx = row.get("fx_valuation") or {}
    if ac == "metals" and vw is True:
        dev = row.get("deviation_pct")
        return f"wired — {dev:+.2f}%" if dev is not None else "wired"
    if ac == "fx" and (fx.get("wired") or fx.get("deviation_pct") is not None):
        return f"fx_valuation block — {fx.get('deviation_pct')}%"
    if ac in {"grains"} and vw is True:
        return "pillar wired"
    if vw is True:
        return "pillar wired"
    if ac == "metals":
        return "unwired (early history) or integrity gate"
    if ac == "fx":
        return "pillar unwired; check fx_valuation attach"
    return "unwired"


def export_status(block: dict[str, Any]) -> str:
    if block.get("wired") is True and block.get("deviation_pct") is not None:
        return "exported"
    if block.get("wired") is True:
        return "partial"
    return "not exported"


def build_audit() -> dict[str, Any]:
    val = load_json(ROOT / "data" / "valuation_latest.json")
    inst = val.get("instruments") or {}
    conf = load_json(ROOT / "web-dashboard/public/data/confluence_history_latest.json")
    records = conf.get("records") or []
    latest_week = max((str(r.get("date") or "") for r in records if r.get("date")), default="")
    conf_latest = {
        str(r.get("market")): r
        for r in records
        if str(r.get("date") or "") == latest_week and r.get("market")
    }

    rows: list[dict[str, Any]] = []
    wired_count = 0
    for market in RADAR:
        block = inst.get(market) or {}
        ac = ASSET_CLASS[market]
        spec = MODEL_SPEC[ac]
        wired = block.get("wired") is True and block.get("deviation_pct") is not None
        if wired:
            wired_count += 1
        conf_row = conf_latest.get(market)
        rows.append(
            {
                "instrument": market,
                "priority": PRIORITY.get(market, 0),
                "asset_class": ac,
                "valuation_model": block.get("model_id") or spec["model_id"],
                "methodology": spec["methodology"],
                "required_inputs": spec["inputs"],
                "trust_grading": block.get("trust_grade") or block.get("confidence") or spec["trust"],
                "deviation_pct": block.get("deviation_pct"),
                "valuation_state": block.get("valuation_bias") or block.get("valuation_state"),
                "export_status": export_status(block),
                "radar_status": radar_display_status(market, block, ac),
                "confluence_status": confluence_status(conf_row, ac),
                "remaining_blockers": BLOCKERS.get(market, "—" if wired else block.get("valuation_reason", "—")),
                "completion": "done" if wired else "pending",
            }
        )

    done = [r for r in rows if r["completion"] == "done"]
    pending = [r for r in rows if r["completion"] == "pending"]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": "RADAR_ELIGIBLE (30 instruments)",
        "valuation_latest_generated_at": val.get("generated_at"),
        "confluence_generated_at": conf.get("generated_at"),
        "confluence_latest_week": latest_week,
        "summary": {
            "total_radar_instruments": len(RADAR),
            "valuation_export_wired": wired_count,
            "coverage_pct": round(wired_count / len(RADAR) * 100, 1),
            "remaining": len(RADAR) - wired_count,
            "goal_journal_v1_threshold": "≥27/30 (90%)",
            "gap_to_goal": max(0, 27 - wired_count),
        },
        "completed_groups": {
            "fx_live": [r["instrument"] for r in done if r["asset_class"] == "fx"],
            "metals_live": [r["instrument"] for r in done if r["asset_class"] == "metals"],
            "agri_live": [r["instrument"] for r in done if r["asset_class"] == "grains"],
        },
        "priority_roadmap": {
            "priority_1": [r for r in rows if r["priority"] == 1],
            "priority_2": [r for r in rows if r["priority"] == 2],
            "priority_3": [r for r in rows if r["priority"] == 3],
            "priority_4": [r for r in rows if r["priority"] == 4],
            "priority_5": [r for r in rows if r["priority"] == 5],
        },
        "instruments": rows,
        "completed": done,
        "pending": pending,
    }


def render_markdown(doc: dict[str, Any]) -> str:
    s = doc["summary"]
    lines = [
        "# Valuation Completion Audit — Journal V1 Gate",
        "",
        f"- **Generated:** {doc['generated_at'][:10]}",
        f"- **Scope:** {doc['scope']}",
        f"- **Sources:** `valuation_latest.json` ({doc['valuation_latest_generated_at'][:10]}), "
        f"`confluence_history_latest.json` ({doc['confluence_generated_at'][:10]}, week {doc['confluence_latest_week']})",
        "",
        "## Executive summary",
        "",
        f"| Metric | Value |",
        f"|---|---:|",
        f"| Valuation export wired | **{s['valuation_export_wired']} / {s['total_radar_instruments']}** ({s['coverage_pct']}%) |",
        f"| Remaining to wire | {s['remaining']} |",
        f"| Journal V1 target (90%) | {s['goal_journal_v1_threshold']} — **{s['gap_to_goal']} instruments short** |",
        "",
        "### Completed pillars",
        "",
        "- **FX (6/8):** " + ", ".join(doc["completed_groups"]["fx_live"] or ["—"]),
        "- **Metals (5/5):** " + ", ".join(doc["completed_groups"]["metals_live"] or ["—"]),
        "- **Agriculture (4/6 radar):** " + ", ".join(doc["completed_groups"]["agri_live"] or ["—"]),
        "",
        "---",
        "",
        "## Full instrument matrix",
        "",
        "| Priority | Instrument | Model | Trust | Deviation | State | Export | Radar | Confluence | Blocker |",
        "|---:|---|---|---|---:|---|---|---|---|---|",
    ]
    for r in doc["instruments"]:
        pri = r["priority"] or "—"
        dev = f"{r['deviation_pct']:+.2f}%" if r["deviation_pct"] is not None else "—"
        state = r["valuation_state"] or "—"
        trust = r["trust_grading"] or "—"
        blocker = (r["remaining_blockers"] or "—")[:80].replace("|", "/")
        lines.append(
            f"| {pri} | {r['instrument']} | `{r['valuation_model']}` | {trust} | {dev} | {state} | "
            f"{r['export_status']} | {r['radar_status'][:40]} | {r['confluence_status'][:35]} | {blocker} |"
        )

    for prio_key, title in [
        ("priority_1", "Priority 1 — FX gaps + Rates"),
        ("priority_2", "Priority 2 — Energy"),
        ("priority_3", "Priority 3 — US Indices"),
        ("priority_4", "Priority 4 — Crypto"),
        ("priority_5", "Priority 5 — Softs"),
    ]:
        items = doc["priority_roadmap"][prio_key]
        if not items:
            continue
        lines.extend(["", f"## {title}", ""])
        for r in items:
            status = "✅ Done" if r["completion"] == "done" else "⏳ Pending"
            lines.append(f"### {r['instrument']} — {status}")
            lines.append("")
            lines.append(f"- **Model:** `{r['valuation_model']}`")
            lines.append(f"- **Methodology:** {r['methodology']}")
            lines.append(f"- **Required inputs:** {r['required_inputs']}")
            lines.append(f"- **Trust grading:** {r['trust_grading']}")
            lines.append(f"- **Export:** {r['export_status']} | **Radar:** {r['radar_status']} | **Confluence:** {r['confluence_status']}")
            if r["completion"] == "done":
                lines.append(f"- **Output:** {r['deviation_pct']:+.2f}% — {r['valuation_state']}")
            else:
                lines.append(f"- **Blocker:** {r['remaining_blockers']}")
            lines.append("")

    lines.extend(
        [
            "---",
            "",
            "## Wiring architecture (reference)",
            "",
            "| Asset class | Export path | Radar cell | Instrument page | Confluence attach |",
            "|---|---|---|---|---|",
            "| FX | `valuation_latest` + `fx_valuation_v3_audit` | `fxValuationV3Display` | `buildChartWorkstation` v3_dev | `fx_valuation_fields_for_market` + pillar |",
            "| Metals | `valuation_latest` | `metalsValuationDisplay` | `buildChartWorkstation` metals_v1 | `pillar_fields` + metals overlay |",
            "| Grains | `valuation_latest` | `agriValuationDisplay` | `buildChartWorkstation` agri | `pillar_fields` (integrity gate) |",
            "| Other | stub in `engine.py` | Location fallback | unavailable subtitle | unwired |",
            "",
            "## Recommended build sequence to 90%",
            "",
            "1. **P1 Rates V1** (+5) — DGS2/DGS10/DGS30/DFII10 already in `macro_cache`; add `rates_curve_fair_value_v1` router.",
            "2. **P1 GBP repair** (+1) — extend BoE policy history; re-run FX V3 audit.",
            "3. **P1 DXY basket** (+1) — `usd_broad_fair_value_v1` from wired G10 crosses.",
            "4. **P2 Energy V1** (+2) — EIA inventory API + `energy_inventory_dxy_v3`.",
            "5. **P3 Indices ERP** (+3) — Yale Shiller CAPE + DGS10; ES primary, NQ/YM relative.",
            "6. **P4 Bitcoin macro** (+1) — `crypto_liquidity_risk_v3` phase 1 (macro-only).",
            "7. **P5 Softs** (+2) — origin balance sheets for Coffee/Cocoa.",
            "",
            f"*At 15/30 today, completing P1+P2 alone reaches 23/30 (77%). Full sequence reaches 30/30.*",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    doc = build_audit()
    out_dir = ROOT / "data" / "audits"
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "valuation_completion_audit.json"
    md_path = out_dir / "valuation_completion_audit.md"
    json_path.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(doc), encoding="utf-8")
    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}")
    print("Summary:", doc["summary"])


if __name__ == "__main__":
    main()
