#!/usr/bin/env python3
"""Phase 1 valuation inventory — discovery/audit only (no logic changes)."""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

DATA = ROOT / "data"
PUBLIC = ROOT / "web-dashboard" / "public" / "data"
OUT_MD = DATA / "audits" / "valuation_inventory.md"
OUT_JSON = DATA / "audits" / "valuation_inventory.json"

RECOMMENDED: dict[str, str] = {
    "fx": "macro rates, yield differentials, real yields, inflation, DXY/Treasury regime",
    "metals": "real yields, USD/DXY, inflation expectations, positioning overlay",
    "indices": "earnings yield, CAPE, treasury yields, equity risk premium, liquidity",
    "commodities": "inventory/stocks-to-use, DXY, seasonality (subgroup-specific)",
    "energy": "EIA inventories, DXY, seasonality",
    "grains": "USDA/WASDE stocks-to-use, DXY, seasonality",
    "softs": "balance sheets, DXY, weather, seasonal adjustment",
    "crypto": "liquidity, DXY, real yields, risk appetite",
    "macro": "N/A — macro series are valuation inputs, not valuation targets",
    "bonds": "N/A — yield instruments are inputs; relative value vs policy/inflation",
    "other": "undefined",
}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_fx_pair(instrument_id: str) -> str | None:
    from hptl.fx.fx_valuation import resolve_pair_currencies

    r = resolve_pair_currencies(instrument_id)
    return f"{r[0]}/{r[1]}" if r else None


def _valuation_subgroup(asset_class: str, subgroup: str | None) -> str:
    if asset_class == "commodities":
        sg = subgroup or ""
        if sg == "energy":
            return "energy"
        if sg == "ag":
            return "grains"
        if sg == "soft":
            return "softs"
    return asset_class or "other"


def _load_engines() -> list[dict[str, Any]]:
    return [
        {
            "id": "fx_carry_real_yield_v3",
            "path": "src/hptl/valuation/fx_carry_real_yield_v3.py",
            "status": "live",
            "asset_classes": ["fx"],
            "description": "Real-yield + policy-rate regression fair value (V3 pillar)",
        },
        {
            "id": "fx_institutional_valuation_v2",
            "path": "src/hptl/fx/fx_institutional_valuation.py",
            "status": "live_parallel",
            "asset_classes": ["fx"],
            "description": "Currency scoring + institutional pair valuation (V2 export)",
        },
        {
            "id": "fx_valuation_v1",
            "path": "src/hptl/fx/fx_valuation.py",
            "status": "legacy_confluence",
            "asset_classes": ["fx"],
            "description": "Yield-differential scoring with 52w price percentile overlay",
        },
        {
            "id": "fx_fair_value_stub",
            "path": "src/hptl/fx/fx_fair_value.py",
            "status": "disabled_stub",
            "asset_classes": ["fx"],
            "description": "Reserved regression structure; always unavailable",
        },
        {
            "id": "indices_erp_cape_v2_audit",
            "path": "src/hptl/valuation/index_valuation_v2_audit.py",
            "status": "audit_only",
            "asset_classes": ["indices"],
            "description": "S&P 500 CAPE/ERP audit — not wired to pillar",
        },
        {
            "id": "index_macro_valuation_experimental",
            "path": "src/hptl/valuation/index_macro_valuation.py",
            "status": "disabled_experimental",
            "asset_classes": ["indices"],
            "description": "UMCSENT+DGS10 shadow — explicitly not wired",
        },
        {
            "id": "valuation_engine_router",
            "path": "src/hptl/valuation/engine.py",
            "status": "live_router",
            "asset_classes": ["all"],
            "description": "Routes FX to V3; others return UNAVAILABLE + roadmap",
        },
        {
            "id": "location_engine",
            "path": "src/hptl/location/engine.py",
            "status": "live_non_valuation",
            "asset_classes": ["all"],
            "description": "52w price percentile — shown in ValuationCell for non-FX (not fundamental valuation)",
        },
    ]


def _load_roadmap() -> dict[str, dict[str, str]]:
    from hptl.valuation.engine import ASSET_CLASS_ROADMAP

    return dict(ASSET_CLASS_ROADMAP)


def _load_v3_pair_status() -> dict[str, dict[str, Any]]:
    doc = _read_json(DATA / "audits" / "fx_valuation_v3_audit.json")
    out: dict[str, dict[str, Any]] = {}
    for row in doc.get("rows") or []:
        pair = str(row.get("pair") or "")
        if pair and pair not in out:
            out[pair] = row
    pairs_doc = _read_json(PUBLIC / "fx_valuation_v3_latest.json").get("pairs") or {}
    for pair, block in pairs_doc.items():
        if pair not in out:
            out[pair] = block
        else:
            out[pair] = {**out[pair], **{k: v for k, v in block.items() if k not in out[pair]}}
    return out


def _load_v2_pairs() -> set[str]:
    doc = _read_json(PUBLIC / "fx_valuation_latest.json")
    pairs = doc.get("pairs") or []
    if isinstance(pairs, list):
        return {str(p.get("pair")) for p in pairs if p.get("pair")}
    if isinstance(pairs, dict):
        return set(pairs.keys())
    return set()


def _classify_instrument(
    *,
    instrument_id: str,
    display_name: str,
    asset_class: str,
    subgroup: str | None,
    pillar: dict[str, Any],
    v3_pairs: dict[str, dict[str, Any]],
    v2_pairs: set[str],
    roadmap: dict[str, dict[str, str]],
) -> dict[str, Any]:
    val_subgroup = _valuation_subgroup(asset_class, subgroup)
    pair_id = _resolve_fx_pair(instrument_id) if asset_class == "fx" else None
    v3 = v3_pairs.get(pair_id or "") if pair_id else None
    pillar_wired = bool(pillar.get("wired"))
    model_id = pillar.get("model_id") or pillar.get("valuation_model_id")

    entry: dict[str, Any] = {
        "instrument_id": instrument_id,
        "display_name": display_name,
        "asset_class": asset_class,
        "subgroup": subgroup,
        "valuation_subgroup": val_subgroup,
        "fx_pair_id": pair_id,
        "valuation_engine_used": None,
        "valuation_status": "UNKNOWN",
    }

    if pillar_wired:
        entry["valuation_status"] = "PASS"
        entry["valuation_engine_used"] = model_id or "fx_carry_real_yield_v3"
        entry["detail"] = {
            "source_files": [
                "src/hptl/valuation/fx_carry_real_yield_v3.py",
                "src/hptl/valuation/fx_v3_audit.py",
                "src/hptl/valuation/export.py",
                "web-dashboard/src/components/ValuationCell.jsx",
                "web-dashboard/src/components/FxValuationV3Panel.jsx",
            ],
            "data_sources": [
                "price_store (spot weekly/daily)",
                "fx currency rates (policy, 2Y yields, CPI)",
                "DXY / Treasury regime from price_store + macro cache",
                "data/audits/fx_valuation_data_foundation_audit.json",
            ],
            "calculation_summary": (
                "Log-linear regression of weekly spot on 2Y yield diff + policy rate diff; "
                "fair value from fitted coefficients; deviation_pct vs spot; "
                "DXY/Treasury regime labels as macro context (not in regression)."
            ),
            "output_fields": [
                "fair_value",
                "deviation_pct",
                "valuation_state",
                "valuation_bias",
                "confidence",
                "drivers",
                "dxy_regime",
                "treasury_regime",
                "explanation",
                "wired",
            ],
            "pillar_fields": {k: pillar.get(k) for k in (
                "valuation_state", "valuation_bias", "fair_value", "deviation_pct",
                "confidence", "audit_status", "wired",
            )},
        }
        return entry

    # Index V2 audit (S&P only) — partial, not wired
    if instrument_id in ("S&P 500 / ES", "US SPX 500"):
        entry["valuation_status"] = "PARTIAL"
        entry["valuation_engine_used"] = "indices_erp_cape_v2_audit"
        entry["detail"] = {
            "what_works": "Offline audit computes CAPE percentile, earnings yield, ERP vs DGS10 when data sources available",
            "what_missing": "Not promoted to valuation_latest.json; FRED key often missing; no live dashboard panel",
            "blockers": [
                "integration_status: not wired to live scanner or valuation pillar",
                "FRED/Yale Shiller source availability",
                "Planned pillar model indices_erp_cape_v3 not implemented",
            ],
            "source_files": ["src/hptl/valuation/index_valuation_v2_audit.py"],
        }
        return entry

    if asset_class == "fx" and pair_id:
        in_v3_scope = pair_id in {
            "EUR/USD", "GBP/USD", "AUD/USD", "NZD/USD", "USD/JPY", "USD/CHF",
            "USD/CAD", "EUR/JPY", "AUD/JPY", "NZD/JPY", "EUR/GBP", "EUR/AUD", "GBP/JPY",
        }
        audit_status = (v3 or {}).get("audit_status")
        in_v2 = pair_id in v2_pairs

        if in_v3_scope and audit_status == "PASS":
            entry["valuation_status"] = "PARTIAL"
            entry["valuation_engine_used"] = "fx_carry_real_yield_v3"
            entry["detail"] = {
                "what_works": "V3 model computes fair value; audit PASS",
                "what_missing": "Not wired to valuation pillar / scanner (live-scope or duplicate pair)",
                "blockers": [(v3 or {}).get("valuation_reason") or "live wiring gate"],
            }
            return entry

        if in_v3_scope and audit_status == "FAIL":
            entry["valuation_status"] = "PARTIAL"
            entry["valuation_engine_used"] = "fx_carry_real_yield_v3"
            entry["detail"] = {
                "what_works": "V3 regression runs but fails audit gate (R² or aligned obs)",
                "what_missing": "Fair value not promoted; wired=false",
                "blockers": [
                    f"audit_status=FAIL r_squared={(v3 or {}).get('r_squared')}",
                    (v3 or {}).get("valuation_reason"),
                ],
            }
            return entry

        if in_v2:
            entry["valuation_status"] = "PARTIAL"
            entry["valuation_engine_used"] = "fx_institutional_valuation_v2"
            entry["detail"] = {
                "what_works": "V2 currency scores + pair institutional valuation in fx_valuation_latest.json",
                "what_missing": "Not wired to fundamental valuation pillar; scanner uses V3 only for FX",
                "blockers": ["Parallel legacy track; pillar requires V3 audit PASS"],
                "source_files": [
                    "src/hptl/fx/fx_institutional_valuation.py",
                    "src/hptl/fx/fx_valuation_export.py",
                    "web-dashboard/src/components/FxValuationPanel.jsx",
                ],
            }
            return entry

        if pair_id:
            entry["valuation_status"] = "PARTIAL"
            entry["valuation_engine_used"] = "fx_valuation_v1_confluence"
            entry["detail"] = {
                "what_works": "Confluence may attach V1 yield-diff fields if pair resolves",
                "what_missing": "No fair-value regression; no V2/V3 export coverage",
                "blockers": ["Exotic/emerging FX cross — no institutional export row"],
            }
            return entry

    # Experimental disabled
    if val_subgroup == "indices" and instrument_id not in ("S&P 500 / ES", "US SPX 500"):
        rm = roadmap.get("indices", {})
        entry["valuation_status"] = "MISSING"
        entry["valuation_engine_used"] = rm.get("model_id")
        entry["detail"] = {
            "framework_exists": True,
            "planned_phase": rm.get("phase"),
            "recommended_category": RECOMMENDED["indices"],
            "note": "index_macro_valuation.py exists but DISABLED for all indices",
        }
        return entry

    if val_subgroup in roadmap:
        rm = roadmap[val_subgroup]
        entry["valuation_status"] = "MISSING"
        entry["valuation_engine_used"] = rm.get("model_id")
        entry["detail"] = {
            "framework_exists": True,
            "planned_phase": rm.get("phase"),
            "planned_drivers": rm.get("drivers"),
            "recommended_category": RECOMMENDED.get(val_subgroup, RECOMMENDED.get(asset_class, "")),
        }
        return entry

    if asset_class in ("macro", "bonds"):
        entry["valuation_status"] = "DISABLED"
        entry["valuation_engine_used"] = None
        entry["detail"] = {
            "reason": "Macro/bond instruments are valuation inputs, not fair-value targets in current architecture",
            "recommended_category": RECOMMENDED[asset_class],
        }
        return entry

    entry["valuation_status"] = "MISSING"
    entry["valuation_engine_used"] = None
    entry["detail"] = {
        "framework_exists": False,
        "recommended_category": RECOMMENDED.get(asset_class, RECOMMENDED["other"]),
    }
    return entry


def _pipelines() -> dict[str, list[str]]:
    return {
        "fx": [
            "Raw: price_store daily/weekly, fx currency_rates.json, macro caches (DGS10, DXY)",
            "Transform: fx_macro_history, currency_rates adapters, foundation audit",
            "Engine: fx_carry_real_yield_v3 (pillar) | fx_institutional_valuation (V2 parallel)",
            "Export: fx_valuation_v3_latest.json, valuation_latest.json, fx_valuation_latest.json",
            "Dashboard: ValuationCell, FxValuationV3Panel, FxValuationPanel (V2), buildChartWorkstation",
        ],
        "indices": [
            "Raw: FRED/Yale Shiller CAPE, FMP ^GSPC (audit), DGS10",
            "Transform: index_valuation_v2_audit percentile ranks",
            "Engine: indices_erp_cape_v2_audit (audit-only) — pillar router returns UNAVAILABLE",
            "Export: data/audits/index_valuation_v2_audit.json only",
            "Dashboard: ValuationCell shows Location (not valuation); no index fair-value panel",
        ],
        "metals": [
            "Raw: price_store, macro (rates/DXY) — inputs only today",
            "Transform: none for valuation",
            "Engine: engine.compute_valuation → UNAVAILABLE (planned metals_real_yield_dxy_v3)",
            "Export: valuation_latest.json (wired=false)",
            "Dashboard: ValuationCell → Location percentile",
        ],
        "energy": [
            "Raw: EIA/macro planned; price_store today",
            "Transform: none for valuation",
            "Engine: UNAVAILABLE (planned energy_inventory_dxy_v3)",
            "Export: valuation_latest.json",
            "Dashboard: Location percentile",
        ],
        "grains": [
            "Raw: USDA/WASDE planned; price_store today",
            "Engine: UNAVAILABLE (planned grains_stocks_to_use_v3)",
            "Export: valuation_latest.json",
            "Dashboard: Location percentile",
        ],
        "softs": [
            "Engine: UNAVAILABLE (planned softs_balance_sheet_v3)",
            "Dashboard: Location percentile",
        ],
        "crypto": [
            "Engine: UNAVAILABLE (planned crypto_liquidity_risk_v3)",
            "Dashboard: Location percentile",
        ],
        "macro_bonds": [
            "Role: valuation inputs only (yields, curve)",
            "Engine: DISABLED as valuation targets",
            "Dashboard: no valuation column semantics",
        ],
    }


def _dead_code() -> list[dict[str, str]]:
    return [
        {
            "path": "src/hptl/fx/fx_fair_value.py",
            "category": "abandoned_stub",
            "note": "V1 prep-only; estimate_fair_value always unavailable",
        },
        {
            "path": "src/hptl/valuation/index_macro_valuation.py",
            "category": "disabled_experimental",
            "note": "Explicitly NOT wired; UMCSENT proxy only",
        },
        {
            "path": "docs/VALUATION_ENGINE_PLAN.md",
            "category": "stale_docs",
            "note": "Predates V3 FX implementation",
        },
        {
            "path": "web-dashboard/src/components/FxValuationV3DevPanel.jsx",
            "category": "deprecated_alias",
            "note": "Alias of FxValuationV3Panel",
        },
        {
            "path": "src/hptl/fx/fx_valuation.py + fx_valuation_attach.py",
            "category": "duplicate_legacy",
            "note": "V1 yield-diff parallel to V2/V3 — still attached in confluence",
        },
        {
            "path": "src/hptl/fx/fx_institutional_valuation.py + fx_valuation_export.py",
            "category": "duplicate_parallel",
            "note": "V2 track not integrated into valuation pillar",
        },
        {
            "path": "backend/tests/shadow_valuation_sp500.py",
            "category": "shadow_test",
            "note": "Offline shadow only",
        },
    ]


def build_inventory() -> dict[str, Any]:
    from hptl.confluence.build_decision_table import TARGET_MARKETS
    from hptl.markets.instrument_registry import get_instrument

    registry_path = DATA / "config" / "instrument_registry.json"
    registry_doc = _read_json(registry_path)
    registry_instruments = registry_doc.get("instruments") or []

    pillar_doc = _read_json(DATA / "valuation_latest.json")
    pillar_instruments = pillar_doc.get("instruments") or {}
    v3_pairs = _load_v3_pair_status()
    v2_pairs = _load_v2_pairs()
    roadmap = _load_roadmap()

    instruments_out: list[dict[str, Any]] = []
    for iid in TARGET_MARKETS:
        spec = get_instrument(iid)
        reg = next((x for x in registry_instruments if x.get("instrument_id") == iid), {})
        display = reg.get("display_name") or iid
        asset_class = (spec.asset_class if spec else reg.get("asset_class")) or "other"
        subgroup = (spec.subgroup if spec else reg.get("subgroup"))
        pillar = pillar_instruments.get(iid) or {}
        instruments_out.append(
            _classify_instrument(
                instrument_id=iid,
                display_name=display,
                asset_class=asset_class,
                subgroup=subgroup,
                pillar=pillar,
                v3_pairs=v3_pairs,
                v2_pairs=v2_pairs,
                roadmap=roadmap,
            )
        )

    status_counts = Counter(i["valuation_status"] for i in instruments_out)
    by_class: dict[str, Counter] = defaultdict(Counter)
    for i in instruments_out:
        by_class[i["asset_class"]][i["valuation_status"]] += 1

    summary_rows = []
    for ac in sorted(by_class.keys()):
        c = by_class[ac]
        total = sum(c.values())
        summary_rows.append({
            "asset_class": ac,
            "instruments": total,
            "covered_pass": c.get("PASS", 0),
            "partial": c.get("PARTIAL", 0),
            "missing": c.get("MISSING", 0),
            "disabled": c.get("DISABLED", 0),
            "unknown": c.get("UNKNOWN", 0),
        })

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "phase": "1_inventory_only",
        "note": "Discovery audit — no valuation logic modified",
        "universe": {
            "total_instruments": len(instruments_out),
            "source": "hptl.confluence.build_decision_table.TARGET_MARKETS",
            "registry_path": str(registry_path.relative_to(ROOT)),
        },
        "status_summary": dict(status_counts),
        "asset_class_summary": summary_rows,
        "engines": _load_engines(),
        "roadmap": roadmap,
        "pipelines_by_asset_class": _pipelines(),
        "dead_code_and_duplicates": _dead_code(),
        "exports": [
            {"id": "valuation_latest", "paths": ["data/valuation_latest.json", "web-dashboard/public/data/valuation_latest.json"]},
            {"id": "fx_valuation_v3_latest", "paths": ["web-dashboard/public/data/fx_valuation_v3_latest.json"]},
            {"id": "fx_valuation_latest_v2", "paths": ["data/processed/fx_valuation_latest.json", "web-dashboard/public/data/fx_valuation_latest.json"]},
            {"id": "fx_valuation_history_latest", "paths": ["web-dashboard/public/data/fx_valuation_history_latest.json"]},
            {"id": "index_valuation_v2_audit", "paths": ["data/audits/index_valuation_v2_audit.json"]},
        ],
        "dashboard_components": [
            "web-dashboard/src/components/ValuationCell.jsx",
            "web-dashboard/src/components/FxValuationV3Panel.jsx",
            "web-dashboard/src/components/FxValuationPanel.jsx",
            "web-dashboard/src/components/FxValuationHistoryChart.jsx",
            "web-dashboard/src/hooks/useFxValuation.js",
            "web-dashboard/src/hooks/useFxValuationV3Dev.js",
        ],
        "instruments": instruments_out,
    }


def _render_md(doc: dict[str, Any]) -> str:
    lines: list[str] = [
        "# HPTL Valuation Inventory (Phase 1)",
        "",
        f"Generated: {doc['generated_at']}",
        "",
        "> Discovery and audit only — no valuation logic was modified.",
        "",
        "## Executive summary",
        "",
        f"- **Universe:** {doc['universe']['total_instruments']} dashboard instruments",
        f"- **PASS (pillar wired):** {doc['status_summary'].get('PASS', 0)}",
        f"- **PARTIAL:** {doc['status_summary'].get('PARTIAL', 0)}",
        f"- **MISSING:** {doc['status_summary'].get('MISSING', 0)}",
        f"- **DISABLED:** {doc['status_summary'].get('DISABLED', 0)}",
        "",
        "Only **FX V3.0** (`fx_carry_real_yield_v3`) is wired to the fundamental valuation pillar. "
        "Non-FX scanner column shows **Location** (52w price percentile), not valuation.",
        "",
        "## Asset class summary",
        "",
        "| Asset Class | Instruments | Covered (PASS) | Partial | Missing | Disabled |",
        "|-------------|------------:|-----------------:|--------:|--------:|---------:|",
    ]
    for row in doc["asset_class_summary"]:
        lines.append(
            f"| {row['asset_class']} | {row['instruments']} | {row['covered_pass']} | "
            f"{row['partial']} | {row['missing']} | {row['disabled']} |"
        )

    lines.extend(["", "## Valuation engines", ""])
    for eng in doc["engines"]:
        lines.append(f"### `{eng['id']}` ({eng['status']})")
        lines.append(f"- Path: `{eng['path']}`")
        lines.append(f"- {eng['description']}")
        lines.append("")

    lines.extend(["## Pipeline by asset class", ""])
    for ac, steps in doc["pipelines_by_asset_class"].items():
        lines.append(f"### {ac}")
        for s in steps:
            lines.append(f"1. {s}")
        lines.append("")

    lines.extend(["## Dead code / duplicates / legacy", ""])
    for d in doc["dead_code_and_duplicates"]:
        lines.append(f"- **`{d['path']}`** — {d['category']}: {d['note']}")
    lines.append("")

    lines.extend(["## Instrument inventory", ""])
    lines.append("| instrument_id | asset_class | engine | status |")
    lines.append("|---------------|-------------|--------|--------|")
    for i in doc["instruments"]:
        eng = i.get("valuation_engine_used") or "—"
        lines.append(f"| {i['instrument_id']} | {i['asset_class']} | {eng} | **{i['valuation_status']}** |")

    pass_inst = [i for i in doc["instruments"] if i["valuation_status"] == "PASS"]
    partial_inst = [i for i in doc["instruments"] if i["valuation_status"] == "PARTIAL"]
    missing_inst = [i for i in doc["instruments"] if i["valuation_status"] == "MISSING"]

    lines.extend(["", "## PASS instruments (detail)", ""])
    for i in pass_inst:
        lines.append(f"### {i['instrument_id']}")
        d = i.get("detail") or {}
        lines.append(f"- Engine: `{i['valuation_engine_used']}`")
        lines.append(f"- Sources: {', '.join(d.get('data_sources', []))}")
        lines.append(f"- Method: {d.get('calculation_summary', '')}")
        pf = d.get("pillar_fields") or {}
        lines.append(f"- Current state: {pf.get('valuation_state')} ({pf.get('deviation_pct')}% dev)")
        lines.append("")

    lines.extend(["## PARTIAL instruments (grouped)", ""])
    by_eng: dict[str, list] = defaultdict(list)
    for i in partial_inst:
        by_eng[i.get("valuation_engine_used") or "unknown"].append(i["instrument_id"])
    for eng, ids in sorted(by_eng.items()):
        lines.append(f"### {eng} ({len(ids)} instruments)")
        sample = next((i for i in partial_inst if i.get("valuation_engine_used") == eng), partial_inst[0])
        d = sample.get("detail") or {}
        if d.get("what_works"):
            lines.append(f"- Works: {d['what_works']}")
        if d.get("what_missing"):
            lines.append(f"- Missing: {d['what_missing']}")
        lines.append(f"- Examples: {', '.join(ids[:8])}{'…' if len(ids) > 8 else ''}")
        lines.append("")

    lines.extend(["## MISSING instruments (by asset class)", ""])
    by_ac: dict[str, list] = defaultdict(list)
    for i in missing_inst:
        by_ac[i["asset_class"]].append(i)
    for ac, items in sorted(by_ac.items()):
        rm = items[0].get("valuation_engine_used") or "—"
        rec = (items[0].get("detail") or {}).get("recommended_category", "")
        lines.append(f"### {ac} ({len(items)} instruments)")
        lines.append(f"- Planned engine: `{rm}`")
        lines.append(f"- Recommended drivers: {rec}")
        lines.append(f"- Instruments: {', '.join(x['instrument_id'] for x in items[:6])}{'…' if len(items) > 6 else ''}")
        lines.append("")

    lines.append("## Full machine-readable inventory")
    lines.append("")
    lines.append("See `data/audits/valuation_inventory.json` for per-instrument detail blocks.")
    return "\n".join(lines)


def main() -> None:
    doc = build_inventory()
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    OUT_MD.write_text(_render_md(doc), encoding="utf-8")
    print(f"Wrote {OUT_JSON}")
    print(f"Wrote {OUT_MD}")
    print("\nStatus summary:", doc["status_summary"])
    print("\nAsset class summary:")
    for row in doc["asset_class_summary"]:
        print(
            f"  {row['asset_class']:12} total={row['instruments']:3} "
            f"PASS={row['covered_pass']:2} PARTIAL={row['partial']:3} "
            f"MISSING={row['missing']:3} DISABLED={row['disabled']:2}"
        )


if __name__ == "__main__":
    main()
