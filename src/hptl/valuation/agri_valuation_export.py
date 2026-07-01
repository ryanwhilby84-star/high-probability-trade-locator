"""Export agriculture valuation artifacts + audits."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.valuation.agri_fundamental_valuation import (
    AGRI_VALUATION_MARKETS,
    build_all_agri_valuations,
    build_data_inventory,
    discover_instrument_data,
)

AGRI_OUT = Path("data/agri_valuation_latest.json")
PUBLIC_AGRI_OUT = PROJECT_ROOT / "web-dashboard/public/data/agri_valuation_latest.json"
INVENTORY_MD = Path("data/audits/agri_valuation_data_inventory.md")
AUDIT_MD = Path("data/audits/agri_valuation_audit.md")


def render_data_inventory_md(inv: dict[str, Any]) -> str:
    lines = [
        "# Agriculture Valuation Data Inventory",
        "",
        f"- Generated: `{inv.get('generated_at')}`",
        f"- Balance sheet ingest dir: `{inv.get('balance_sheet_ingest_dir')}`",
        f"- Expected source: **{inv.get('balance_sheet_expected_source')}**",
        "",
        "| Instrument | Futures | Subgroup | Price | Balance sheet | Aligned pairs | Model | Confidence |",
        "| --- | --- | --- | --- | --- | ---: | --- | --- |",
    ]
    for row in inv.get("instruments") or []:
        price_ok = "✓" if row.get("price_spot") is not None else "✗"
        bs_ok = "✓" if row.get("balance_sheet_on_disk") else "✗"
        lines.append(
            f"| {row.get('market')} | {row.get('futures') or '—'} | {row.get('subgroup')} | "
            f"{price_ok} ({row.get('price_source')}) | {bs_ok} ({row.get('balance_sheet_observations') or 0} obs) | "
            f"{row.get('aligned_price_stu_pairs') or 0} | {row.get('recommended_model_type')} | "
            f"{row.get('confidence')} |"
        )
        for miss in row.get("data_missing") or []:
            lines.append(f"| | | | **Missing:** {miss} | | | | |")
    lines.extend(
        [
            "",
            "## Per-instrument detail",
            "",
        ]
    )
    for row in inv.get("instruments") or []:
        lines.append(f"### {row.get('market')} ({row.get('futures')})")
        lines.append(f"- Price spot: {row.get('price_spot')} · source `{row.get('price_source')}` · depth {row.get('price_depth_bars')}")
        lines.append(f"- Balance sheet on disk: {row.get('balance_sheet_on_disk')} · path `{row.get('balance_sheet_path') or '—'}`")
        lines.append(f"- COT commercial available: {row.get('cot_commercial_available')}")
        if row.get("macro_price_cache_files"):
            lines.append(f"- FRED price cache: {', '.join(row['macro_price_cache_files'])}")
        lines.append(f"- Recommended model: **{row.get('recommended_model_type')}**")
        lines.append(f"- Valuation anchor possible: **{row.get('valuation_anchor_possible')}**")
        lines.append("")
    return "\n".join(lines)


def render_agri_audit_md(payload: dict[str, Any]) -> str:
    lines = [
        "# Agriculture Valuation Audit",
        "",
        f"- Generated: `{payload.get('generated_at')}`",
        f"- Engine: `{payload.get('engine')}`",
        f"- Wired: **{payload.get('summary', {}).get('wired_count')}** / {payload.get('summary', {}).get('total_instruments')}",
        "",
        "## Instruments valued",
        "",
    ]
    valued = [m for m in AGRI_VALUATION_MARKETS if (payload.get("instruments") or {}).get(m, {}).get("wired")]
    if valued:
        for m in valued:
            v = payload["instruments"][m]
            lines.append(
                f"- **{m}**: fair {v.get('fair_value')} · spot {v.get('spot_price')} · "
                f"dev {v.get('deviation_pct')}% · {v.get('model_id')} · confidence {v.get('confidence')}"
            )
    else:
        lines.append("- None (all blocked pending USDA balance sheet ingest).")

    lines.extend(["", "## Instruments unavailable", ""])
    for m in AGRI_VALUATION_MARKETS:
        v = (payload.get("instruments") or {}).get(m) or {}
        if v.get("wired"):
            continue
        reason = v.get("unavailable_reason") or v.get("valuation_reason") or "—"
        inv = discover_instrument_data(m)
        lines.append(f"- **{m}**: {reason}")
        lines.append(f"  - Model attempted: `{v.get('model_id')}` · data depth {v.get('data_depth')}")

    lines.extend(
        [
            "",
            "## Model summary",
            "",
            "| Market | Wired | Model | Fair value | Deviation % | Confidence | Blocker |",
            "| --- | --- | --- | ---: | ---: | --- | --- |",
        ]
    )
    for m in AGRI_VALUATION_MARKETS:
        v = (payload.get("instruments") or {}).get(m) or {}
        lines.append(
            f"| {m} | {'yes' if v.get('wired') else 'no'} | {v.get('model_id') or '—'} | "
            f"{v.get('fair_value') if v.get('fair_value') is not None else '—'} | "
            f"{v.get('deviation_pct') if v.get('deviation_pct') is not None else '—'} | "
            f"{v.get('confidence')} | {(v.get('unavailable_reason') or '—')[:80]} |"
        )
    lines.append("")
    return "\n".join(lines)


def write_agri_valuation_exports(*, as_of_week: str | None = None) -> dict[str, Path]:
    payload = build_all_agri_valuations(as_of_week=as_of_week)
    inv = build_data_inventory()
    text = json.dumps(payload, indent=2, ensure_ascii=False)

    for path in (AGRI_OUT, PUBLIC_AGRI_OUT):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    INVENTORY_MD.parent.mkdir(parents=True, exist_ok=True)
    INVENTORY_MD.write_text(render_data_inventory_md(inv), encoding="utf-8")
    AUDIT_MD.write_text(render_agri_audit_md(payload), encoding="utf-8")

    return {
        "agri_valuation": AGRI_OUT,
        "inventory_md": INVENTORY_MD,
        "audit_md": AUDIT_MD,
    }


def merge_agri_into_valuation_latest(valuation_doc: dict[str, Any]) -> dict[str, Any]:
    """Overlay agri pillar results onto valuation_latest without changing FX rows."""
    agri = build_all_agri_valuations(as_of_week=valuation_doc.get("calendar_week"))
    instruments = dict(valuation_doc.get("instruments") or {})
    for market, row in (agri.get("instruments") or {}).items():
        prev = instruments.get(market) or {}
        merged = {**prev, **row}
        merged["market"] = market
        merged["valuation_pillar"] = "agri_fundamental"
        instruments[market] = merged
    wired = sum(1 for v in instruments.values() if v.get("wired"))
    out = dict(valuation_doc)
    out["instruments"] = instruments
    summary = dict(out.get("summary") or {})
    summary["total_instruments"] = len(instruments)
    summary["wired_count"] = wired
    summary["unavailable_count"] = len(instruments) - wired
    summary["agri_wired_count"] = sum(
        1 for m in AGRI_VALUATION_MARKETS if (instruments.get(m) or {}).get("wired")
    )
    out["summary"] = summary
    out["agri_valuation_summary"] = agri.get("summary")
    return out
