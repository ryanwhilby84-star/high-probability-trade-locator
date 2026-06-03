"""Opportunity Engine distribution report + before/after ranking comparison."""
from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.thesis_tracker.opportunity import build_opportunity
from hptl.thesis_tracker.opportunity_baseline import BASELINE_PATH, evaluate_three_pillar_opportunity

REPORT_JSON = PROJECT_ROOT / "data/opportunity_engine_distribution_report.json"
REPORT_MD = PROJECT_ROOT / "docs/OPPORTUNITY_ENGINE_DISTRIBUTION_REPORT.md"
THESIS_EXPORT = PROJECT_ROOT / "web-dashboard/public/data/thesis_tracker_latest.json"
SCANNER_OUT = PROJECT_ROOT / "web-dashboard/public/data/scanner_latest.json"


def _pillar_pass_label(p: dict[str, Any]) -> str:
    if p.get("pass") is True:
        return "PASS"
    if p.get("pass") is False:
        return "FAIL"
    return "—"


def _row_from_thesis(thesis: dict[str, Any]) -> dict[str, Any]:
    opp = thesis.get("opportunity") if isinstance(thesis.get("opportunity"), dict) else build_opportunity(thesis)
    align = opp.get("alignment") or {}
    pillars = {p["pillar"]: p for p in (align.get("pillars") or []) if isinstance(p, dict) and p.get("pillar")}
    return {
        "market": thesis.get("market"),
        "thesis_id": thesis.get("thesis_id"),
        "institutions": _pillar_pass_label(pillars.get("institutions") or {}),
        "retail": _pillar_pass_label(pillars.get("retail") or {}),
        "location": _pillar_pass_label(pillars.get("location") or {}),
        "valuation": _pillar_pass_label(pillars.get("valuation") or {}),
        "seasonality": _pillar_pass_label(pillars.get("seasonality") or {}),
        "valuation_bias": (thesis.get("snapshots") or [{}])[-1].get("valuation_bias")
        if thesis.get("snapshots")
        else None,
        "seasonality_bias": (thesis.get("snapshots") or [{}])[-1].get("seasonality_bias")
        if thesis.get("snapshots")
        else None,
        "final_score": align.get("pass"),
        "final_state": align.get("label"),
        "action": opp.get("action"),
        "rank_score": opp.get("rank_score"),
    }


def _distribution_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    c = Counter(int(r.get("final_score") or 0) for r in rows)
    return {f"{k}/5": c.get(k, 0) for k in range(6)}


def _compare_rankings(
    baseline: dict[str, Any],
    after_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_market = {r["market"]: r for r in after_rows if r.get("market")}
    base_by = {r["market"]: r for r in (baseline.get("instruments") or []) if r.get("market")}
    changes: list[dict[str, Any]] = []
    for market, after in sorted(by_market.items()):
        before = base_by.get(market) or {}
        b_align = before.get("alignment_label")
        a_align = after.get("final_state")
        b_rank = before.get("rank_score")
        a_rank = after.get("rank_score")
        val_before = (before.get("pillars_pass") or {}).get("valuation")
        val_after = after.get("valuation")
        sea_before = (before.get("pillars_pass") or {}).get("seasonality")
        sea_after = after.get("seasonality")
        pillar_delta = (
            val_before != val_after
            or sea_before != sea_after
            or b_align != a_align
            or b_rank != a_rank
        )
        if not pillar_delta:
            continue
        changes.append(
            {
                "market": market,
                "before_alignment": b_align,
                "after_alignment": a_align,
                "before_rank_score": b_rank,
                "after_rank_score": a_rank,
                "before_action": before.get("action"),
                "after_action": after.get("action"),
                "delta_rank": (a_rank or 0) - (b_rank or 0),
                "valuation": f"{val_before} → {val_after}",
                "seasonality": f"{sea_before} → {sea_after}",
            }
        )
    changes.sort(key=lambda x: (-abs(x.get("delta_rank") or 0), str(x.get("market") or "")))
    return changes


def build_distribution_report(
    *,
    thesis_path: Path | None = None,
    baseline_path: Path | None = None,
) -> dict[str, Any]:
    thesis_path = thesis_path or THESIS_EXPORT
    doc = json.loads(thesis_path.read_text(encoding="utf-8"))
    theses = [t for t in (doc.get("theses") or []) if isinstance(t, dict)]
    rows = [_row_from_thesis(t) for t in theses]
    rows.sort(key=lambda r: (-(r.get("rank_score") or 0), str(r.get("market") or "")))

    baseline: dict[str, Any] | None = None
    bp = baseline_path or BASELINE_PATH
    if bp.exists():
        baseline = json.loads(bp.read_text(encoding="utf-8"))
    else:
        baseline = {
            "instruments": [
                (lambda o: {
                    "market": t.get("market"),
                    "alignment_label": o["alignment"]["label"],
                    "alignment_pass": o["alignment"]["pass"],
                    "action": o["action"],
                    "rank_score": o["rank_score"],
                })(evaluate_three_pillar_opportunity(t))
                for t in theses
            ]
        }

    comparison = _compare_rankings(baseline, rows)

    return {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis_export": str(thesis_path),
        "baseline_source": str(bp) if bp.exists() else "recomputed_stripped",
        "distribution": _distribution_counts(rows),
        "instruments": rows,
        "before_after": {
            "changed_count": len(comparison),
            "unchanged_count": len(rows) - len(comparison),
            "changes": comparison,
        },
    }


def _markdown_report(payload: dict[str, Any]) -> str:
    dist = payload.get("distribution") or {}
    lines = [
        "# Opportunity Engine — 5-Pillar Distribution Report",
        "",
        f"Generated: {payload.get('generated_at')}",
        "",
        "## Alignment distribution (pass count / 5)",
        "",
    ]
    for k in ("0/5", "1/5", "2/5", "3/5", "4/5", "5/5"):
        lines.append(f"- **{k}**: {dist.get(k, 0)} instruments")
    ba = payload.get("before_after") or {}
    lines.extend(
        [
            "",
            "## Before / after (3-pillar baseline → 5-pillar live)",
            "",
            f"- Changed rankings or alignment: **{ba.get('changed_count', 0)}**",
            f"- Unchanged: **{ba.get('unchanged_count', 0)}**",
            "",
            "| Market | Before | After | Rank Δ | Val | Sea | Action |",
            "|--------|--------|-------|--------|-----|-----|--------|",
        ]
    )
    for ch in (ba.get("changes") or [])[:40]:
        lines.append(
            f"| {ch.get('market')} | {ch.get('before_alignment')} | {ch.get('after_alignment')} | "
            f"{ch.get('delta_rank'):+d} | {ch.get('valuation')} | {ch.get('seasonality')} | "
            f"{ch.get('before_action')} → {ch.get('after_action')} |"
        )
    if len(ba.get("changes") or []) > 40:
        lines.append(f"| … | ({len(ba['changes']) - 40} more in JSON) | | | |")
    lines.extend(["", "## Per-instrument (latest week)", "", "| Instrument | Inst | Retail | Loc | Val | Sea | Final | Action |", "|------------|------|--------|-----|-----|-----|-------|--------|"])
    for r in payload.get("instruments") or []:
        lines.append(
            f"| {r.get('market')} | {r.get('institutions')} | {r.get('retail')} | {r.get('location')} | "
            f"{r.get('valuation')} | {r.get('seasonality')} | {r.get('final_state')} | {r.get('action')} |"
        )
    lines.append("")
    lines.append("> Scoring thresholds and action weights were **not** changed for this report.")
    return "\n".join(lines)


def write_scanner_latest(confluence_path: Path | None = None) -> Path:
    """Export latest-week scanner slice from confluence."""
    from hptl.config import PROJECT_ROOT as root

    path = confluence_path or root / "web-dashboard/public/data/confluence_history_latest.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    payload = {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scanner_attention_week": doc.get("scanner_attention_week"),
        "latest_week": doc.get("latest_week"),
    }
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    for out in (SCANNER_OUT, root / "data/scanner_latest.json"):
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text, encoding="utf-8")
    return SCANNER_OUT


def write_reports(payload: dict[str, Any] | None = None) -> dict[str, Path]:
    payload = payload or build_distribution_report()
    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    REPORT_MD.parent.mkdir(parents=True, exist_ok=True)
    REPORT_MD.write_text(_markdown_report(payload), encoding="utf-8")
    return {"json": REPORT_JSON, "markdown": REPORT_MD}
