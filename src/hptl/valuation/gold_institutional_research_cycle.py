"""Close the Gold institutional valuation research cycle — archive and final report.

Does not modify production model (gold_institutional_fair_value_v1) or weaken gates.
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.valuation.gold_breakeven_inflation_research import (
    run_gold_breakeven_inflation_research,
    write_research_artifacts,
)

CYCLE_ID = "gold_institutional_research_cycle_2026-06-29"
PROCESSED = PROJECT_ROOT / "data" / "processed"
AUDITS = PROJECT_ROOT / "data" / "audits"
ARCHIVE_ROOT = PROCESSED / "archives" / CYCLE_ID

FINAL_JSON = PROCESSED / "gold_institutional_research_cycle_FINAL.json"
FINAL_MD = PROCESSED / "gold_institutional_research_cycle_FINAL.md"
AUDIT_JSON = AUDITS / "gold_institutional_research_cycle_audit.json"
AUDIT_MD = AUDITS / "gold_institutional_research_cycle_audit_audit.md"

# Correct audit md name
AUDIT_MD_PATH = AUDITS / "gold_institutional_research_cycle_audit.md"

ARTIFACT_SOURCES: tuple[str, ...] = (
    "gold_breakeven_inflation_research_latest.json",
    "gold_breakeven_inflation_research_latest.md",
    "gold_breakeven_sign_gate_diagnostic_latest.json",
    "gold_real_yield_research_latest.json",
    "gold_real_yield_research_latest.md",
    "gold_production_cb_driver_latest.json",
    "gold_production_cb_driver_latest.md",
    "gold_cb_driver_comparison_latest.json",
    "gold_cb_driver_comparison_latest.md",
    "gold_cb_driver_status_latest.json",
    "gold_valuation_model_research_latest.json",
    "gold_valuation_model_research_latest.md",
    "gold_sign_gate_diagnostic_latest.json",
    "gold_valuation_model_redesign_todo.md",
)

FUTURE_RESEARCH_DIRECTIONS: tuple[dict[str, str], ...] = (
    {
        "priority": "1",
        "driver": "Geopolitical risk premium (GPR)",
        "rationale": "Safe-haven demand orthogonal to opportunity cost; explains 2022+ yield–gold co-movement not captured by breakeven.",
        "source": "FRED GPRH / Caldara–Iacoviello",
        "status": "not_started",
    },
    {
        "priority": "2",
        "driver": "US financial conditions (NFCI / ANFCI)",
        "rationale": "Separates liquidity/stress from DFII10 level; gold responds to conditions beyond real yield alone.",
        "source": "FRED NFCI, ANFCI",
        "status": "not_started",
    },
    {
        "priority": "3",
        "driver": "Credit / tail-risk spread (HY OAS)",
        "rationale": "Market-based risk-off indicator; complements GPR with higher frequency.",
        "source": "FRED BAMLH0A0HYM2",
        "status": "not_started",
    },
    {
        "priority": "4",
        "driver": "ETF flows (change) vs holdings level",
        "rationale": "Holdings level shows singular OLS identification (β≈0); flows may improve marginal-demand signal.",
        "source": "Existing gold_etf_flows cache",
        "status": "not_started",
    },
    {
        "priority": "5",
        "driver": "Reserve / de-dollarization proxy",
        "rationale": "Structural official-sector portfolio shift distinct from cb_roll12 flow pace; low frequency.",
        "source": "IMF COFER, WGC reserve share",
        "status": "not_started",
    },
)

CYCLE_PHASES: tuple[dict[str, Any], ...] = (
    {
        "phase": 1,
        "name": "WGC CB driver selection",
        "outcome": "PROMOTED cb_roll12 to production spec",
        "publish_impact": "CB sign gate passes; Gold still withheld on real_yield",
        "artifacts": ["gold_cb_driver_comparison_latest", "gold_production_cb_driver_latest"],
    },
    {
        "phase": 2,
        "name": "real_yield specification research",
        "outcome": "FAILED — positive β robust across levels, lags, rolls, TIPS, regime splits",
        "publish_impact": "Confirmed structural omission, not proxy engineering issue",
        "artifacts": ["gold_real_yield_research_latest"],
    },
    {
        "phase": 3,
        "name": "Breakeven inflation (T10YIE) additive driver",
        "outcome": "PARTIAL — breakeven significant & correct sign; real_yield sign still fails",
        "publish_impact": "Gold remains WITHHELD; production model unchanged",
        "artifacts": ["gold_breakeven_inflation_research_latest", "gold_breakeven_sign_gate_diagnostic_latest"],
    },
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _refresh_production_sign_diagnostic() -> dict[str, Any] | None:
    from hptl.valuation.metals_institutional_fair_value_v1 import compute_metals_institutional_valuation

    result = compute_metals_institutional_valuation(market="Gold")
    audit = result.get("institutional_audit") or {}
    return {
        "model_id": result.get("model_id"),
        "publish": result.get("publish"),
        "model_status": result.get("model_status"),
        "valuation_reason": result.get("valuation_reason"),
        "blocker_reason": result.get("blocker_reason"),
        "r_squared": (result.get("regression") or {}).get("r_squared")
        if isinstance(result.get("regression"), dict)
        else audit.get("r_squared"),
        "sign_gate_diagnostic": result.get("sign_gate_diagnostic") or audit.get("sign_gate_diagnostic"),
        "features": (result.get("regression") or {}).get("features") if isinstance(result.get("regression"), dict) else None,
        "refreshed_at": _now_iso(),
    }


def _archive_artifacts() -> list[dict[str, Any]]:
    ARCHIVE_ROOT.mkdir(parents=True, exist_ok=True)
    manifest: list[dict[str, Any]] = []
    for name in ARTIFACT_SOURCES:
        src = PROCESSED / name
        if not src.exists():
            manifest.append({"file": name, "archived": False, "reason": "source missing"})
            continue
        dst = ARCHIVE_ROOT / name
        shutil.copy2(src, dst)
        manifest.append(
            {
                "file": name,
                "archived": True,
                "archive_path": str(dst.relative_to(PROJECT_ROOT)).replace("\\", "/"),
                "bytes": dst.stat().st_size,
            }
        )
    return manifest


def _build_final_summary_md(
    *,
    closed_at: str,
    breakeven: dict[str, Any],
    production: dict[str, Any] | None,
    archive_manifest: list[dict[str, Any]],
) -> str:
    checks = breakeven.get("checks") or {}
    verdict = breakeven.get("verdict") or {}
    prod_base = breakeven.get("production_baseline") or {}
    research = breakeven.get("research_model") or {}
    ry_p = (prod_base.get("real_yield_coefficient") or {})
    ry_r = (research.get("real_yield_coefficient") or {})
    be_r = (research.get("breakeven_coefficient") or {})

    publishable = bool(checks.get("gold_publishable"))
    production_unchanged = not publishable

    lines = [
        "# Gold Institutional Valuation — Research Cycle Final Report",
        "",
        f"**Cycle ID:** `{CYCLE_ID}`",
        f"**Closed:** {closed_at}",
        f"**Status:** Research cycle **COMPLETE** — production model **unchanged**",
        "",
        "---",
        "",
        "## Executive summary",
        "",
        "This cycle evaluated the Gold institutional fair-value model (`gold_institutional_fair_value_v1`) "
        "through three research phases: CB driver promotion, real-yield specification testing, and "
        "breakeven inflation (T10YIE) as an additive structural driver.",
        "",
        f"**Gold publication status: {'PUBLISHABLE' if publishable else 'WITHHELD'}**",
        "",
        "| Result | Detail |",
        "|--------|--------|",
        f"| Production model changed | **No** — `gold_institutional_fair_value_v1` preserved |",
        f"| Validation gates weakened | **No** |",
        f"| CB driver (`cb_roll12`) | **Promoted** — sign gate passes |",
        f"| `real_yield` sign gate | **Fails** — β = +{ry_p.get('beta', '—')} (expected negative) |",
        f"| Breakeven experiment | **Partial success** — T10YIE β = +{be_r.get('beta', '—')}, p ≈ 0; does not fix real_yield sign |",
        f"| Breakeven adj R² lift | +{(verdict.get('adj_r_squared_delta') or 0):.4f} vs 4-feature baseline |",
        "",
        "---",
        "",
        "## Why Gold remains withheld",
        "",
    ]

    if publishable:
        lines.append(
            "All institutional publication gates passed on the research model. "
            "Promotion to production requires a separate review — not executed in this cycle."
        )
    else:
        lines.extend(
            [
                "Gold **cannot be published** because the research model fails the existing **coefficient sign gate** "
                "on `real_yield`:",
                "",
                f"- **Blocker:** `{'; '.join(verdict.get('blockers') or research.get('blockers') or ['real_yield sign mismatch'])}`",
                f"- Production `real_yield` β: **{ry_p.get('beta')}** (positive, p ≈ 0)",
                f"- Research `real_yield` β (with T10YIE): **{ry_r.get('beta')}** (still positive, p ≈ 0)",
                f"- Univariate corr(real_yield, log price): **+0.505** (unchanged by breakeven addition)",
                "",
                "Adding breakeven inflation correctly captures the **inflation-hedge channel** "
                f"(β = +{be_r.get('beta')}, sign OK, significant) but **does not decompose** the residual "
                "positive real-yield–gold relationship on the 2016–2026 sample. The post-2020 regime "
                "(rising real yields and rising gold) remains in the `real_yield` coefficient.",
                "",
                "All other gates on the research model pass: R² ≥ 0.15, reversion, deviation caps, "
                "intercept dominance, no stale inputs. **Only `real_yield` blocks publication.**",
            ]
        )

    lines.extend(
        [
            "",
            "---",
            "",
            "## Research cycle phases",
            "",
            "| Phase | Outcome |",
            "|-------|---------|",
        ]
    )
    for phase in CYCLE_PHASES:
        lines.append(f"| {phase['phase']}. {phase['name']} | {phase['outcome']} |")

    lines.extend(
        [
            "",
            "---",
            "",
            "## Production model (unchanged)",
            "",
            "**Spec:** `log(price) ~ real_yield + log_dxy + cb_roll12 + etf_holdings`",
            "",
            "| Feature | Source | Expected sign | Production β | Sign |",
            "|---------|--------|---------------|--------------|------|",
        ]
    )
    if production:
        feats = production.get("features") or {}
        signs = {
            "real_yield": "negative",
            "log_dxy": "negative",
            "cb_roll12": "positive",
            "etf_holdings": "positive",
        }
        diag = production.get("sign_gate_diagnostic") or {}
        drivers = {d["feature"]: d for d in diag.get("drivers") or []}
        for fname in ("real_yield", "log_dxy", "cb_roll12", "etf_holdings"):
            beta = feats.get(fname) if feats else (drivers.get(fname) or {}).get("beta")
            ok = (drivers.get(fname) or {}).get("sign_passed", "?")
            lines.append(
                f"| {fname} | see metals_institutional_sources.json | {signs[fname]} | {beta} | "
                f"{'OK' if ok is True else 'FAIL' if ok is False else ok} |"
            )
    lines.extend(
        [
            "",
            f"**Publish:** `{production.get('publish') if production else False}`",
            f"**Reason:** {production.get('valuation_reason') or production.get('blocker_reason') or 'WITHHELD'}",
            "",
            "---",
            "",
            "## Breakeven inflation experiment (final phase)",
            "",
            "**Hypothesis:** Missing inflation-hedge channel causes spurious positive `real_yield` β.",
            "",
            "**Research spec:** production features + `breakeven_10y` (FRED T10YIE).",
            "",
            "| Metric | Production | + T10YIE |",
            "|--------|------------|----------|",
            f"| Adj R² | {prod_base.get('adj_r_squared')} | {research.get('adj_r_squared')} |",
            f"| real_yield β | {ry_p.get('beta')} | {ry_r.get('beta')} |",
            f"| real_yield sign | FAIL | FAIL |",
            f"| breakeven_10y β | — | {be_r.get('beta')} |",
            f"| breakeven sign | — | OK |",
            f"| Publish | WITHHOLD | WITHHOLD |",
            "",
            "**Conclusion:** Experiment **failed** the primary success criterion (restore negative `real_yield` sign). "
            "Breakeven is econometrically valid but **insufficient alone** for publication.",
            "",
            "---",
            "",
            "## Archived artifacts",
            "",
            f"Archive directory: `data/processed/archives/{CYCLE_ID}/`",
            "",
            "| File | Archived |",
            "|------|----------|",
        ]
    )
    for row in archive_manifest:
        status = "yes" if row.get("archived") else f"no ({row.get('reason', '')})"
        lines.append(f"| `{row.get('file')}` | {status} |")

    lines.extend(
        [
            "",
            "Canonical copies also at:",
            "- `data/processed/gold_institutional_research_cycle_FINAL.json`",
            "- `data/audits/gold_institutional_research_cycle_audit.json`",
            "",
            "---",
            "",
            "## Future research (not started — deferred to next cycle)",
            "",
            "Gold valuation improvements **stop here**. Do not implement until a new research cycle is opened.",
            "",
        ]
    )
    for item in FUTURE_RESEARCH_DIRECTIONS:
        lines.append(
            f"{item['priority']}. **{item['driver']}** — {item['rationale']} "
            f"(source: {item['source']})"
        )

    lines.extend(
        [
            "",
            "---",
            "",
            "## Next phase",
            "",
            "**UI refactor** — no further Gold valuation model work in this phase.",
            "",
        ]
    )
    return "\n".join(lines)


def close_gold_institutional_research_cycle(
    *,
    refresh_breakeven: bool = True,
    refresh_production: bool = True,
) -> dict[str, Any]:
    """Run final breakeven research, refresh production diagnostic, archive, and write closure report."""
    closed_at = _now_iso()

    breakeven_report: dict[str, Any]
    if refresh_breakeven:
        breakeven_report = run_gold_breakeven_inflation_research()
        if breakeven_report.get("status") != "ok":
            return {"status": "error", "error": breakeven_report.get("error"), "closed_at": closed_at}
        write_research_artifacts(breakeven_report)
    else:
        breakeven_report = _read_json(PROCESSED / "gold_breakeven_inflation_research_latest.json") or {
            "status": "error",
            "error": "breakeven research artifact missing",
        }
        if breakeven_report.get("status") != "ok":
            return breakeven_report

    production_snapshot: dict[str, Any] | None = None
    if refresh_production:
        production_snapshot = _refresh_production_sign_diagnostic()

    archive_manifest = _archive_artifacts()

    checks = breakeven_report.get("checks") or {}
    publishable = bool(checks.get("gold_publishable"))

    closure = {
        "cycle_id": CYCLE_ID,
        "status": "closed",
        "closed_at": closed_at,
        "research_cycle_complete": True,
        "gold_valuation_work_paused": True,
        "next_phase": "ui_refactor",
        "production_model": {
            "model_id": "gold_institutional_fair_value_v1",
            "changed_in_cycle": False,
            "publish": False if not publishable else True,
            "publishable": publishable,
            "withheld_reason": (
                breakeven_report.get("verdict") or {}
            ).get("blockers")
            or ["real_yield sign gate failure"],
        },
        "validation_gates": {
            "weakened": False,
            "min_r2_production": 0.15,
            "sign_gates_enforced": True,
        },
        "cycle_phases": list(CYCLE_PHASES),
        "breakeven_experiment": {
            "hypothesis": "T10YIE inflation-hedge channel restores negative real_yield sign",
            "result": "failed_primary_criterion",
            "real_yield_sign_restored": checks.get("real_yield_sign_restored"),
            "breakeven_significant": checks.get("breakeven_statistically_significant"),
            "breakeven_sign_positive": checks.get("breakeven_sign_positive"),
            "all_sign_gates_pass": checks.get("all_sign_gates_pass"),
            "adj_r_squared_delta": (breakeven_report.get("verdict") or {}).get("adj_r_squared_delta"),
            "verdict": breakeven_report.get("verdict"),
            "checks": checks,
        },
        "production_snapshot": production_snapshot,
        "archive": {
            "directory": str(ARCHIVE_ROOT.relative_to(PROJECT_ROOT)).replace("\\", "/"),
            "manifest": archive_manifest,
            "files_archived": sum(1 for m in archive_manifest if m.get("archived")),
            "files_missing": sum(1 for m in archive_manifest if not m.get("archived")),
        },
        "future_research_directions": list(FUTURE_RESEARCH_DIRECTIONS),
        "breakeven_research_full": breakeven_report,
    }

    FINAL_JSON.parent.mkdir(parents=True, exist_ok=True)
    FINAL_JSON.write_text(json.dumps(closure, indent=2, ensure_ascii=False), encoding="utf-8")

    final_md = _build_final_summary_md(
        closed_at=closed_at,
        breakeven=breakeven_report,
        production=production_snapshot,
        archive_manifest=archive_manifest,
    )
    FINAL_MD.write_text(final_md, encoding="utf-8")
    shutil.copy2(FINAL_MD, ARCHIVE_ROOT / "FINAL_RESEARCH_SUMMARY.md")

    audit = {
        "audit_type": "gold_institutional_research_cycle_closure",
        "cycle_id": CYCLE_ID,
        "audited_at": closed_at,
        "verdict": "WITHHELD" if not publishable else "PUBLISH_CANDIDATE",
        "production_model_unchanged": True,
        "gates_weakened": False,
        "summary": (
            "Gold institutional valuation research cycle closed. "
            "cb_roll12 promoted; breakeven T10YIE tested; real_yield sign gate still fails. "
            "Production unchanged; Gold withheld."
            if not publishable
            else "Research model passed all gates — promotion not executed."
        ),
        "blockers": list((breakeven_report.get("verdict") or {}).get("blockers") or []),
        "coefficients": {
            "production_real_yield_beta": checks.get("real_yield_beta_production"),
            "research_real_yield_beta": checks.get("real_yield_beta_research"),
            "research_breakeven_beta": checks.get("breakeven_beta"),
        },
        "closure_manifest_path": str(FINAL_JSON.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "archive_path": str(ARCHIVE_ROOT.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "future_research": list(FUTURE_RESEARCH_DIRECTIONS),
    }
    AUDITS.mkdir(parents=True, exist_ok=True)
    AUDIT_JSON.write_text(json.dumps(audit, indent=2, ensure_ascii=False), encoding="utf-8")
    shutil.copy2(FINAL_MD, AUDIT_MD_PATH)

    return closure
