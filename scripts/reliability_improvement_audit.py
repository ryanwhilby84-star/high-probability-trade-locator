"""Reliability-first valuation roadmap — improvement audit generator."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
VAL_PATH = ROOT / "data" / "valuation_latest.json"
BACKTEST_PATH = ROOT / "data" / "audits" / "metals_valuation_v1_backtest.json"
OUT_PATH = ROOT / "data" / "audits" / "reliability_improvement_audit.md"

ISSUE_TYPES = (
    "Data quality",
    "Data freshness",
    "Missing inputs",
    "Weak model structure",
    "Weak explanatory power",
    "Excessive forecast error",
    "Insufficient history",
)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _pillar(model_id: str) -> str:
    if model_id == "fx_carry_real_yield_v3":
        return "fx"
    if model_id == "metals_real_yield_v1":
        return "metals"
    if "agri" in model_id:
        return "agri"
    if model_id in {"rates_curve_fair_value_v1", "usd_broad_fair_value_v1"}:
        return "macro"
    return "other"


def _reliability_score(block: dict[str, Any]) -> float | None:
    return block.get("confidence_v2_score")


def _classify_issues(block: dict[str, Any], backtest: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    reg = block.get("regression") or {}
    r2 = reg.get("r_squared")
    n = reg.get("n") or block.get("data_depth") or 0
    stale = block.get("stale_inputs") or []
    missing = block.get("missing_inputs") or []
    subs = block.get("confidence_subscores") or {}
    market = block.get("market", "")
    bt = (backtest.get("markets") or {}).get(market) or {}
    mad = bt.get("mean_abs_deviation_pct")
    dev = block.get("deviation_pct")
    model_id = block.get("model_id", "")

    if missing:
        issues.append("Missing inputs")
    if stale:
        cpi_only = all("cpi_yoy" in s for s in stale)
        if cpi_only:
            issues.append("Data freshness")
        else:
            issues.append("Data freshness")
            if any(".y2" in s or ".y10" in s or "policy" in s for s in stale):
                issues.append("Data quality")
    if r2 is not None and r2 < 0.18:
        issues.append("Weak explanatory power")
    if r2 is None and "agri" in model_id:
        issues.append("Weak model structure")
    if mad is not None and mad > 22:
        issues.append("Excessive forecast error")
    if dev is not None and mad is not None and abs(dev) > mad * 1.5:
        issues.append("Excessive forecast error")
    if n and n < 24 and "agri" in model_id:
        issues.append("Insufficient history")
    if subs.get("fit_score", 100) < 40:
        issues.append("Weak explanatory power")
    if model_id == "metals_real_yield_v1" and market in {"Copper / HG"}:
        issues.append("Missing inputs")
    return list(dict.fromkeys(issues))


def _fix_plan(market: str, block: dict[str, Any], issues: list[str]) -> dict[str, Any]:
    model_id = block.get("model_id", "")
    reg = block.get("regression") or {}
    r2 = reg.get("r_squared")
    stale = block.get("stale_inputs") or []
    conf = str(block.get("confidence", "")).lower()

    fixes: list[str] = []
    data_req: list[str] = []
    model_req: list[str] = []
    effort = "M"
    fixable = True
    expected = "Material reliability gain — target High confidence with defensible signal."

    if "Data freshness" in issues and stale:
        if all("cpi_yoy" in s for s in stale):
            fixes.append("Automate CPI YoY refresh on release (FRED/OECD) into `fx_currency_rates.json`; set `cpi_yoy_as_of` and `field_live.cpi_yoy=true`.")
            data_req.append("Monthly CPI release feeds for G10 legs")
            effort = "S"
            expected = "Data score → High; removes artificial freshness penalty without changing model math."
        if any(".y2" in s or ".y10" in s for s in stale):
            fixes.append("Repair NZD/CHF (and affected leg) yield history loaders — extend cache backfill and daily refresh.")
            data_req.append("Central bank / FRED 2Y–10Y daily series through latest session")
            effort = "M"

    if model_id == "fx_carry_real_yield_v3" and r2 is not None and r2 < 0.15:
        fixes.append("EUR/USD: re-specify regression — add term structure / relative growth / energy terms or reduce pair to diagnostic-only until R²≥0.18.")
        model_req.append("Alternative FX fair-value spec (not carry-only)")
        effort = "L"
        fixable = True
        expected = "Either publishable fit (R²≥0.18) or remove from scanner until fixed."

    if model_id == "metals_real_yield_v1":
        bt_mad = None
        if BACKTEST_PATH.exists():
            bt = json.loads(BACKTEST_PATH.read_text(encoding="utf-8"))
            bt_mad = (bt.get("markets") or {}).get(market, {}).get("mean_abs_deviation_pct")
        dev = abs(block.get("deviation_pct") or 0)
        if market == "Silver" or (bt_mad and bt_mad > 25):
            fixes.append("Add breakeven inflation / real-rate term; model residual percentile gate before publishing extreme Overvalued labels.")
            model_req.extend(["TIPS breakeven (e.g. T10YIE)", "Residual z-score cap or two-factor error band"])
            effort = "L"
            expected = "R² target ≥0.35; MAD ≤20%; deviation signals within historical error band."
        elif market == "Gold" and dev > 40:
            fixes.append("Extend macro set (inflation expectations, financial conditions); validate post-2022 regime in backtest.")
            model_req.append("Inflation expectations, optional ETF flow proxy")
            effort = "L"
            expected = "Lower MAD; deviation above fair value requires out-of-sample confirmation."
        elif market == "Copper / HG":
            fixes.append("Wire China PMI / industrial production term (already reserved in spec); re-audit R² gate after wiring.")
            model_req.append("China PMI series (CHINAMANUFPMIMEI or successor)")
            effort = "M"
            expected = "R² lift from ~0.22 toward ≥0.30; industrial fair value more defensible."
        elif market == "Platinum" and abs(block.get("deviation_pct") or 0) > 30:
            fixes.append("PGM-specific demand proxy (auto catalyst / EV transition index) — two-factor model insufficient for cycle extremes.")
            model_req.append("PGM demand-side indicator")
            effort = "L"

    if "agri" in model_id:
        n = block.get("data_depth") or 0
        fixes.append("Backfill aligned price↔STU panel to ≥24 obs; promote to `agri_stu_regression_v1` when R²≥0.25.")
        data_req.append("USDA PSD historical alignment + canonical price timeline join")
        if n < 24:
            effort = "M"
        else:
            effort = "S"
        expected = "Regression path with measurable R²; agri no longer percentile-only fallback."

    if model_id in {"rates_curve_fair_value_v1", "usd_broad_fair_value_v1"}:
        dev = abs(block.get("deviation_pct") or 0)
        if dev > 15:
            fixes.append("Rates fair value: validate unit scale and driver set; large deviation on macro rates suggests spec or anchor mismatch.")
            model_req.append("Driver audit (2s10s vs real yield vs policy)")
            effort = "M"
            expected = "Deviation within ±10% fair band under normal regimes."

    if not fixes:
        if conf == "high":
            fixes.append("Maintain current pipeline; add out-of-sample backtest export to UI (not more confidence layers).")
            effort = "S"
            expected = "Sustain High; validate with rolling 52w R² stability."
        else:
            fixes.append("Review composite sub-scores; address lowest component first.")
            expected = "Move to High when fit≥65 and data≥55 sustained."

    return {
        "fixes": fixes,
        "data_required": data_req,
        "model_required": model_req,
        "effort": effort,
        "fixable": fixable,
        "expected": expected,
    }


def _entry(market: str, block: dict[str, Any], backtest: dict[str, Any]) -> dict[str, Any]:
    reg = block.get("regression") or {}
    bt = (backtest.get("markets") or {}).get(market) or {}
    issues = _classify_issues(block, backtest)
    plan = _fix_plan(market, block, issues)
    subs = block.get("confidence_subscores") or {}
    bands = block.get("confidence_subscore_bands") or {}
    weaknesses = []
    if issues:
        weaknesses.extend(issues)
    if block.get("stale_inputs"):
        weaknesses.append(f"Stale: {', '.join(block['stale_inputs'][:4])}")
    if bt.get("mean_abs_deviation_pct"):
        weaknesses.append(f"MAD {bt['mean_abs_deviation_pct']}%")
    if reg.get("r_squared") is not None and reg["r_squared"] < 0.25:
        weaknesses.append(f"Low R² ({reg['r_squared']:.3f})")
    if abs(block.get("deviation_pct") or 0) > 25:
        weaknesses.append(f"Large published deviation ({block.get('deviation_pct')}%)")

    return {
        "market": market,
        "pair": block.get("pair"),
        "pillar": _pillar(block.get("model_id", "")),
        "model_id": block.get("model_id"),
        "confidence": block.get("confidence"),
        "reliability_score": _reliability_score(block),
        "r_squared": reg.get("r_squared"),
        "n": reg.get("n") or block.get("data_depth"),
        "mad_pct": bt.get("mean_abs_deviation_pct"),
        "fwd_corr": bt.get("forward_return_correlation"),
        "deviation_pct": block.get("deviation_pct"),
        "issues": issues,
        "weaknesses": weaknesses,
        "subscores": subs,
        "bands": bands,
        "plan": plan,
        "priority": _priority(block, issues, plan),
    }


def _priority(block: dict[str, Any], issues: list[str], plan: dict[str, Any]) -> int:
    """Lower number = higher priority."""
    dev = abs(block.get("deviation_pct") or 0)
    conf = str(block.get("confidence", "")).lower()
    reg = block.get("regression") or {}
    r2 = reg.get("r_squared") or 0
    scanner_visible = block.get("wired") is True

    if not scanner_visible:
        return 99
    # Large misleading signal on scanner
    if dev > 40 and conf in {"high", "medium"}:
        return 1
    if conf == "low" and dev > 20:
        return 2
    if "Weak explanatory power" in issues and r2 < 0.12:
        return 3
    if conf in {"low", "medium"} and "Data freshness" in issues:
        return 4
    if _pillar(block.get("model_id", "")) == "agri":
        return 5
    if conf == "medium":
        return 6
    if conf == "high" and dev > 25:
        return 7
    return 8


def _render_entry(e: dict[str, Any]) -> list[str]:
    p = e["plan"]
    lines = [
        f"### {e['market']}" + (f" ({e['pair']})" if e.get("pair") else ""),
        "",
        "**Current state**",
        "",
        f"| Field | Value |",
        f"|---|---|",
        f"| Reliability (v2 score) | {e['reliability_score']} |",
        f"| Confidence | {e['confidence']} |",
        f"| R² | {e['r_squared'] if e['r_squared'] is not None else '—'} |",
        f"| Sample | {e['n'] or '—'} |",
        f"| Mean \\|deviation\\| (backtest) | {e['mad_pct'] if e['mad_pct'] is not None else '—'}% |",
        f"| 4W forward corr. | {e['fwd_corr'] if e['fwd_corr'] is not None else '—'} |",
        f"| Published deviation | {e['deviation_pct']}% |",
        f"| Fit / Data / Error | {e['bands'].get('fit', '—')} / {e['bands'].get('data', '—')} / {e['bands'].get('error', '—')} |",
        "",
        f"**Key weaknesses:** {', '.join(e['weaknesses']) or '—'}",
        "",
        f"**Issue classification:** {', '.join(e['issues']) or '—'}",
        "",
        "**Target state**",
        "",
    ]
    for fix in p["fixes"]:
        lines.append(f"- {fix}")
    if p["data_required"]:
        lines.append(f"- **Data required:** {', '.join(p['data_required'])}")
    if p["model_required"]:
        lines.append(f"- **Model inputs required:** {', '.join(p['model_required'])}")
    lines.extend(
        [
            f"- **Fixable:** {'Yes' if p['fixable'] else 'No — consider unwiring'}",
            f"- **Effort:** {p['effort']} (S=days, M=1–2 weeks, L=multi-sprint)",
            f"- **Expected improvement:** {p['expected']}",
            "",
        ]
    )
    return lines


def generate_report() -> str:
    val = _load_json(VAL_PATH)
    backtest = _load_json(BACKTEST_PATH) if BACKTEST_PATH.exists() else {}
    instruments = val.get("instruments") or {}

    entries = []
    for market, block in instruments.items():
        if not block.get("wired"):
            continue
        b = dict(block)
        b["market"] = market
        conf = str(b.get("confidence", "")).lower()
        if conf in {"low", "medium"}:
            entries.append(_entry(market, b, backtest))
        elif conf == "high":
            dev = abs(b.get("deviation_pct") or 0)
            reg = b.get("regression") or {}
            bt = (backtest.get("markets") or {}).get(market) or {}
            mad = bt.get("mean_abs_deviation_pct")
            if dev > 30 or (mad and dev > mad * 1.3) or (reg.get("r_squared") and reg["r_squared"] < 0.35 and dev > 20):
                entries.append(_entry(market, b, backtest))

    entries.sort(key=lambda e: (e["priority"], -(e["reliability_score"] or 0)))

    low_med = sum(
        1
        for m, b in instruments.items()
        if b.get("wired") and str(b.get("confidence", "")).lower() in {"low", "medium"}
    )

    lines = [
        "# Reliability Improvement Audit",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        f"Source: `{VAL_PATH.relative_to(ROOT)}`",
        "",
        "## Principle",
        "",
        "Validation and confidence layers are **temporary engineering tools**. This audit identifies",
        "what must be **fixed** so valuations become reliable by default — not what warnings to add.",
        "",
        "> Do not ask: *How do we explain why this valuation is unreliable?*",
        "> Ask: *What must be improved so this valuation becomes reliable?*",
        "",
        "## Executive summary",
        "",
        f"- **Wired valuations:** {sum(1 for b in instruments.values() if b.get('wired'))}",
        f"- **Below High confidence:** {low_med} (require reliability work before scanner default-trust)",
        f"- **Highest-impact fixes:** data pipeline refresh (FX CPI/yields), metals macro extension (Silver/Gold/Copper), agri regression promotion",
        "",
        "## Priority roadmap (impact order)",
        "",
        "| P | Market | Confidence | Score | Primary issue | Effort | Fix type |",
        "|---:|---|---|---:|---|---|---|",
    ]

    for e in entries[:15]:
        primary = e["issues"][0] if e["issues"] else "Monitor"
        lines.append(
            f"| {e['priority']} | {e['market']} | {e['confidence']} | {e['reliability_score']} | {primary} | {e['plan']['effort']} | "
            f"{'Data' if 'Data' in primary else 'Model' if 'Weak' in primary or 'Missing' in primary else 'Mixed'} |"
        )

    lines.extend(
        [
            "",
            "## Pillar-level reliability gaps",
            "",
            "### FX (`fx_carry_real_yield_v3`)",
            "",
            "| Gap | Root cause | Fix | Effort |",
            "|---|---|---|---|",
            "| CPI flagged stale on all majors | Annual CPI not refreshed in `fx_currency_rates.json` after release | Release-triggered CPI ingest + `field_live` flags | **S** |",
            "| NZD/CHF yield stale | Yield cache gaps for non-USD legs | BoE/SNB/RBNZ loader backfill | **M** |",
            "| EUR/USD weak R² (0.10) | Carry-only spec insufficient for EUR/USD | Re-spec or de-publish until R²≥0.18 | **L** |",
            "| No FX backtest in export | Engineering debt | Rolling 52w deviation + hit-rate audit artifact | **M** |",
            "",
            "### Metals (`metals_real_yield_v1`)",
            "",
            "| Gap | Root cause | Fix | Effort |",
            "|---|---|---|---|",
            "| Silver +97% Overvalued, MAD 28% | Two-factor macro misses regime shifts | Breakeven inflation + residual gate | **L** |",
            "| Gold +51% Overvalued, MAD 22% | Real yield + DXY only | Inflation expectations, regime dummy post-2022 | **L** |",
            "| Copper +40%, China PMI unwired | Industrial term placeholder | Wire PMI term + re-audit | **M** |",
            "| Forward return corr. ≈ 0 | Model tracks level poorly out-of-sample | Error-aware publish gate (MAD-based) | **M** |",
            "",
            "### Agriculture (`agri_*`)",
            "",
            "| Gap | Root cause | Fix | Effort |",
            "|---|---|---|---|",
            "| 100% Low confidence | Percentile fallback only (no R² path) | Align price+STU to ≥24 obs; regression promotion | **M** |",
            "| No agri backtest | Engineering debt | STU fair-value rolling error export | **M** |",
            "| WASDE freshness not in model | Data score proxy only | PSD `as_of` in ingest + release calendar | **S** |",
            "",
            "### Macro rates / DXY (Phase 1 partial)",
            "",
            "| Gap | Root cause | Fix | Effort |",
            "|---|---|---|---|",
            "| 10Y Real Yield +73% deviation | Spec / anchor mismatch suspected | Unit + driver audit on rates_curve_fair_value_v1 | **M** |",
            "",
            "---",
            "",
            "## Instrument reliability plans",
            "",
        ]
    )

    for e in entries:
        lines.extend(_render_entry(e))

    lines.extend(
        [
            "## Success criteria (roadmap exit)",
            "",
            "1. **Scanner default trust:** ≥80% of wired scanner valuations at High confidence *because model quality supports it*, not because thresholds were loosened.",
            "2. **Deviation discipline:** No wired instrument publishes >±40% Overvalued/Undervalued unless MAD-backed and out-of-sample validated.",
            "3. **Data-first:** FX CPI/yield freshness issues resolved in data layer — not compensated in confidence math.",
            "4. **Agri regression:** ≥3 priority markets on `agri_stu_regression_v1` with R²≥0.25.",
            "5. **Validation layers shrink:** Confidence explanations remain, but fewer instruments depend on them for trader caution.",
            "",
            "## What we stop doing",
            "",
            "- Adding new confidence / trust / warning tiers without a paired model or data fix ticket.",
            "- Publishing extreme deviation labels on weak R² models without error-band justification.",
            "- Treating percentile-only agri fair value as production-grade valuation.",
            "",
        ]
    )

    return "\n".join(lines) + "\n"


def main() -> None:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(generate_report(), encoding="utf-8")
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
