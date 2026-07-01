"""Metals valuation V1 export, audit artifacts, and valuation_latest merge."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.valuation.metals_valuation_v1 import (
    METALS_MARKETS,
    MODEL_ID,
    build_all_metals_valuations,
    run_backtest_diagnostics,
)

METALS_OUT = Path("data/metals_valuation_latest.json")
PUBLIC_METALS_OUT = PROJECT_ROOT / "web-dashboard/public/data/metals_valuation_latest.json"
DESIGN_MD = Path("data/audits/metals_valuation_v1_design.md")
BACKTEST_JSON = Path("data/audits/metals_valuation_v1_backtest.json")
AUDIT_MD = Path("data/audits/metals_valuation_v1_audit.md")


def render_design_md(payload: dict[str, Any], backtest: dict[str, Any]) -> str:
    lines = [
        "# Metals Valuation V1 — Model Design",
        "",
        f"- Generated: `{payload.get('generated_at')}`",
        f"- Engine: **`{MODEL_ID}`**",
        f"- Phase: {payload.get('valuation_phase')}",
        "",
        "## Objective",
        "",
        "Institutional macro fair value for precious and industrial metals using existing",
        "FRED macro_cache infrastructure — no location percentile substitution.",
        "",
        "## Input series",
        "",
        "| Series | FRED ID | Role |",
        "| --- | --- | --- |",
        "| 10Y real yield | DFII10 | Primary discount-rate / opportunity-cost driver |",
        "| Broad USD index | DTWEXBGS | Dollar overlay (fallback: DX canonical timeline) |",
        "| Metal spot | canonical_price_timeline | Dependent variable |",
        "| China manufacturing PMI | CHINAMANUFPMIMEI | Copper placeholder (V1.1 — not in regression until audit pass) |",
        "",
        "## Regression",
        "",
        "```",
        "log(price) = β0 + β1·real_yield + β2·log(DXY) [+ β3·china_pmi when wired]",
        "fair_value = exp(predicted log price at current macro)",
        "deviation_pct = (spot − fair) / fair × 100",
        "```",
        "",
        "## Output labels",
        "",
        "| Deviation | Label |",
        "| --- | --- |",
        "| ≤ −5% | Undervalued |",
        "| −5% to +5% | Fair Value |",
        "| ≥ +5% | Overvalued |",
        "",
        "## Tier behaviour",
        "",
        "| Tier | Markets | Extras |",
        "| --- | --- | --- |",
        "| Premium | Gold, Silver | Composite score from price/fair ratio percentile |",
        "| Industrial | Copper / HG | China PMI architecture block (placeholder) |",
        "| PGM | Platinum, Palladium | Residual percentile + macro regression |",
        "",
        "## Trust grades",
        "",
        "| Grade | Criteria |",
        "| --- | --- |",
        "| A | n ≥ 156 weeks, R² ≥ 0.15, macro inputs fresh |",
        "| B | n ≥ 52 weeks, R² ≥ 0.08 |",
        "| C | Below B thresholds — display with caution |",
        "",
        "## Gates",
        "",
        f"- Minimum aligned weekly observations: **{52}**",
        f"- Minimum R²: **{0.08}**",
        "- Does not modify confluence scoring, COT, seasonality, or dashboard layout.",
        "",
        "## Wired markets",
        "",
    ]
    for m in METALS_MARKETS:
        v = (payload.get("instruments") or {}).get(m) or {}
        if v.get("wired"):
            lines.append(
                f"- **{m}**: dev {v.get('deviation_pct')}% · fair {v.get('fair_value')} · "
                f"spot {v.get('spot_price')} · trust **{v.get('trust_grade')}** · {v.get('valuation_bias')}"
            )
        else:
            lines.append(f"- **{m}**: unavailable — {v.get('unavailable_reason') or v.get('valuation_reason')}")

    lines.extend(["", "## Backtest diagnostics (deviation vs forward return)", ""])
    for m in METALS_MARKETS:
        b = (backtest.get("markets") or {}).get(m) or {}
        if not b.get("available"):
            lines.append(f"- **{m}**: {b.get('reason', 'n/a')}")
            continue
        lines.append(
            f"- **{m}**: R²={b.get('r_squared')} · n={b.get('n_obs')} · "
            f"trust {b.get('trust_grade')} · "
            f"{b.get('forward_weeks')}W corr={b.get('forward_return_correlation')} · "
            f"MAD={b.get('mean_abs_deviation_pct')}%"
        )
    lines.append("")
    return "\n".join(lines)


def render_audit_md(payload: dict[str, Any]) -> str:
    lines = [
        "# Metals Valuation V1 Audit",
        "",
        f"- Generated: `{payload.get('generated_at')}`",
        f"- Engine: `{payload.get('engine')}`",
        f"- Wired: **{payload.get('summary', {}).get('wired_count')}** / {payload.get('summary', {}).get('total_instruments')}",
        "",
        "## Example outputs",
        "",
        "| Market | Spot | Fair | Deviation % | Bias | Trust | R² | n |",
        "| --- | ---: | ---: | ---: | --- | --- | ---: | ---: |",
    ]
    for m in METALS_MARKETS:
        v = (payload.get("instruments") or {}).get(m) or {}
        reg = v.get("regression") or {}
        lines.append(
            f"| {m} | {v.get('spot_price') or '—'} | {v.get('fair_value') or '—'} | "
            f"{v.get('deviation_pct') if v.get('deviation_pct') is not None else '—'} | "
            f"{v.get('valuation_bias') or '—'} | {v.get('trust_grade') or '—'} | "
            f"{reg.get('r_squared') if reg.get('r_squared') is not None else '—'} | "
            f"{reg.get('n') if reg.get('n') is not None else '—'} |"
        )
    lines.append("")
    return "\n".join(lines)


def write_metals_valuation_exports(*, as_of_week: str | None = None) -> dict[str, Path]:
    payload = build_all_metals_valuations(as_of_week=as_of_week)
    backtest = run_backtest_diagnostics()

    text = json.dumps(payload, indent=2, ensure_ascii=False)
    for path in (METALS_OUT, PUBLIC_METALS_OUT):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    BACKTEST_JSON.parent.mkdir(parents=True, exist_ok=True)
    BACKTEST_JSON.write_text(json.dumps(backtest, indent=2), encoding="utf-8")
    DESIGN_MD.write_text(render_design_md(payload, backtest), encoding="utf-8")
    AUDIT_MD.write_text(render_audit_md(payload), encoding="utf-8")

    return {
        "met.md": DESIGN_MD,
        "backtest": BACKTEST_JSON,
        "audit_md": AUDIT_MD,
        "metals_valuation": METALS_OUT,
    }


def merge_metals_into_valuation_latest(valuation_doc: dict[str, Any]) -> dict[str, Any]:
    instruments = dict(valuation_doc.get("instruments") or {})
    for market in METALS_MARKETS:
        row = dict(instruments.get(market) or {})
        row["market"] = market
        row["valuation_pillar"] = "metals_real_yield"
        instruments[market] = row

    wired = sum(1 for v in instruments.values() if v.get("wired"))
    metals_wired = sum(1 for m in METALS_MARKETS if (instruments.get(m) or {}).get("wired"))
    out = dict(valuation_doc)
    out["instruments"] = instruments
    summary = dict(out.get("summary") or {})
    summary["wired_count"] = wired
    summary["unavailable_count"] = len(instruments) - wired
    summary["metals_wired_count"] = metals_wired
    out["summary"] = summary
    out["metals_pillar_engine"] = MODEL_ID
    out["metals_valuation_summary"] = {
        "total_instruments": len(METALS_MARKETS),
        "wired_count": metals_wired,
    }
    note = out.get("note") or ""
    if "metals_real_yield_v1" not in note:
        out["note"] = (
            note.rstrip()
            + " Metals pillar = metals_real_yield_v1 (DFII10 + DTWEXBGS macro regression)."
        )
    return out
