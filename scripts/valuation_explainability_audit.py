"""Generate valuation explainability audit artifacts (Phase 1 — transparency only)."""
from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
VAL_PATH = ROOT / "data" / "valuation_latest.json"
BACKTEST_PATH = ROOT / "data" / "audits" / "metals_valuation_v1_backtest.json"
OUT_DIR = ROOT / "data" / "audits"
PUBLIC_DATA = ROOT / "web-dashboard" / "public" / "data"

MODEL_INVENTORY = OUT_DIR / "valuation_model_inventory.md"
EXPLAIN_AUDIT = OUT_DIR / "valuation_explainability_audit.md"
EXAMPLES_JSON = PUBLIC_DATA / "valuation_explainability_examples.json"

EXAMPLE_MARKETS = [
    "Gold",
    "Silver",
    "Copper / HG",
    "Soybeans",
    "Canadian Dollar / 6C",
]


def _fx_fit_tier(n: int | None, r2: float | None) -> str:
    if not n or r2 is None or n < 52 or r2 < 0.08:
        return "None"
    if n >= 156 and r2 >= 0.25:
        return "High"
    if n >= 52 and r2 >= 0.18:
        return "Medium"
    return "Low"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _model_inventory_section() -> list[str]:
    return [
        "# Valuation Model Inventory",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "Operational pillar models only (exported to `valuation_latest.json`).",
        "",
        "## FX — `fx_carry_real_yield_v3`",
        "",
        "| Field | Detail |",
        "|---|---|",
        "| **Fair value** | log(spot) ~ 2Y yield diff (+ optional policy diff) + real-yield & inflation adjustments + DXY/Treasury regime tilt |",
        "| **Inputs** | Spot, policy rate, 2Y/10Y, CPI YoY (both legs), DXY percentile, US Treasury 2s10s |",
        "| **Deviation** | `(spot − fair) / fair × 100`; ±2% = Fair Value |",
        "| **Sample** | Daily aligned panel; regression n typically 2,500+ for G10 majors |",
        "| **Historical period** | ~2016-present (price store alignment) |",
        "| **R² gate** | ≥ 0.08 to publish fair value |",
        "| **Confidence** | None if missing/stale or R²<0.08; Low if stale CPI/policy/yield flags; High if n≥156 & R²≥0.25 fresh; Medium if n≥52 & R²≥0.18 fresh |",
        "| **Trust** | Audit PASS + foundation PASS + live scope required to wire |",
        "| **Backtest** | Not exported to UI (foundation audit + aligned obs in `fx_valuation_data_foundation_audit.json`) |",
        "",
        "## Metals — `metals_real_yield_v1`",
        "",
        "| Field | Detail |",
        "|---|---|",
        "| **Fair value** | `log(price) = β0 + β1·real_yield(DFII10) + β2·log(DXY)` |",
        "| **Inputs** | Canonical weekly metal price, FRED DFII10, DTWEXBGS (DXY fallback: DX timeline) |",
        "| **Deviation** | `(spot − fair) / fair × 100`; ±5% = Fair Value |",
        "| **Sample** | ISO weekly aligned panel (~389 obs for Gold/Silver) |",
        "| **Historical period** | ~2016-present |",
        "| **R² gate** | ≥ 0.08 (B); ≥ 0.15 for trust A |",
        "| **Trust grade** | A: n≥156 & R²≥0.15 & fresh; B: n≥52 & R²≥0.08; C otherwise |",
        "| **Confidence** | A→medium, B→low, C→none (display mapping) |",
        "| **Backtest** | `data/audits/metals_valuation_v1_backtest.json` — MAD deviation, 4W forward corr |",
        "",
        "## Agriculture — `agri_fundamental_valuation`",
        "",
        "| Field | Detail |",
        "|---|---|",
        "| **Fair value** | USDA PSD stocks-to-use regression or percentile vs price |",
        "| **Inputs** | Balance-sheet S/U, canonical spot, instrument-specific PSD file |",
        "| **Deviation** | `(spot − fair) / fair × 100`; ±5% = Fair Value |",
        "| **Sample** | Balance-sheet observation count (target ≥24 for medium+) |",
        "| **Models** | `agri_stu_regression_v1`, `agri_stu_percentile_v1` |",
        "| **Confidence** | high: R²≥0.25 & n≥24 regression; medium: n≥24; low: n≥12; else none |",
        "| **Backtest** | Not yet exported |",
        "",
    ]


def _fx_confidence_audit(instruments: dict[str, Any]) -> dict[str, Any]:
    rows = []
    for market, block in instruments.items():
        if block.get("model_id") != "fx_carry_real_yield_v3" or not block.get("wired"):
            continue
        reg = block.get("regression") or {}
        n = int(reg.get("n") or 0)
        r2 = reg.get("r_squared")
        conf = block.get("confidence")
        stale = block.get("stale_inputs") or []
        fit = _fx_fit_tier(n, r2 if isinstance(r2, (int, float)) else None)
        rows.append(
            {
                "market": market,
                "pair": block.get("pair"),
                "published_confidence": conf,
                "model_fit_tier": fit,
                "r_squared": r2,
                "n": n,
                "stale_inputs": stale,
                "downgraded_by_stale": bool(stale) and fit in {"High", "Medium"},
            }
        )

    all_low = all(r["published_confidence"] == "Low" for r in rows) if rows else False
    stale_downgrades = sum(1 for r in rows if r["downgraded_by_stale"])

    return {
        "wired_fx_count": len(rows),
        "all_published_low": all_low,
        "stale_downgrade_count": stale_downgrades,
        "finding": (
            "All wired FX pairs publish Confidence: Low because stale_inputs (primarily annual CPI YoY) "
            "force the Low tier even when R² supports High/Medium model-fit."
            if stale_downgrades == len(rows) and rows
            else "FX confidence varies by pair; see per-row table."
        ),
        "rows": rows,
    }


def _example_snapshot(market: str, block: dict[str, Any], backtest: dict[str, Any]) -> dict[str, Any]:
    reg = block.get("regression") or {}
    bt = (backtest.get("markets") or {}).get(market) or {}
    return {
        "market": market,
        "model_id": block.get("model_id"),
        "wired": block.get("wired"),
        "spot": block.get("spot_price"),
        "fair_value": block.get("fair_value"),
        "deviation_pct": block.get("deviation_pct"),
        "state": block.get("valuation_state"),
        "confidence": block.get("confidence"),
        "trust_grade": block.get("trust_grade"),
        "r_squared": reg.get("r_squared"),
        "n": reg.get("n"),
        "driver_summary": block.get("driver_summary"),
        "drivers": block.get("drivers"),
        "stale_inputs": block.get("stale_inputs"),
        "backtest_mad_pct": bt.get("mean_abs_deviation_pct"),
        "backtest_fwd_corr": bt.get("forward_return_correlation"),
    }


def main() -> None:
    val = _load_json(VAL_PATH)
    backtest = _load_json(BACKTEST_PATH) if BACKTEST_PATH.exists() else {}
    instruments = val.get("instruments") or {}

    # Copy backtest to public for workstation hook
    PUBLIC_DATA.mkdir(parents=True, exist_ok=True)
    if BACKTEST_PATH.exists():
        shutil.copy(BACKTEST_PATH, PUBLIC_DATA / "metals_valuation_v1_backtest.json")

    fx_audit = _fx_confidence_audit(instruments)
    examples = {
        m: _example_snapshot(m, instruments[m], backtest)
        for m in EXAMPLE_MARKETS
        if m in instruments
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    MODEL_INVENTORY.write_text("\n".join(_model_inventory_section()) + "\n", encoding="utf-8")

    lines = [
        "# Valuation Explainability Audit",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        f"Source: `{VAL_PATH.relative_to(ROOT)}`",
        "",
        "## Problem statement",
        "",
        "Large deviation labels (e.g. Silver +97% Overvalued) require visible evidence:",
        "inputs, model fit, trust, and confidence rationale — not just a scanner cell.",
        "",
        "## Universal workstation specification",
        "",
        "Every valued instrument exposes five sections in `ValuationWorkstationPanel`:",
        "",
        "1. **Valuation summary** — spot, fair, deviation %, state, confidence, trust grade",
        "2. **Model details** — model id, sample size, window, R², export timestamp",
        "3. **Driver breakdown** — factor-level evidence (FX diffs / metals macro / agri S/U)",
        "4. **Historical performance** — R², n, mean |deviation|, forward correlation (metals backtest)",
        "5. **Trust assessment** — plain-English why confidence/trust is High/Medium/Low",
        "",
        "Component: `web-dashboard/src/components/ValuationWorkstationPanel.jsx`",
        "",
        "## Confidence framework audit (FX)",
        "",
        fx_audit["finding"],
        "",
        f"- Wired FX instruments: **{fx_audit['wired_fx_count']}**",
        f"- All published Low: **{fx_audit['all_published_low']}**",
        f"- Downgraded by stale flags despite strong fit: **{fx_audit['stale_downgrade_count']}**",
        "",
        "| Market | Pair | Published | Fit tier | R² | n | Stale inputs |",
        "|---|---|---|---|---:|---:|---|",
    ]
    for r in fx_audit["rows"]:
        stale = ", ".join(r["stale_inputs"][:3]) + ("…" if len(r["stale_inputs"]) > 3 else "")
        lines.append(
            f"| {r['market']} | {r['pair']} | {r['published_confidence']} | {r['model_fit_tier']} | "
            f"{r['r_squared']} | {r['n']} | {stale or '—'} |"
        )

    lines.extend(
        [
            "",
            "### FX confidence findings",
            "",
            "1. **Is all-Low correct?** Partially — fair values publish (audit PASS) but confidence is capped at Low when `stale_inputs` is non-empty.",
            "2. **Are models genuinely low quality?** No — several pairs have R² 0.32–0.90; fit tier would be Medium/High without stale CPI flags.",
            "3. **Is the framework working?** Yes mechanically; it is **over-conservative** for differentiation because annual CPI YoY always flags stale.",
            "4. **Meaningful differentiation?** Published confidence does not differentiate wired FX today; use **model-fit tier** in trust section for honest nuance.",
            "",
            "## Screenshot-ready examples",
            "",
        ]
    )

    for m, ex in examples.items():
        lines.extend(
            [
                f"### {m}",
                "",
                f"- Spot: {ex.get('spot')} · Fair: {ex.get('fair_value')} · Dev: {ex.get('deviation_pct')}%",
                f"- State: {ex.get('state')} · Confidence: {ex.get('confidence')} · Trust: {ex.get('trust_grade') or '—'}",
                f"- R²: {ex.get('r_squared')} · n: {ex.get('n')}",
                f"- Drivers: {ex.get('driver_summary') or '—'}",
            ]
        )
        if ex.get("backtest_mad_pct") is not None:
            lines.append(
                f"- Backtest MAD: {ex.get('backtest_mad_pct')}% · 4W fwd corr: {ex.get('backtest_fwd_corr')}"
            )
        if ex.get("stale_inputs"):
            lines.append(f"- Stale inputs: {', '.join(ex['stale_inputs'])}")
        lines.append("")

    EXPLAIN_AUDIT.write_text("\n".join(lines) + "\n", encoding="utf-8")

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fx_confidence_audit": fx_audit,
        "examples": examples,
        "workstation_spec": "ValuationWorkstationPanel — 5 sections",
    }
    EXAMPLES_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Wrote {MODEL_INVENTORY}")
    print(f"Wrote {EXPLAIN_AUDIT}")
    print(f"Wrote {EXAMPLES_JSON}")


if __name__ == "__main__":
    main()
