"""Export soybean valuation-vs-price research data for the dashboard.

This is deliberately source-agnostic: ingestion writes point-in-time soybean
observations to data/processed/soybean_valuation/inputs.json.  This exporter
fits scarcity value using only prior observations, computes the dual-anchor
series, and writes a compact dashboard JSON artifact.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.valuation.soybean_valuation_v1 import SoybeanInputs, compute, fit_inverse_stu

INPUT_PATH = PROJECT_ROOT / "data" / "processed" / "soybean_valuation" / "inputs.json"
OUTPUT_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "soybean_valuation_research.json"


def _load_rows(path: Path = INPUT_PATH) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    doc = json.loads(path.read_text(encoding="utf-8"))
    rows = doc.get("observations") if isinstance(doc, dict) else doc
    return sorted([r for r in (rows or []) if isinstance(r, dict)], key=lambda r: str(r.get("as_of", "")))


def build_walk_forward(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Compute each valuation using a scarcity fit trained only on earlier rows."""
    out: list[dict[str, Any]] = []
    training: list[tuple[float, float]] = []
    for row in rows:
        try:
            ending = float(row["ending_stocks"])
            total_use = float(row["total_use"])
            market = float(row["market_price_usd_per_bushel"])
            stu = ending / total_use if total_use > 0 else 0.0
        except (KeyError, TypeError, ValueError, ZeroDivisionError):
            continue

        fit = fit_inverse_stu(training)
        if fit.get("alpha") is not None:
            result = compute(
                SoybeanInputs(
                    as_of=str(row["as_of"]),
                    market_price_usd_per_bushel=market,
                    ending_stocks=ending,
                    total_use=total_use,
                    meal_usd_per_short_ton=row.get("meal_usd_per_short_ton"),
                    oil_cents_per_lb=row.get("oil_cents_per_lb"),
                    required_crush_margin_usd_per_bushel=float(row.get("required_crush_margin_usd_per_bushel", 1.50)),
                ),
                fit,
            )
            out.append({
                "date": result["as_of"],
                "market_price": result["market_price"],
                "fair_value": result["fair_value"],
                "scarcity_value": result["scarcity_fair_value"],
                "crush_value": result["crush_implied_value"],
                "deviation_pct": result["deviation_pct"],
                "state": result["valuation_state"],
                "stocks_to_use": result["stocks_to_use"],
                "fit_r2": fit.get("r2"),
                "fit_n": fit.get("n"),
            })

        # Add the current observation only after its valuation has been computed.
        if stu > 0 and market > 0:
            training.append((stu, market))
    return out


def export(path: Path = OUTPUT_PATH) -> dict[str, Any]:
    rows = _load_rows()
    series = build_walk_forward(rows)
    latest = series[-1] if series else None
    payload = {
        "market": "Soybeans",
        "model_id": "soybean_dual_anchor_v1",
        "method": "walk_forward",
        "point_in_time_safe": True,
        "observations": len(series),
        "latest": latest,
        "series": series,
        "source_status": {
            "input_path": str(INPUT_PATH.relative_to(PROJECT_ROOT)),
            "input_exists": INPUT_PATH.exists(),
            "note": "Requires timestamped USDA balance-sheet observations and bean/meal/oil market inputs. No synthetic fallback is used.",
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


if __name__ == "__main__":
    doc = export()
    print(f"Soybean valuation research: {doc['observations']} walk-forward observations")
    print(f"Wrote {OUTPUT_PATH}")
