"""One-off verification for seasonality outlier filter export."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXPORT = ROOT / "data" / "processed" / "seasonality_price_latest.json"
AUDIT = ROOT / "data" / "audits" / "seasonality_outlier_filter_report.json"


def max_keys(rows: list, keys: tuple[str, ...]) -> float:
    m = 0.0
    for row in rows:
        for k in keys:
            v = row.get(k)
            if isinstance(v, (int, float)):
                m = max(m, float(v))
    return m


def summarize(market: str, block: dict) -> dict:
    chart = block.get("chart_series") or []
    return {
        "trust_grade": block.get("trust_grade"),
        "data_quality_warning": block.get("data_quality_warning"),
        "max_actual": max_keys(chart, ("actual",)),
        "max_seasonal_10y": max_keys(chart, ("seasonal_10y",)),
        "max_seasonal_5y": max_keys(chart, ("seasonal_5y",)),
        "max_seasonal_3y": max_keys(chart, ("seasonal_3y",)),
        "max_proj_10y": max_keys(chart, ("proj_10y",)),
        "max_proj_5y": max_keys(chart, ("proj_5y",)),
        "max_proj_3y": max_keys(chart, ("proj_3y",)),
        "max_any_indexed": max_keys(chart, ("actual", "seasonal_10y", "seasonal_5y", "seasonal_3y")),
        "max_any_proj": max_keys(chart, ("proj_10y", "proj_5y", "proj_3y")),
        "outlier_filter_audit": block.get("outlier_filter_audit"),
    }


def main() -> None:
    doc = json.loads(EXPORT.read_text(encoding="utf-8"))
    markets = doc.get("markets") or {}
    print("generated_at:", doc.get("generated_at"))
    for name in ("Copper / HG", "Corn"):
        print(f"\n=== {name} ===")
        print(json.dumps(summarize(name, markets[name]), indent=2))
    if AUDIT.exists():
        print("\n=== audit report ===")
        print(AUDIT.read_text(encoding="utf-8")[:4000])


if __name__ == "__main__":
    main()
