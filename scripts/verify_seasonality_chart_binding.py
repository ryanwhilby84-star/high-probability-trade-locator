#!/usr/bin/env python3
"""Verify Seasonality Workstation chart binding (wiring only — no methodology changes).

Mirrors web-dashboard selectPriceUnitFullYear + Freeze full_year.index selection.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.markets.usd_index_identity import ICE_DXY_ID  # noqa: E402
from hptl.seasonality_workstation.payload import (  # noqa: E402
    build_seasonality_workstation_payload,
)

OUT = ROOT / "data" / "audits" / "seasonality_freeze_v1" / "chart_binding_verification.json"


def select_price_unit_full_year(pack: dict, *, series_mode: str, use_smoothed: bool = False):
    """Mirror SeasonalityCharts.jsx selectPriceUnitFullYear."""
    if not pack or not pack.get("available"):
        return None, None
    if series_mode == "roadmap":
        if use_smoothed and (pack.get("smoothed") or {}).get("full_year"):
            return "payload.seasonal_roadmap.smoothed.full_year", pack["smoothed"]["full_year"]
        if (pack.get("unsmoothed") or {}).get("full_year"):
            return "payload.seasonal_roadmap.unsmoothed.full_year", pack["unsmoothed"]["full_year"]
        return None, []
    if series_mode == "mean_return":
        return "payload.seasonal_price_path.full_year", pack.get("full_year") or []
    return None, []


def values(points, key: str) -> list[float]:
    return [float(p[key]) for p in points if p.get(key) is not None]


def fingerprint(vals: list[float], *, dataset: str, source_path: str, source_fn: str) -> dict:
    return {
        "datasetName": dataset,
        "sourcePath": source_path,
        "sourceFunction": source_fn,
        "n": len(vals),
        "first5": [round(v, 6) for v in vals[:5]],
        "last5": [round(v, 6) for v in vals[-5:]],
        "min": round(min(vals), 6) if vals else None,
        "max": round(max(vals), 6) if vals else None,
        "amplitude": round(max(vals) - min(vals), 6) if vals else None,
    }


def main() -> int:
    payload = build_seasonality_workstation_payload(ICE_DXY_ID, lookback="15Y")
    assert payload.get("status") == "ok", payload.get("error")

    roadmap = payload["seasonal_roadmap"]
    mean_ret = payload["seasonal_price_path"]
    freeze = payload["normalised_seasonality"]

    rm_path, rm_pts = select_price_unit_full_year(roadmap, series_mode="roadmap", use_smoothed=True)
    pp_path, pp_pts = select_price_unit_full_year(mean_ret, series_mode="mean_return")
    fz_path, fz_pts = "payload.normalised_seasonality.full_year", freeze.get("full_year") or []

    rm_vals = values(rm_pts, "price")
    pp_vals = values(pp_pts, "price")
    fz_vals = values(fz_pts, "index")

    n = min(len(rm_vals), len(pp_vals))
    max_diff_rm_pp = max(abs(rm_vals[i] - pp_vals[i]) for i in range(n)) if n else None

    # Mean-return must NOT resolve to roadmap smoothed/unsmoothed even if those keys existed
    poisoned = {
        **mean_ret,
        "smoothed": roadmap.get("smoothed"),
        "unsmoothed": roadmap.get("unsmoothed"),
    }
    poison_path, poison_pts = select_price_unit_full_year(poisoned, series_mode="mean_return")
    poison_vals = values(poison_pts, "price")
    poison_ok = poison_vals == pp_vals and poison_path == "payload.seasonal_price_path.full_year"

    report = {
        "status": "ok",
        "instrument_id": ICE_DXY_ID,
        "lookback": "15Y",
        "data_flow": [
            "build_seasonal_roadmap_curve / build_seasonal_price_path_curve / build_normalised_seasonal_curve",
            "engine.build_seasonality_research → payload.build_seasonality_workstation_payload",
            "GET /api/seasonality-workstation/{id} via scripts/build_seasonality_workstation_payload.py",
            "SeasonalityWorkstationPage fetch → SeasonalityWorkstation selectors",
            "SeasonalRoadmapChart | SeasonalPricePathChart | NormalisedSeasonalityChart",
        ],
        "bindings": {
            "roadmap": fingerprint(
                rm_vals,
                dataset=roadmap["method"]["version"],
                source_path=rm_path,
                source_fn="build_seasonal_roadmap_curve",
            ),
            "mean_return": fingerprint(
                pp_vals,
                dataset=mean_ret["method"]["version"],
                source_path=pp_path,
                source_fn="build_seasonal_price_path_curve",
            ),
            "freeze_index": fingerprint(
                fz_vals,
                dataset=freeze["method"]["version"],
                source_path=fz_path,
                source_fn="build_normalised_seasonal_curve",
            ),
        },
        "proof_distinct_plotted_series": {
            "roadmap_vs_mean_return_max_abs_diff": round(max_diff_rm_pp, 6) if max_diff_rm_pp is not None else None,
            "roadmap_vs_mean_return_distinct": bool(max_diff_rm_pp and max_diff_rm_pp > 1e-6),
            "freeze_uses_index_not_price": abs(sum(fz_vals) / len(fz_vals)) < 5 if fz_vals else False,
            "mean_return_ignores_roadmap_bundles_when_poisoned": poison_ok,
        },
        "wiring_correction": {
            "issue": (
                "usePriceUnitSeasonalSeries previously used a shared fallback: "
                "if useSmoothed=false, any pack.unsmoothed.full_year was preferred over pack.full_year. "
                "That could cross-bind Mean-return onto a Roadmap-shaped bundle if keys were ever shared."
            ),
            "fix": (
                "Explicit seriesMode in selectPriceUnitFullYear: "
                "roadmap → smoothed/unsmoothed only; mean_return → full_year only; "
                "freeze → normalised.full_year.index via NormalisedSeasonalityChart."
            ),
            "component": "web-dashboard/src/seasonality_workstation/SeasonalityCharts.jsx",
            "selector": "selectPriceUnitFullYear / seriesMode prop on PriceUnitSeasonalChart",
        },
        "success": {
            "roadmap_displays_roadmap_dataset": rm_path == "payload.seasonal_roadmap.smoothed.full_year",
            "each_methodology_unique_dataset": bool(
                max_diff_rm_pp and max_diff_rm_pp > 1e-6 and poison_ok and fz_vals
            ),
        },
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report["success"], indent=2))
    print(json.dumps(report["proof_distinct_plotted_series"], indent=2))
    print("wrote", OUT)
    return 0 if report["success"]["each_methodology_unique_dataset"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
