#!/usr/bin/env python3
"""Compare Seasonal Roadmap v1 turning points vs Bernd reference + mean-return path.

Read-only audit. Does not invent a new methodology.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.markets.usd_index_identity import ICE_DXY_ID  # noqa: E402
from hptl.seasonality_workstation.indexed_seasonality import (  # noqa: E402
    load_daily_closes_for_seasonality,
)
from hptl.seasonality_workstation.seasonal_price_path import (  # noqa: E402
    build_seasonal_price_path,
)
from hptl.seasonality_workstation.seasonal_roadmap import (  # noqa: E402
    build_seasonal_roadmap,
)

OUT = ROOT / "data" / "audits" / "seasonality_freeze_v1"
ASOF = "2026-07-23"
LOOKBACK = 15

# Bernd / OTC reference landmarks (Jan–Dec narrative from prior validation;
# Oct→Sep display wraps the same seasonal shape). Approximate month targets.
BERND_LANDMARKS = [
    {"kind": "peak", "month": "Dec", "note": "late-year / Dec peak"},
    {"kind": "trough", "month": "Feb", "note": "early-Feb trough"},
    {"kind": "peak", "month": "Mar", "note": "late-Mar peak"},
]


def td_month(td: int, d_len: int) -> str:
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    idx = min(11, int((td - 1) / max(d_len - 1, 1) * 12))
    return months[idx]


def major_landmarks(path: list[float]) -> dict:
    n = len(path)
    amin = min(range(n), key=lambda i: path[i])
    amax = max(range(n), key=lambda i: path[i])
    h1 = range(0, n // 2)
    h2 = range(n // 2, n)
    return {
        "global_trough": {
            "trading_day": amin + 1,
            "month": td_month(amin + 1, n),
            "value": round(path[amin], 4),
        },
        "global_peak": {
            "trading_day": amax + 1,
            "month": td_month(amax + 1, n),
            "value": round(path[amax], 4),
        },
        "h1_peak": {
            "trading_day": max(h1, key=lambda i: path[i]) + 1,
            "month": td_month(max(h1, key=lambda i: path[i]) + 1, n),
            "value": round(path[max(h1, key=lambda i: path[i])], 4),
        },
        "h1_trough": {
            "trading_day": min(h1, key=lambda i: path[i]) + 1,
            "month": td_month(min(h1, key=lambda i: path[i]) + 1, n),
            "value": round(path[min(h1, key=lambda i: path[i])], 4),
        },
        "h2_peak": {
            "trading_day": max(h2, key=lambda i: path[i]) + 1,
            "month": td_month(max(h2, key=lambda i: path[i]) + 1, n),
            "value": round(path[max(h2, key=lambda i: path[i])], 4),
        },
        "h2_trough": {
            "trading_day": min(h2, key=lambda i: path[i]) + 1,
            "month": td_month(min(h2, key=lambda i: path[i]) + 1, n),
            "value": round(path[min(h2, key=lambda i: path[i])], 4),
        },
        "amplitude": round(max(path) - min(path), 4),
    }


def month_distance(a: str, b: str) -> int:
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    ia, ib = months.index(a), months.index(b)
    return min((ia - ib) % 12, (ib - ia) % 12)


def score_vs_bernd(lm: dict) -> dict:
    """Score how well landmarks hit Bernd's Dec peak / Feb trough / Mar peak narrative."""
    # Prefer H2 peak near Dec, H1 trough near Feb, H1 peak near Mar
    checks = [
        ("dec_peak", lm["h2_peak"]["month"], "Dec"),
        ("feb_trough", lm["h1_trough"]["month"], "Feb"),
        ("mar_peak", lm["h1_peak"]["month"], "Mar"),
    ]
    # Also allow global peak in Nov/Dec and global/h1 trough in Jan/Feb
    alt = [
        ("global_peak_late", lm["global_peak"]["month"], ["Nov", "Dec"]),
        ("global_or_h1_trough_early", lm["global_trough"]["month"], ["Jan", "Feb"]),
    ]
    detail = []
    score = 0
    for name, got, want in checks:
        dist = month_distance(got, want)
        hit = dist <= 1
        score += 1 if hit else 0
        detail.append({"check": name, "got": got, "want": want, "month_dist": dist, "hit": hit})
    for name, got, wants in alt:
        hit = got in wants
        score += 0.5 if hit else 0
        detail.append({"check": name, "got": got, "want": wants, "hit": hit})
    return {"score": score, "max_score": 4.0, "detail": detail}


def path_corr(a: list[float], b: list[float]) -> float | None:
    n = min(len(a), len(b))
    if n < 3:
        return None
    a2, b2 = a[:n], b[:n]
    ma, mb = sum(a2) / n, sum(b2) / n
    num = sum((a2[i] - ma) * (b2[i] - mb) for i in range(n))
    den = (sum((x - ma) ** 2 for x in a2) ** 0.5) * (sum((x - mb) ** 2 for x in b2) ** 0.5)
    return None if den == 0 else round(num / den, 4)


def main() -> None:
    daily, meta = load_daily_closes_for_seasonality(ICE_DXY_ID)
    if not daily:
        raise SystemExit(f"no daily: {meta}")

    road = build_seasonal_roadmap(daily, asof=ASOF, lookback_years=LOOKBACK, smooth=5)
    mean_ret = build_seasonal_price_path(daily, asof=ASOF, lookback_years=LOOKBACK)
    assert road["available"] and mean_ret["available"]

    # Compare both smoothed roadmap and unsmoothed; primary = SMA(5) as default display
    road_sm = road["prices_smooth"] or road["prices_raw"]
    road_raw = road["prices_raw"]
    mean_px = mean_ret["prices"]

    lm_road = major_landmarks(road_sm)
    lm_road_raw = major_landmarks(road_raw)
    lm_mean = major_landmarks(mean_px)

    sc_road = score_vs_bernd(lm_road)
    sc_mean = score_vs_bernd(lm_mean)

    closer = None
    if sc_road["score"] > sc_mean["score"] + 0.25:
        closer = "roadmap_materially_closer"
    elif sc_mean["score"] > sc_road["score"] + 0.25:
        closer = "mean_return_materially_closer"
    else:
        closer = "no_material_difference_in_turning_point_score"

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "asof": ASOF,
        "lookback_years": LOOKBACK,
        "bernd_reference_landmarks": BERND_LANDMARKS,
        "roadmap": {
            "method": road["method"]["version"],
            "sample_years": road["sample_years"],
            "D": road["D"],
            "amplitude_sma5": lm_road["amplitude"],
            "amplitude_raw": lm_road_raw["amplitude"],
            "landmarks_sma5": lm_road,
            "bernd_score": sc_road,
            "forecast_stats_note": "separate; see roadmap.forecast_stats in payload",
            "forecast_stats": road["forecast_stats"],
        },
        "mean_return_path": {
            "method": mean_ret["method"]["version"],
            "sample_years": mean_ret["sample_years"],
            "D": mean_ret["D"],
            "amplitude": lm_mean["amplitude"],
            "landmarks": lm_mean,
            "bernd_score": sc_mean,
        },
        "path_corr_roadmap_vs_mean_return": path_corr(road_sm, mean_px),
        "verdict": {
            "roadmap_turning_points_materially_closer_to_bernd": closer
            == "roadmap_materially_closer",
            "comparison": closer,
            "roadmap_score": sc_road["score"],
            "mean_return_score": sc_mean["score"],
            "interpretation": (
                "Seasonal Roadmap (avg indexed year paths) is materially closer to Bernd's "
                "major turning-point sequence than the mean-return cumsum path."
                if closer == "roadmap_materially_closer"
                else (
                    "Mean-return path scores closer on this coarse Bernd landmark checklist."
                    if closer == "mean_return_materially_closer"
                    else (
                        "Neither path is materially closer to Bernd's Dec/Feb/Mar landmark "
                        "sequence under this checklist; Roadmap and mean-return remain distinct "
                        "products but share a similar coarse seasonal calendar on ICE DXY."
                    )
                )
            ),
        },
    }

    OUT.mkdir(parents=True, exist_ok=True)
    out_json = OUT / "seasonal_roadmap_vs_bernd_compare.json"
    out_json.write_text(json.dumps(report, indent=2), encoding="utf-8")

    md = [
        "# Seasonal Roadmap v1 vs Bernd + mean-return path",
        "",
        f"- As-of: `{ASOF}` · Lookback: `{LOOKBACK}Y` · ICE DXY Yahoo",
        f"- Roadmap Bernd score: **{sc_road['score']}** / {sc_road['max_score']}",
        f"- Mean-return Bernd score: **{sc_mean['score']}** / {sc_mean['max_score']}",
        f"- Verdict: **{closer}**",
        "",
        report["verdict"]["interpretation"],
        "",
        "## Roadmap landmarks (SMA5)",
        "",
        "```",
        json.dumps(lm_road, indent=2),
        "```",
        "",
        "## Mean-return landmarks",
        "",
        "```",
        json.dumps(lm_mean, indent=2),
        "```",
        "",
        f"- Path corr (roadmap SMA5 vs mean-return): `{report['path_corr_roadmap_vs_mean_return']}`",
        f"- Roadmap amplitude SMA5 / raw: `{lm_road['amplitude']}` / `{lm_road_raw['amplitude']}`",
        f"- Mean-return amplitude: `{lm_mean['amplitude']}`",
        "",
    ]
    out_md = OUT / "seasonal_roadmap_vs_bernd_compare.md"
    out_md.write_text("\n".join(md), encoding="utf-8")
    print(json.dumps(report["verdict"], indent=2))
    print("wrote", out_json)


if __name__ == "__main__":
    main()
