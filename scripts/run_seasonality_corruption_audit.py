"""Seasonality Data Corruption Audit — full pipeline diagnostics for one instrument."""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

SEA_EXPORT = ROOT / "web-dashboard/public/data/seasonality_price_latest.json"

from hptl.prices.canonical_timeline import load_canonical_timeline  # noqa: E402
from hptl.seasonality.seasonality_engine import (  # noqa: E402
    avg_path,
    compute_seasonality_price_block,
    normalized_year_path,
    project_forward,
    year_week_closes,
)
from hptl.seasonality.seasonality_price_bars import weekly_closes_for_instrument  # noqa: E402
from hptl.seasonality.seasonality_trust import attach_trust_metadata  # noqa: E402


def stats(vals: list[float]) -> dict:
    if not vals:
        return {"min": None, "max": None, "median": None, "count": 0}
    s = sorted(vals)
    return {
        "min": min(s),
        "max": max(s),
        "median": statistics.median(s),
        "count": len(s),
    }


def find_worst_instrument() -> str:
    """Scan export for highest indexed value across all markets."""
    doc = json.loads(SEA_EXPORT.read_text(encoding="utf-8"))
    worst_m, worst_v = "", 0.0
    for m, b in (doc.get("markets") or {}).items():
        for r in b.get("chart_series") or []:
            for k in ("actual", "seasonal_10y", "seasonal_5y", "seasonal_3y", "proj_10y", "proj_5y", "proj_3y"):
                v = r.get(k)
                if isinstance(v, (int, float)) and v > worst_v:
                    worst_v, worst_m = float(v), m
    return worst_m or "Copper / HG"


def top_indexed(entries: list[tuple[str, int, float, str | None]], n=20):
    return sorted(entries, key=lambda x: abs(x[2]), reverse=True)[:n]


def audit_market(market: str) -> dict:
    tl = load_canonical_timeline(market)
    daily = list(tl.daily_closes()) if tl else []
    daily_closes = [c for _, c in daily]

    bars, bar_source, _ = weekly_closes_for_instrument(market)
    weekly_closes = [c for _, c in bars]

    yw = year_week_closes(bars)
    block = compute_seasonality_price_block(
        market, bars, price_store_key=market, bar_source=bar_source,
        canonical_source=tl.canonical_source if tl else None,
        canonical_symbol=tl.canonical_symbol if tl else None,
    )
    block = attach_trust_metadata(block, bars)

    current_year = block.get("current_year")
    anchor_week = block.get("current_week") or 1
    current_path_raw = normalized_year_path(yw.get(current_year, {}))

    hist_years = sorted(y for y in yw if y < current_year)
    years_10y = hist_years[-10:] if len(hist_years) >= 10 else hist_years
    avg_10y = avg_path(years_10y, yw) if years_10y else {}
    anchor_index = current_path_raw.get(anchor_week) or block.get("latest_price", {}).get("index")
    proj_10y = (
        project_forward(anchor_week=anchor_week, anchor_index=anchor_index or 100.0, avg=avg_10y)
        if years_10y and anchor_index
        else {}
    )

    # Export payload
    export_doc = json.loads(SEA_EXPORT.read_text(encoding="utf-8"))
    export_block = (export_doc.get("markets") or {}).get(market) or {}
    export_cs = export_block.get("chart_series") or []

    # UI rows simulation (mirrors seasonalityProjectionModel.js)
    ui_rows = []
    for r in export_cs:
        week = r.get("week")
        actual = r.get("actual")
        ui_rows.append({
            "week": week,
            "currentYearPath": actual if week <= anchor_week and actual is not None else None,
            "seasonal_10y": r.get("seasonal_10y"),
            "forwardSeasonalPath": r.get("proj_10y") if week >= anchor_week else None,
        })

    def collect_indexed(rows, field_map):
        out = []
        for r in rows:
            for label, key in field_map.items():
                v = r.get(key)
                if isinstance(v, (int, float)):
                    out.append((label, r.get("week"), float(v), None))
        return out

    export_indexed = collect_indexed(
        export_cs,
        {
            "actual": "actual",
            "seasonal_10y": "seasonal_10y",
            "seasonal_5y": "seasonal_5y",
            "seasonal_3y": "seasonal_3y",
            "proj_10y": "proj_10y",
        },
    )
    ui_indexed = collect_indexed(
        ui_rows,
        {"currentYearPath": "currentYearPath", "seasonal_10y": "seasonal_10y", "forwardSeasonalPath": "forwardSeasonalPath"},
    )

    cy_indexed = list(current_path_raw.values())
    s10_vals = [v for v in avg_10y.values() if v is not None]
    proj_vals = [v for v in proj_10y.values() if v is not None]

    # Map extreme weekly dates
    weekly_date_map = {d: c for d, c in bars}
    spike_weeks = []
    for w, idx in sorted(current_path_raw.items(), key=lambda x: -x[1])[:20]:
        # find ISO week ending date from yw current year
        wc = yw.get(current_year, {})
        close = wc.get(w)
        spike_weeks.append({"week": w, "indexed": round(idx, 2), "weekly_close": close})

    return {
        "market": market,
        "symbol": tl.canonical_symbol if tl else export_block.get("canonical_symbol"),
        "data_source": tl.canonical_source if tl else export_block.get("canonical_source"),
        "bar_source": bar_source,
        "trust_grade": block.get("trust_grade"),
        "earliest_date": daily[0][0] if daily else (bars[0][0] if bars else None),
        "latest_date": daily[-1][0] if daily else (bars[-1][0] if bars else None),
        "current_year": current_year,
        "current_week": anchor_week,
        "stages": {
            "raw_daily": stats(daily_closes),
            "weekly_aggregated": stats(weekly_closes),
            "indexed_current_year": stats(cy_indexed),
            "seasonal_10y_avg": stats(s10_vals),
            "forward_proj_10y": stats(proj_vals),
            "export_payload_indexed": stats([v for _, _, v, _ in export_indexed]),
            "ui_chart_rows_indexed": stats([v for _, _, v, _ in ui_indexed]),
        },
        "top20_export_indexed": [
            {"field": f, "week": w, "value": round(v, 2)} for f, w, v, _ in top_indexed(export_indexed, 20)
        ],
        "top20_current_year_spikes": spike_weeks[:20],
        "top20_weekly_closes": [
            {"date": d, "close": round(c, 4)} for d, c in sorted(bars, key=lambda x: -x[1])[:20]
        ],
        "unit_break_sample": _unit_break_sample(daily),
    }


def _unit_break_sample(daily: list[tuple[str, float]], limit=8) -> list[dict]:
    out = []
    prev = None
    for d, c in daily:
        if prev and prev[1] > 0 and (c / prev[1] > 10 or prev[1] / c > 10):
            out.append({
                "from_date": prev[0],
                "from_close": round(prev[1], 4),
                "to_date": d,
                "to_close": round(c, 4),
                "ratio": round(c / prev[1], 1),
            })
            if len(out) >= limit:
                break
        prev = (d, c)
    return out


def main():
    market = find_worst_instrument()
    result = audit_market(market)
    out_json = ROOT / "data/audits/seasonality_corruption_audit_result.json"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
