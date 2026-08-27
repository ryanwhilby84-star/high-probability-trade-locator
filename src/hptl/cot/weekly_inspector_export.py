"""Compact weekly inspector export (separate from research markers JSON)."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.cot.weekly_inspector_flow import (
    MEASURE,
    MEASURE_LABEL,
    PCT_CHG_MILD,
    PCT_CHG_STRONG,
    build_weekly_inspector_series,
)

COT3Y_PATHS = (
    PROCESSED_DIR / "cot_3y_series_latest.json",
    PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_3y_series_latest.json",
    PROJECT_ROOT / "data" / "cot_3y_series_latest.json",
)

CANONICAL_PATH = PROCESSED_DIR / "cot_weekly_inspector_latest.json"
PUBLIC_PATH = (
    PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_weekly_inspector_latest.json"
)
DIST_PATH = PROJECT_ROOT / "web-dashboard" / "dist" / "data" / "cot_weekly_inspector_latest.json"
DATA_PATH = PROJECT_ROOT / "data" / "cot_weekly_inspector_latest.json"

DIR_CODES = {
    "strongly_increasing": 0,
    "increasing": 1,
    "stable": 2,
    "decreasing": 3,
    "strongly_decreasing": 4,
    "unknown": 5,
}
DIR_FROM = {v: k for k, v in DIR_CODES.items()}

TEMP_CODES = {
    "heating_rapidly": 0,
    "heating": 1,
    "cooling_from_extreme": 2,
    "deepening_extreme": 3,
    "recovering": 4,
    "building": 5,
    "weakening": 6,
    "elevated_stable": 7,
    "depressed_stable": 8,
    "neutral": 9,
    "unknown": 10,
    "recovering_strong": 11,
}
TEMP_FROM = {v: k for k, v in TEMP_CODES.items()}

REL_CODES = {
    "aligned": 0,
    "opposed": 1,
    "strong_opposition": 2,
    "mixed": 3,
    "unavailable": 4,
}
REL_FROM = {v: k for k, v in REL_CODES.items()}

FLOW_CODES = {
    "opposition_widening_rapidly": 0,
    "opposition_narrowing_rapidly": 1,
    "opposition_widening": 2,
    "opposition_narrowing": 3,
    "spread_widening": 4,
    "spread_narrowing": 5,
    "stable": 6,
    "unavailable": 7,
}
FLOW_FROM = {v: k for k, v in FLOW_CODES.items()}

ARROW = {
    "strongly_increasing": "▲▲",
    "increasing": "▲",
    "stable": "→",
    "decreasing": "▼",
    "strongly_decreasing": "▼▼",
    "unknown": "·",
}

STATE_FROM_TEMP = {
    "heating_rapidly": "Deeper into extreme",
    "heating": "Deeper into extreme",
    "cooling_from_extreme": "Cooling from extreme",
    "deepening_extreme": "Deeper into low extreme",
    "recovering": "Moving out of extreme",
    "recovering_strong": "Strong rotation away from extreme",
    "building": "Rotation strengthening",
    "weakening": "Rotation weakening",
    "elevated_stable": "Elevated / stable",
    "depressed_stable": "Depressed / stable",
    "neutral": "Neutral",
    "unknown": "Unavailable",
}


def _load_cot3y() -> dict[str, Any]:
    for p in COT3Y_PATHS:
        if p.is_file():
            return json.loads(p.read_text(encoding="utf-8"))
    return {"markets": {}}


def _write(path: Path, payload: dict[str, Any]) -> None:
    from hptl.cot.json_safety import sanitize_for_json

    path.parent.mkdir(parents=True, exist_ok=True)
    safe = sanitize_for_json(payload)
    path.write_text(json.dumps(safe, separators=(",", ":"), allow_nan=False), encoding="utf-8")


def _pack_group(g: dict[str, Any]) -> list[Any]:
    """Compact: [net,w1,w4,w12,pct,p1,p4,p12,obs,dir,temp,extreme]."""
    return [
        g.get("net"),
        g.get("weekly_change"),
        g.get("four_week_change"),
        g.get("twelve_week_change"),
        g.get("percentile"),
        g.get("percentile_change_1w"),
        g.get("percentile_change_4w"),
        g.get("percentile_change_12w"),
        g.get("percentile_observation_count"),
        DIR_CODES.get(g.get("direction") or "unknown", 5),
        TEMP_CODES.get(g.get("temperature") or "unknown", 10),
        1 if g.get("is_extreme") else 0,
    ]


def _pack_cross(x: dict[str, Any]) -> list[Any]:
    """[c,nc,nr,cn_spread,cn_spct,cn1,cn4,nr_spread,nr_spct,rel,flow]."""
    return [
        x.get("commercial_percentile"),
        x.get("noncommercial_percentile"),
        x.get("nonreportable_percentile"),
        x.get("comm_nc_spread"),
        x.get("comm_nc_spread_percentile"),
        x.get("comm_nc_spread_change_1w"),
        x.get("comm_nc_spread_change_4w"),
        x.get("comm_nr_spread"),
        x.get("comm_nr_spread_percentile"),
        REL_CODES.get(x.get("relationship") or "unavailable", 4),
        FLOW_CODES.get(x.get("flow") or "unavailable", 7),
    ]


def compact_market_weeks(full: dict[str, Any]) -> dict[str, Any]:
    rows = []
    for w in full.get("weeks") or []:
        rows.append(
            [
                w.get("date"),
                _pack_group(w.get("commercial") or {}),
                _pack_group(w.get("noncommercial") or {}),
                _pack_group(w.get("nonreportable") or {}),
                _pack_cross(w.get("cross") or {}),
            ]
        )
    return {
        "available": bool(full.get("available")),
        "week_count": len(rows),
        "rows": rows,
    }


def expand_group(arr: list[Any] | None) -> dict[str, Any]:
    a = arr or []
    while len(a) < 12:
        a.append(None)
    direction = DIR_FROM.get(a[9], "unknown")
    temperature = TEMP_FROM.get(a[10], "unknown")
    return {
        "net": a[0],
        "weekly_change": a[1],
        "four_week_change": a[2],
        "twelve_week_change": a[3],
        "percentile": a[4],
        "percentile_change_1w": a[5],
        "percentile_change_4w": a[6],
        "percentile_change_12w": a[7],
        "percentile_observation_count": a[8],
        "direction": direction,
        "direction_arrow": ARROW.get(direction, "·"),
        "temperature": temperature,
        "state_label": STATE_FROM_TEMP.get(temperature, "Unavailable"),
        "is_extreme": bool(a[11]),
        "measure": MEASURE,
    }


def expand_cross(arr: list[Any] | None) -> dict[str, Any]:
    a = arr or []
    while len(a) < 11:
        a.append(None)
    return {
        "commercial_percentile": a[0],
        "noncommercial_percentile": a[1],
        "nonreportable_percentile": a[2],
        "comm_nc_spread": a[3],
        "comm_nc_spread_percentile": a[4],
        "comm_nc_spread_change_1w": a[5],
        "comm_nc_spread_change_4w": a[6],
        "comm_nr_spread": a[7],
        "comm_nr_spread_percentile": a[8],
        "relationship": REL_FROM.get(a[9], "unavailable"),
        "flow": FLOW_FROM.get(a[10], "unavailable"),
        "measure": MEASURE,
    }


def expand_compact_market(block: dict[str, Any]) -> dict[str, Any]:
    """Expand compact rows → weekly_inspector shape consumed by the frontend."""
    weeks = []
    for row in block.get("rows") or []:
        if not row:
            continue
        date = row[0]
        c = expand_group(row[1] if len(row) > 1 else None)
        nc = expand_group(row[2] if len(row) > 2 else None)
        nr = expand_group(row[3] if len(row) > 3 else None)
        cross = expand_cross(row[4] if len(row) > 4 else None)
        weeks.append(
            {
                "date": date,
                "commercial": c,
                "noncommercial": nc,
                "nonreportable": nr,
                "cross": cross,
            }
        )
    return {
        "available": bool(block.get("available")),
        "measure": MEASURE,
        "measure_label": MEASURE_LABEL,
        "weeks": weeks,
        "week_count": len(weeks),
    }


def run_weekly_inspector_export(
    *,
    cot3y: dict[str, Any] | None = None,
    markets: list[str] | None = None,
) -> dict[str, Any]:
    doc = cot3y if cot3y is not None else _load_cot3y()
    all_markets = doc.get("markets") or {}
    selected = sorted(str(k) for k in all_markets.keys()) if markets is None else list(markets)

    out_markets: dict[str, Any] = {}
    for mid in selected:
        block = all_markets.get(mid)
        resolved = mid
        if not block:
            for k, v in all_markets.items():
                if mid.lower() in str(k).lower():
                    block = v
                    resolved = str(k)
                    break
        if not block:
            out_markets[mid] = {"available": False, "week_count": 0, "rows": []}
            continue
        series = list(block.get("series") or [])
        full = build_weekly_inspector_series(series)
        out_markets[resolved] = compact_market_weeks(full)

    payload = {
        "version": "cot_weekly_inspector_v1",
        "engine": "weekly_inspector_flow",
        "measure": MEASURE,
        "measure_label": MEASURE_LABEL,
        "direction_thresholds": {
            "strong_percentile_points": PCT_CHG_STRONG,
            "mild_percentile_points": PCT_CHG_MILD,
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "markets": out_markets,
        "summary": {
            "markets": len(out_markets),
            "available": sum(1 for m in out_markets.values() if m.get("available")),
        },
    }

    for path in (CANONICAL_PATH, PUBLIC_PATH, DATA_PATH):
        _write(path, payload)
    if DIST_PATH.parent.is_dir():
        try:
            shutil.copy2(PUBLIC_PATH, DIST_PATH)
        except OSError:
            _write(DIST_PATH, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export compact weekly inspector series")
    parser.add_argument("--market", action="append", dest="markets")
    args = parser.parse_args(argv)
    payload = run_weekly_inspector_export(markets=args.markets)
    s = payload.get("summary") or {}
    size = PUBLIC_PATH.stat().st_size if PUBLIC_PATH.is_file() else 0
    print(
        f"weekly_inspector export: available={s.get('available')}/{s.get('markets')} "
        f"bytes={size} path={PUBLIC_PATH}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
