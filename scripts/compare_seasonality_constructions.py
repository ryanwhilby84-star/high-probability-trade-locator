#!/usr/bin/env python3
"""Head-to-head seasonal constructions on ICE DXY for reference morphology.

Reference label: ``OTC 15yr Seasonality v2 15 40 85 20.00``
Likely method family: average daily % change by day-of-year (often trading-day
aligned), cumulative sum, light smoothing, ~15Y lookback.

Writes comparison SVG + JSON. Publishes nothing into the product.
"""

from __future__ import annotations

import json
import math
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.markets.usd_index_identity import ICE_DXY_ID  # noqa: E402
from hptl.seasonality_workstation.indexed_seasonality import (  # noqa: E402
    calendar_doy,
    load_daily_closes_for_seasonality,
)

OUT_DIR = ROOT / "data" / "audits" / "seasonality_construction_compare"


def _parse(d: str) -> date:
    return datetime.strptime(d[:10], "%Y-%m-%d").date()


def _mean(xs: list[float]) -> float | None:
    return sum(xs) / len(xs) if xs else None


def _median(xs: list[float]) -> float | None:
    if not xs:
        return None
    ys = sorted(xs)
    n = len(ys)
    return ys[n // 2] if n % 2 else 0.5 * (ys[n // 2 - 1] + ys[n // 2])


def _trimmed_mean(xs: list[float], frac: float = 0.1) -> float | None:
    if not xs:
        return None
    ys = sorted(xs)
    k = int(len(ys) * frac)
    core = ys[k : len(ys) - k] if len(ys) - 2 * k >= 1 else ys
    return _mean(core)


def _centered_sma(vals: list[float], window: int) -> list[float]:
    if window <= 1:
        return list(vals)
    half = window // 2
    n = len(vals)
    out = []
    for i in range(n):
        chunk = vals[max(0, i - half) : min(n, i + half + 1)]
        out.append(sum(chunk) / len(chunk))
    return out


def prominent_extrema(path: list[float], min_prom_frac: float = 0.07) -> int:
    """Count peaks/troughs with prominence >= fraction of full range."""
    if len(path) < 5:
        return 0
    rng = max(path) - min(path)
    if rng <= 0:
        return 0
    thr = rng * min_prom_frac
    count = 0
    for i in range(2, len(path) - 2):
        if path[i] >= path[i - 1] and path[i] >= path[i + 1] and path[i] > path[i - 2] and path[i] > path[i + 2]:
            left = path[i] - min(path[max(0, i - 30) : i + 1])
            right = path[i] - min(path[i : min(len(path), i + 31)])
            if min(left, right) >= thr:
                count += 1
        if path[i] <= path[i - 1] and path[i] <= path[i + 1] and path[i] < path[i - 2] and path[i] < path[i + 2]:
            left = max(path[max(0, i - 30) : i + 1]) - path[i]
            right = max(path[i : min(len(path), i + 31)]) - path[i]
            if min(left, right) >= thr:
                count += 1
    return count


def zigzag_turns(path: list[float], thr_frac: float = 0.08) -> int:
    """Major swing count via percent zigzag on the path range."""
    if len(path) < 3:
        return 0
    rng = max(path) - min(path)
    if rng <= 0:
        return 0
    thr = rng * thr_frac
    pivots = [0]
    direction = 0
    extreme_i = 0
    for i in range(1, len(path)):
        move = path[i] - path[extreme_i]
        if direction >= 0 and move <= -thr:
            pivots.append(extreme_i)
            direction = -1
            extreme_i = i
        elif direction <= 0 and move >= thr:
            pivots.append(extreme_i)
            direction = 1
            extreme_i = i
        elif direction > 0 and path[i] >= path[extreme_i]:
            extreme_i = i
        elif direction < 0 and path[i] <= path[extreme_i]:
            extreme_i = i
        elif direction == 0:
            if abs(move) >= thr:
                direction = 1 if move > 0 else -1
                extreme_i = i
    pivots.append(extreme_i)
    # unique ordered
    uniq = []
    for p in pivots:
        if not uniq or p != uniq[-1]:
            uniq.append(p)
    return max(0, len(uniq) - 2)  # internal turns


@dataclass
class Score:
    name: str
    lookback: int
    points: int
    range_pts: float
    prominent_extrema: int
    zigzag_turns: int
    sign_flip_rate: float
    passes_structure: bool
    family: str


def score(name: str, path: list[float], lookback: int, family: str) -> Score:
    if len(path) < 50:
        return Score(name, lookback, len(path), 0, 0, 0, 1.0, False, family)
    dfs = [path[i] - path[i - 1] for i in range(1, len(path))]
    flips = sum(1 for i in range(1, len(dfs)) if dfs[i] * dfs[i - 1] < 0)
    flip_rate = flips / len(dfs)
    pe = prominent_extrema(path, 0.07)
    zz = zigzag_turns(path, 0.08)
    rng = max(path) - min(path)
    # Structure like the reference: several major swings, not one slope, not noise
    passes = (
        zz >= 6
        and zz <= 18
        and pe >= 6
        and flip_rate <= 0.22
        and rng >= 1.5
    )
    return Score(
        name=name,
        lookback=lookback,
        points=len(path),
        range_pts=round(rng, 3),
        prominent_extrema=pe,
        zigzag_turns=zz,
        sign_flip_rate=round(flip_rate, 4),
        passes_structure=passes,
        family=family,
    )


def years_scope(by_year: dict, asof_year: int, lookback: int) -> list[int]:
    return [y for y in sorted(by_year) if asof_year - lookback <= y < asof_year]


def path_trading_day_return_cumsum(
    by_year: dict,
    asof_year: int,
    lookback: int,
    *,
    agg: str = "mean",
    smooth: int = 15,
) -> list[float]:
    years = years_scope(by_year, asof_year, lookback)
    buckets: dict[int, list[float]] = {}
    for y in years:
        rows = sorted(by_year[y], key=lambda t: t[0])
        if len(rows) < 180:
            continue
        for i in range(1, len(rows)):
            r = rows[i][1] / rows[i - 1][1] - 1.0
            buckets.setdefault(i, []).append(r)
    if not buckets:
        return []
    max_td = min(max(buckets), 260)
    avgs = []
    for td in range(1, max_td + 1):
        xs = buckets.get(td) or []
        if agg == "median":
            v = _median(xs)
        elif agg == "trimmed":
            v = _trimmed_mean(xs)
        else:
            v = _mean(xs)
        avgs.append(0.0 if v is None else v)
    avgs = _centered_sma(avgs, smooth)
    cum = 0.0
    out = []
    for r in avgs:
        cum += r
        out.append(100.0 * (1.0 + cum))
    return out


def path_trading_day_indexed(
    by_year: dict,
    asof_year: int,
    lookback: int,
    *,
    agg: str = "mean",
    smooth: int = 9,
) -> list[float]:
    years = years_scope(by_year, asof_year, lookback)
    buckets: dict[int, list[float]] = {}
    for y in years:
        rows = sorted(by_year[y], key=lambda t: t[0])
        if len(rows) < 180:
            continue
        base = rows[0][1]
        if base <= 0:
            continue
        for i, (_, c) in enumerate(rows, start=1):
            buckets.setdefault(i, []).append((c / base) * 100.0)
    if not buckets:
        return []
    max_td = min(max(buckets), 260)
    vals = []
    for td in range(1, max_td + 1):
        xs = buckets.get(td) or []
        if agg == "median":
            v = _median(xs)
        else:
            v = _mean(xs)
        vals.append(v)
    # fill holes
    filled = []
    last = 100.0
    for v in vals:
        if v is None:
            filled.append(last)
        else:
            filled.append(v)
            last = v
    return _centered_sma(filled, smooth)


def path_calendar_return_cumsum(
    by_year: dict,
    asof_year: int,
    lookback: int,
    *,
    agg: str = "mean",
    smooth: int = 15,
) -> list[float]:
    years = years_scope(by_year, asof_year, lookback)
    buckets: dict[int, list[float]] = {d: [] for d in range(1, 366)}
    for y in years:
        rows = sorted(by_year[y], key=lambda t: t[0])
        if len(rows) < 180:
            continue
        for i in range(1, len(rows)):
            doy = calendar_doy(rows[i][0])
            r = rows[i][1] / rows[i - 1][1] - 1.0
            buckets[doy].append(r)
    avgs = []
    for doy in range(1, 366):
        xs = buckets[doy]
        if agg == "median":
            v = _median(xs)
        elif agg == "trimmed":
            v = _trimmed_mean(xs)
        else:
            v = _mean(xs)
        avgs.append(0.0 if v is None else v)
    avgs = _centered_sma(avgs, max(1, smooth // 2))  # returns smoother shorter
    cum = 0.0
    path = []
    for r in avgs:
        cum += r
        path.append(100.0 * (1.0 + cum))
    return _centered_sma(path, smooth)


def path_to_doy_curve(path: list[float]) -> dict[int, float]:
    """Stretch an N-point path onto DOY 1..365 for shared plotting."""
    if not path:
        return {}
    n = len(path)
    out = {}
    for doy in range(1, 366):
        pos = (doy - 1) / 364 * (n - 1)
        i = int(pos)
        t = pos - i
        if i >= n - 1:
            out[doy] = path[-1]
        else:
            out[doy] = path[i] * (1 - t) + path[i + 1] * t
    return out


def write_svg(
    panels: list[tuple[str, dict[int, float], Score]],
    asof_doy: int,
    path: Path,
) -> None:
    panel_h = 150
    header = 36
    w = 1100
    left, right, top_pad, bottom = 48, 16, 16, 26
    h = header + len(panels) * panel_h
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" style="background:#0b1220">',
        '<text x="14" y="16" fill="#e2e8f0" font-size="13">'
        "ICE DXY construction comparison vs reference morphology "
        "(OTC 15yr Seasonality v2 — avg daily returns, cumulated, light smooth)"
        "</text>",
        '<text x="14" y="32" fill="#94a3b8" font-size="11">'
        "PASS needs zigzag turns 6–18 and prominent extrema ≥6 (major swings, not a slope, not noise). "
        "Grey=to date, blue=forward."
        "</text>",
    ]
    months = [(1, "J"), (32, "F"), (60, "M"), (91, "A"), (121, "M"), (152, "J"),
              (182, "J"), (213, "A"), (244, "S"), (274, "O"), (305, "N"), (335, "D")]

    for pi, (title, curve, sc) in enumerate(panels):
        y0 = header + pi * panel_h
        vals = [(d, curve[d]) for d in range(1, 366) if d in curve]
        ymin = min(v for _, v in vals)
        ymax = max(v for _, v in vals)
        pad = (ymax - ymin) * 0.15 or 0.4
        ymin -= pad
        ymax += pad

        def xy(doy: int, v: float) -> tuple[float, float]:
            x = left + (doy - 1) / 364 * (w - left - right)
            y = y0 + top_pad + (1 - (v - ymin) / (ymax - ymin)) * (panel_h - top_pad - bottom)
            return x, y

        def path_d(seg: list[tuple[int, float]], color: str) -> str:
            parts = []
            for i, (d, v) in enumerate(seg):
                x, yy = xy(d, v)
                parts.append(("M" if i == 0 else "L") + f"{x:.1f},{yy:.1f}")
            return f'<path d="{" ".join(parts)}" fill="none" stroke="{color}" stroke-width="1.9"/>'

        badge = "#22c55e" if sc.passes_structure else "#f87171"
        lines.append(
            f'<text x="14" y="{y0 + 12}" fill="{badge}" font-size="11">'
            f"{'PASS' if sc.passes_structure else 'fail'} | {title} | "
            f"zz={sc.zigzag_turns} prom={sc.prominent_extrema} "
            f"range={sc.range_pts} flip={sc.sign_flip_rate}"
            f"</text>"
        )
        for m, lab in months:
            x, _ = xy(m, ymin)
            lines.append(f'<text x="{x:.0f}" y="{y0 + panel_h - 4}" fill="#64748b" font-size="9">{lab}</text>')
        xnow, _ = xy(asof_doy, ymin)
        lines.append(
            f'<line x1="{xnow:.1f}" y1="{y0 + top_pad}" x2="{xnow:.1f}" '
            f'y2="{y0 + panel_h - bottom}" stroke="#cbd5e1" stroke-width="0.9"/>'
        )
        hist = [(d, v) for d, v in vals if d <= asof_doy]
        fwd = [(d, v) for d, v in vals if d >= asof_doy]
        lines.append(path_d(hist, "#94a3b8"))
        lines.append(path_d(fwd, "#3b82f6"))
    lines.append("</svg>")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    daily, meta = load_daily_closes_for_seasonality(ICE_DXY_ID)
    asof = daily[-1][0]
    asof_d = _parse(asof)
    asof_year = asof_d.year
    asof_doy = calendar_doy(asof_d)

    by_year: dict[int, list[tuple[date, float]]] = {}
    for d_s, c in daily:
        d = _parse(d_s)
        if d.year >= asof_year:
            continue
        by_year.setdefault(d.year, []).append((d, c))

    jobs: list[tuple[str, list[float], int, str]] = []

    # Rejected production (level + heavy gauss) for contrast
    from hptl.seasonality_workstation.indexed_seasonality import build_normalised_seasonal_curve

    pack = build_normalised_seasonal_curve(daily, lookback_years=10, smooth=14)
    if pack.get("available"):
        cur = [pack["curve"][str(d)] for d in range(1, 366) if pack["curve"].get(str(d)) is not None]
        jobs.append(("REJECTED_level_median_gauss14_10Y", cur, 10, "rejected"))

    # Trading-day return cumsum (classic TV / Season Chart family)
    for lb in (10, 15):
        for sm in (9, 15, 21, 31, 41):
            for agg in ("mean", "trimmed"):
                p = path_trading_day_return_cumsum(by_year, asof_year, lb, agg=agg, smooth=sm)
                jobs.append((f"tdoy_retCum_{agg}_sma{sm}_{lb}Y", p, lb, "tdoy_ret_cumsum"))

    # Trading-day indexed average (level paths, light smooth)
    for lb in (10, 15):
        for sm in (5, 9, 15, 21):
            for agg in ("mean", "median"):
                p = path_trading_day_indexed(by_year, asof_year, lb, agg=agg, smooth=sm)
                jobs.append((f"tdoy_idx_{agg}_sma{sm}_{lb}Y", p, lb, "tdoy_indexed"))

    # Calendar DOY return cumsum
    for lb in (10, 15):
        for sm in (9, 15, 21, 31):
            p = path_calendar_return_cumsum(by_year, asof_year, lb, agg="mean", smooth=sm)
            jobs.append((f"cal_retCum_mean_sma{sm}_{lb}Y", p, lb, "cal_ret_cumsum"))

    scored_full = []
    for name, path, lb, family in jobs:
        if not path:
            continue
        sc = score(name, path, lb, family)
        curve = path_to_doy_curve(path)
        scored_full.append((name, curve, sc, path))

    scored_full.sort(
        key=lambda t: (
            not t[2].passes_structure,
            -t[2].zigzag_turns,
            -t[2].prominent_extrema,
            abs(t[2].zigzag_turns - 10),
        )
    )

    # Panels: rejected + all PASS + best near-misses
    panels = []
    seen = set()
    for name, curve, sc, _ in scored_full:
        if name.startswith("REJECTED") and name not in seen:
            panels.append((name, curve, sc))
            seen.add(name)
    for name, curve, sc, _ in scored_full:
        if sc.passes_structure and name not in seen:
            panels.append((name, curve, sc))
            seen.add(name)
    for name, curve, sc, _ in scored_full:
        if len(panels) >= 14:
            break
        if name not in seen and sc.zigzag_turns >= 5:
            panels.append((name, curve, sc))
            seen.add(name)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    svg = OUT_DIR / "dxy_construction_comparison.svg"
    write_svg(panels, asof_doy, svg)

    report = {
        "asof": asof,
        "price_instrument": meta.get("price_instrument_id"),
        "reference": {
            "label": "OTC 15yr Seasonality v2 15 40 85 20.00",
            "inferred_lookback": 15,
            "inferred_method": (
                "Average daily percent returns aligned by day-of-year "
                "(trading-day alignment common), cumulative sum, light smoothing. "
                "NOT heavy-smoothed average of indexed price levels."
            ),
        },
        "gate": {
            "zigzag_turns": "6..18",
            "prominent_extrema_min": 6,
            "max_flip_rate": 0.22,
            "min_range": 1.5,
        },
        "candidates": [asdict(sc) for _, _, sc, _ in scored_full],
        "passing": [asdict(sc) for _, _, sc, _ in scored_full if sc.passes_structure],
        "svg": str(svg.relative_to(ROOT)),
    }
    (OUT_DIR / "dxy_construction_comparison.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )

    print(f"asof={asof} candidates={len(scored_full)} PASS={len(report['passing'])}")
    print("\nRanked (structure first):")
    for _, _, sc, _ in scored_full[:20]:
        flag = "PASS" if sc.passes_structure else "fail"
        print(
            f"  [{flag}] {sc.name}: zz={sc.zigzag_turns} prom={sc.prominent_extrema} "
            f"range={sc.range_pts} flip={sc.sign_flip_rate}"
        )
    print(f"\nSVG: {svg}")
    if not report["passing"]:
        print("\nNo construction passed. Do not publish.")
        return 2

    # Write the best PASS alone for inspection
    best_name, best_curve, best_sc, _ = next(
        t for t in scored_full if t[2].passes_structure
    )
    write_svg([(best_name, best_curve, best_sc)], asof_doy, OUT_DIR / "dxy_best_pass.svg")
    print(f"Best PASS: {best_name}")
    print(f"Best SVG: {OUT_DIR / 'dxy_best_pass.svg'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
