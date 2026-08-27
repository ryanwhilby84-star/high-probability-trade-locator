"""Probe densify+median+gaussian seasonal path and write SVG for visual QA."""

from __future__ import annotations

import math
from datetime import datetime
from pathlib import Path

from hptl.markets.usd_index_identity import ICE_DXY_ID
from hptl.seasonality_workstation.indexed_seasonality import (
    _mean,
    _median,
    calendar_doy,
    load_daily_closes_for_seasonality,
)

ROOT = Path(__file__).resolve().parents[1]


def densify(rows: list) -> dict[int, float]:
    rows = sorted(rows)
    base = rows[0][1]
    sparse = {calendar_doy(d): (c / base) * 100 for d, c in rows}
    keys = sorted(sparse)
    out: dict[int, float] = {}
    for doy in range(1, 366):
        if doy in sparse:
            out[doy] = sparse[doy]
            continue
        left = [k for k in keys if k < doy]
        right = [k for k in keys if k > doy]
        if not left or not right:
            continue
        a, b = left[-1], right[0]
        t = (doy - a) / (b - a)
        out[doy] = sparse[a] + t * (sparse[b] - sparse[a])
    return out


def gauss(curve: dict[int, float | None], sigma: float = 14.0) -> dict[int, float | None]:
    keys = list(range(1, 366))
    vals = [curve.get(k) for k in keys]
    radius = int(sigma * 3)
    out: dict[int, float | None] = {}
    for i, k in enumerate(keys):
        if vals[i] is None:
            out[k] = None
            continue
        wsum = vsum = 0.0
        for j in range(max(0, i - radius), min(len(keys), i + radius + 1)):
            if vals[j] is None:
                continue
            w = math.exp(-0.5 * ((j - i) / sigma) ** 2)
            wsum += w
            vsum += w * vals[j]
        out[k] = vsum / wsum if wsum else None
    return out


def write_svg(curve: dict[int, float | None], asof: str, asof_doy: int, n: int, path: Path) -> None:
    vals = [(d, curve[d]) for d in range(1, 366) if curve.get(d) is not None]
    ymin = min(v for _, v in vals)
    ymax = max(v for _, v in vals)
    pad = (ymax - ymin) * 0.12 or 0.5
    ymin -= pad
    ymax += pad
    w, h = 900, 280
    left, right, top, bottom = 40, 20, 24, 40

    def xy(doy: int, v: float) -> tuple[float, float]:
        x = left + (doy - 1) / (365 - 1) * (w - left - right)
        y = top + (1 - (v - ymin) / (ymax - ymin)) * (h - top - bottom)
        return x, y

    def path_d(seg: list[tuple[int, float]]) -> str:
        parts = []
        for i, (d, v) in enumerate(seg):
            x, y = xy(d, v)
            parts.append(("M" if i == 0 else "L") + f"{x:.1f},{y:.1f}")
        return " ".join(parts)

    hist = [(d, v) for d, v in vals if d <= asof_doy]
    fwd = [(d, v) for d, v in vals if d >= asof_doy]
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" style="background:#0b1220">',
        f'<text x="40" y="16" fill="#94a3b8" font-size="12">'
        f"DXY seasonal path redesign — densify + median + gaussian σ=14 · n={n} · asof {asof}"
        f"</text>",
    ]
    for m, label in [
        (1, "Jan"),
        (32, "Feb"),
        (60, "Mar"),
        (91, "Apr"),
        (121, "May"),
        (152, "Jun"),
        (182, "Jul"),
        (213, "Aug"),
        (244, "Sep"),
        (274, "Oct"),
        (305, "Nov"),
        (335, "Dec"),
    ]:
        x, _ = xy(m, ymin)
        lines.append(f'<text x="{x:.0f}" y="{h - 8}" fill="#64748b" font-size="10">{label}</text>')
    xnow, _ = xy(asof_doy, ymin)
    lines.append(
        f'<line x1="{xnow:.1f}" y1="{top}" x2="{xnow:.1f}" y2="{h - bottom}" '
        f'stroke="#e2e8f0" stroke-width="1"/>'
    )
    lines.append(f'<path d="{path_d(hist)}" fill="none" stroke="#94a3b8" stroke-width="2"/>')
    lines.append(f'<path d="{path_d(fwd)}" fill="none" stroke="#ef4444" stroke-width="2.2"/>')
    lines.append("</svg>")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    from hptl.seasonality_workstation.indexed_seasonality import (
        build_normalised_seasonal_curve,
    )

    daily, _ = load_daily_closes_for_seasonality(ICE_DXY_ID)
    pack = build_normalised_seasonal_curve(daily)
    assert pack.get("available"), pack
    curve = {int(k): v for k, v in pack["curve"].items() if v is not None}
    q = pack["quality"]
    print(
        f"production n={pack['sample_size']} range={q['range']} "
        f"flip_rate={q['sign_flip_rate']} extrema={q['extrema']}"
    )
    print("horizons", pack["horizons"])
    out = ROOT / "data/audits/indexed_seasonality_validation/dxy_seasonal_path_production.svg"
    write_svg(curve, pack["asof"], pack["asof_doy"], pack["sample_size"], out)
    print("wrote", out)


if __name__ == "__main__":
    main()
