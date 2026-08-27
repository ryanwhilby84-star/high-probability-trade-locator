#!/usr/bin/env python3
"""Read-only validation: export Freeze v1.0 ICE DXY chart + shape facts for reference compare.

Does not modify seasonality methodology.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.markets.usd_index_identity import ICE_DX_YAHOO_SYMBOL, ICE_DXY_ID  # noqa: E402
from hptl.prices.price_store import load_instrument_record_internal  # noqa: E402
from hptl.seasonality_workstation.indexed_seasonality import (  # noqa: E402
    FREEZE_SMOOTH_WINDOW,
    METHOD_VERSION,
    build_freeze_v1_path,
    build_normalised_seasonal_curve,
    load_daily_closes_for_seasonality,
)

OUT = ROOT / "data" / "audits" / "seasonality_freeze_v1"


def zigzag_turns(path: list[float], frac: float = 0.08) -> int:
    rng = max(path) - min(path)
    if rng <= 0:
        return 0
    thr = rng * frac
    piv = 0
    direction = 0
    ex = 0
    for i in range(1, len(path)):
        move = path[i] - path[ex]
        if direction >= 0 and move <= -thr:
            piv += 1
            direction = -1
            ex = i
        elif direction <= 0 and move >= thr:
            piv += 1
            direction = 1
            ex = i
        elif direction > 0 and path[i] >= path[ex]:
            ex = i
        elif direction < 0 and path[i] <= path[ex]:
            ex = i
        elif direction == 0 and abs(move) >= thr:
            direction = 1 if move > 0 else -1
            ex = i
    return max(0, piv - 1)


def major_extrema(path: list[float], frac: float = 0.12) -> list[dict]:
    """Prominent peaks/troughs for qualitative comparison to reference months."""
    rng = max(path) - min(path)
    thr = rng * frac
    out = []
    for i in range(2, len(path) - 2):
        if path[i] >= path[i - 1] and path[i] >= path[i + 1] and path[i] > path[i - 2] and path[i] > path[i + 2]:
            left = path[i] - min(path[max(0, i - 25) : i + 1])
            right = path[i] - min(path[i : min(len(path), i + 26)])
            if min(left, right) >= thr:
                out.append({"kind": "peak", "trading_day": i + 1, "value": path[i]})
        if path[i] <= path[i - 1] and path[i] <= path[i + 1] and path[i] < path[i - 2] and path[i] < path[i + 2]:
            left = max(path[max(0, i - 25) : i + 1]) - path[i]
            right = max(path[i : min(len(path), i + 26)]) - path[i]
            if min(left, right) >= thr:
                out.append({"kind": "trough", "trading_day": i + 1, "value": path[i]})
    return out


def td_to_approx_month(td: int, d_len: int) -> str:
    # Map trading day onto ~12 equal month buckets for narrative compare
    months = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    idx = min(11, int((td - 1) / max(d_len - 1, 1) * 12))
    return months[idx]


def write_svg(pack: dict, smoothed: list[float], path: Path) -> None:
    asof = pack["asof"]
    asof_td = pack["asof_trading_day"]
    d_len = len(smoothed)
    w, h, left, right, top, bottom = 1000, 320, 50, 30, 42, 40
    ymin, ymax = min(smoothed), max(smoothed)
    pad = (ymax - ymin) * 0.12 or 0.2
    ymin -= pad
    ymax += pad

    def xy(td: int, v: float) -> tuple[float, float]:
        x = left + (td - 1) / (d_len - 1) * (w - left - right)
        y = top + (1 - (v - ymin) / (ymax - ymin)) * (h - top - bottom)
        return x, y

    def path_d(seg: list[tuple[int, float]], color: str, width: float) -> str:
        parts = []
        for i, (td, v) in enumerate(seg):
            x, y = xy(td, v)
            parts.append(("M" if i == 0 else "L") + f"{x:.1f},{y:.1f}")
        return f'<path d="{" ".join(parts)}" fill="none" stroke="{color}" stroke-width="{width}"/>'

    vals = list(enumerate(smoothed, start=1))
    hist = [(td, v) for td, v in vals if td <= asof_td]
    fwd = [(td, v) for td, v in vals if td >= asof_td]
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" style="background:#0b1220">',
        f'<text x="16" y="18" fill="#e2e8f0" font-size="13">'
        f"HPTL Freeze v1.0 — ICE DXY ({ICE_DX_YAHOO_SYMBOL}) — "
        f"N={pack['sample_size']} D={d_len} SMA={FREEZE_SMOOTH_WINDOW} asof={asof}"
        f"</text>",
        '<text x="16" y="34" fill="#94a3b8" font-size="11">'
        "Centered seasonal % · grey=to today · red=forward · dashed=0% · read-only validation export"
        "</text>",
    ]
    for td, lab in [
        (1, "Jan"),
        (22, "Feb"),
        (43, "Mar"),
        (64, "Apr"),
        (85, "May"),
        (106, "Jun"),
        (127, "Jul"),
        (148, "Aug"),
        (169, "Sep"),
        (190, "Oct"),
        (211, "Nov"),
        (232, "Dec"),
    ]:
        if td <= d_len:
            x, _ = xy(td, ymin)
            lines.append(f'<text x="{x:.0f}" y="{h - 10}" fill="#64748b" font-size="10">{lab}</text>')
    xnow, _ = xy(asof_td, ymin)
    y0 = xy(1, 0.0)[1]
    lines.append(
        f'<line x1="{xnow:.1f}" y1="{top}" x2="{xnow:.1f}" y2="{h - bottom}" stroke="#e2e8f0" stroke-width="1"/>'
    )
    lines.append(
        f'<line x1="{left}" y1="{y0:.1f}" x2="{w - right}" y2="{y0:.1f}" '
        f'stroke="#334155" stroke-width="0.8" stroke-dasharray="4 3"/>'
    )
    lines.append(path_d(hist, "#94a3b8", 2))
    lines.append(path_d(fwd, "#ef4444", 2.2))
    for p in pack.get("weekly_points") or []:
        td = p.get("trading_day")
        if not td or td <= asof_td:
            continue
        x, y = xy(td, p["index"])
        lines.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="2.5" fill="#fca5a5" stroke="#ef4444"/>'
        )
    lines.append("</svg>")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    daily, meta = load_daily_closes_for_seasonality(ICE_DXY_ID)
    rec = load_instrument_record_internal(meta.get("price_instrument_id") or ICE_DXY_ID) or {}
    scale = rec.get("price_scale") or {}
    core = build_freeze_v1_path(daily, lookback_years=15, smooth=FREEZE_SMOOTH_WINDOW)
    pack = build_normalised_seasonal_curve(daily, lookback_years=15, smooth=FREEZE_SMOOTH_WINDOW)
    if not core.get("available") or not pack.get("available"):
        print("FAIL", core.get("reason") or pack.get("reason"))
        return 1

    smoothed = core["smoothed"]
    raw = core["raw"]
    extrema = major_extrema(smoothed)
    for e in extrema:
        e["approx_month"] = td_to_approx_month(e["trading_day"], core["D"])

    svg = OUT / "hptl_freeze_v1_ice_dxy.svg"
    write_svg(pack, smoothed, svg)

    # Reference morphology notes (from provided screenshot; not digitized series)
    reference = {
        "label": "OTC Year Seasonality v2 (Bernd Skorupinski video screenshot)",
        "ticker_on_chart": "ICEUS DX1!",
        "x_axis": "seasonal year shown as Oct→Sep (rolling display), not necessarily Jan→Dec",
        "observed_structure": [
            "Up Oct → mid/late Dec peak",
            "Sharp decline late Dec → early Feb trough",
            "Recovery Feb → late Mar peak",
            "Decline late Mar → early May",
            "Modest bounce then sideways/down into mid-Jun",
            "Blue forward from ~mid-Jun: gradual decline through Jul–Sep",
        ],
        "visual_character": "frequent small wiggles; sharper turning points; price-like micro-structure",
        "coloring": "grey historical / blue forward",
        "asof_in_screenshot_approx": "mid-June (grey/blue transition)",
    }

    hptl = {
        "method_version": METHOD_VERSION,
        "instrument_id": meta.get("price_instrument_id"),
        "yahoo_symbol": ICE_DX_YAHOO_SYMBOL,
        "price_scale": scale,
        "asof": pack["asof"],
        "asof_trading_day": pack["asof_trading_day"],
        "N": core["N"],
        "D": core["D"],
        "sample_years": core["sample_years"],
        "mu": core["mu"],
        "smooth_window": FREEZE_SMOOTH_WINDOW,
        "raw_range_pct": [min(raw), max(raw)],
        "smoothed_range_pct": [min(smoothed), max(smoothed)],
        "zigzag_turns_raw": zigzag_turns(raw),
        "zigzag_turns_smoothed": zigzag_turns(smoothed),
        "major_extrema": extrema,
        "horizons": pack["horizons"],
        "daily_bars": len(daily),
        "first_last_bar": [daily[0], daily[-1]],
        "svg": str(svg.relative_to(ROOT)).replace("\\", "/"),
    }

    # Objective difference bullets (shape-level; no digitization of reference)
    diffs = []
    ref_months = {e["approx_month"] for e in extrema}
    # Check presence of Feb trough / Dec peak / Mar peak proxies
    kinds_by_month = {}
    for e in extrema:
        kinds_by_month.setdefault(e["approx_month"], []).append(e["kind"])
    diffs.append(
        {
            "topic": "calendar_window",
            "observation": (
                "Reference pane is drawn on an Oct→Sep seasonal window; "
                "HPTL Freeze v1.0 exports a Jan→Dec trading-day axis."
            ),
        }
    )
    diffs.append(
        {
            "topic": "asof_position",
            "observation": (
                f"Reference grey/blue split ≈ mid-June; HPTL asof={pack['asof']} "
                f"(trading day {pack['asof_trading_day']}/ {core['D']}), ~late July."
            ),
        }
    )
    diffs.append(
        {
            "topic": "amplitude",
            "observation": (
                f"HPTL smoothed centered range ≈ {hptl['smoothed_range_pct'][1]-hptl['smoothed_range_pct'][0]:.2f} "
                "percentage points. Reference vertical scale is unknown (autoscaled pane); "
                "absolute amplitude cannot be compared numerically without digitizing their series."
            ),
        }
    )
    diffs.append(
        {
            "topic": "microstructure",
            "observation": (
                f"HPTL zigzag_turns smoothed={hptl['zigzag_turns_smoothed']} vs raw={hptl['zigzag_turns_raw']}. "
                "Reference visually shows more high-frequency wiggles than a SMA(5) centered path typically retains."
            ),
        }
    )
    diffs.append(
        {
            "topic": "turning_points",
            "observation": (
                "Reference narrative: Dec peak, early-Feb trough, late-Mar peak, then May softening. "
                f"HPTL major extrema (approx month): {extrema}."
            ),
        }
    )
    diffs.append(
        {
            "topic": "contract_identity",
            "observation": (
                "Reference chart header uses ICEUS DX1! (TradingView continuous). "
                f"HPTL uses Yahoo {ICE_DX_YAHOO_SYMBOL} stored as ICE DXY continuous."
            ),
        }
    )

    causes_ranked = [
        {
            "rank": 1,
            "cause": "Continuous contract construction (Yahoo DX-Y.NYB vs TradingView ICEUS DX1!)",
            "likelihood": "highest",
            "why": (
                "Different continuous/back-adjusted DX series can shift seasonal peaks by weeks "
                "even under identical averaging maths. Both claim ICE DX futures but are not the same vendor tape."
            ),
        },
        {
            "rank": 2,
            "cause": "Back-adjustment methodology",
            "likelihood": "very high",
            "why": (
                "Ratio vs difference roll adjustment changes historical percentage paths used in Step 1. "
                "Seasonality is computed on those adjusted prints."
            ),
        },
        {
            "rank": 3,
            "cause": "Display calendar / seasonal window (Oct→Sep pane vs Jan→Dec trading-day axis)",
            "likelihood": "high",
            "why": (
                "Reference x-axis starts in October; HPTL plot starts in January. "
                "Same underlying curve can look dissimilar when wrapped/rotated on the page."
            ),
        },
        {
            "rank": 4,
            "cause": "As-of date mismatch (screenshot ~mid-June vs HPTL latest bar ~late July)",
            "likelihood": "high",
            "why": (
                "Grey/blue split and the visible forward segment depend on asof. "
                "Comparing different asof dates mixes calendar position with methodology."
            ),
        },
        {
            "rank": 5,
            "cause": "Smoothing implementation (OTC proprietary smooth vs Freeze SMA 5)",
            "likelihood": "medium-high",
            "why": (
                "Reference retains sharper micro-wiggles. Freeze v1.0 applies a fixed centered SMA(5). "
                "Their 'v2' smooth is unknown and may be lighter or differently applied."
            ),
        },
        {
            "rank": 6,
            "cause": "Trading-day alignment / holiday calendars",
            "likelihood": "medium",
            "why": (
                "Freeze aligns by trading-day-of-year index with D=min year length. "
                "OTC may use calendar-day buckets, a fixed 252 map, or a different holiday calendar."
            ),
        },
        {
            "rank": 7,
            "cause": "Lookback period endpoints (which 15 years, inclusion rules)",
            "likelihood": "medium",
            "why": (
                "HPTL uses 15 complete years excluding the incomplete current year. "
                "OTC label historically varied (10yr/15yr); endpoint years may differ."
            ),
        },
        {
            "rank": 8,
            "cause": "Price field (session close vs official settlement)",
            "likelihood": "lower-medium",
            "why": (
                "Yahoo continuous typically exposes a session close. "
                "Institutional DX seasonality often uses settlement. Differences are usually small day-to-day but accumulate."
            ),
        },
        {
            "rank": 9,
            "cause": "Freeze v1.0 implementation error",
            "likelihood": "lowest among listed (given equation verification PASS)",
            "why": (
                "Synthetic and ICE DXY independent recomputes of Steps 1–4 passed. "
                "Residual visual gap is more consistent with data/display factors than broken arithmetic."
            ),
        },
    ]

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "read_only_validation",
        "reference": reference,
        "hptl_freeze_v1": hptl,
        "observed_differences": diffs,
        "causes_ranked": causes_ranked,
        "highest_value_next_investigation": {
            "action": (
                "Rebuild the identical Freeze v1.0 Steps 1–4 on a second ICE DX continuous series "
                "(preferably TradingView/DX1!-equivalent or an institutional back-adjusted DX continuous) "
                "for the same 15 complete years and same asof, then overlay the two centered paths."
            ),
            "why": (
                "If the second vendor's Freeze curve moves materially toward the reference turning-point "
                "calendar (Dec peak / early-Feb trough / late-Mar peak), the gap is data/contract "
                "construction — not HPTL maths. If both vendor curves agree with each other and still "
                "disagree with OTC, investigate OTC display window/smooth next."
            ),
            "success_criterion": (
                "Quantify peak/trough date shifts (in trading days) between Yahoo DX-Y.NYB and the "
                "alternate continuous under unchanged Freeze v1.0 equations."
            ),
        },
        "implementation_status": (
            "Freeze v1.0 treated as verified and frozen. No algorithm changes in this validation."
        ),
    }

    json_path = OUT / "freeze_v1_vs_reference_validation.json"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    md = [
        "# Freeze v1.0 vs OTC/Bernd reference — validation (read-only)",
        "",
        f"Generated: {report['generated_at']}",
        "",
        "## HPTL chart",
        "",
        f"- SVG: `{hptl['svg']}`",
        f"- Instrument: `{hptl['instrument_id']}` / Yahoo `{hptl['yahoo_symbol']}`",
        f"- N={hptl['N']} complete years, D={hptl['D']} trading days, SMA={hptl['smooth_window']}, asof={hptl['asof']}",
        f"- Smoothed centered range: [{hptl['smoothed_range_pct'][0]:.3f}, {hptl['smoothed_range_pct'][1]:.3f}] %",
        "",
        "## Reference (screenshot)",
        "",
        f"- Label: {reference['label']}",
        f"- Ticker: {reference['ticker_on_chart']}",
        f"- Axis: {reference['x_axis']}",
        "",
        "## Ranked causes of difference",
        "",
    ]
    for c in causes_ranked:
        md.append(f"{c['rank']}. **{c['cause']}** ({c['likelihood']})")
        md.append(f"   - {c['why']}")
        md.append("")
    md.extend(
        [
            "## Highest-value next investigation",
            "",
            report["highest_value_next_investigation"]["action"],
            "",
            report["highest_value_next_investigation"]["why"],
            "",
        ]
    )
    md_path = OUT / "freeze_v1_vs_reference_validation.md"
    md_path.write_text("\n".join(md), encoding="utf-8")
    try:
        print(md_path.read_text(encoding="utf-8"))
    except UnicodeEncodeError:
        print(md_path.read_text(encoding="utf-8").encode("ascii", "replace").decode("ascii"))
    print(f"\nJSON: {json_path}")
    print(f"SVG:  {svg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
