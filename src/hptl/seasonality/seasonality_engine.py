"""Generic forward-looking seasonality engine (visual audit layer only)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

BULL_FORWARD_PCT = 1.0
BEAR_FORWARD_PCT = -1.0
MIN_HIST_YEARS_FOR_FULL = 3
MIN_SAMPLE_YEARS_FOR_DIRECTION = 5
INSUFFICIENT_HISTORY = "Insufficient history"
LOW_SAMPLE_RELIABILITY = "Low sample reliability"


def iso_week(date: str) -> tuple[int, int]:
    dt = pd.Timestamp(str(date)[:10])
    cal = dt.isocalendar()
    year = int(cal.year)
    week = int(cal.week)
    if week > 52:
        week = 52
    return year, week


def year_week_closes(bars: list[tuple[str, float]]) -> dict[int, dict[int, float]]:
    yw: dict[int, dict[int, float]] = {}
    for date, close in bars:
        year, week = iso_week(date)
        yw.setdefault(year, {})[week] = close
    return yw


def normalized_year_path(week_closes: dict[int, float]) -> dict[int, float]:
    """Rebase each year to 100 at ISO week 1 when available, else earliest week."""
    if not week_closes:
        return {}
    base_week = 1 if 1 in week_closes else min(week_closes.keys())
    base = week_closes.get(base_week)
    if base is None or base == 0:
        return {}
    return {w: (c / base) * 100.0 for w, c in week_closes.items()}


def build_chart_series(
    *,
    anchor_week: int,
    anchor_index: float | None,
    current_path_raw: dict[int, float],
    avg_3y: dict[int, float | None],
    avg_5y: dict[int, float | None],
    avg_10y: dict[int, float | None],
    proj_3y: dict[int, float | None],
    proj_5y: dict[int, float | None],
    proj_10y: dict[int, float | None],
    yw: dict[int, dict[int, float]],
    current_year: int,
) -> list[dict[str, Any]]:
    """Unified weeks 1–52 for a single trader-grade chart (all index space)."""
    rows: list[dict[str, Any]] = []
    primary_seasonal = avg_3y or avg_5y or avg_10y

    for w in range(1, 53):
        actual = current_path_raw.get(w) if w <= anchor_week else None
        if actual is None and w <= anchor_week and w in (yw.get(current_year) or {}):
            # Week has a close but was missing from normalized path (zero base edge case).
            actual = current_path_raw.get(w)

        s3 = avg_3y.get(w)
        s5 = avg_5y.get(w) if avg_5y else None
        s10 = avg_10y.get(w) if avg_10y else None
        seasonal_primary = s3 if s3 is not None else (s5 if s5 is not None else s10)

        p3 = proj_3y.get(w) if w >= anchor_week else None
        p5 = proj_5y.get(w) if w >= anchor_week else None
        p10 = proj_10y.get(w) if w >= anchor_week else None

        div = None
        if actual is not None and seasonal_primary is not None:
            div = actual - seasonal_primary

        close = (yw.get(current_year) or {}).get(w)

        rows.append(
            {
                "week": w,
                "close": close,
                "actual": round(actual, 2) if actual is not None else None,
                "seasonal_3y": round(s3, 2) if s3 is not None else None,
                "seasonal_5y": round(s5, 2) if s5 is not None else None,
                "seasonal_10y": round(s10, 2) if s10 is not None else None,
                "proj_3y": round(p3, 2) if p3 is not None else None,
                "proj_5y": round(p5, 2) if p5 is not None else None,
                "proj_10y": round(p10, 2) if p10 is not None else None,
                "divergence": round(div, 2) if div is not None else None,
                "is_anchor": w == anchor_week,
                "is_forward": w > anchor_week,
            }
        )

    return rows


def divergence_read(
    *,
    anchor_week: int,
    anchor_index: float | None,
    avg_3y: dict[int, float | None],
    avg_5y: dict[int, float | None],
    avg_10y: dict[int, float | None],
) -> dict[str, Any]:
    """Summary: current indexed price vs primary seasonal average at anchor week."""
    ref = avg_3y.get(anchor_week)
    label = "3Y"
    if ref is None and avg_5y:
        ref = avg_5y.get(anchor_week)
        label = "5Y"
    if ref is None and avg_10y:
        ref = avg_10y.get(anchor_week)
        label = "10Y"
    if anchor_index is None or ref is None:
        return {"available": False}
    div = anchor_index - ref
    return {
        "available": True,
        "anchor_week": anchor_week,
        "actual_index": round(anchor_index, 2),
        "seasonal_index": round(ref, 2),
        "seasonal_window": label,
        "divergence": round(div, 2),
        "position": "above" if div > 0.5 else "below" if div < -0.5 else "inline",
        "summary": (
            f"Current index {anchor_index:.1f} is {abs(div):.1f} pts "
            f"{'above' if div > 0 else 'below' if div < 0 else 'on'} "
            f"{label} seasonal ({ref:.1f}) at week {anchor_week}."
        ),
    }


def avg_path(years: list[int], yw: dict[int, dict[int, float]]) -> dict[int, float | None]:
    out: dict[int, float | None] = {}
    for week in range(1, 53):
        vals = []
        for y in years:
            path = normalized_year_path(yw.get(y, {}))
            if week in path:
                vals.append(path[week])
        out[week] = sum(vals) / len(vals) if vals else None
    return out


def direction(pct: float | None) -> str:
    if pct is None:
        return "Neutral"
    if pct >= BULL_FORWARD_PCT:
        return "Bullish"
    if pct <= BEAR_FORWARD_PCT:
        return "Bearish"
    return "Neutral"


def _forward_direction_label(*, avg_ret: float | None, sample_years: int) -> str:
    """Direction label gated by minimum historical sample size."""
    if sample_years < MIN_SAMPLE_YEARS_FOR_DIRECTION:
        if sample_years <= 0:
            return INSUFFICIENT_HISTORY
        return LOW_SAMPLE_RELIABILITY
    return direction(avg_ret)


def forward_window_read(
    *,
    current_week: int,
    horizon: int,
    hist_years: list[int],
    yw: dict[int, dict[int, float]],
) -> dict[str, Any]:
    end_week = min(52, current_week + horizon)
    if current_week >= 52:
        return {
            "weeks": horizon,
            "avg_return_pct": None,
            "direction": "Neutral",
            "sample_years": 0,
            "available": False,
        }

    rets: list[float] = []
    for y in hist_years:
        path = normalized_year_path(yw.get(y, {}))
        if current_week in path and end_week in path and path[current_week] != 0:
            rets.append((path[end_week] / path[current_week] - 1.0) * 100.0)

    avg_ret = sum(rets) / len(rets) if rets else None
    wins = sum(1 for r in rets if r > 0)
    win_rate_pct = round(wins / len(rets) * 100.0, 1) if rets else None
    sample_years = len(rets)
    dir_label = _forward_direction_label(avg_ret=avg_ret, sample_years=sample_years)
    reliable = sample_years >= MIN_SAMPLE_YEARS_FOR_DIRECTION and avg_ret is not None
    return {
        "weeks": end_week - current_week,
        "avg_return_pct": round(avg_ret, 2) if avg_ret is not None else None,
        "direction": dir_label,
        "sample_years": sample_years,
        "sample_reliability": (
            "reliable"
            if reliable
            else LOW_SAMPLE_RELIABILITY
            if sample_years > 0
            else INSUFFICIENT_HISTORY
        ),
        "win_rate_pct": win_rate_pct,
        "available": reliable,
    }


def build_hist_year_paths(
    yw: dict[int, dict[int, float]],
    years: list[int],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for y in years:
        path = normalized_year_path(yw.get(y, {}))
        if not path:
            continue
        out.append(
            {
                "year": y,
                "points": [{"week": w, "index": round(v, 2)} for w, v in sorted(path.items())],
            }
        )
    return out


def path_alignment_label(divergence_read: dict[str, Any] | None) -> str:
    """Plain label for current price vs seasonal path."""
    if not divergence_read or not divergence_read.get("available"):
        return "Unknown"
    div = divergence_read.get("divergence")
    pos = str(divergence_read.get("position") or "")
    if div is not None and abs(float(div)) >= 8.0:
        return "Diverging from seasonal path"
    if pos == "above":
        return "Above seasonal path"
    if pos == "below":
        return "Below seasonal path"
    return "Following seasonal path"


def seasonal_phase(forward_8w: dict[str, Any] | None) -> str:
    if not forward_8w or not forward_8w.get("available"):
        rel = str((forward_8w or {}).get("sample_reliability") or "")
        if rel in {LOW_SAMPLE_RELIABILITY, INSUFFICIENT_HISTORY}:
            return rel
        return "Unknown"
    d = str(forward_8w.get("direction") or "Neutral")
    if d == "Bullish":
        return "Bullish phase"
    if d == "Bearish":
        return "Bearish phase"
    if d in {LOW_SAMPLE_RELIABILITY, INSUFFICIENT_HISTORY}:
        return d
    return "Neutral phase"


def build_seasonality_timeline(
    bars: list[tuple[str, float]],
    *,
    yw: dict[int, dict[int, float]],
    latest_date: str,
    anchor_week: int,
    anchor_index: float | None,
    avg_3y: dict[int, float | None],
    avg_5y: dict[int, float | None],
    avg_10y: dict[int, float | None],
    proj_3y: dict[int, float | None],
    proj_5y: dict[int, float | None],
    proj_10y: dict[int, float | None],
    max_years: int = 10,
) -> list[dict[str, Any]]:
    """Date-aligned price + seasonality for the last *max_years* (weekly bars)."""
    if not bars:
        return []

    latest_ts = pd.Timestamp(str(latest_date)[:10])
    cutoff = latest_ts - pd.DateOffset(years=max_years)
    rows: list[dict[str, Any]] = []

    for date, close in bars:
        ts = pd.Timestamp(str(date)[:10])
        if ts < cutoff:
            continue
        year, week = iso_week(date)
        path = normalized_year_path(yw.get(year, {}))
        actual_idx = path.get(week)
        rows.append(
            {
                "date": str(date)[:10],
                "label": str(date)[:10],
                "price": round(float(close), 6),
                "iso_week": week,
                "seasonal_actual": round(actual_idx, 2) if actual_idx is not None else None,
                "seasonal_3y": round(v, 2) if (v := avg_3y.get(week)) is not None else None,
                "seasonal_5y": round(v, 2) if (v := avg_5y.get(week)) is not None else None,
                "seasonal_10y": round(v, 2) if (v := avg_10y.get(week)) is not None else None,
                "proj_3y": None,
                "proj_5y": None,
                "proj_10y": None,
                "is_projection": False,
            }
        )

    if not rows or anchor_index is None:
        return rows

    cursor = latest_ts
    for w in range(anchor_week + 1, 53):
        p3 = proj_3y.get(w)
        p5 = proj_5y.get(w)
        p10 = proj_10y.get(w)
        if p3 is None and p5 is None and p10 is None:
            continue
        cursor = cursor + pd.Timedelta(days=7)
        rows.append(
            {
                "date": cursor.strftime("%Y-%m-%d"),
                "label": cursor.strftime("%Y-%m-%d"),
                "price": None,
                "iso_week": w,
                "seasonal_actual": None,
                "seasonal_3y": None,
                "seasonal_5y": None,
                "seasonal_10y": None,
                "proj_3y": round(p3, 2) if p3 is not None else None,
                "proj_5y": round(p5, 2) if p5 is not None else None,
                "proj_10y": round(p10, 2) if p10 is not None else None,
                "is_projection": True,
            }
        )

    return rows


def project_forward(
    *,
    anchor_week: int,
    anchor_index: float,
    avg: dict[int, float | None],
) -> dict[int, float | None]:
    base = avg.get(anchor_week)
    if base is None or base == 0:
        return {}
    out: dict[int, float | None] = {}
    for w in range(anchor_week, 53):
        v = avg.get(w)
        if v is not None:
            out[w] = anchor_index * (v / base)
        else:
            out[w] = None
    return out


def confidence(
    *,
    current_week: int,
    horizon: int,
    years_3y: list[int],
    years_5y: list[int],
    years_10y: list[int],
    yw: dict[int, dict[int, float]],
) -> dict[str, Any]:
    windows: list[tuple[str, list[int]]] = []
    if years_3y:
        windows.append(("3Y", years_3y))
    if years_5y:
        windows.append(("5Y", years_5y))
    if years_10y:
        windows.append(("10Y", years_10y))

    eligible = [(label, years) for label, years in windows if len(years) >= MIN_SAMPLE_YEARS_FOR_DIRECTION]
    if not eligible:
        max_years = max((len(y) for _, y in windows), default=0)
        return {
            "level": LOW_SAMPLE_RELIABILITY if max_years > 0 else INSUFFICIENT_HISTORY,
            "detail": (
                f"Only {max_years} historical year(s) — need {MIN_SAMPLE_YEARS_FOR_DIRECTION}+ "
                "for seasonal confidence."
            ),
            "agreement": 0,
            "windows": len(windows),
            "horizon_weeks": horizon,
            "min_sample_years": max_years,
        }

    dirs: list[tuple[str, str]] = []
    for label, years in eligible:
        r = forward_window_read(current_week=current_week, horizon=horizon, hist_years=years, yw=yw)
        if r.get("available"):
            dirs.append((label, r["direction"]))

    if not dirs:
        return {"level": "Weak", "detail": "Insufficient historical sample for confidence.", "agreement": 0, "windows": 0}

    directions = [d for _, d in dirs]
    bullish = directions.count("Bullish")
    bearish = directions.count("Bearish")
    neutral = directions.count("Neutral")
    n = len(directions)

    if n >= 2 and (bullish == n or bearish == n):
        level = "Strong"
        dominant = "bullish" if bullish == n else "bearish"
        detail = f"{'/'.join(l for l, _ in dirs)} agree {dominant} over the next {horizon} weeks."
    elif n >= 2 and max(bullish, bearish, neutral) >= 2:
        level = "Medium"
        parts = [f"{l}: {d}" for l, d in dirs]
        detail = f"Partial agreement ({', '.join(parts)})."
    else:
        level = "Weak"
        parts = [f"{l}: {d}" for l, d in dirs]
        detail = f"Mixed signals ({', '.join(parts)})."

    return {
        "level": level,
        "detail": detail,
        "agreement": max(bullish, bearish, neutral),
        "windows": n,
        "horizon_weeks": horizon,
    }


def build_summary(
    *,
    market: str,
    current_week: int,
    anchor_index: float | None,
    read_8w: dict[str, Any],
    read_12w: dict[str, Any],
    confidence_block: dict[str, Any],
    latest_date: str,
) -> str:
    parts: list[str] = []
    if anchor_index is not None:
        parts.append(
            f"As of {latest_date} (week {current_week}), the {market} index stands at "
            f"{anchor_index:.1f} (rebased 100 at week 1)."
        )
    if read_8w.get("available"):
        parts.append(
            f"Seasonal history suggests the next {read_8w['weeks']} weeks are typically "
            f"{read_8w['direction'].lower()} ({read_8w['avg_return_pct']:+.2f}% avg, n={read_8w['sample_years']})."
        )
    if read_12w.get("available") and read_12w["weeks"] != read_8w.get("weeks"):
        parts.append(
            f"Over {read_12w['weeks']} weeks, historical average is {read_12w['avg_return_pct']:+.2f}% "
            f"({read_12w['direction'].lower()}, n={read_12w['sample_years']})."
        )
    parts.append(
        f"Seasonality confidence: {confidence_block.get('level', 'Weak')} — "
        f"{confidence_block.get('detail', '')}"
    )
    parts.append("Forward-looking audit only — not a trade signal.")
    return " ".join(p for p in parts if p)


def compute_seasonality_price_block(
    market: str,
    bars: list[tuple[str, float]],
    *,
    price_store_key: str,
    bar_source: str = "weekly",
    canonical_source: str | None = None,
    canonical_symbol: str | None = None,
    price_derivation: str | None = None,
    proxy: bool | None = None,
    proxy_explanation: str | None = None,
) -> dict[str, Any]:
    """Build full seasonality chart block from chronological weekly closes."""
    if not bars:
        return {"market": market, "available": False, "reason": "No weekly price bars."}

    yw = year_week_closes(bars)
    all_years = sorted(yw.keys())
    if not all_years:
        return {"market": market, "available": False, "reason": "No year/week price mapping."}

    latest_date = bars[-1][0]
    latest_close = bars[-1][1]
    latest_year, latest_week = iso_week(latest_date)
    current_year = latest_year

    hist_years = [y for y in all_years if y < current_year]
    years_count = len(hist_years)

    windows: list[str] = []
    years_3y = hist_years[-3:] if years_count >= 3 else hist_years[:]
    years_5y = hist_years[-5:] if years_count >= 5 else []
    years_10y = hist_years[-10:] if years_count >= 10 else []

    if years_count >= 3:
        windows.append("3Y")
    if years_count >= 5:
        windows.append("5Y")
    if years_count >= 10:
        windows.append("10Y")

    current_path_raw = normalized_year_path(yw.get(current_year, {}))
    if not current_path_raw:
        return {
            "market": market,
            "available": False,
            "reason": "No current-year weekly price path.",
            "years_of_history": years_count,
        }

    anchor_week = latest_week if latest_week in current_path_raw else max(w for w in current_path_raw if w <= latest_week)
    anchor_index = current_path_raw.get(anchor_week)
    anchor_close = yw.get(current_year, {}).get(anchor_week, latest_close)

    avg_3y = avg_path(years_3y, yw) if years_3y else {}
    avg_5y = avg_path(years_5y, yw) if years_5y else {}
    avg_10y = avg_path(years_10y, yw) if years_10y else {}

    proj_3y = project_forward(anchor_week=anchor_week, anchor_index=anchor_index or 100.0, avg=avg_3y) if years_3y else {}
    proj_5y = project_forward(anchor_week=anchor_week, anchor_index=anchor_index or 100.0, avg=avg_5y) if years_5y else {}
    proj_10y = project_forward(anchor_week=anchor_week, anchor_index=anchor_index or 100.0, avg=avg_10y) if years_10y else {}

    current_path_series: list[dict[str, Any]] = []
    for w in range(1, anchor_week + 1):
        idx = current_path_raw.get(w)
        if idx is None:
            continue
        close = yw.get(current_year, {}).get(w)
        current_path_series.append({"week": w, "index": round(idx, 2), "close": close})

    forward_projection: list[dict[str, Any]] = []
    for w in range(anchor_week, 53):
        forward_projection.append(
            {
                "week": w,
                "anchor": round(anchor_index, 2) if w == anchor_week and anchor_index is not None else None,
                "proj_3y": round(proj_3y[w], 2) if w in proj_3y and proj_3y[w] is not None else None,
                "proj_5y": round(proj_5y[w], 2) if w in proj_5y and proj_5y[w] is not None else None,
                "proj_10y": round(proj_10y[w], 2) if w in proj_10y and proj_10y[w] is not None else None,
            }
        )

    ref_years_4 = years_10y or years_5y or years_3y
    ref_years_8 = years_10y or years_5y or years_3y
    ref_years_12 = years_10y or years_5y or years_3y

    read_4w = forward_window_read(current_week=anchor_week, horizon=4, hist_years=ref_years_4, yw=yw)
    read_8w = forward_window_read(current_week=anchor_week, horizon=8, hist_years=ref_years_8, yw=yw)
    read_12w = forward_window_read(current_week=anchor_week, horizon=12, hist_years=ref_years_12, yw=yw)

    confidence_block = confidence(
        current_week=anchor_week,
        horizon=8,
        years_3y=years_3y,
        years_5y=years_5y,
        years_10y=years_10y,
        yw=yw,
    )

    availability_note = None
    if years_count < MIN_HIST_YEARS_FOR_FULL:
        availability_note = f"Only {years_count} year(s) of price history — forward seasonality is limited."
    elif years_count < 5:
        availability_note = "Only 3Y seasonality available."
    elif years_count < 10:
        availability_note = "3Y and 5Y seasonality available; 10Y not available."

    price_stale_note = None
    cot_now = pd.Timestamp(datetime.now(timezone.utc).date())
    price_age = (cot_now - pd.Timestamp(latest_date)).days
    if price_age > 14:
        price_stale_note = (
            f"Latest price is {latest_date} ({price_age} days old). "
            "Forward projection anchors to this close."
        )

    forward_available = bool(forward_projection) and any(
        proj_3y.get(w) is not None for w in range(anchor_week + 1, 53)
    )

    chart_series = build_chart_series(
        anchor_week=anchor_week,
        anchor_index=anchor_index,
        current_path_raw=current_path_raw,
        avg_3y=avg_3y,
        avg_5y=avg_5y,
        avg_10y=avg_10y,
        proj_3y=proj_3y,
        proj_5y=proj_5y,
        proj_10y=proj_10y,
        yw=yw,
        current_year=current_year,
    )

    div_read = divergence_read(
        anchor_week=anchor_week,
        anchor_index=anchor_index,
        avg_3y=avg_3y,
        avg_5y=avg_5y,
        avg_10y=avg_10y,
    )

    ref_hist_years = years_10y or years_5y or years_3y
    hist_year_paths = build_hist_year_paths(yw, ref_hist_years)
    path_alignment = path_alignment_label(div_read)
    phase = seasonal_phase(read_8w)

    summary = build_summary(
        market=market,
        current_week=anchor_week,
        anchor_index=anchor_index,
        read_8w=read_8w,
        read_12w=read_12w,
        confidence_block=confidence_block,
        latest_date=latest_date,
    )

    years_available = len(all_years)
    sample_size_3y = len(years_3y)
    weekly_bars_count = len(bars)

    timeline_series = build_seasonality_timeline(
        bars,
        yw=yw,
        latest_date=latest_date,
        anchor_week=anchor_week,
        anchor_index=anchor_index,
        avg_3y=avg_3y,
        avg_5y=avg_5y,
        avg_10y=avg_10y,
        proj_3y=proj_3y,
        proj_5y=proj_5y,
        proj_10y=proj_10y,
        max_years=10,
    )
    timeline_start = timeline_series[0]["date"] if timeline_series else None
    timeline_end = timeline_series[-1]["date"] if timeline_series else None

    return {
        "market": market,
        "available": True,
        "price_store_key": price_store_key,
        "bar_source": bar_source,
        "price_derivation": price_derivation or bar_source,
        "canonical_source": canonical_source,
        "canonical_symbol": canonical_symbol,
        "proxy": proxy,
        "proxy_explanation": proxy_explanation,
        "years_available": years_available,
        "years_used": years_count,
        "sample_size": sample_size_3y,
        "weekly_bars_count": weekly_bars_count,
        "latest_price": {
            "date": latest_date,
            "close": latest_close,
            "week": anchor_week,
            "index": round(anchor_index, 2) if anchor_index is not None else None,
        },
        "current_year": current_year,
        "current_week": anchor_week,
        "years_of_history": years_count,
        "windows_available": windows,
        "forward_projection_available": forward_available,
        "availability_note": availability_note,
        "price_stale_note": price_stale_note,
        "chart_series": chart_series,
        "divergence_read": div_read,
        "path_alignment": path_alignment,
        "seasonal_phase": phase,
        "hist_year_paths": hist_year_paths,
        "timeline_series": timeline_series,
        "timeline_start": timeline_start,
        "timeline_end": timeline_end,
        "timeline_anchor_date": latest_date,
        "current_path": current_path_series,
        "forward_projection": forward_projection,
        "forward_read": {
            "next_4w": read_4w,
            "next_8w": read_8w,
            "next_12w": read_12w,
            "summary": summary,
        },
        "confidence": confidence_block,
    }
