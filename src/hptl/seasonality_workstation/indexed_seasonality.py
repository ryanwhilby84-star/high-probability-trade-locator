"""HPTL Seasonality Engine — Freeze v1.0.

Methodology is frozen. Do not invent alternatives or tune parameters.

Equations
---------
1) Normalise each historical year y, trading day d:
       NormalizedPrice_d,y = (ActualPrice_d,y / ActualPrice_1,y - 1) * 100
   Absolute price levels are never averaged.

2) Average seasonal curve:
       RawSeasonal_d = (1/N) * sum_y NormalizedPrice_d,y

3) Centre:
       mu = (1/D) * sum_d RawSeasonal_d
       CenteredSeasonal_d = RawSeasonal_d - mu

4) Smooth with one fixed global centered SMA (FREEZE_SMOOTH_WINDOW).
   Same pipeline for every market. No instrument-specific rules.

Data rules: complete years only; exclude incomplete current year;
fixed lookback (15Y); one consistent price source per instrument.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

METHOD_VERSION = "freeze_v1.0"
METHOD_NAME = "freeze_v1_normalized_pct_mean_centered"

DEFAULT_LOOKBACK_YEARS = 15
# Frozen global smoothing — not tuned per market or for visual match.
FREEZE_SMOOTH_WINDOW = 5
DEFAULT_SMOOTH = FREEZE_SMOOTH_WINDOW
DEFAULT_AGG = "mean"  # Freeze v1.0 specifies the arithmetic mean only.
FORWARD_CALENDAR_DAYS = 200
MIN_BARS_PER_YEAR = 180


def _parse(d: str) -> date:
    return datetime.strptime(str(d)[:10], "%Y-%m-%d").date()


def calendar_doy(d: date) -> int:
    """Calendar day-of-year helper for display / asof mapping only."""
    yd = d.timetuple().tm_yday
    leap = d.year % 4 == 0 and (d.year % 100 != 0 or d.year % 400 == 0)
    if leap and d.month == 2 and d.day == 29:
        return 365
    if leap and (d.month, d.day) > (2, 29):
        return yd - 1
    return min(yd, 365)


def centered_sma(vals: list[float], window: int) -> list[float]:
    """Fixed global smoother: centered simple moving average."""
    if window <= 1:
        return list(vals)
    half = window // 2
    n = len(vals)
    out: list[float] = []
    for i in range(n):
        chunk = vals[max(0, i - half) : min(n, i + half + 1)]
        out.append(sum(chunk) / len(chunk))
    return out


def load_daily_closes_for_seasonality(instrument_id: str) -> tuple[list[tuple[str, float]], dict[str, Any]]:
    """Load daily closes; resolve DX COT id → explicit ICE DXY price id."""
    from hptl.markets.usd_index_identity import ICE_DXY_ID, seasonality_preferred_id
    from hptl.seasonality_workstation.returns import load_daily_closes

    preferred = seasonality_preferred_id(instrument_id)
    closes, source, err = load_daily_closes(preferred)
    meta = {
        "requested_instrument_id": instrument_id,
        "price_instrument_id": preferred,
        "source": source,
        "error": err,
        "ice_dxy_id": ICE_DXY_ID,
    }
    if (not closes or err) and preferred != instrument_id:
        closes2, source2, err2 = load_daily_closes(instrument_id)
        if closes2 and not err2:
            from hptl.prices.price_store import load_instrument_record_internal

            rec = load_instrument_record_internal(instrument_id) or {}
            scale = rec.get("price_scale") or {}
            if scale.get("series_id") == "DTWEXBGS" or scale.get("is_proxy"):
                meta["error"] = "refused_fred_broad_proxy_for_dxy_seasonality"
                return [], meta
            return closes2, {**meta, "price_instrument_id": instrument_id, "source": source2, "error": None}
    return closes, meta


def complete_year_bars(
    daily: list[tuple[str, float]],
    *,
    asof: str,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    min_bars: int = MIN_BARS_PER_YEAR,
) -> dict[int, list[tuple[date, float]]]:
    """Complete historical calendar years only; exclude asof's year."""
    asof_d = _parse(asof)
    asof_year = asof_d.year
    by_year: dict[int, list[tuple[date, float]]] = {}
    for d_s, c in daily:
        if d_s > asof:
            continue
        d = _parse(d_s)
        if d.year >= asof_year:
            continue
        by_year.setdefault(d.year, []).append((d, float(c)))

    out: dict[int, list[tuple[date, float]]] = {}
    for y in sorted(by_year):
        if y < asof_year - lookback_years or y >= asof_year:
            continue
        rows = sorted(by_year[y], key=lambda t: t[0])
        if len(rows) >= min_bars and rows[0][1] > 0:
            out[y] = rows
    return out


def normalize_year_pct(rows: list[tuple[date, float]]) -> list[float]:
    """Step 1: NormalizedPrice_d = (P_d / P_1 - 1) * 100 for trading days d=1..n."""
    if not rows:
        return []
    base = rows[0][1]
    if base <= 0:
        return []
    return [(c / base - 1.0) * 100.0 for _, c in rows]


def average_normalized_paths(
    year_paths: dict[int, list[float]],
) -> tuple[list[float], int]:
    """Step 2: RawSeasonal_d = mean across years; D = min path length (common trading days)."""
    if not year_paths:
        return [], 0
    d_len = min(len(p) for p in year_paths.values())
    if d_len < 2:
        return [], 0
    n = len(year_paths)
    raw = []
    for d in range(d_len):
        s = sum(p[d] for p in year_paths.values())
        raw.append(s / n)
    return raw, d_len


def centre_path(raw: list[float]) -> tuple[list[float], float]:
    """Step 3: CenteredSeasonal_d = RawSeasonal_d - mu."""
    if not raw:
        return [], 0.0
    mu = sum(raw) / len(raw)
    return [v - mu for v in raw], mu


def smooth_path(centered: list[float], window: int = FREEZE_SMOOTH_WINDOW) -> list[float]:
    """Step 4: fixed global centered SMA."""
    return centered_sma(centered, window)


def build_freeze_v1_path(
    daily: list[tuple[str, float]],
    *,
    asof: str | None = None,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    smooth: int = FREEZE_SMOOTH_WINDOW,
) -> dict[str, Any]:
    """Run Freeze v1.0 steps 1–4 and return intermediates for verification."""
    if not daily:
        return {"available": False, "reason": "no_daily_bars"}
    asof = asof or daily[-1][0]
    asof = max((d for d, _ in daily if d <= asof), default=daily[-1][0])

    years = complete_year_bars(daily, asof=asof, lookback_years=lookback_years)
    if len(years) < 5:
        return {
            "available": False,
            "reason": "insufficient_complete_years",
            "sample_size": len(years),
            "sample_years": sorted(years.keys()),
        }

    year_norm: dict[int, list[float]] = {}
    for y, rows in years.items():
        path = normalize_year_pct(rows)
        if len(path) >= MIN_BARS_PER_YEAR:
            year_norm[y] = path

    raw, d_len = average_normalized_paths(year_norm)
    if d_len < MIN_BARS_PER_YEAR:
        return {"available": False, "reason": "insufficient_common_trading_days", "D": d_len}

    centered, mu = centre_path(raw)
    smoothed = smooth_path(centered, window=int(smooth))

    return {
        "available": True,
        "asof": asof,
        "sample_years": sorted(year_norm.keys()),
        "sample_size": len(year_norm),
        "D": d_len,
        "N": len(year_norm),
        "mu": mu,
        "raw": raw,
        "centered": centered,
        "smoothed": smoothed,
        "year_bars": {y: years[y] for y in year_norm},
        "method": {
            "version": METHOD_VERSION,
            "name": METHOD_NAME,
            "lookback_years": lookback_years,
            "alignment": "trading_day_of_year",
            "normalisation": "(P_d / P_1 - 1) * 100",
            "aggregation": "arithmetic_mean",
            "centering": "subtract_mean_of_raw_seasonal",
            "smooth_kind": "centered_sma",
            "smooth": int(smooth),
            "excludes_incomplete_current_year": True,
        },
    }


def _trading_day_index_for_asof(
    daily: list[tuple[str, float]], asof: str
) -> int:
    """1-based trading day index of asof within its calendar year."""
    asof_d = _parse(asof)
    n = 0
    for d_s, _ in daily:
        d = _parse(d_s)
        if d.year != asof_d.year:
            continue
        if d_s > asof:
            break
        n += 1
    return max(1, n)


def _date_axis_for_trading_days(
    daily: list[tuple[str, float]],
    *,
    asof: str,
    d_len: int,
) -> list[str]:
    """Map trading-day index 1..D onto calendar dates in the asof year."""
    asof_d = _parse(asof)
    asof_year = asof_d.year

    def _shift_to_asof_year(d_s: str) -> str:
        d = _parse(d_s)
        try:
            return date(asof_year, d.month, d.day).isoformat()
        except ValueError:
            # Feb 29 -> Feb 28 in non-leap asof year
            return date(asof_year, d.month, min(d.day, 28)).isoformat()

    # Prefer last complete year as the trading-day calendar template, relabeled to asof year
    template_year = asof_year - 1
    template = [d_s for d_s, _ in daily if _parse(d_s).year == template_year]
    if len(template) >= d_len:
        return [_shift_to_asof_year(d_s) for d_s in template[:d_len]]

    # Fall back: synthesize weekdays from Jan 1 of asof year
    dates: list[str] = []
    cur = date(asof_year, 1, 1)
    while len(dates) < d_len:
        if cur.weekday() < 5:
            dates.append(cur.isoformat())
        cur += timedelta(days=1)
    return dates


def build_normalised_seasonal_curve(
    daily: list[tuple[str, float]],
    *,
    asof: str | None = None,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    smooth: float | int = FREEZE_SMOOTH_WINDOW,
    aggregation: str = DEFAULT_AGG,
) -> dict[str, Any]:
    """Workstation payload built from Freeze v1.0 (aggregation must be mean)."""
    if aggregation != "mean":
        return {
            "available": False,
            "reason": "freeze_v1_requires_mean_aggregation",
            "message": "Freeze v1.0 specifies arithmetic mean only.",
        }

    core = build_freeze_v1_path(
        daily, asof=asof, lookback_years=lookback_years, smooth=int(smooth)
    )
    if not core.get("available"):
        return core

    asof = core["asof"]
    asof_d = _parse(asof)
    smoothed: list[float] = core["smoothed"]
    d_len = core["D"]
    date_axis = _date_axis_for_trading_days(daily, asof=asof, d_len=d_len)
    asof_td = _trading_day_index_for_asof(daily, asof)
    asof_td = min(max(1, asof_td), d_len)
    asof_level = smoothed[asof_td - 1]

    full_year: list[dict[str, Any]] = []
    for i, v in enumerate(smoothed):
        td = i + 1
        dt = date_axis[i] if i < len(date_axis) else (asof_d + timedelta(days=i)).isoformat()
        if td < asof_td:
            segment = "historical"
        elif td == asof_td:
            segment = "today"
            dt = asof
        else:
            segment = "forward"
        full_year.append(
            {
                "trading_day": td,
                "doy": calendar_doy(_parse(dt)) if len(dt) >= 10 else td,
                "date": dt,
                "index": round(v, 6),  # centered seasonal % path
                "segment": segment,
            }
        )

    historical = [p for p in full_year if p["segment"] in ("historical", "today")]

    def _next_trading_date(d0: date) -> date:
        nxt = d0 + timedelta(days=1)
        while nxt.weekday() >= 5:
            nxt += timedelta(days=1)
        return nxt

    # Forward continuation along the seasonal path from today's trading day.
    # Calendar dates advance from asof by trading days (template axis is shape-only).
    forward: list[dict[str, Any]] = []
    cursor = asof_d
    for offset, i in enumerate(range(asof_td - 1, d_len)):
        v = smoothed[i]
        td = i + 1
        if offset == 0:
            dt = asof
        else:
            cursor = _next_trading_date(cursor)
            dt = cursor.isoformat()
        cum = v - asof_level
        forward.append(
            {
                "trading_day": td,
                "doy": calendar_doy(_parse(dt)),
                "date": dt,
                "index": round(v, 6),
                "segment": "today" if offset == 0 else "forward",
                "offset_days": offset,
                "offset_trading_days": offset,
                "cumulative_return": round(cum / 100.0, 6),
                "cumulative_pct_points": round(cum, 6),
            }
        )

    # Wrap into early-year seasonal days if needed for forward horizons
    if len(forward) < 90:
        wrap_needed = 90 - len(forward)
        for j in range(wrap_needed):
            i = j % d_len
            v = smoothed[i]
            cum = v - asof_level
            cursor = _next_trading_date(cursor)
            forward.append(
                {
                    "trading_day": i + 1,
                    "doy": calendar_doy(cursor),
                    "date": cursor.isoformat(),
                    "index": round(v, 6),
                    "segment": "forward",
                    "offset_days": forward[-1]["offset_days"] + 1,
                    "offset_trading_days": forward[-1]["offset_trading_days"] + 1,
                    "cumulative_return": round(cum / 100.0, 6),
                    "cumulative_pct_points": round(cum, 6),
                }
            )

    def _horizon(trading_days: int) -> dict[str, Any]:
        hit = next((p for p in forward if p.get("offset_trading_days") == trading_days), None)
        if not hit:
            cands = [
                p
                for p in forward
                if p.get("offset_trading_days") is not None
                and p["offset_trading_days"] <= trading_days
            ]
            hit = cands[-1] if cands else None
        if not hit:
            return {"days": trading_days, "direction": "NA", "median_move_pct": None}
        # median_move_pct here = seasonal path move in percentage points
        move = hit["cumulative_pct_points"]
        direction = "UP" if move > 0.2 else "DOWN" if move < -0.2 else "FLAT"
        return {
            "days": trading_days,
            "weeks": round(trading_days / 5, 1),
            "direction": direction,
            "median_move_pct": round(move, 3),
            "index": hit["index"],
            "date": hit["date"],
        }

    # Frequency: fraction of sample years where price rose over ~40 trading days (~8w)
    pos = neg = 0
    horizon_td = 40
    for y, path in {y: normalize_year_pct(core["year_bars"][y]) for y in core["sample_years"]}.items():
        if asof_td - 1 + horizon_td >= len(path) or asof_td - 1 >= len(path):
            continue
        a = path[asof_td - 1]
        b = path[asof_td - 1 + horizon_td]
        if b - a > 0:
            pos += 1
        else:
            neg += 1
    n_freq = pos + neg

    weekly_points = [p for p in forward if p.get("offset_trading_days", -1) % 5 == 0]

    curve = {str(i + 1): round(v, 6) for i, v in enumerate(smoothed)}

    return {
        "available": True,
        "method": core["method"],
        "asof": asof,
        "asof_doy": calendar_doy(asof_d),
        "asof_trading_day": asof_td,
        "asof_index": round(asof_level, 6),
        "sample_years": core["sample_years"],
        "sample_size": core["sample_size"],
        "D": d_len,
        "mu": round(core["mu"], 6),
        "curve_raw": {str(i + 1): round(v, 6) for i, v in enumerate(core["raw"])},
        "curve_centered": {str(i + 1): round(v, 6) for i, v in enumerate(core["centered"])},
        "curve": curve,
        "full_year": full_year,
        "historical": historical,
        "forward": forward,
        "weekly_points": weekly_points,
        "horizons": {
            "4w": _horizon(20),
            "8w": _horizon(40),
            "12w": _horizon(60),
        },
        "positive_frequency_8w": None if not n_freq else round(pos / n_freq, 3),
        "negative_frequency_8w": None if not n_freq else round(neg / n_freq, 3),
    }


def walk_forward_hit_rate(
    daily: list[tuple[str, float]],
    *,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    smooth: float | int = FREEZE_SMOOTH_WINDOW,
    horizon_days: int = 40,
) -> dict[str, Any]:
    """July-centred walk-forward directional hit rate on Freeze v1.0 path."""
    if len(daily) < 800:
        return {"n": 0, "hit_rate": None}
    years = sorted({_parse(d).year for d, _ in daily})
    hits: list[int] = []
    date_to_idx = {d: i for i, (d, _) in enumerate(daily)}
    for y in years[lookback_years:-1]:
        target = date(y, 7, 17)
        cands = [
            d
            for d, _ in daily
            if _parse(d).year == y and abs((_parse(d) - target).days) <= 10
        ]
        if not cands:
            continue
        asof = min(cands, key=lambda d: abs((_parse(d) - target).days))
        pack = build_normalised_seasonal_curve(
            daily, asof=asof, lookback_years=lookback_years, smooth=smooth
        )
        if not pack.get("available"):
            continue
        h = (pack.get("horizons") or {}).get("8w") or {}
        pred = h.get("median_move_pct")
        if pred is None:
            continue
        i0 = date_to_idx.get(asof)
        if i0 is None:
            continue
        asof_d = _parse(asof)
        # ~40 trading days ≈ 56 calendar days
        target_d = (asof_d + timedelta(days=56)).isoformat()
        j = next((i for i, (d, _) in enumerate(daily) if d >= target_d and i > i0), None)
        if j is None:
            continue
        realised = daily[j][1] / daily[i0][1] - 1.0
        pred_r = pred / 100.0
        agree = (pred_r > 0 and realised > 0) or (pred_r < 0 and realised < 0)
        hits.append(1 if agree else 0)
    return {
        "n": len(hits),
        "hit_rate": None if not hits else round(sum(hits) / len(hits), 3),
        "horizon_days": horizon_days,
    }


# Legacy aliases kept so older imports do not break.
DEFAULT_LOOKBACK_YEARS_ALIAS = DEFAULT_LOOKBACK_YEARS
DEFAULT_GAUSS_SIGMA = FREEZE_SMOOTH_WINDOW
