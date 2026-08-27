"""Seasonal Price Path v1 — prototype product (separate from Freeze v1.0).

Product: a price-like seasonal path answering
"Historically, how has this market tended to move from this point in the year?"

Equations
---------
1) For each complete historical year y, trading day d >= 2:
       r_{y,d} = P_{y,d} / P_{y,d-1} - 1
   with r_{y,1} = 0.

2) Average seasonal return:
       r̄_d = (1/N) * sum_y r_{y,d}

3) Cumulative seasonal index (NOT centred):
       I_1 = 1
       I_d = I_{d-1} * (1 + r̄_d)

4) As-of price rebase:
       S_d = P_asof * (I_d / I_asof)

Display: grey for d <= asof, blue for d > asof. Axis in price units.

Freeze v1.0 is untouched and remains the normalised index product.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from hptl.seasonality_workstation.indexed_seasonality import (
    DEFAULT_LOOKBACK_YEARS,
    MIN_BARS_PER_YEAR,
    _date_axis_for_trading_days,
    _trading_day_index_for_asof,
    calendar_doy,
    complete_year_bars,
)

METHOD_VERSION = "seasonal_price_path_v1"
METHOD_NAME = "avg_daily_return_cumsum_asof_rebase"
PRODUCT_NAME = "Seasonal Price Path"


def _parse(d: str) -> date:
    return datetime.strptime(str(d)[:10], "%Y-%m-%d").date()


def year_daily_returns(rows: list[tuple[date, float]]) -> list[float]:
    """Step 1: simple day-to-day returns; first day is 0."""
    if not rows:
        return []
    out = [0.0]
    for i in range(1, len(rows)):
        prev = rows[i - 1][1]
        cur = rows[i][1]
        if prev <= 0:
            out.append(0.0)
        else:
            out.append(cur / prev - 1.0)
    return out


def average_daily_returns(year_rets: dict[int, list[float]]) -> tuple[list[float], int]:
    """Step 2: mean return per trading-day index; D = min path length."""
    if not year_rets:
        return [], 0
    d_len = min(len(p) for p in year_rets.values())
    if d_len < 2:
        return [], 0
    n = len(year_rets)
    avg = []
    for d in range(d_len):
        avg.append(sum(p[d] for p in year_rets.values()) / n)
    return avg, d_len


def cumulative_index(avg_rets: list[float]) -> list[float]:
    """Step 3: I_1=1, I_d = I_{d-1}*(1+r̄_d). Never centred."""
    if not avg_rets:
        return []
    out = [1.0]
    for i in range(1, len(avg_rets)):
        out.append(out[-1] * (1.0 + avg_rets[i]))
    return out


def rebase_to_price(index: list[float], *, asof_td: int, anchor_price: float) -> list[float]:
    """Step 4: S_d = P_asof * (I_d / I_asof)."""
    if not index or anchor_price <= 0:
        return []
    i0 = min(max(1, asof_td), len(index)) - 1
    base = index[i0]
    if base <= 0:
        return []
    scale = anchor_price / base
    return [v * scale for v in index]


def _anchor_close(daily: list[tuple[str, float]], asof: str) -> float | None:
    last = None
    for d_s, c in daily:
        if d_s > asof:
            break
        last = float(c)
    return last


def build_seasonal_price_path(
    daily: list[tuple[str, float]],
    *,
    asof: str | None = None,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
) -> dict[str, Any]:
    """Core Seasonal Price Path maths (no display packaging)."""
    if not daily:
        return {"available": False, "reason": "no_daily_bars"}

    asof = asof or daily[-1][0]
    asof = max((d for d, _ in daily if d <= asof), default=daily[-1][0])
    anchor = _anchor_close(daily, asof)
    if anchor is None or anchor <= 0:
        return {"available": False, "reason": "no_anchor_price"}

    years = complete_year_bars(
        daily, asof=asof, lookback_years=lookback_years, min_bars=MIN_BARS_PER_YEAR
    )
    if len(years) < 5:
        return {
            "available": False,
            "reason": "insufficient_complete_years",
            "sample_size": len(years),
            "sample_years": sorted(years.keys()),
        }

    year_rets = {y: year_daily_returns(rows) for y, rows in years.items()}
    year_rets = {y: r for y, r in year_rets.items() if len(r) >= MIN_BARS_PER_YEAR}
    if len(year_rets) < 5:
        return {
            "available": False,
            "reason": "insufficient_return_years",
            "sample_size": len(year_rets),
        }

    avg_rets, d_len = average_daily_returns(year_rets)
    if d_len < MIN_BARS_PER_YEAR:
        return {"available": False, "reason": "insufficient_common_trading_days", "D": d_len}

    index = cumulative_index(avg_rets)
    asof_td = _trading_day_index_for_asof(daily, asof)
    asof_td = min(max(1, asof_td), d_len)
    prices = rebase_to_price(index, asof_td=asof_td, anchor_price=anchor)

    return {
        "available": True,
        "asof": asof,
        "anchor_price": anchor,
        "sample_years": sorted(year_rets.keys()),
        "sample_size": len(year_rets),
        "D": d_len,
        "asof_trading_day": asof_td,
        "avg_returns": avg_rets,
        "index": index,
        "prices": prices,
        "year_bars": {y: years[y] for y in year_rets},
        "method": {
            "version": METHOD_VERSION,
            "name": METHOD_NAME,
            "product": PRODUCT_NAME,
            "lookback_years": lookback_years,
            "alignment": "trading_day_of_year",
            "aggregation": "arithmetic_mean_of_daily_returns",
            "path": "cumulative_product",
            "centering": "none",
            "rebase": "asof_price",
            "smooth": None,
            "excludes_incomplete_current_year": True,
            "units": "price",
        },
    }


def build_seasonal_price_path_curve(
    daily: list[tuple[str, float]],
    *,
    asof: str | None = None,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
) -> dict[str, Any]:
    """Workstation payload for Seasonal Price Path view (price units, grey/blue)."""
    core = build_seasonal_price_path(daily, asof=asof, lookback_years=lookback_years)
    if not core.get("available"):
        return core

    asof = core["asof"]
    asof_d = _parse(asof)
    prices: list[float] = core["prices"]
    index: list[float] = core["index"]
    d_len = core["D"]
    asof_td = core["asof_trading_day"]
    date_axis = _date_axis_for_trading_days(daily, asof=asof, d_len=d_len)
    asof_price = prices[asof_td - 1]

    def _next_trading_date(d0: date) -> date:
        nxt = d0 + timedelta(days=1)
        while nxt.weekday() >= 5:
            nxt += timedelta(days=1)
        return nxt

    full_year: list[dict[str, Any]] = []
    for i, px in enumerate(prices):
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
                "price": round(px, 6),
                "index": round(index[i], 8),
                "segment": segment,
            }
        )

    historical = [p for p in full_year if p["segment"] in ("historical", "today")]

    forward: list[dict[str, Any]] = []
    cursor = asof_d
    for offset, i in enumerate(range(asof_td - 1, d_len)):
        px = prices[i]
        td = i + 1
        if offset == 0:
            dt = asof
        else:
            cursor = _next_trading_date(cursor)
            dt = cursor.isoformat()
        cum_ret = px / asof_price - 1.0 if asof_price else 0.0
        forward.append(
            {
                "trading_day": td,
                "doy": calendar_doy(_parse(dt)),
                "date": dt,
                "price": round(px, 6),
                "index": round(index[i], 8),
                "segment": "today" if offset == 0 else "forward",
                "offset_trading_days": offset,
                "cumulative_return": round(cum_ret, 6),
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
            return {"days": trading_days, "direction": "NA", "expected_move_pct": None}
        move = hit["cumulative_return"] * 100.0
        direction = "UP" if move > 0.2 else "DOWN" if move < -0.2 else "FLAT"
        return {
            "days": trading_days,
            "weeks": round(trading_days / 5, 1),
            "direction": direction,
            "expected_move_pct": round(move, 3),
            "price": hit["price"],
            "date": hit["date"],
        }

    return {
        "available": True,
        "method": core["method"],
        "asof": asof,
        "asof_doy": calendar_doy(asof_d),
        "asof_trading_day": asof_td,
        "anchor_price": round(core["anchor_price"], 6),
        "asof_price": round(asof_price, 6),
        "sample_years": core["sample_years"],
        "sample_size": core["sample_size"],
        "D": d_len,
        "full_year": full_year,
        "historical": historical,
        "forward": forward,
        "horizons": {
            "20d": _horizon(20),
            "40d": _horizon(40),
            "60d": _horizon(60),
        },
        "product_note": (
            "Price-like seasonal path (not Freeze v1.0). "
            "Average daily returns → cumulative index → rebase to as-of price. "
            "No centering. Axis in price units."
        ),
    }
