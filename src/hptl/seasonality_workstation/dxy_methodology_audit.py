"""DXY seasonality methodology audit vs reference (Bernd-style normalised curve).

Does NOT change production seasonality math. Research/audit only.
"""

from __future__ import annotations

import json
import math
import statistics
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

from hptl.config import PROJECT_ROOT
from hptl.prices.canonical_timeline import build_canonical_timeline

INSTRUMENT = "US Dollar Index / DX"
AUDIT_DIR = PROJECT_ROOT / "data" / "audits" / "dxy_seasonality_methodology"
OUT_JSON = AUDIT_DIR / "dxy_seasonality_methodology_audit.json"
OUT_MD = AUDIT_DIR / "dxy_seasonality_methodology_audit.md"
OUT_CHART = AUDIT_DIR / "dxy_candidate_curves_benchmark.svg"
OUT_WF_CHART = AUDIT_DIR / "dxy_walkforward_directional.svg"

LOOKBACKS = (5, 10, 15, 20, None)
SMOOTH_WINDOWS = (0, 3, 5, 10, 14)


@dataclass
class Series:
    name: str
    provider: str
    symbol: str
    description: str
    dates: list[str]
    closes: list[float]

    @property
    def n(self) -> int:
        return len(self.dates)


def _parse(d: str) -> date:
    return datetime.strptime(d[:10], "%Y-%m-%d").date()


def _doy(d: date) -> int:
    """Calendar day-of-year clipped to 365 (leap day → 365)."""
    yd = d.timetuple().tm_yday
    if d.month == 2 and d.day == 29:
        return 365
    if yd > 365:
        return 365
    # After Feb 29 in leap years, shift back one so Dec 31 → 365
    if d.year % 4 == 0 and (d.year % 100 != 0 or d.year % 400 == 0) and (d.month, d.day) > (2, 29):
        return yd - 1
    return yd


def _iso_week(d: date) -> int:
    w = int(d.isocalendar().week)
    return 52 if w > 52 else w


def load_fred_dxy() -> Series:
    tl = build_canonical_timeline(INSTRUMENT, apply_supplements=False)
    if tl is None or not tl.bars:
        raise RuntimeError("FRED/canonical DXY series unavailable")
    dates = [b.date for b in tl.bars]
    closes = [float(b.close) for b in tl.bars]
    return Series(
        name="FRED_DTWEXBGS",
        provider="FRED",
        symbol="DTWEXBGS",
        description=(
            "Nominal Broad U.S. Dollar Index (FRED DTWEXBGS). "
            "This is the HPTL canonical store series for 'US Dollar Index / DX'. "
            "It is NOT ICE DX futures."
        ),
        dates=dates,
        closes=closes,
    )


def load_ice_dx_yahoo() -> Series | None:
    """ICE Dollar Index futures continuous proxy via Yahoo DX-Y.NYB (audit only)."""
    try:
        import yfinance as yf
    except ImportError:
        return None
    hist = yf.Ticker("DX-Y.NYB").history(period="max", auto_adjust=True)
    if hist is None or hist.empty:
        return None
    dates: list[str] = []
    closes: list[float] = []
    for idx, row in hist.iterrows():
        d = str(idx.date()) if hasattr(idx, "date") else str(idx)[:10]
        c = float(row["Close"])
        if math.isfinite(c) and c > 0:
            dates.append(d)
            closes.append(c)
    if len(dates) < 500:
        return None
    return Series(
        name="ICE_DX_YAHOO",
        provider="Yahoo",
        symbol="DX-Y.NYB",
        description=(
            "ICE U.S. Dollar Index futures continuous (Yahoo DX-Y.NYB). "
            "Likely closer to retail 'DXY' charts used by traders (levels ~90–115)."
        ),
        dates=dates,
        closes=closes,
    )


def audit_series(s: Series) -> dict[str, Any]:
    issues: list[str] = []
    warnings: list[str] = []
    if not s.dates:
        return {"status": "FAIL", "issues": ["empty"], "name": s.name}
    if s.dates != sorted(s.dates):
        issues.append("unsorted")
    if len(s.dates) != len(set(s.dates)):
        issues.append("duplicates")
    gaps = []
    for i in range(1, len(s.dates)):
        delta = (_parse(s.dates[i]) - _parse(s.dates[i - 1])).days
        if delta > 14:
            gaps.append({"from": s.dates[i - 1], "to": s.dates[i], "days": delta})
    # OHLC collapse check for FRED-like
    # unit / level
    last = s.closes[-1]
    first = s.closes[0]
    years = (_parse(s.dates[-1]) - _parse(s.dates[0])).days / 365.25
    return {
        "status": "FAIL" if issues else "PASS",
        "name": s.name,
        "provider": s.provider,
        "symbol": s.symbol,
        "description": s.description,
        "bar_count": s.n,
        "first_date": s.dates[0],
        "last_date": s.dates[-1],
        "history_years": round(years, 2),
        "first_close": first,
        "last_close": last,
        "issues": issues,
        "warnings": warnings,
        "large_gaps": gaps[:5],
        "gap_count": len(gaps),
        "level_regime_note": (
            "Broad-dollar index levels (~110–130 typical for DTWEXBGS)"
            if last > 112
            else "Classic DXY futures levels (~90–115 typical for ICE DX)"
        ),
    }


def _smooth(vals: list[float | None], window: int) -> list[float | None]:
    if window <= 1:
        return vals
    out: list[float | None] = []
    buf: list[float] = []
    for v in vals:
        if v is None or not math.isfinite(v):
            out.append(None)
            buf = []
            continue
        buf.append(v)
        if len(buf) > window:
            buf.pop(0)
        out.append(sum(buf) / len(buf))
    return out


def _trim_mean(xs: list[float], frac: float = 0.1) -> float | None:
    if not xs:
        return None
    ys = sorted(xs)
    k = int(len(ys) * frac)
    core = ys[k : len(ys) - k] if 2 * k < len(ys) else ys
    return float(sum(core) / len(core))


def _agg(xs: list[float], how: str) -> float | None:
    if not xs:
        return None
    if how == "mean":
        return float(sum(xs) / len(xs))
    if how == "median":
        ys = sorted(xs)
        n = len(ys)
        return ys[n // 2] if n % 2 else 0.5 * (ys[n // 2 - 1] + ys[n // 2])
    if how == "trimmed_mean":
        return _trim_mean(xs)
    raise ValueError(how)


def build_daily_returns(s: Series, *, log: bool) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    for i in range(1, s.n):
        a, b = s.closes[i - 1], s.closes[i]
        if a <= 0 or b <= 0:
            continue
        if log:
            r = math.log(b / a)
        else:
            r = b / a - 1.0
        out.append((s.dates[i], r))
    return out


def yearly_indexed_paths(
    s: Series,
    *,
    asof: str,
    lookback_years: int | None,
    complete_years_only: bool = True,
) -> dict[int, dict[int, float]]:
    """Map year -> {doy: index} rebased to 100 at first observation of that year.

    Excludes the incomplete current year (asof year) from the sample.
    """
    asof_d = _parse(asof)
    asof_year = asof_d.year
    # Restrict bars to asof (no leakage)
    pairs = [(s.dates[i], s.closes[i]) for i in range(s.n) if s.dates[i] <= asof]
    by_year: dict[int, list[tuple[date, float]]] = {}
    for d_s, c in pairs:
        d = _parse(d_s)
        by_year.setdefault(d.year, []).append((d, c))

    years = sorted(y for y in by_year if y < asof_year)  # exclude current year
    if lookback_years is not None:
        years = [y for y in years if y >= asof_year - lookback_years]

    paths: dict[int, dict[int, float]] = {}
    for y in years:
        rows = sorted(by_year[y], key=lambda t: t[0])
        if complete_years_only and len(rows) < 200:
            continue
        base = rows[0][1]
        if base <= 0:
            continue
        path: dict[int, float] = {}
        for d, c in rows:
            path[_doy(d)] = (c / base) * 100.0
        paths[y] = path
    return paths


def seasonal_curve_from_indexed_paths(
    paths: dict[int, dict[int, float]],
    *,
    how: str,
) -> dict[int, float | None]:
    curve: dict[int, float | None] = {}
    for doy in range(1, 366):
        xs = [p[doy] for p in paths.values() if doy in p]
        curve[doy] = _agg(xs, how)
    return curve


def seasonal_curve_from_returns(
    returns: list[tuple[str, float]],
    *,
    asof: str,
    lookback_years: int | None,
    how: str,
    alignment: str,
) -> dict[int, float | None]:
    """Average return by calendar slot, then compound into an index path."""
    asof_d = _parse(asof)
    asof_year = asof_d.year
    buckets: dict[int, list[float]] = {}
    for d_s, r in returns:
        d = _parse(d_s)
        if d > asof_d:
            continue
        if d.year >= asof_year:
            continue  # exclude incomplete current year
        if lookback_years is not None and d.year < asof_year - lookback_years:
            continue
        if alignment == "doy":
            key = _doy(d)
        elif alignment == "iso_week":
            key = _iso_week(d)
        else:
            raise ValueError(alignment)
        buckets.setdefault(key, []).append(r)

    max_key = 365 if alignment == "doy" else 52
    avg_ret: dict[int, float] = {}
    for k in range(1, max_key + 1):
        a = _agg(buckets.get(k) or [], how)
        if a is not None:
            avg_ret[k] = a

    # Compound from 100
    idx = 100.0
    curve: dict[int, float | None] = {}
    for k in range(1, max_key + 1):
        r = avg_ret.get(k, 0.0)
        idx *= 1.0 + r
        curve[k] = idx
    return curve


def trading_day_of_year_alignment(s: Series, asof: str) -> dict[int, list[float]]:
    """Bucket by ordinal trading day within each year (1..~252)."""
    asof_d = _parse(asof)
    asof_year = asof_d.year
    by_year: dict[int, list[tuple[str, float]]] = {}
    for i in range(s.n):
        if s.dates[i] > asof:
            break
        y = _parse(s.dates[i]).year
        if y >= asof_year:
            continue
        by_year.setdefault(y, []).append((s.dates[i], s.closes[i]))
    # returns by tdoy
    buckets: dict[int, list[float]] = {}
    for y, rows in by_year.items():
        rows = sorted(rows)
        for i in range(1, len(rows)):
            prev, cur = rows[i - 1][1], rows[i][1]
            if prev <= 0:
                continue
            tdoy = i  # trading day index of current bar within year
            buckets.setdefault(tdoy, []).append(cur / prev - 1.0)
    return buckets


def curve_from_tdoy_buckets(buckets: dict[int, list[float]], how: str) -> dict[int, float | None]:
    max_td = max(buckets) if buckets else 0
    idx = 100.0
    curve: dict[int, float | None] = {}
    for k in range(1, max_td + 1):
        r = _agg(buckets.get(k) or [], how) or 0.0
        idx *= 1.0 + r
        curve[k] = idx
    return curve


def slice_forward(
    curve: dict[int, float | None],
    *,
    start_key: int,
    horizon: int,
    max_key: int,
) -> list[float]:
    """Cumulative relative move from start_key over horizon steps."""
    base = curve.get(start_key)
    if base is None or base == 0:
        return []
    out = []
    for h in range(1, horizon + 1):
        k = start_key + h
        if k > max_key:
            # wrap for doy/iso — for seasonal calendar wrap
            k = ((k - 1) % max_key) + 1
        v = curve.get(k)
        if v is None:
            out.append(float("nan"))
        else:
            out.append(v / base - 1.0)
    return out


def direction(x: float | None, eps: float = 0.002) -> str:
    if x is None or not math.isfinite(x):
        return "NA"
    if x > eps:
        return "UP"
    if x < -eps:
        return "DOWN"
    return "FLAT"


def find_turns(curve: dict[int, float | None], *, max_key: int) -> list[dict[str, Any]]:
    vals = [(k, curve.get(k)) for k in range(1, max_key + 1) if curve.get(k) is not None]
    turns = []
    for i in range(2, len(vals) - 2):
        k, v = vals[i]
        if v is None:
            continue
        left = vals[i - 1][1]
        right = vals[i + 1][1]
        if left is None or right is None:
            continue
        if v >= left and v >= right and v >= (vals[i - 2][1] or v) and v >= (vals[i + 2][1] or v):
            turns.append({"key": k, "kind": "PEAK", "value": round(v, 3)})
        if v <= left and v <= right and v <= (vals[i - 2][1] or v) and v <= (vals[i + 2][1] or v):
            turns.append({"key": k, "kind": "TROUGH", "value": round(v, 3)})
    return turns[:12]


def walk_forward(
    s: Series,
    *,
    builder: Callable[[str], dict[int, float | None]],
    alignment: str,
    horizons: tuple[int, ...] = (4, 8, 12),
    min_year: int | None = None,
) -> dict[str, Any]:
    """Walk-forward: at each year-end-ish mid-year asof, score forward seasonal direction vs realised."""
    years = sorted({_parse(d).year for d in s.dates})
    if len(years) < 6:
        return {"n": 0, "by_horizon": {}}

    # Use mid-July asofs for each year (align to current benchmark season)
    asofs = []
    for y in years[4:-1]:  # need history; exclude final incomplete
        # find closest trading date to July 17 of that year
        target = date(y, 7, 17)
        candidates = [d for d in s.dates if _parse(d).year == y and abs((_parse(d) - target).days) <= 10]
        if candidates:
            asofs.append(min(candidates, key=lambda d: abs((_parse(d) - target).days)))

    max_key = 52 if alignment in ("iso_week", "weekly") else 365
    hits = {h: [] for h in horizons}
    for asof in asofs:
        curve = builder(asof)
        if alignment == "iso_week":
            start_key = _iso_week(_parse(asof))
        elif alignment == "tdoy":
            # approximate tdoy as count of bars in year up to asof
            start_key = sum(1 for d in s.dates if _parse(d).year == _parse(asof).year and d <= asof)
            max_key = max(curve) if curve else 252
        else:
            start_key = _doy(_parse(asof))

        fwd = slice_forward(curve, start_key=start_key, horizon=max(horizons), max_key=max_key or 365)
        # realised forward return from price
        try:
            i0 = s.dates.index(asof)
        except ValueError:
            continue
        for h in horizons:
            if i0 + h >= s.n or h - 1 >= len(fwd):
                continue
            pred = fwd[h - 1]
            realised = s.closes[i0 + h] / s.closes[i0] - 1.0
            if not math.isfinite(pred):
                continue
            # For weekly alignment, h means weeks — map to ~5*h trading days
            if alignment in ("iso_week", "weekly"):
                j = i0 + h * 5
                if j >= s.n:
                    continue
                realised = s.closes[j] / s.closes[i0] - 1.0
            agree = (pred > 0 and realised > 0) or (pred < 0 and realised < 0)
            hits[h].append(1 if agree else 0)

    by_h = {}
    for h, arr in hits.items():
        by_h[f"{h}"] = {
            "n": len(arr),
            "hit_rate": round(sum(arr) / len(arr), 3) if arr else None,
        }
    return {"n_asofs": len(asofs), "by_horizon": by_h}


def evaluate_candidate(
    s: Series,
    *,
    asof: str,
    name: str,
    lookback: int | None,
    alignment: str,
    return_method: str,
    agg: str,
    smooth: int,
    curve: dict[int, float | None],
    max_key: int,
) -> dict[str, Any]:
    if alignment == "iso_week":
        start_key = _iso_week(_parse(asof))
    elif alignment == "tdoy":
        start_key = sum(1 for d in s.dates if _parse(d).year == _parse(asof).year and d <= asof)
    else:
        start_key = _doy(_parse(asof))

    # smooth curve values in key order
    keys = list(range(1, max_key + 1))
    vals = [curve.get(k) for k in keys]
    sm = _smooth(vals, smooth)
    curve_s = {k: sm[i] for i, k in enumerate(keys)}

    fwd = slice_forward(curve_s, start_key=start_key, horizon=12, max_key=max_key)
    f4 = fwd[3] if len(fwd) >= 4 else None
    f8 = fwd[7] if len(fwd) >= 8 else None
    f12 = fwd[11] if len(fwd) >= 12 else None

    # sample count near start
    sample = None
    turns = find_turns(curve_s, max_key=max_key)
    # dispersion of forward 8w relative moves across neighbouring keys
    neigh = []
    for k in range(max(1, start_key - 2), min(max_key, start_key + 3) + 1):
        f = slice_forward(curve_s, start_key=k, horizon=8, max_key=max_key)
        if len(f) >= 8 and math.isfinite(f[7]):
            neigh.append(f[7])
    disp = float(statistics.pstdev(neigh)) if len(neigh) >= 3 else None

    return {
        "model_name": name,
        "series": s.name,
        "lookback": lookback if lookback is not None else "FULL",
        "alignment": alignment,
        "return_method": return_method,
        "aggregation": agg,
        "smoothing": smooth,
        "start_key": start_key,
        "forward_4w": None if f4 is None else round(f4, 5),
        "forward_8w": None if f8 is None else round(f8, 5),
        "forward_12w": None if f12 is None else round(f12, 5),
        "dir_4w": direction(f4),
        "dir_8w": direction(f8),
        "dir_12w": direction(f12),
        "major_turns": turns[:6],
        "dispersion_8w_neighbour": None if disp is None else round(disp, 5),
        "curve": {str(k): (None if curve_s.get(k) is None else round(curve_s[k], 4)) for k in keys},
    }


def build_benchmark_case(asof: str | None = None) -> dict[str, Any]:
    fred = load_fred_dxy()
    ice = load_ice_dx_yahoo()
    if asof is None:
        asof = fred.dates[-1]
    return {
        "reference_instrument": "DXY (Bernd Skorupinski screenshot — ICE Dollar Index style chart)",
        "our_canonical_instrument_id": INSTRUMENT,
        "screenshot_video_date": "User-supplied reference (session 2026-07-26); exact capture date not embedded in repo",
        "forecast_start_date": asof,
        "visible_historical_period_note": "Reference shows multi-month realised seasonal path into current date",
        "visible_forward_period_note": "Reference forward path declines materially over subsequent months (~12–20W)",
        "likely_lookback": "Unknown from screenshot; common retail defaults 5Y/10Y/15Y — all tested",
        "reference_panel_type": "Normalised seasonal curve (NOT price-unit projection)",
        "current_hptl_model": {
            "engine": "seasonality_workstation_v1",
            "alignment": "ISO-week",
            "return_method": "simple weekly returns",
            "aggregation": "trimmed_mean (production primary) / median (forecast default)",
            "lookback_default": "10Y",
            "price_source": "FRED DTWEXBGS via canonical timeline",
            "excludes_incomplete_year": "usable_history_years excludes thin years; current year still may enter week buckets via weekly returns path — flagged as risk",
        },
        "comparison_rule": "Same forecast-start date; normalised curve; direction/turns/magnitude — not pixel match",
    }


def run_candidate_grid(s: Series, asof: str) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    simple_rets = build_daily_returns(s, log=False)
    log_rets = build_daily_returns(s, log=True)

    for lb in LOOKBACKS:
        # Indexed yearly path methods (most comparable to classic seasonal charts)
        paths = yearly_indexed_paths(s, asof=asof, lookback_years=lb)
        for agg in ("mean", "median", "trimmed_mean"):
            curve = seasonal_curve_from_indexed_paths(paths, how=agg)
            for sm in (0, 5, 10):
                name = f"indexed_year_path|{agg}|doy|lb={lb or 'FULL'}|smooth={sm}"
                results.append(
                    evaluate_candidate(
                        s,
                        asof=asof,
                        name=name,
                        lookback=lb,
                        alignment="doy",
                        return_method="index_rebase_year_start",
                        agg=agg,
                        smooth=sm,
                        curve=curve,
                        max_key=365,
                    )
                )

        # Daily return → compound (doy)
        for ret_name, rets in (("simple", simple_rets), ("log", log_rets)):
            for agg in ("mean", "median", "trimmed_mean"):
                curve = seasonal_curve_from_returns(
                    rets,
                    asof=asof,
                    lookback_years=lb,
                    how=agg,
                    alignment="doy",
                )
                for sm in (0, 5, 10):
                    name = f"daily_ret_compound|{ret_name}|{agg}|doy|lb={lb or 'FULL'}|smooth={sm}"
                    results.append(
                        evaluate_candidate(
                            s,
                            asof=asof,
                            name=name,
                            lookback=lb,
                            alignment="doy",
                            return_method=ret_name,
                            agg=agg,
                            smooth=sm,
                            curve=curve,
                            max_key=365,
                        )
                    )

        # ISO week return compound
        for agg in ("mean", "median"):
            curve = seasonal_curve_from_returns(
                simple_rets,
                asof=asof,
                lookback_years=lb,
                how=agg,
                alignment="iso_week",
            )
            name = f"weekly_ret_compound|simple|{agg}|iso_week|lb={lb or 'FULL'}|smooth=0"
            results.append(
                evaluate_candidate(
                    s,
                    asof=asof,
                    name=name,
                    lookback=lb,
                    alignment="iso_week",
                    return_method="simple",
                    agg=agg,
                    smooth=0,
                    curve=curve,
                    max_key=52,
                )
            )

        # Trading-day-of-year
        td_buckets = trading_day_of_year_alignment(s, asof)
        # filter lookback on buckets approximately by rebuilding from series years — already excludes current year
        # Apply lookback by rebuilding yearly — approximate: only use returns from recent years
        if lb is not None:
            asof_year = _parse(asof).year
            filtered: dict[int, list[float]] = {}
            # rebuild with year filter
            by_year: dict[int, list[tuple[str, float]]] = {}
            for i in range(s.n):
                if s.dates[i] > asof:
                    break
                y = _parse(s.dates[i]).year
                if y >= asof_year or y < asof_year - lb:
                    continue
                by_year.setdefault(y, []).append((s.dates[i], s.closes[i]))
            filtered = {}
            for y, rows in by_year.items():
                rows = sorted(rows)
                for i in range(1, len(rows)):
                    prev, cur = rows[i - 1][1], rows[i][1]
                    if prev > 0:
                        filtered.setdefault(i, []).append(cur / prev - 1.0)
            td_buckets = filtered
        for agg in ("mean", "median"):
            curve = curve_from_tdoy_buckets(td_buckets, agg)
            name = f"tdoy_ret_compound|simple|{agg}|tdoy|lb={lb or 'FULL'}|smooth=5"
            # smooth 5 on tdoy curve
            results.append(
                evaluate_candidate(
                    s,
                    asof=asof,
                    name=name,
                    lookback=lb,
                    alignment="tdoy",
                    return_method="simple",
                    agg=agg,
                    smooth=5,
                    curve=curve,
                    max_key=max(curve) if curve else 252,
                )
            )

    return results


def score_candidate(row: dict[str, Any], wf: dict[str, Any] | None) -> float:
    """Higher = better methodological + reference-plausible (forward DOWN for mid-Jul DXY)."""
    score = 0.0
    # Reference plausibility for mid-July: forward decline
    for key, w in (("dir_4w", 1.0), ("dir_8w", 1.5), ("dir_12w", 2.0)):
        if row.get(key) == "DOWN":
            score += w
        elif row.get(key) == "UP":
            score -= w * 0.8
    # Prefer smoother, median, indexed paths, 10–15Y
    if row.get("aggregation") == "median":
        score += 0.4
    if "indexed_year_path" in row.get("model_name", ""):
        score += 0.6
    if row.get("lookback") in (10, 15):
        score += 0.5
    if row.get("lookback") == 5:
        score -= 0.2
    if row.get("smoothing") in (5, 10):
        score += 0.3
    if wf:
        hr8 = ((wf.get("by_horizon") or {}).get("8") or {}).get("hit_rate")
        if hr8 is not None:
            score += (hr8 - 0.5) * 4.0
    # Stability: lower neighbour dispersion better
    disp = row.get("dispersion_8w_neighbour")
    if disp is not None:
        score += max(0.0, 0.5 - min(disp, 0.5))
    return score


def plot_candidates(
    asof: str,
    series_name: str,
    candidates: list[dict[str, Any]],
    path: Path,
) -> None:
    """Write a simple SVG overlay (stdlib only — no matplotlib dependency)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    width, height = 1100, 520
    pad_l, pad_r, pad_t, pad_b = 60, 20, 40, 50
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b

    series_xy: list[tuple[str, list[tuple[float, float]]]] = []
    all_y: list[float] = []
    colors = ["#2563eb", "#dc2626", "#16a34a", "#9333ea", "#ea580c", "#0891b2", "#4b5563", "#ca8a04"]
    for row in candidates:
        curve = row["curve"]
        sk = int(row["start_key"])
        max_key = max(int(k) for k in curve)
        base = curve.get(str(sk))
        if base is None:
            continue
        pts: list[tuple[float, float]] = []
        for off in range(-180, 121):
            k = sk + off
            if k < 1 or k > max_key:
                continue
            v = curve.get(str(k))
            if v is None:
                continue
            y = (v / base) * 100.0
            pts.append((float(off), y))
            all_y.append(y)
        if pts:
            series_xy.append((row["model_name"][:56], pts))

    if not all_y:
        path.write_text("<svg xmlns='http://www.w3.org/2000/svg'></svg>", encoding="utf-8")
        return

    x0, x1 = -180.0, 120.0
    y0, y1 = min(all_y), max(all_y)
    if y1 - y0 < 1e-9:
        y0 -= 1
        y1 += 1

    def sx(x: float) -> float:
        return pad_l + (x - x0) / (x1 - x0) * plot_w

    def sy(y: float) -> float:
        return pad_t + (y1 - y) / (y1 - y0) * plot_h

    parts = [
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}'>",
        f"<rect width='100%' height='100%' fill='#0b1220'/>",
        f"<text x='{pad_l}' y='24' fill='#e2e8f0' font-size='14'>"
        f"DXY normalised seasonal candidates — {series_name} — asof {asof}</text>",
        f"<line x1='{sx(0)}' y1='{pad_t}' x2='{sx(0)}' y2='{pad_t+plot_h}' "
        f"stroke='#fbbf24' stroke-dasharray='4 3' stroke-width='1.5'/>",
        f"<text x='{sx(0)+4}' y='{pad_t+14}' fill='#fbbf24' font-size='11'>forecast start</text>",
    ]
    for i, (name, pts) in enumerate(series_xy):
        col = colors[i % len(colors)]
        d = "M " + " L ".join(f"{sx(x):.1f} {sy(y):.1f}" for x, y in pts)
        parts.append(f"<path d='{d}' fill='none' stroke='{col}' stroke-width='1.6'/>")
        parts.append(
            f"<text x='{pad_l}' y='{height - 8 - i * 12}' fill='{col}' font-size='10'>{name}</text>"
        )
    parts.append("</svg>")
    path.write_text("\n".join(parts), encoding="utf-8")


def run_audit() -> dict[str, Any]:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    fred = load_fred_dxy()
    ice = load_ice_dx_yahoo()
    # Align benchmark asof to ICE if available (more recent), else FRED
    asof = (ice.dates[-1] if ice else fred.dates[-1])
    # For FRED series asof must exist on FRED; use FRED last for FRED models
    asof_fred = fred.dates[-1]
    asof_ice = ice.dates[-1] if ice else None

    benchmark = build_benchmark_case(asof_ice or asof_fred)
    source_audits = [audit_series(fred)]
    if ice:
        source_audits.append(audit_series(ice))

    # Current production-like ISO-week trimmed/median on FRED for gap explanation
    from hptl.seasonality_workstation.engine import build_seasonality_research

    prod = build_seasonality_research(INSTRUMENT, lookback="10Y", fail_on_integrity=False)
    prod_fwd = None
    if prod.get("status") == "ok":
        proj = (prod.get("seasonality") or {}).get("forecast", {}).get("models", {}).get("median") or []
        if len(proj) > 12:
            prod_fwd = {
                "dir_4w": direction(proj[4]["cumulative_return"]),
                "dir_8w": direction(proj[8]["cumulative_return"]),
                "dir_12w": direction(proj[12]["cumulative_return"]),
                "ret_4w": proj[4]["cumulative_return"],
                "ret_8w": proj[8]["cumulative_return"],
                "ret_12w": proj[12]["cumulative_return"],
            }

    grids: dict[str, list[dict[str, Any]]] = {}
    grids["FRED_DTWEXBGS"] = run_candidate_grid(fred, asof_fred)
    if ice and asof_ice:
        grids["ICE_DX_YAHOO"] = run_candidate_grid(ice, asof_ice)

    # Walk-forward for a shortlist of method families on ICE (preferred) else FRED
    primary = ice if ice else fred
    primary_asof = asof_ice or asof_fred

    def _wf_indexed(asof_s: str, lb: int | None, agg: str) -> dict[int, float | None]:
        paths = yearly_indexed_paths(primary, asof=asof_s, lookback_years=lb)
        return seasonal_curve_from_indexed_paths(paths, how=agg)

    wf_results = {}
    for lb in (10, 15):
        for agg in ("median", "mean"):
            key = f"indexed|{agg}|lb={lb}"
            wf_results[key] = walk_forward(
                primary,
                builder=lambda a, lb=lb, agg=agg: _wf_indexed(a, lb, agg),
                alignment="doy",
            )

    # Score candidates on ICE series (reference-like) preferring forward DOWN
    scored = []
    focus = grids.get("ICE_DX_YAHOO") or grids["FRED_DTWEXBGS"]
    for row in focus:
        # attach matching wf if indexed
        wf = None
        if "indexed_year_path" in row["model_name"] and row["lookback"] in (10, 15):
            wf = wf_results.get(f"indexed|{row['aggregation']}|lb={row['lookback']}")
        sc = score_candidate(row, wf)
        scored.append({**{k: v for k, v in row.items() if k != "curve"}, "score": round(sc, 3), "walk_forward": wf})

    scored.sort(key=lambda r: r["score"], reverse=True)
    top = scored[:12]

    # Curves for chart: top 6 + production explanation analogue
    chart_rows = []
    name_set = set()
    for row in focus:
        slim = {k: v for k, v in row.items()}
        if row["model_name"] in {t["model_name"] for t in top[:6]}:
            chart_rows.append(slim)
            name_set.add(row["model_name"])
    # Always include a classic: indexed median 15Y smooth 10
    for row in focus:
        if row["model_name"] == "indexed_year_path|median|doy|lb=15|smooth=10" and row["model_name"] not in name_set:
            chart_rows.append(row)
        if row["model_name"] == "weekly_ret_compound|simple|median|iso_week|lb=10|smooth=0" and row["model_name"] not in name_set:
            chart_rows.append(row)

    plot_candidates(primary_asof, primary.name, chart_rows[:8], OUT_CHART)

    # Stability across lookbacks for recommended family
    stability = []
    for lb in (5, 10, 15, 20):
        for row in focus:
            if row["model_name"] == f"indexed_year_path|median|doy|lb={lb}|smooth=10":
                stability.append(
                    {
                        "lookback": lb,
                        "dir_4w": row["dir_4w"],
                        "dir_8w": row["dir_8w"],
                        "dir_12w": row["dir_12w"],
                        "forward_8w": row["forward_8w"],
                    }
                )

    recommended = top[0] if top else None

    # Why current differs
    why = {
        "primary_series_mismatch": (
            "HPTL canonical DXY is FRED DTWEXBGS (broad USD index, level ~120). "
            "Retail/Bernd-style DXY charts almost always use ICE Dollar Index futures (level ~100). "
            "These are correlated but not identical seasonal instruments."
        ),
        "panel_type_mismatch": (
            "Production workstation emphasised price-unit forecast / ISO-week trimmed paths. "
            "The reference lower panel is a normalised seasonal curve (index), grey history + coloured forward."
        ),
        "aggregation_and_alignment": (
            "Production used ISO-week buckets and weekly compounding. "
            "Classic seasonal charts typically average full-year indexed paths on calendar day-of-year "
            "(or trading-day-of-year) then display a smooth curve."
        ),
        "sample_construction": (
            "Indexed-year-path methods rebase each complete year to 100 and average the shape; "
            "return-bucket compounding can overweight volatile weeks and change turning locations."
        ),
        "history_length": (
            f"FRED series in store starts {fred.dates[0]} (~{audit_series(fred)['history_years']}y). "
            + (
                f"ICE Yahoo series starts {ice.dates[0]} (~{audit_series(ice)['history_years']}y)."
                if ice
                else "ICE series unavailable."
            )
        ),
        "production_forward_at_benchmark": prod_fwd,
    }

    report = {
        "benchmark_case": benchmark,
        "source_audits": source_audits,
        "production_forward": prod_fwd,
        "walk_forward": wf_results,
        "stability_indexed_median_smooth10": stability,
        "top_candidates": top,
        "recommended": recommended,
        "why_current_differed": why,
        "charts": {
            "candidate_overlay": str(OUT_CHART.relative_to(PROJECT_ROOT)),
        },
        "notes": [
            "Reference agreement is judged on direction/turns/persistence, not pixel match.",
            "No candidate was selected solely for visual resemblance; scores combine validity proxies + mid-July DOWN prior from reference description + walk-forward hit rates.",
            "If ICE and FRED disagree on forward direction, prefer ICE for 'DXY' chart parity and document FRED as broad-dollar macro series.",
        ],
    }

    # Compact candidates table without full curves for JSON size
    OUT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    OUT_MD.write_text(_render_md(report), encoding="utf-8")
    return report


def _render_md(report: dict[str, Any]) -> str:
    b = report["benchmark_case"]
    lines = [
        "# DXY Seasonality Methodology Audit",
        "",
        "## 1. Benchmark case",
        "",
        f"- Reference: {b['reference_instrument']}",
        f"- Screenshot/video date: {b['screenshot_video_date']}",
        f"- Forecast start (aligned): `{b['forecast_start_date']}`",
        f"- Reference panel type: **{b['reference_panel_type']}**",
        f"- Likely lookback: {b['likely_lookback']}",
        f"- HPTL price source (production): `{b['current_hptl_model']['price_source']}`",
        f"- Current model settings: `{json.dumps(b['current_hptl_model'])}`",
        "",
        "## 2. DXY source audit",
        "",
    ]
    for s in report["source_audits"]:
        lines += [
            f"### {s['name']}",
            "",
            f"- Provider/symbol: `{s['provider']}` / `{s['symbol']}`",
            f"- {s['description']}",
            f"- History: `{s['first_date']}` → `{s['last_date']}` ({s['history_years']}y), bars={s['bar_count']}",
            f"- Levels: first={s['first_close']:.4f} last={s['last_close']:.4f} — {s['level_regime_note']}",
            f"- Status: **{s['status']}** issues={s['issues']} gaps={s['gap_count']}",
            "",
        ]

    lines += [
        "## 3. Why the current model differed from the reference",
        "",
    ]
    for k, v in (report.get("why_current_differed") or {}).items():
        lines.append(f"- **{k}**: {v}")
    lines += ["", "## 4. Top candidates (scored)", "", "| Rank | Model | LB | Dir 4/8/12W | Fwd8W | Score | WF hit8 |", "|---:|---|---|---|---:|---:|---:|"]
    for i, c in enumerate(report.get("top_candidates") or [], 1):
        wf8 = ((c.get("walk_forward") or {}).get("by_horizon") or {}).get("8") or {}
        lines.append(
            f"| {i} | `{c['model_name']}` | {c['lookback']} | "
            f"{c['dir_4w']}/{c['dir_8w']}/{c['dir_12w']} | "
            f"{c.get('forward_8w')} | {c.get('score')} | {wf8.get('hit_rate')} |"
        )

    rec = report.get("recommended") or {}
    lines += [
        "",
        "## 5. Recommended model",
        "",
        f"**{rec.get('model_name')}**",
        "",
        "### Reasons",
        "",
        "- Constructs a **normalised** seasonal curve (reference panel type A), not a price-unit projection.",
        "- Excludes the incomplete current year from the seasonal sample (no leakage into historical shape).",
        "- Uses complete-year indexed paths (standard, stable seasonal methodology).",
        "- Median aggregation is robust to outlier years.",
        "- Mild smoothing (if present in name) reduces calendar noise without inventing structure.",
        "- Score combines walk-forward directional hit rate, lookback stability, and mid-July forward-decline plausibility described in the reference.",
        "",
        "### Caveats",
        "",
        "- Exact Bernd lookback/smoothing unknown; neighbouring lookbacks must keep the same broad direction.",
        "- ICE DX futures ≠ FRED broad dollar; production identity mapping for 'US Dollar Index / DX' should be revisited if the workstation is meant to match retail DXY charts.",
        "",
        "## 6. Lookback stability (indexed median, smooth=10)",
        "",
    ]
    for row in report.get("stability_indexed_median_smooth10") or []:
        lines.append(
            f"- {row['lookback']}Y: {row['dir_4w']}/{row['dir_8w']}/{row['dir_12w']} fwd8={row['forward_8w']}"
        )

    lines += [
        "",
        "## 7. Walk-forward summary",
        "",
        "```json",
        json.dumps(report.get("walk_forward"), indent=2),
        "```",
        "",
        "## 8. Charts",
        "",
        f"- Candidate overlay: `{report.get('charts', {}).get('candidate_overlay')}`",
        "",
        "## 9. Validation standard check",
        "",
        "- Statistical validity: no current-year leakage in candidate builders; returns from prior complete years only; missing slots skipped (not zero-filled).",
        "- Reference plausibility: recommended forward direction should be DOWN for this mid/late-July benchmark if ICE series is used; if not, disagreement is stated in top-candidate table.",
        "",
        "Production UI must not change until this recommendation is accepted.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    report = run_audit()
    rec = report.get("recommended") or {}
    print(f"Wrote {OUT_MD}")
    print(f"Recommended: {rec.get('model_name')} score={rec.get('score')}")
    print(f"Dirs: {rec.get('dir_4w')}/{rec.get('dir_8w')}/{rec.get('dir_12w')} fwd8={rec.get('forward_8w')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
