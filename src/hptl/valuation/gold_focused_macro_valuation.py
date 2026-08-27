"""Gold Focused Macro Valuation Engine (research only).

Deliberately specified combined model using only major Gold drivers:
  DXY, US2Y, US30Y, Real10Y, Inflation, Central-bank net purchases.

Four controlled variants only (A–D). No broad feature search.

Does NOT modify production valuation, prices_latest, COT, Scanner, or Seasonality.
"""

from __future__ import annotations

import csv
import json
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
from scipy.optimize import lsq_linear

from hptl.config import PROJECT_ROOT
from hptl.fx.fx_macro_history import load_fred_daily_map
from hptl.valuation.gold_macro_tier1_discovery import _asof_series, _load_dx_daily
from hptl.valuation.metals_institutional_drivers import _load_cache_series

CB_CACHE_REL = "data/cache/metals_drivers/wgc_cb_gold_net_purchases.json"
from hptl.valuation.gold_monetary_equilibrium_research import (
    _daily_to_weekly_iso,
    ensure_oanda_gold_research_cache,
    load_public_monthly_gold,
)
from hptl.valuation.gold_phase2_macro_physical_discovery import _trend_dev, _zscore_past
from hptl.valuation.gold_structural_valuation_research import (
    MONTHLY_PUBLICATION_LAG_DAYS,
    _asof_with_lag,
    _classify_deviation,
    _deviation_series,
    _finite_ffill,
    _first_complete_index,
    _weekly_prices,
)
from hptl.valuation.metals_valuation_v1 import MODEL_ID as PUBLISHED_GOLD_MODEL_ID

AUDIT_DIR = PROJECT_ROOT / "data" / "audits" / "gold_focused_macro_valuation"
CHART_DIR = AUDIT_DIR / "charts"
REPORT_MD = AUDIT_DIR / "gold_focused_macro_report.md"
COMPARE_CSV = AUDIT_DIR / "gold_model_comparison.csv"
COEF_CSV = AUDIT_DIR / "gold_coefficients.csv"
FWD_CSV = AUDIT_DIR / "gold_forward_returns.csv"
EPISODE_CSV = AUDIT_DIR / "gold_valuation_episodes.csv"
JSON_OUT = AUDIT_DIR / "gold_focused_macro_ranking.json"

# Quarterly CB / GDT-style publication lag (quarter end → usable).
CB_PUBLICATION_LAG_DAYS = 75
HORIZONS = (4, 13, 26, 52)
MIN_TRAIN = 156
STEP = 13  # quarterly-ish coefficient updates
LEVEL_ANCHOR_WEEKS = 260  # trailing mean of log(Gold); past-only

# Fixed valuation bands (also report expanding percentiles).
BANDS = (
    ("materially_undervalued", None, -15.0),
    ("undervalued", -15.0, -5.0),
    ("near_fair_value", -5.0, 5.0),
    ("overvalued", 5.0, 15.0),
    ("materially_overvalued", 15.0, None),
)


def _parse_iso(d: str) -> date:
    return date.fromisoformat(str(d)[:10])


def _add_days(iso: str, days: int) -> str:
    return (_parse_iso(iso) + timedelta(days=days)).isoformat()


# ---------------------------------------------------------------------------
# Panel
# ---------------------------------------------------------------------------


def _build_gold_weekly(start: str = "2003-01-01") -> tuple[list[str], list[float], dict[str, Any]]:
    """Longest practical weekly Gold without mutating production store."""
    try:
        oanda = ensure_oanda_gold_research_cache(force_refresh=False)
        oanda_weeks = _daily_to_weekly_iso(list(oanda.get("daily") or []))
    except Exception:
        oanda_weeks = []
        oanda = {"n": 0, "start": None, "end": None}
    canon = dict(_weekly_prices("Gold"))
    pub = load_public_monthly_gold(force_refresh=False)

    week_set: set[str] = set()
    d0 = date.fromisoformat(start)
    d1 = date.today()
    cur = d0
    while cur <= d1:
        if cur.weekday() == 4:
            week_set.add(cur.isoformat())
        cur += timedelta(days=1)
    for d, _ in oanda_weeks:
        if d >= start:
            week_set.add(d)
    for d in canon:
        if d >= start:
            week_set.add(d)
    weeks = sorted(week_set)

    oanda_map = dict(oanda_weeks)
    monthly_asof = _asof_with_lag(pub["series"], weeks, lag_days=14)
    gold: list[float | None] = []
    src: list[str] = []
    for i, w in enumerate(weeks):
        if w in canon:
            gold.append(canon[w])
            src.append("canonical")
        elif w in oanda_map:
            gold.append(oanda_map[w])
            src.append("oanda_research")
        elif monthly_asof[i] is not None:
            gold.append(float(monthly_asof[i]))  # type: ignore[arg-type]
            src.append("public_monthly")
        else:
            gold.append(None)
            src.append("missing")

    keep = [i for i, g in enumerate(gold) if g is not None and g > 0]
    dates = [weeks[i] for i in keep]
    prices = [float(gold[i]) for i in keep]  # type: ignore[arg-type]
    counts: dict[str, int] = {}
    for i in keep:
        counts[src[i]] = counts.get(src[i], 0) + 1
    meta = {
        "n": len(dates),
        "start": dates[0] if dates else None,
        "end": dates[-1] if dates else None,
        "source_counts": counts,
        "oanda_research_n": oanda.get("n"),
        "note": "Research panel; production prices_latest not modified.",
    }
    return dates, prices, meta


def _cpi_yoy(cpi_weekly: list[float]) -> list[float | None]:
    out: list[float | None] = [None] * len(cpi_weekly)
    for i in range(len(cpi_weekly)):
        if i >= 52 and cpi_weekly[i - 52] > 0:
            out[i] = 100.0 * (cpi_weekly[i] / cpi_weekly[i - 52] - 1.0)
    return out


def _real_yield_series(dates: list[str]) -> tuple[list[float | None], dict[str, Any]]:
    """Prefer DFII10; fall back to DGS10 − CPI YoY proxy where TIPS missing."""
    dfii = load_fred_daily_map("DFII10", observation_start="2000-01-01")
    dgs10 = load_fred_daily_map("DGS10", observation_start="1970-01-01")
    cpi = load_fred_daily_map("CPIAUCSL", observation_start="1970-01-01")
    tips = _asof_series(dfii, dates)
    n10 = _asof_series(dgs10, dates)
    cpi_w = _finite_ffill(_asof_with_lag(cpi, dates, lag_days=MONTHLY_PUBLICATION_LAG_DAYS))
    cpi_f = [float(v) if v is not None else float("nan") for v in cpi_w]
    yoy = _cpi_yoy(cpi_f)
    out: list[float | None] = []
    n_tips = n_proxy = 0
    for i, d in enumerate(dates):
        if tips[i] is not None and math.isfinite(float(tips[i])):
            out.append(float(tips[i]))
            n_tips += 1
        elif n10[i] is not None and yoy[i] is not None:
            out.append(float(n10[i]) - float(yoy[i]))
            n_proxy += 1
        else:
            out.append(None)
    return out, {
        "primary": "DFII10",
        "proxy": "DGS10 - CPI_YoY",
        "n_tips": n_tips,
        "n_proxy": n_proxy,
    }


def _load_cb_quarterly_with_lag(dates: list[str]) -> tuple[list[float | None], dict[str, Any]]:
    """Carry latest published quarterly CB net purchases (tonnes); apply publication lag."""
    native = _load_cache_series(CB_CACHE_REL)
    monthly = sorted(native.items())
    meta = {
        "cache": CB_CACHE_REL,
        "n_native_obs": len(monthly),
        "frequency": "quarterly_or_monthly_native",
        "publication_lag_days": CB_PUBLICATION_LAG_DAYS,
        "engineering": "trailing_4_obs_sum (approx annual net tonnes), then as-of with lag",
        "missing_note": (
            "WGC xlsx requires Goldhub login; current series bootstrapped from public "
            "GDT HTML tables (short history). Carry-forward after lag — not interpolated."
        ),
    }
    if len(monthly) < 4:
        return [None] * len(dates), {**meta, "available": False}

    dates_m = [d for d, _ in monthly]
    vals = [float(v) for _, v in monthly]
    # Trailing 4 native prints ≈ annual for quarterly GDT.
    use: dict[str, float] = {}
    for i, d in enumerate(dates_m):
        use[d] = sum(vals[max(0, i - 3) : i + 1])

    shifted: dict[str, float] = {}
    for d, v in use.items():
        shifted[_add_days(d, CB_PUBLICATION_LAG_DAYS)] = float(v)
    series = _asof_series(shifted, dates)
    meta["available"] = any(v is not None for v in series)
    meta["first_usable"] = next((dates[i] for i, v in enumerate(series) if v is not None), None)
    meta["native_start"] = dates_m[0] if dates_m else None
    meta["native_end"] = dates_m[-1] if dates_m else None
    return series, meta


def build_focused_panel(*, start: str = "2003-01-01") -> dict[str, Any]:
    dates, prices, gold_meta = _build_gold_weekly(start=start)
    dx = _load_dx_daily()
    dgs2 = load_fred_daily_map("DGS2", observation_start="1970-01-01")
    dgs30 = load_fred_daily_map("DGS30", observation_start="1970-01-01")
    cpi = load_fred_daily_map("CPIAUCSL", observation_start="1970-01-01")
    t10yie = load_fred_daily_map("T10YIE", observation_start="2000-01-01")

    dxy = _finite_ffill(_asof_series(dx, dates))
    us2 = _finite_ffill(_asof_series(dgs2, dates))
    us30 = _finite_ffill(_asof_series(dgs30, dates))
    cpi_w = _finite_ffill(_asof_with_lag(cpi, dates, lag_days=MONTHLY_PUBLICATION_LAG_DAYS))
    be_w = _finite_ffill(_asof_series(t10yie, dates))
    real_w, real_meta = _real_yield_series(dates)
    cb_w, cb_meta = _load_cb_quarterly_with_lag(dates)

    # Inflation: CPI YoY when available; else breakeven
    cpi_f = [float(v) if v is not None else float("nan") for v in cpi_w]
    infl_cpi = _cpi_yoy(cpi_f)
    infl: list[float | None] = []
    infl_src = []
    for i in range(len(dates)):
        if infl_cpi[i] is not None:
            infl.append(float(infl_cpi[i]))
            infl_src.append("cpi_yoy")
        elif be_w[i] is not None:
            infl.append(float(be_w[i]))  # type: ignore[arg-type]
            infl_src.append("t10yie")
        else:
            infl.append(None)
            infl_src.append("missing")

    start_i = _first_complete_index([dxy, us2, us30, real_w, infl])
    if start_i is None:
        raise RuntimeError("Focused Gold panel: no complete core overlap")

    def trim(xs: list[Any]) -> list[Any]:
        return xs[start_i:]

    dates = trim(dates)
    prices = trim(prices)
    dxy = trim(dxy)
    us2 = trim(us2)
    us30 = trim(us30)
    real_w = trim(real_w)
    infl = trim(infl)
    cb_w = trim(cb_w)
    infl_src = trim(infl_src)

    # Dense floats for core
    raw = {
        "dxy": [float(v) for v in dxy],  # type: ignore[arg-type]
        "us2y": [float(v) for v in us2],  # type: ignore[arg-type]
        "us30y": [float(v) for v in us30],  # type: ignore[arg-type]
        "real10y": [float(v) for v in real_w],  # type: ignore[arg-type]
        "inflation": [float(v) for v in infl],  # type: ignore[arg-type]
        "cb_demand": cb_w,  # may contain None early
    }

    return {
        "dates": dates,
        "prices": prices,
        "log_gold": [math.log(p) for p in prices],
        "raw": raw,
        "infl_source": infl_src,
        "meta": {
            "gold": gold_meta,
            "real_yield": real_meta,
            "cb": cb_meta,
            "n_weeks": len(dates),
            "start": dates[0],
            "end": dates[-1],
            "published_model_untouched": PUBLISHED_GOLD_MODEL_ID,
        },
    }


# ---------------------------------------------------------------------------
# Normalisation (one sensible transform per driver)
# ---------------------------------------------------------------------------


def _select_transforms(raw: dict[str, list[Any]]) -> tuple[dict[str, list[float | None]], dict[str, str]]:
    """Controlled transform choice — not a large search."""
    dxy = raw["dxy"]
    us2 = raw["us2y"]
    us30 = raw["us30y"]
    real = raw["real10y"]
    infl = raw["inflation"]
    cb = [float(v) if v is not None and math.isfinite(float(v)) else float("nan") for v in raw["cb_demand"]]

    # DXY: z-score of log level (unit-free dollar pressure)
    log_dxy = [math.log(v) for v in dxy]
    # Rates / real: z-score 156 (level opportunity cost, comparable units)
    # Inflation: keep as YoY pp, then z-score 104 (already rate-like)
    # CB: z-score 104 of annualised net tonnes when history allows
    transforms = {
        "dxy": _zscore_past(log_dxy, 156),
        "us2y": _zscore_past(us2, 156),
        "us30y": _zscore_past(us30, 156),
        "real10y": _zscore_past(real, 156),
        "inflation": _zscore_past(infl, 104),
        "cb_demand": _zscore_past(
            [0.0 if math.isnan(v) else v for v in cb], 104
        ),
    }
    # Mask CB z where original missing
    for i, v in enumerate(raw["cb_demand"]):
        if v is None:
            transforms["cb_demand"][i] = None

    chosen = {
        "dxy": "zscore_156(log_level)",
        "us2y": "zscore_156(level)",
        "us30y": "zscore_156(level)",
        "real10y": "zscore_156(level)",
        "inflation": "zscore_104(cpi_yoy_or_breakeven)",
        "cb_demand": "zscore_104(trailing_annual_net_tonnes)",
    }
    # Fallback note: trend_dev available but not selected for primary path
    _ = _trend_dev(us2, 104)
    return transforms, chosen


# ---------------------------------------------------------------------------
# Constrained OLS
# ---------------------------------------------------------------------------


def _sign_bounds(feature_names: list[str]) -> tuple[list[float], list[float]]:
    """Bounds for [intercept, b1, ...]. Free intercept; constrained slopes."""
    lo = [-np.inf]
    hi = [np.inf]
    for f in feature_names:
        if f in {"dxy", "us2y", "real10y"}:
            lo.append(-np.inf)
            hi.append(0.0)  # must be <= 0
        elif f in {"inflation", "cb_demand"}:
            lo.append(0.0)
            hi.append(np.inf)
        elif f == "us30y":
            lo.append(-np.inf)
            hi.append(np.inf)  # tested freely
        elif f == "rate_factor":
            lo.append(-np.inf)
            hi.append(0.0)
        else:
            lo.append(-np.inf)
            hi.append(np.inf)
    return lo, hi


def _constrained_ols_slopes(
    y_demean: list[float], cols: list[list[float]], feature_names: list[str]
) -> tuple[list[float], float | None]:
    """Sign-constrained slopes on demeaned log-gold (no intercept in X)."""
    n = len(y_demean)
    if n < len(feature_names) + 5:
        return [], None
    X = np.column_stack([np.asarray(c, float) for c in cols])
    yy = np.asarray(y_demean, float)
    lo_full, hi_full = _sign_bounds(feature_names)
    lo, hi = lo_full[1:], hi_full[1:]  # drop intercept bounds
    try:
        res = lsq_linear(X, yy, bounds=(lo, hi), method="bvls", max_iter=200)
        beta = [float(b) for b in res.x]
    except Exception:
        beta_arr, _, _, _ = np.linalg.lstsq(X, yy, rcond=None)
        beta = [float(b) for b in beta_arr]
    yhat = X @ np.asarray(beta)
    ss_res = float(np.sum((yy - yhat) ** 2))
    ss_tot = float(np.sum((yy - yy.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else None
    return beta, r2


def _walk_forward_fair(
    y: list[float],
    cols: list[list[float]],
    feature_names: list[str],
    *,
    min_train: int = MIN_TRAIN,
    step: int = STEP,
) -> tuple[list[float | None], list[dict[str, float]], dict[str, Any]]:
    """Expanding walk-forward with past-only trailing level anchor.

    log(fair_t) = mean(log Gold over trailing LEVEL_ANCHOR_WEEKS ending at t-1)
                  + β' z_t

    Trailing (not full-expanding) anchor keeps fair value in the current gold
    regime; z-scored macro drivers explain valuation pressure around that level.
    """
    n = len(y)
    fair: list[float | None] = [None] * n
    coef_rows: list[dict[str, float]] = []
    preds: list[float] = []
    actuals: list[float] = []
    t = min_train
    while t < n:
        # Fit slopes on train residuals vs each observation's own trailing anchor.
        y_demean: list[float] = []
        cols_fit: list[list[float]] = [[] for _ in cols]
        for i in range(t):
            a0 = max(0, i - LEVEL_ANCHOR_WEEKS)
            if i - a0 < 52:
                continue
            mu_i = sum(y[a0:i]) / (i - a0)
            y_demean.append(y[i] - mu_i)
            for j, c in enumerate(cols):
                cols_fit[j].append(c[i])
        if len(y_demean) < max(40, len(feature_names) + 10):
            t += step
            continue
        slopes, r2 = _constrained_ols_slopes(y_demean, cols_fit, feature_names)
        if not slopes or r2 is None:
            t += step
            continue
        row = {f: slopes[i] for i, f in enumerate(feature_names)}
        row["level_anchor_weeks"] = float(LEVEL_ANCHOR_WEEKS)
        coef_rows.append(row)
        end = min(t + step, n)
        for i in range(t, end):
            a0 = max(0, i - LEVEL_ANCHOR_WEEKS)
            mu = sum(y[a0:i]) / max(1, i - a0)
            pred = mu + sum(slopes[j] * cols[j][i] for j in range(len(feature_names)))
            fair[i] = pred
            preds.append(pred)
            actuals.append(y[i])
        t += step

    oos = {"oos_r2": None, "oos_rmse": None, "oos_mae": None, "n_oos": len(preds)}
    if len(preds) >= 20:
        err2 = [(p - a) ** 2 for p, a in zip(preds, actuals)]
        mae = sum(abs(p - a) for p, a in zip(preds, actuals)) / len(preds)
        rmse = math.sqrt(sum(err2) / len(err2))
        mean_a = sum(actuals) / len(actuals)
        ss_tot = sum((a - mean_a) ** 2 for a in actuals)
        oos_r2 = 1.0 - sum(err2) / ss_tot if ss_tot > 0 else None
        oos.update(
            {
                "oos_r2": round(oos_r2, 4) if oos_r2 is not None else None,
                "oos_rmse": round(rmse, 6),
                "oos_mae": round(mae, 6),
            }
        )
    return fair, coef_rows, oos


# ---------------------------------------------------------------------------
# Valuation diagnostics
# ---------------------------------------------------------------------------


def _forward_bucket_stats(
    dates: list[str],
    prices: list[float],
    deviations: list[float | None],
    *,
    horizons: tuple[int, ...] = HORIZONS,
) -> list[dict[str, Any]]:
    n = len(prices)
    rows: list[dict[str, Any]] = []
    for bucket, lo, hi in BANDS:
        for h in horizons:
            rets: list[float] = []
            mae: list[float] = []
            # Episode counting: contiguous runs in bucket
            in_ep = False
            episodes = 0
            for i in range(n - h):
                d = deviations[i]
                if d is None:
                    in_ep = False
                    continue
                ok = True
                if lo is not None and not (d > lo):
                    ok = False
                if hi is not None and not (d <= hi):
                    ok = False
                if not ok:
                    in_ep = False
                    continue
                if not in_ep:
                    episodes += 1
                    in_ep = True
                fwd = 100.0 * (prices[i + h] / prices[i] - 1.0)
                rets.append(fwd)
                path = [100.0 * (prices[j] / prices[i] - 1.0) for j in range(i, i + h + 1)]
                mae.append(min(path) if d < 0 else max(path))
            if not rets:
                rows.append(
                    {
                        "bucket": bucket,
                        "horizon_weeks": h,
                        "n": 0,
                        "n_episodes": 0,
                        "mean_return_pct": None,
                        "median_return_pct": None,
                        "positive_return_rate": None,
                        "max_adverse_excursion_mean": None,
                    }
                )
                continue
            rs = sorted(rets)
            mid = len(rs) // 2
            med = rs[mid] if len(rs) % 2 else 0.5 * (rs[mid - 1] + rs[mid])
            rows.append(
                {
                    "bucket": bucket,
                    "horizon_weeks": h,
                    "n": len(rets),
                    "n_episodes": episodes,
                    "mean_return_pct": round(sum(rets) / len(rets), 3),
                    "median_return_pct": round(med, 3),
                    "positive_return_rate": round(sum(1 for r in rets if r > 0) / len(rets), 3),
                    "max_adverse_excursion_mean": round(sum(mae) / len(mae), 3),
                }
            )
    return rows


def _pooled_spread(fwd: list[dict[str, Any]], *, horizon: int = 13) -> dict[str, Any]:
    under_n = over_n = 0
    under_s = over_s = 0.0
    for r in fwd:
        if r.get("horizon_weeks") != horizon or not r.get("n"):
            continue
        m = r.get("mean_return_pct")
        if m is None:
            continue
        if r["bucket"] in {"materially_undervalued", "undervalued"}:
            under_n += int(r["n"])
            under_s += float(m) * int(r["n"])
        elif r["bucket"] in {"materially_overvalued", "overvalued"}:
            over_n += int(r["n"])
            over_s += float(m) * int(r["n"])
    if under_n < 8 or over_n < 8:
        return {"ok": False, "spread_pp": None}
    u = under_s / under_n
    o = over_s / over_n
    return {
        "ok": True,
        "under_mean_pct": round(u, 3),
        "over_mean_pct": round(o, 3),
        "under_n": under_n,
        "over_n": over_n,
        "spread_pp": round(u - o, 3),
    }


def _expanding_dev_percentiles(deviations: list[float | None]) -> dict[str, Any]:
    """Past-only percentile thresholds of |signed| deviation at tip."""
    hist: list[float] = []
    tip = None
    for d in deviations:
        if d is None:
            continue
        tip = d
        hist.append(d)
    if len(hist) < 40:
        return {"ok": False}
    s = sorted(hist[:-1] if len(hist) > 1 else hist)  # past-only vs tip
    def pct(p: float) -> float:
        i = int(round((len(s) - 1) * p))
        return s[max(0, min(len(s) - 1, i))]

    return {
        "ok": True,
        "tip_deviation_pct": tip,
        "p10": round(pct(0.10), 2),
        "p25": round(pct(0.25), 2),
        "p50": round(pct(0.50), 2),
        "p75": round(pct(0.75), 2),
        "p90": round(pct(0.90), 2),
        "note": "Expanding historical distribution of walk-forward deviations (past-only).",
    }


def _decade_spreads(
    dates: list[str], prices: list[float], deviations: list[float | None]
) -> list[dict[str, Any]]:
    decades = sorted({d[:3] + "0s" for d in dates})
    rows = []
    for dec in decades:
        mask = [d.startswith(dec[:3]) for d in dates]
        if sum(mask) < 80:
            continue
        d2 = [dates[i] for i, m in enumerate(mask) if m]
        p2 = [prices[i] for i, m in enumerate(mask) if m]
        v2 = [deviations[i] for i, m in enumerate(mask) if m]
        fwd = _forward_bucket_stats(d2, p2, v2, horizons=(13,))
        sp = _pooled_spread(fwd, horizon=13)
        rows.append({"decade": dec, **sp})
    return rows


def _signs_logical(coefs: dict[str, float], names: list[str]) -> dict[str, Any]:
    detail = {}
    ok = True
    for f in names:
        c = coefs.get(f)
        if c is None:
            continue
        if f in {"dxy", "us2y", "real10y", "rate_factor"}:
            good = c <= 1e-9
            detail[f] = {"coef": c, "expected": "<=0", "ok": good}
            ok = ok and good
        elif f in {"inflation", "cb_demand"}:
            good = c >= -1e-9
            detail[f] = {"coef": c, "expected": ">=0", "ok": good}
            ok = ok and good
        else:
            detail[f] = {"coef": c, "expected": "free", "ok": True}
    return {"ok": ok, "detail": detail}


# ---------------------------------------------------------------------------
# Models A–D
# ---------------------------------------------------------------------------


def _model_specs() -> list[dict[str, Any]]:
    return [
        {
            "id": "A_full",
            "label": "Model A — Full focused model",
            "features": ["dxy", "us2y", "us30y", "real10y", "inflation", "cb_demand"],
            "requires_cb": True,
        },
        {
            "id": "B_no_cb",
            "label": "Model B — No central-bank demand",
            "features": ["dxy", "us2y", "us30y", "real10y", "inflation"],
            "requires_cb": False,
        },
        {
            "id": "C_rates_compressed",
            "label": "Model C — Rates compressed (2Y+30Y factor)",
            "features": ["dxy", "rate_factor", "real10y", "inflation", "cb_demand"],
            "requires_cb": True,
            "compress_rates": True,
        },
        {
            "id": "D_core",
            "label": "Model D — Core (DXY + real yield + inflation + CB)",
            "features": ["dxy", "real10y", "inflation", "cb_demand"],
            "requires_cb": True,
        },
    ]


def _align_model(
    dates: list[str],
    y: list[float],
    prices: list[float],
    feat: dict[str, list[float | None]],
    names: list[str],
    *,
    require_cb: bool,
) -> tuple[list[str], list[float], list[float], dict[str, list[float]]]:
    out_d, out_y, out_p = [], [], []
    out_x: dict[str, list[float]] = {f: [] for f in names}
    for i, d in enumerate(dates):
        vals = []
        ok = True
        for f in names:
            v = feat[f][i]
            if v is None or not math.isfinite(float(v)):
                ok = False
                break
            vals.append(float(v))
        if require_cb and "cb_demand" in names:
            if feat["cb_demand"][i] is None:
                ok = False
        if not ok:
            continue
        out_d.append(d)
        out_y.append(y[i])
        out_p.append(prices[i])
        for f, v in zip(names, vals):
            out_x[f].append(v)
    return out_d, out_y, out_p, out_x


def _evaluate_model(panel: dict[str, Any], transforms: dict[str, list[float | None]], spec: dict[str, Any]) -> dict[str, Any]:
    dates = panel["dates"]
    y = panel["log_gold"]
    prices = panel["prices"]
    feat = dict(transforms)

    if spec.get("compress_rates"):
        # Equal-weight average of normalised 2Y and 30Y (both already z-scored).
        rf: list[float | None] = []
        for i in range(len(dates)):
            a, b = feat["us2y"][i], feat["us30y"][i]
            if a is None or b is None:
                rf.append(None)
            else:
                rf.append(0.5 * (float(a) + float(b)))
        feat["rate_factor"] = rf

    names = list(spec["features"])
    d_al, y_al, p_al, x_al = _align_model(
        dates, y, prices, feat, names, require_cb=bool(spec.get("requires_cb"))
    )
    min_train = MIN_TRAIN if len(y_al) >= MIN_TRAIN + 40 else max(52, len(y_al) // 3)
    if len(y_al) < min_train + 20:
        return {
            "id": spec["id"],
            "ok": False,
            "reason": f"short_sample n={len(y_al)}",
            "label": spec["label"],
        }

    cols = [x_al[f] for f in names]
    fair, coef_path, oos = _walk_forward_fair(
        y_al, cols, names, min_train=min_train, step=STEP
    )
    deviations = _deviation_series(p_al, fair)
    fwd = _forward_bucket_stats(d_al, p_al, deviations)
    spread13 = _pooled_spread(fwd, horizon=13)
    spread52 = _pooled_spread(fwd, horizon=52)
    pct = _expanding_dev_percentiles(deviations)
    decades = _decade_spreads(d_al, p_al, deviations)

    tip_beta = coef_path[-1] if coef_path else {}
    coefs = {k: v for k, v in tip_beta.items() if k != "intercept"}
    signs = _signs_logical(coefs, names)

    # Tip fair / deviation
    tip_fair = None
    tip_dev = None
    tip_price = p_al[-1]
    for i in range(len(fair) - 1, -1, -1):
        if fair[i] is not None:
            tip_fair = math.exp(float(fair[i]))
            tip_dev = deviations[i]
            tip_price = p_al[i]
            break

    # Usefulness score (valuation-first)
    score = 0.0
    if signs["ok"]:
        score += 25
    if spread13.get("spread_pp") is not None and float(spread13["spread_pp"]) > 0:
        score += min(40.0, float(spread13["spread_pp"]) / 5.0 * 40.0)
    if spread52.get("spread_pp") is not None and float(spread52["spread_pp"]) > 0:
        score += 15
    # Decade robustness: share of decades with positive spread
    dec_pos = [d for d in decades if d.get("spread_pp") is not None and float(d["spread_pp"]) > 0]
    if decades:
        score += 20.0 * (len(dec_pos) / len(decades))

    return {
        "id": spec["id"],
        "ok": True,
        "label": spec["label"],
        "features": names,
        "n_weeks": len(y_al),
        "sample_start": d_al[0],
        "sample_end": d_al[-1],
        "min_train": min_train,
        "tip_coefficients": tip_beta,
        "signs": signs,
        "oos": oos,
        "forward_returns": fwd,
        "spread_13w": spread13,
        "spread_52w": spread52,
        "deviation_percentiles": pct,
        "decade_spreads": decades,
        "usefulness_score": round(score, 1),
        "tip_price": tip_price,
        "tip_fair_value": round(tip_fair, 3) if tip_fair else None,
        "tip_deviation_pct": round(float(tip_dev), 3) if tip_dev is not None else None,
        "tip_bucket": _classify_deviation(float(tip_dev)) if tip_dev is not None else None,
        "coef_path_n": len(coef_path),
        "_coef_path": coef_path,
        "_dates": d_al,
        "_prices": p_al,
        "_fair_logs": fair,
        "_deviations": deviations,
        "_cb": list(x_al["cb_demand"]) if "cb_demand" in x_al else [None] * len(d_al),
    }


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------


def _write_main_chart(
    path: Path,
    *,
    title: str,
    dates: list[str],
    prices: list[float],
    fair_logs: list[float | None],
    deviations: list[float | None],
    cb: list[float | None],
) -> None:
    w, h = 1200, 780
    pad_l, pad_r, pad_t = 55, 20, 36
    y0, y1, y2 = pad_t, 320, 520
    plot_w = w - pad_l - pad_r

    pairs = []
    for d, px, fl, dv, c in zip(dates, prices, fair_logs, deviations, cb):
        if fl is None or dv is None:
            continue
        pairs.append((d, px, math.exp(fl), dv, c))
    if len(pairs) < 10:
        path.write_text(f"<svg xmlns='http://www.w3.org/2000/svg' width='{w}' height='{h}'><text x='20' y='40'>Insufficient data</text></svg>", encoding="utf-8")
        return

    def x_of(i: int) -> float:
        return pad_l + (i / max(1, len(pairs) - 1)) * plot_w

    pxs = [p[1] for p in pairs]
    fvs = [p[2] for p in pairs]
    dvs = [p[3] for p in pairs]
    ymin, ymax = min(min(pxs), min(fvs)), max(max(pxs), max(fvs))
    if ymax <= ymin:
        ymax = ymin + 1
    dmin, dmax = min(dvs), max(dvs)
    if abs(dmax - dmin) < 1e-9:
        dmax = dmin + 1

    def yp(v: float) -> float:
        return y0 + (1 - (v - ymin) / (ymax - ymin)) * (y1 - y0 - 10)

    def yd(v: float) -> float:
        return y1 + 20 + (1 - (v - dmin) / (dmax - dmin)) * (y2 - y1 - 40)

    def poly(vals: list[float], yfun: Any, color: str, width: float = 1.6) -> str:
        pts = " ".join(f"{x_of(i):.1f},{yfun(v):.1f}" for i, v in enumerate(vals))
        return f'<polyline fill="none" stroke="{color}" stroke-width="{width}" points="{pts}"/>'

    # Band fills in deviation pane
    bands_svg = []
    for lo, hi, color in [(-100, -15, "#22c55e33"), (-15, -5, "#86efac22"), (5, 15, "#fca5a522"), (15, 100, "#ef444433")]:
        top = yd(min(dmax, hi))
        bot = yd(max(dmin, lo))
        if bot < top:
            top, bot = bot, top
        bands_svg.append(
            f'<rect x="{pad_l}" y="{top:.1f}" width="{plot_w}" height="{max(1, bot-top):.1f}" fill="{color}"/>'
        )

    # CB bars (bottom)
    cb_vals = [p[4] if p[4] is not None and math.isfinite(float(p[4])) else None for p in pairs]
    cb_finite = [float(v) for v in cb_vals if v is not None]
    cb_svg = []
    if cb_finite:
        cmin, cmax = min(cb_finite), max(cb_finite)
        if abs(cmax - cmin) < 1e-9:
            cmax = cmin + 1
        base = h - 30
        top = y2 + 30

        def yc(v: float) -> float:
            return top + (1 - (v - cmin) / (cmax - cmin)) * (base - top)

        for i, v in enumerate(cb_vals):
            if v is None:
                continue
            x = x_of(i)
            y = yc(float(v))
            cb_svg.append(
                f'<line x1="{x:.1f}" y1="{base}" x2="{x:.1f}" y2="{y:.1f}" stroke="#a78bfa" stroke-width="1"/>'
            )

    markers = []
    for mark, label in [("2008-09-15", "GFC"), ("2020-03-15", "COVID"), ("2022-03-15", "2022"), ("2024-01-01", "2024+")]:
        idx = next((i for i, p in enumerate(pairs) if p[0] >= mark), None)
        if idx is None:
            continue
        x = x_of(idx)
        markers.append(f'<line x1="{x:.1f}" y1="{y0}" x2="{x:.1f}" y2="{y2}" stroke="#475569" stroke-dasharray="3 3"/>')
        markers.append(f'<text x="{x+2:.1f}" y="{y0+12}" fill="#94a3b8" font-size="10">{label}</text>')

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" style="background:#0b1220;font-family:Segoe UI,Arial,sans-serif">',
        f'<text x="{pad_l}" y="22" fill="#e2e8f0" font-size="16">{title}</text>',
        *markers,
        poly(pxs, yp, "#38bdf8", 1.8),
        poly(fvs, yp, "#f472b6", 1.8),
        *bands_svg,
        poly(dvs, yd, "#a3e635", 1.4),
        f'<line x1="{pad_l}" y1="{yd(0):.1f}" x2="{w-pad_r}" y2="{yd(0):.1f}" stroke="#64748b" stroke-dasharray="4 3"/>',
        *cb_svg,
        f'<text x="{pad_l}" y="{y0+12}" fill="#38bdf8" font-size="11">Gold</text>',
        f'<text x="{pad_l+50}" y="{y0+12}" fill="#f472b6" font-size="11">Fair value</text>',
        f'<text x="{pad_l}" y="{y1+16}" fill="#a3e635" font-size="11">Deviation %</text>',
        f'<text x="{pad_l}" y="{y2+20}" fill="#a78bfa" font-size="11">CB demand (norm, when available)</text>',
        f'<text x="{pad_l}" y="{h-10}" fill="#94a3b8" font-size="10">{pairs[0][0]} → {pairs[-1][0]} · n={len(pairs)}</text>',
        "</svg>",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _rank_and_verdict(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ok = [r for r in rows if r.get("ok")]
    ok.sort(key=lambda r: float(r.get("usefulness_score") or 0), reverse=True)
    for i, r in enumerate(ok, 1):
        r["rank"] = i
    if not ok:
        return {"verdict": "REJECT", "best_model": None, "narrative": "No model evaluated."}

    best = ok[0]
    sp = (best.get("spread_13w") or {}).get("spread_pp")
    signs_ok = bool((best.get("signs") or {}).get("ok"))
    score = float(best.get("usefulness_score") or 0)

    tip_dev = best.get("tip_deviation_pct")
    tip_extreme = tip_dev is not None and abs(float(tip_dev)) > 40.0
    n_weeks = int(best.get("n_weeks") or 0)
    short_history = n_weeks < 520  # < ~10y weekly
    decades = best.get("decade_spreads") or []
    single_decade = len(decades) <= 1
    under_n = int((best.get("spread_13w") or {}).get("under_n") or 0)
    tip_coefs = best.get("tip_coefficients") or {}
    cb_inert = "cb_demand" in (best.get("features") or []) and abs(
        float(tip_coefs.get("cb_demand") or 0.0)
    ) < 1e-9
    # Long-sample no-CB check (robustness): look up B in ranking rows if present.
    b_row = next((r for r in ok if r.get("id") == "B_no_cb"), None)
    b_spread = (b_row.get("spread_13w") or {}).get("spread_pp") if b_row else None
    long_sample_ok = b_spread is not None and float(b_spread) > 0

    promote_ok = (
        signs_ok
        and sp is not None
        and float(sp) > 3.0
        and score >= 70
        and not tip_extreme
        and not short_history
        and not single_decade
        and under_n >= 25
        and long_sample_ok
    )

    if promote_ok:
        verdict = "PROMOTE"
        narrative = (
            f"{best['id']} is practically usable: sensible signs, "
            f"under-over 13w spread={sp}pp, tip deviation={best.get('tip_deviation_pct')}%."
        )
    elif signs_ok and (
        (sp is not None and float(sp) > 0 and score >= 40)
        or (score >= 55 and not tip_extreme)
    ):
        caveats = []
        if tip_extreme:
            caveats.append("tip deviation extreme vs price")
        if short_history:
            caveats.append("estimation window still short")
        if single_decade:
            caveats.append("results concentrated in one decade")
        if under_n < 25:
            caveats.append(f"few undervalued observations (n={under_n})")
        if cb_inert:
            caveats.append("CB coefficient inert at bound (0)")
        if b_spread is not None and float(b_spread) <= 0:
            caveats.append(f"long-sample Model B spread13={b_spread} (not confirmatory)")
        verdict = "USEFUL_BUT_RESEARCH"
        narrative = (
            f"{best['id']} produces a usable research fair-value context "
            f"(spread13={sp}pp, score={score}, tip_dev={tip_dev}%, "
            f"tip_fv={best.get('tip_fair_value')}). Not promotion-ready"
            + (f" ({'; '.join(caveats)})" if caveats else "")
            + ". Treat as workstation research overlay beside COT/seasonality."
        )
    elif score >= 35 and signs_ok:
        verdict = "USEFUL_BUT_RESEARCH"
        narrative = (
            f"{best['id']} has logical coefficients but weak/mixed valuation "
            f"direction (spread13={sp}, tip_dev={tip_dev}). "
            f"Useful for driver-context charts only."
        )
    else:
        verdict = "REJECT"
        narrative = (
            f"Focused macro variants do not provide reliable valuation context. "
            f"Best was {best['id']} (score={score}, spread13={sp})."
        )
    return {"verdict": verdict, "best_model": best["id"], "narrative": narrative}


def run_gold_focused_macro_valuation(*, start: str = "2003-01-01") -> dict[str, Any]:
    t0 = datetime.now(timezone.utc)
    # Ensure CB cache from manual CSV if present
    try:
        from hptl.data_sources.cb_gold_purchases_ingest import ingest_cb_gold_purchases

        ingest_cb_gold_purchases(write_status=False)
    except Exception:
        pass

    panel = build_focused_panel(start=start)
    transforms, transform_choice = _select_transforms(panel["raw"])
    results = [_evaluate_model(panel, transforms, spec) for spec in _model_specs()]
    verdict = _rank_and_verdict(results)
    ok_rows = [r for r in results if r.get("ok")]
    ok_rows.sort(key=lambda r: float(r.get("usefulness_score") or 0), reverse=True)

    CHART_DIR.mkdir(parents=True, exist_ok=True)
    charts = []
    best_id = verdict.get("best_model")
    for r in ok_rows:
        if r["id"] != best_id and r["id"] not in {"A_full", "B_no_cb", "D_core"}:
            continue
        path = CHART_DIR / f"{r['id']}_sync.svg"
        _write_main_chart(
            path,
            title=f"Gold focused macro — {r['id']}",
            dates=r["_dates"],
            prices=r["_prices"],
            fair_logs=r["_fair_logs"],
            deviations=r["_deviations"],
            cb=list(r.get("_cb") or [None] * len(r["_dates"])),
        )
        rel = str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")
        r["chart"] = rel
        charts.append(rel)

    public = []
    for r in ok_rows:
        public.append({k: v for k, v in r.items() if not str(k).startswith("_")})

    payload = {
        "generated_at": t0.replace(microsecond=0).isoformat(),
        "ok": True,
        "research_only": True,
        "published_models_untouched": {
            "gold_model_id": PUBLISHED_GOLD_MODEL_ID,
            "prices_latest_not_modified": True,
        },
        "panel": panel["meta"],
        "transform_choice": transform_choice,
        "ranking": public,
        "verdict": verdict,
        "charts": charts,
        "runtime_sec": round((datetime.now(timezone.utc) - t0).total_seconds(), 2),
        "_private": ok_rows,
        "_failed": [r for r in results if not r.get("ok")],
    }
    return payload


def render_markdown(payload: dict[str, Any]) -> str:
    v = payload.get("verdict") or {}
    panel = payload.get("panel") or {}
    lines = [
        "# Gold Focused Macro Valuation",
        "",
        f"Generated: `{payload.get('generated_at')}`",
        "",
        "**Research only — not deployed.**",
        "",
        f"**Verdict: {v.get('verdict')}**",
        "",
        v.get("narrative") or "",
        "",
        "## Panel",
        "",
        f"- Weeks: **{panel.get('n_weeks')}** ({panel.get('start')} → {panel.get('end')})",
        f"- Gold sources: `{((panel.get('gold') or {}).get('source_counts'))}`",
        f"- Real yield: `{(panel.get('real_yield') or {})}`",
        f"- CB: `{(panel.get('cb') or {})}`",
        "",
        "## Transforms (one per driver)",
        "",
    ]
    for k, val in (payload.get("transform_choice") or {}).items():
        lines.append(f"- `{k}`: {val}")
    lines.extend(
        [
            "",
            "## Model comparison",
            "",
            "| Rank | Model | Score | Signs | Spread13 | Spread52 | OOS R2 | Tip FV | Tip Dev | Bucket |",
            "| ---: | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for r in payload.get("ranking") or []:
        lines.append(
            f"| {r.get('rank')} | `{r.get('id')}` | {r.get('usefulness_score')} | "
            f"{(r.get('signs') or {}).get('ok')} | "
            f"{(r.get('spread_13w') or {}).get('spread_pp')} | "
            f"{(r.get('spread_52w') or {}).get('spread_pp')} | "
            f"{(r.get('oos') or {}).get('oos_r2')} | "
            f"{r.get('tip_fair_value')} | {r.get('tip_deviation_pct')} | {r.get('tip_bucket')} |"
        )

    best = next((r for r in (payload.get("ranking") or []) if r.get("id") == v.get("best_model")), None)
    lines.extend(["", "## Best model detail", ""])
    if best:
        lines.extend(
            [
                f"- **ID:** `{best.get('id')}` — {best.get('label')}",
                f"- **Features:** {best.get('features')}",
                f"- **Tip coefficients:** `{best.get('tip_coefficients')}`",
                f"- **Current price / fair / deviation:** "
                f"{best.get('tip_price')} / {best.get('tip_fair_value')} / {best.get('tip_deviation_pct')}%",
                f"- **Bucket:** {best.get('tip_bucket')}",
                f"- **Chart:** `{best.get('chart')}`",
                "",
                "### 13-week bucket returns",
                "",
                "| Bucket | n | Episodes | Mean % | Median % | Hit | MAE |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for fr in best.get("forward_returns") or []:
            if fr.get("horizon_weeks") != 13:
                continue
            lines.append(
                f"| {fr.get('bucket')} | {fr.get('n')} | {fr.get('n_episodes')} | "
                f"{fr.get('mean_return_pct')} | {fr.get('median_return_pct')} | "
                f"{fr.get('positive_return_rate')} | {fr.get('max_adverse_excursion_mean')} |"
            )
        lines.extend(["", "### Where it works / does not", ""])
        lines.append(
            "- Works as a **macro pressure gauge**: fair value moves with dollar, "
            "real yields and inflation in the constrained direction."
        )
        lines.append(
            "- Does **not** claim Natural Gas-style physical mean reversion; "
            "persistent deviations can last through policy regimes."
        )
        cb = panel.get("cb") or {}
        lines.append(
            f"- CB demand history is short/limited (`n_native_obs={cb.get('n_native_obs')}`, "
            f"`first_usable={cb.get('first_usable')}`): Models A/C/D are constrained by that window."
        )
        lines.append(
            "- Model B (no CB) covers the longer post-2003 sample and is the robustness check."
        )

    lines.extend(
        [
            "",
            "## Safety",
            "",
            f"- Published Gold model untouched: `{PUBLISHED_GOLD_MODEL_ID}`",
            "- No production price/COT/Scanner/Seasonality mutation",
            "- Outputs under `data/audits/gold_focused_macro_valuation/`",
            "",
            f"Runtime: {payload.get('runtime_sec')}s",
            "",
        ]
    )
    return "\n".join(lines)


def write_outputs(payload: dict[str, Any]) -> dict[str, str]:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    private = list(payload.get("_private") or [])
    public = {k: v for k, v in payload.items() if not str(k).startswith("_")}
    JSON_OUT.write_text(json.dumps(public, indent=2, ensure_ascii=False), encoding="utf-8")
    REPORT_MD.write_text(render_markdown(public), encoding="utf-8")

    with COMPARE_CSV.open("w", newline="", encoding="utf-8") as fh:
        fields = [
            "rank",
            "id",
            "usefulness_score",
            "signs_ok",
            "spread_13w_pp",
            "spread_52w_pp",
            "oos_r2",
            "oos_rmse",
            "n_weeks",
            "sample_start",
            "sample_end",
            "tip_fair_value",
            "tip_deviation_pct",
            "tip_bucket",
        ]
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in private:
            w.writerow(
                {
                    "rank": r.get("rank"),
                    "id": r.get("id"),
                    "usefulness_score": r.get("usefulness_score"),
                    "signs_ok": (r.get("signs") or {}).get("ok"),
                    "spread_13w_pp": (r.get("spread_13w") or {}).get("spread_pp"),
                    "spread_52w_pp": (r.get("spread_52w") or {}).get("spread_pp"),
                    "oos_r2": (r.get("oos") or {}).get("oos_r2"),
                    "oos_rmse": (r.get("oos") or {}).get("oos_rmse"),
                    "n_weeks": r.get("n_weeks"),
                    "sample_start": r.get("sample_start"),
                    "sample_end": r.get("sample_end"),
                    "tip_fair_value": r.get("tip_fair_value"),
                    "tip_deviation_pct": r.get("tip_deviation_pct"),
                    "tip_bucket": r.get("tip_bucket"),
                }
            )

    with COEF_CSV.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["model_id", "feature", "tip_coefficient"])
        for r in private:
            tip = r.get("tip_coefficients") or {}
            for f, c in tip.items():
                w.writerow([r.get("id"), f, c])

    with FWD_CSV.open("w", newline="", encoding="utf-8") as fh:
        fields = [
            "model_id",
            "bucket",
            "horizon_weeks",
            "n",
            "n_episodes",
            "mean_return_pct",
            "median_return_pct",
            "positive_return_rate",
            "max_adverse_excursion_mean",
        ]
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in private:
            for fr in r.get("forward_returns") or []:
                w.writerow({"model_id": r.get("id"), **fr})

    with EPISODE_CSV.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["model_id", "bucket", "horizon_weeks", "n_observations", "n_episodes"])
        for r in private:
            for fr in r.get("forward_returns") or []:
                w.writerow(
                    [
                        r.get("id"),
                        fr.get("bucket"),
                        fr.get("horizon_weeks"),
                        fr.get("n"),
                        fr.get("n_episodes"),
                    ]
                )

    return {
        "report": str(REPORT_MD.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "comparison_csv": str(COMPARE_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "coefficients_csv": str(COEF_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "forward_csv": str(FWD_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "episodes_csv": str(EPISODE_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "ranking_json": str(JSON_OUT.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "charts_dir": str(CHART_DIR.relative_to(PROJECT_ROOT)).replace("\\", "/"),
    }
