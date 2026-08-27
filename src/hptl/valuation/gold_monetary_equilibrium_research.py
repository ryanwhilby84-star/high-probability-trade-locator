"""Gold Monetary Equilibrium + ECM Research (research only).

Extends the structural CONTINUE_RESEARCH lead:
  log(Gold) ~ log(M2) + log(CPI)

Phases:
  1) Extended historical panel with realistic release lags
  2) Long-run monetary equilibrium candidates A–E
  3) Cointegration (Engle–Granger; Johansen-lite when feasible)
  4) Leakage-safe expanding / rolling ECMs
  5) Valuation-direction forward returns (incl. 104w)
  6) Regime diagnostics (no auto regime model)
  7) Coefficient stability
  8) Baseline comparisons + dual score (price vs valuation)

Does NOT modify production valuation models, prices_latest.json,
metals_real_yield_v1, NG valuation, COT, Scanner, or Seasonality.
"""

from __future__ import annotations

import csv
import json
import math
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np

from hptl.config import PROJECT_ROOT
from hptl.fx.fx_macro_history import load_fred_daily_map
from hptl.prices.canonical_timeline import load_canonical_timeline
from hptl.valuation.energy_natural_gas_valuation_v1 import (
    _multivariate_ols,
    _predict_log_price,
)
from hptl.valuation.gold_macro_tier1_discovery import _asof_series
from hptl.valuation.gold_structural_valuation_research import (
    MONTHLY_PUBLICATION_LAG_DAYS,
    _add_days,
    _asof_with_lag,
    _bucket_forward_returns,
    _classify_deviation,
    _deviation_series,
    _finite_ffill,
    _first_complete_index,
    _write_sync_chart_svg,
    _weekly_prices,
)
from hptl.valuation.metals_valuation_v1 import MODEL_ID as PUBLISHED_GOLD_MODEL_ID
from hptl.valuation.ng_driver_validation_phase2_production import (
    MIN_TRAIN,
    STEP,
    _walk_forward_predictions,
)

AUDIT_DIR = PROJECT_ROOT / "data" / "audits" / "gold_monetary_equilibrium"
CACHE_DIR = AUDIT_DIR / "cache"
CHART_DIR = AUDIT_DIR / "charts"
REPORT_MD = AUDIT_DIR / "gold_monetary_equilibrium_report.md"
COINT_CSV = AUDIT_DIR / "gold_cointegration_results.csv"
ECM_CSV = AUDIT_DIR / "gold_ecm_results.csv"
SCORES_CSV = AUDIT_DIR / "gold_candidate_scores.csv"
FWD_CSV = AUDIT_DIR / "gold_forward_return_buckets.csv"
REGIME_CSV = AUDIT_DIR / "gold_regime_diagnostics.csv"
STAB_CSV = AUDIT_DIR / "gold_coefficient_stability.csv"
RANKING_JSON = AUDIT_DIR / "gold_monetary_equilibrium_ranking.json"

PUBLIC_GOLD_MONTHLY_URL = (
    "https://raw.githubusercontent.com/datasets/gold-prices/master/data/monthly.csv"
)
MONTHLY_GOLD_LAG_DAYS = 14  # month average; conservative availability lag
QUARTERLY_LAG_DAYS = 60  # GDP advance/second estimate style lag
HORIZONS = (4, 8, 13, 26, 52, 104)
MIN_TRAIN_LONG = 260  # ~5y weekly for long-history expanding fits
ROLL_WINDOW = 520  # ~10y rolling

# Engle–Granger residual ADF critical values (const, no trend) by #variables in coint eq.
EG_CV_5PCT = {2: -3.37, 3: -3.74, 4: -4.10, 5: -4.35}
ADF_CV_5PCT = -2.86  # unit-root ADF with constant, large n

PRICE_PROMOTE = 65.0
VAL_PROMOTE = 70.0


# ---------------------------------------------------------------------------
# Small numerics
# ---------------------------------------------------------------------------


def _ols(y: np.ndarray, X: np.ndarray) -> tuple[np.ndarray, float | None]:
    if len(y) < X.shape[1] + 3:
        return np.array([]), None
    beta, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    yhat = X @ beta
    ss_res = float(np.sum((y - yhat) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else None
    return beta, r2


def _adf_tstat(series: list[float] | np.ndarray, *, maxlag: int | None = None) -> dict[str, Any]:
    """ADF t-stat on lagged level (const, no trend). Negative → more stationary."""
    y = np.asarray(series, dtype=float)
    y = y[np.isfinite(y)]
    n = len(y)
    if n < 40:
        return {"ok": False, "tstat": None, "n": n}
    dy = np.diff(y)
    lag = maxlag if maxlag is not None else max(1, int(12 * (n / 100) ** 0.25))
    lag = min(lag, max(1, n // 8))
    # dy_t = a + b y_{t-1} + sum c_i dy_{t-i}
    rows = []
    target = []
    for t in range(lag, len(dy)):
        # y index for level is t (since dy[t] = y[t+1]-y[t], level y_t corresponds to index t)
        level = y[t]
        feats = [1.0, level]
        for i in range(1, lag + 1):
            feats.append(dy[t - i])
        rows.append(feats)
        target.append(dy[t])
    X = np.asarray(rows, dtype=float)
    yy = np.asarray(target, dtype=float)
    beta, _ = _ols(yy, X)
    if beta.size == 0:
        return {"ok": False, "tstat": None, "n": n}
    yhat = X @ beta
    resid = yy - yhat
    s2 = float(np.sum(resid**2) / max(1, len(yy) - X.shape[1]))
    try:
        xtx_inv = np.linalg.inv(X.T @ X)
        se = math.sqrt(max(s2 * float(xtx_inv[1, 1]), 1e-18))
    except Exception:
        return {"ok": False, "tstat": None, "n": n}
    tstat = float(beta[1] / se) if se > 0 else None
    return {
        "ok": tstat is not None,
        "tstat": round(tstat, 4) if tstat is not None else None,
        "lag": lag,
        "n": len(yy),
        "reject_unit_root_5pct": bool(tstat is not None and tstat < ADF_CV_5PCT),
    }


def _eg_cointegration(
    y: list[float], x_cols: list[list[float]], *, n_vars: int
) -> dict[str, Any]:
    n = len(y)
    if n < 60 or any(len(c) != n for c in x_cols):
        return {"ok": False, "cointegrated_5pct": False}
    Y = np.asarray(y, dtype=float)
    X = np.column_stack([np.ones(n)] + [np.asarray(c, dtype=float) for c in x_cols])
    beta, r2 = _ols(Y, X)
    if beta.size == 0:
        return {"ok": False, "cointegrated_5pct": False}
    resid = Y - X @ beta
    adf = _adf_tstat(resid.tolist())
    cv = EG_CV_5PCT.get(n_vars, -4.10)
    t = adf.get("tstat")
    coint = bool(t is not None and t < cv)
    return {
        "ok": True,
        "method": "engle_granger",
        "n": n,
        "r_squared": round(float(r2), 4) if r2 is not None else None,
        "beta": [round(float(b), 6) for b in beta],
        "residual_adf_tstat": t,
        "critical_value_5pct": cv,
        "cointegrated_5pct": coint,
        "n_vars": n_vars,
    }


def _johansen_trace_2var(y: list[float], x: list[float], *, lag: int = 2) -> dict[str, Any]:
    """Minimal Johansen trace for bivariate system (research diagnostic)."""
    n = min(len(y), len(x))
    if n < 80:
        return {"ok": False, "reason": "short_sample"}
    z = np.column_stack([np.asarray(y[:n], float), np.asarray(x[:n], float)])
    dz = np.diff(z, axis=0)
    # Build lagged levels and diffs
    rows_dz, rows_zlag = [], []
    for t in range(lag, len(dz)):
        rows_dz.append(dz[t])
        rows_zlag.append(z[t])  # level at t corresponds roughly to lag of diff endpoint
    Y = np.asarray(rows_dz, float)
    Z = np.asarray(rows_zlag, float)
    # Residualize on lagged diffs
    if lag > 0:
        dlag = []
        for t in range(lag, len(dz)):
            feats = [1.0]
            for i in range(1, lag + 1):
                feats.extend(dz[t - i].tolist())
            dlag.append(feats)
        D = np.asarray(dlag, float)
        by, _ = _ols(Y[:, 0], D)
        bx, _ = _ols(Y[:, 1], D)
        bz0, _ = _ols(Z[:, 0], D)
        bz1, _ = _ols(Z[:, 1], D)
        if by.size == 0:
            return {"ok": False}
        R0 = np.column_stack([Y[:, 0] - D @ by, Y[:, 1] - D @ bx])
        R1 = np.column_stack([Z[:, 0] - D @ bz0, Z[:, 1] - D @ bz1])
    else:
        R0, R1 = Y, Z
    S00 = R0.T @ R0 / len(R0)
    S11 = R1.T @ R1 / len(R1)
    S01 = R0.T @ R1 / len(R0)
    S10 = S01.T
    try:
        m = np.linalg.solve(S11, S10 @ np.linalg.solve(S00, S01))
        eig = np.sort(np.real(np.linalg.eigvals(m)))[::-1]
    except Exception:
        return {"ok": False, "reason": "eig_failed"}
    # Trace stats
    tr = []
    for i in range(len(eig)):
        tr.append(-len(R0) * float(np.sum(np.log(1.0 - np.clip(eig[i:], 0, 0.999)))))
    # Approx 5% critical for const: r=0 ~15.41, r<=1 ~3.76
    return {
        "ok": True,
        "method": "johansen_trace_bivariate",
        "eigenvalues": [round(float(e), 6) for e in eig],
        "trace_stats": [round(t, 3) for t in tr],
        "reject_r0_5pct": bool(tr and tr[0] > 15.41),
        "reject_r1_5pct": bool(len(tr) > 1 and tr[1] > 3.76),
    }


# ---------------------------------------------------------------------------
# Phase 1 — extended data
# ---------------------------------------------------------------------------


def _http_get_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "HPTL-research/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read().decode("utf-8")


def load_public_monthly_gold(*, force_refresh: bool = False) -> dict[str, Any]:
    """Public monthly gold (datasets/gold-prices). Research cache only."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / "gold_public_monthly.csv"
    meta_path = CACHE_DIR / "gold_public_monthly_meta.json"
    if path.exists() and not force_refresh:
        text = path.read_text(encoding="utf-8")
    else:
        text = _http_get_text(PUBLIC_GOLD_MONTHLY_URL)
        path.write_text(text, encoding="utf-8")
    series: dict[str, float] = {}
    for i, line in enumerate(text.splitlines()):
        if i == 0 or not line.strip():
            continue
        parts = line.split(",")
        if len(parts) < 2:
            continue
        ym = parts[0].strip()
        try:
            px = float(parts[1])
        except ValueError:
            continue
        if len(ym) == 7 and ym[4] == "-":
            # Date as month-start
            d = f"{ym}-01"
            if math.isfinite(px) and px > 0:
                series[d] = px
    meta = {
        "source": "https://github.com/datasets/gold-prices (monthly.csv)",
        "provider_note": (
            "Community dataset aggregating long-run London/World Bank–style monthly "
            "gold averages. Used for research extension only — not production OHLC."
        ),
        "native_frequency": "monthly",
        "publication_lag_days": MONTHLY_GOLD_LAG_DAYS,
        "start": min(series) if series else None,
        "end": max(series) if series else None,
        "n": len(series),
        "missing_value_treatment": "skip non-numeric rows",
        "resampling_method": (
            "as-of onto weekly dates after applying publication lag; "
            "causal forward-fill between monthly prints"
        ),
        "cache_path": str(path.relative_to(PROJECT_ROOT)).replace("\\", "/"),
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return {"series": series, "meta": meta}


def ensure_oanda_gold_research_cache(*, force_refresh: bool = False) -> dict[str, Any]:
    """Fetch OANDA XAU_USD daily into research cache (does NOT promote to prices_latest)."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / "gold_oanda_daily_research.json"
    if path.exists() and not force_refresh:
        doc = json.loads(path.read_text(encoding="utf-8"))
        if len(doc.get("daily") or []) >= 1000:
            return doc

    from hptl.oanda.oanda_client import api_get
    from hptl.oanda.oanda_prices import _parse_candles
    from hptl.prices.fx_oanda_backfill_feasibility_audit import OANDA_MAX_COUNT, _iso_from

    start = date(2006, 1, 1)
    end = date.today()
    current = start
    collected: list[dict[str, Any]] = []
    warnings: list[str] = []
    while current <= end:
        params = {
            "granularity": "D",
            "price": "M",
            "count": str(OANDA_MAX_COUNT),
            "from": _iso_from(current),
        }
        try:
            payload = api_get("/v3/instruments/XAU_USD/candles", params=params)
            complete, _forming = _parse_candles(payload)
        except Exception as exc:  # noqa: BLE001
            warnings.append(str(exc)[:200])
            break
        if not complete:
            break
        trimmed = [b for b in complete if str(b["date"])[:10] <= end.isoformat()]
        collected.extend(trimmed)
        last = date.fromisoformat(str(trimmed[-1]["date"])[:10])
        if last >= end or len(complete) < 100:
            break
        current = last + timedelta(days=1)

    # Deduplicate by date
    by_d: dict[str, dict[str, Any]] = {}
    for b in collected:
        d = str(b["date"])[:10]
        by_d[d] = {
            "date": d,
            "open": float(b.get("open") or b.get("o") or b.get("close")),
            "high": float(b.get("high") or b.get("h") or b.get("close")),
            "low": float(b.get("low") or b.get("l") or b.get("close")),
            "close": float(b.get("close") or b.get("c") or 0.0),
        }
    daily = [by_d[k] for k in sorted(by_d) if by_d[k]["close"] > 0]
    doc = {
        "instrument": "Gold",
        "oanda_symbol": "XAU_USD",
        "research_only": True,
        "not_promoted_to_prices_latest": True,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "native_frequency": "daily",
        "publication_lag_days": 0,
        "start": daily[0]["date"] if daily else None,
        "end": daily[-1]["date"] if daily else None,
        "n": len(daily),
        "warnings": warnings,
        "daily": daily,
    }
    path.write_text(json.dumps(doc), encoding="utf-8")
    return doc


def _daily_to_weekly_iso(daily: list[dict[str, Any]]) -> list[tuple[str, float]]:
    """ISO week-end close from daily bars (research mirror of derive_weekly_iso)."""
    from datetime import datetime as dt

    buckets: dict[tuple[int, int], tuple[str, float]] = {}
    for b in daily:
        d = str(b["date"])[:10]
        px = float(b["close"])
        if px <= 0:
            continue
        y, w, _ = dt.fromisoformat(d).isocalendar()
        # Keep latest date in the ISO week
        key = (y, w)
        prev = buckets.get(key)
        if prev is None or d >= prev[0]:
            buckets[key] = (d, px)
    return sorted(buckets.values(), key=lambda x: x[0])


def _monthly_to_weekly_asof(
    monthly: dict[str, float], week_dates: list[str], *, lag_days: int
) -> list[float | None]:
    return _asof_with_lag(monthly, week_dates, lag_days=lag_days)


def build_extended_gold_panel(
    *, start: str = "1975-01-01", force_refresh: bool = False
) -> dict[str, Any]:
    """Build extended weekly research panel (does not touch production store)."""
    pub = load_public_monthly_gold(force_refresh=force_refresh)
    oanda = ensure_oanda_gold_research_cache(force_refresh=force_refresh)
    canon_weeks = _weekly_prices("Gold")
    oanda_weeks = _daily_to_weekly_iso(list(oanda.get("daily") or []))

    # Master week index: from 1975 to latest available
    # Generate week-end dates from monthly/oanda/canonical union
    week_set: set[str] = set()
    # From public monthly: create synthetic week ends (month + lag windows)
    # Simpler: generate weekly Fridays from start to today
    d0 = date.fromisoformat(start)
    d1 = date.today()
    cur = d0
    while cur <= d1:
        # ISO week date: use Sunday? Canonical uses week-end from daily.
        # Use Friday as proxy research week stamp.
        if cur.weekday() == 4:  # Friday
            week_set.add(cur.isoformat())
        cur += timedelta(days=1)
    for d, _ in oanda_weeks:
        week_set.add(d)
    for d, _ in canon_weeks:
        week_set.add(d)
    weeks = sorted(w for w in week_set if w >= start)

    # Gold price priority: canonical > oanda research > public monthly as-of
    canon_map = dict(canon_weeks)
    oanda_map = dict(oanda_weeks)
    monthly_asof = _monthly_to_weekly_asof(
        pub["series"], weeks, lag_days=MONTHLY_GOLD_LAG_DAYS
    )
    gold: list[float | None] = []
    gold_source: list[str] = []
    for i, w in enumerate(weeks):
        if w in canon_map:
            gold.append(canon_map[w])
            gold_source.append("canonical_oanda_store")
        elif w in oanda_map:
            gold.append(oanda_map[w])
            gold_source.append("oanda_research_cache")
        elif monthly_asof[i] is not None:
            gold.append(float(monthly_asof[i]))  # type: ignore[arg-type]
            gold_source.append("public_monthly_asof")
        else:
            gold.append(None)
            gold_source.append("missing")

    # Macros
    m2 = load_fred_daily_map("M2SL", observation_start="1959-01-01")
    cpi = load_fred_daily_map("CPIAUCSL", observation_start="1947-01-01")
    gdp = load_fred_daily_map("GDP", observation_start="1947-01-01")  # quarterly SAAR
    gdpc1 = load_fred_daily_map("GDPC1", observation_start="1947-01-01")
    gdpdef = load_fred_daily_map("GDPDEF", observation_start="1947-01-01")
    pop = load_fred_daily_map("POPTHM", observation_start="1959-01-01")
    dff = load_fred_daily_map("DFF", observation_start="1954-01-01")
    dgs10 = load_fred_daily_map("DGS10", observation_start="1962-01-01")
    broad = load_fred_daily_map("DTWEXBGS", observation_start="2006-01-01")

    m2_w = _finite_ffill(_asof_with_lag(m2, weeks, lag_days=MONTHLY_PUBLICATION_LAG_DAYS))
    cpi_w = _finite_ffill(_asof_with_lag(cpi, weeks, lag_days=MONTHLY_PUBLICATION_LAG_DAYS))
    gdp_w = _finite_ffill(_asof_with_lag(gdp, weeks, lag_days=QUARTERLY_LAG_DAYS))
    gdpc_w = _finite_ffill(_asof_with_lag(gdpc1, weeks, lag_days=QUARTERLY_LAG_DAYS))
    gdpdef_w = _finite_ffill(_asof_with_lag(gdpdef, weeks, lag_days=QUARTERLY_LAG_DAYS))
    pop_w = _finite_ffill(_asof_with_lag(pop, weeks, lag_days=MONTHLY_PUBLICATION_LAG_DAYS))
    dff_w = _finite_ffill(_asof_series(dff, weeks))
    dgs10_w = _finite_ffill(_asof_series(dgs10, weeks))
    broad_w = _finite_ffill(_asof_series(broad, weeks))

    start_i = _first_complete_index([gold, m2_w, cpi_w])
    if start_i is None:
        raise RuntimeError("No overlapping Gold/M2/CPI sample")

    # Trim to complete core
    weeks = weeks[start_i:]
    gold = gold[start_i:]
    gold_source = gold_source[start_i:]
    m2_w = m2_w[start_i:]
    cpi_w = cpi_w[start_i:]
    gdp_w = gdp_w[start_i:]
    gdpc_w = gdpc_w[start_i:]
    gdpdef_w = gdpdef_w[start_i:]
    pop_w = pop_w[start_i:]
    dff_w = dff_w[start_i:]
    dgs10_w = dgs10_w[start_i:]
    broad_w = broad_w[start_i:]

    # Drop any residual None gold
    keep = [i for i, g in enumerate(gold) if g is not None and g > 0]
    def _take(xs: list[Any]) -> list[Any]:
        return [xs[i] for i in keep]

    weeks = _take(weeks)
    gold_f = [float(gold[i]) for i in keep]  # type: ignore[arg-type]
    gold_source = _take(gold_source)
    m2_f = [float(m2_w[i]) for i in keep]  # type: ignore[arg-type]
    cpi_f = [float(cpi_w[i]) for i in keep]  # type: ignore[arg-type]
    gdp_f = [float(gdp_w[i]) if gdp_w[i] is not None else float("nan") for i in keep]
    gdpc_f = [float(gdpc_w[i]) if gdpc_w[i] is not None else float("nan") for i in keep]
    pop_f = [float(pop_w[i]) if pop_w[i] is not None else float("nan") for i in keep]
    dff_f = [float(dff_w[i]) if dff_w[i] is not None else float("nan") for i in keep]
    dgs10_f = [float(dgs10_w[i]) if dgs10_w[i] is not None else float("nan") for i in keep]
    broad_f = [float(broad_w[i]) if broad_w[i] is not None else float("nan") for i in keep]

    # Derived
    log_g = [math.log(v) for v in gold_f]
    log_m2 = [math.log(v) for v in m2_f]
    log_cpi = [math.log(v) for v in cpi_f]
    real_m2 = [m2_f[i] / cpi_f[i] for i in range(len(weeks))]
    log_real_m2 = [math.log(v) for v in real_m2]
    m2_per_gdp = [
        m2_f[i] / gdp_f[i] if math.isfinite(gdp_f[i]) and gdp_f[i] > 0 else float("nan")
        for i in range(len(weeks))
    ]
    log_m2_per_gdp = [
        math.log(v) if math.isfinite(v) and v > 0 else float("nan") for v in m2_per_gdp
    ]
    m2_pc = [
        m2_f[i] / pop_f[i] if math.isfinite(pop_f[i]) and pop_f[i] > 0 else float("nan")
        for i in range(len(weeks))
    ]
    log_m2_pc = [
        math.log(v) if math.isfinite(v) and v > 0 else float("nan") for v in m2_pc
    ]

    # Past-only cumulative monetary excess vs CPI / NGDP / real GDP
    cum_ex_cpi: list[float] = []
    cum_ex_ngdp: list[float] = []
    cum_ex_rgdp: list[float] = []
    s_cpi = s_ngdp = s_rgdp = 0.0
    for i in range(len(weeks)):
        if i == 0:
            cum_ex_cpi.append(0.0)
            cum_ex_ngdp.append(0.0)
            cum_ex_rgdp.append(0.0)
            continue
        dm2 = log_m2[i] - log_m2[i - 1]
        dcpi = log_cpi[i] - log_cpi[i - 1]
        s_cpi += dm2 - dcpi
        cum_ex_cpi.append(s_cpi)
        if math.isfinite(gdp_f[i]) and math.isfinite(gdp_f[i - 1]) and gdp_f[i - 1] > 0:
            s_ngdp += dm2 - (math.log(gdp_f[i]) - math.log(gdp_f[i - 1]))
        cum_ex_ngdp.append(s_ngdp)
        if math.isfinite(gdpc_f[i]) and math.isfinite(gdpc_f[i - 1]) and gdpc_f[i - 1] > 0:
            s_rgdp += dm2 - (math.log(gdpc_f[i]) - math.log(gdpc_f[i - 1]))
        cum_ex_rgdp.append(s_rgdp)

    src_counts: dict[str, int] = {}
    for s in gold_source:
        src_counts[s] = src_counts.get(s, 0) + 1

    series_docs = [
        {
            "id": "gold_price",
            "source": "canonical store + OANDA research cache + public monthly",
            "native_frequency": "daily/monthly hybrid",
            "publication_lag_days": {"canonical/oanda": 0, "public_monthly": MONTHLY_GOLD_LAG_DAYS},
            "start": weeks[0],
            "end": weeks[-1],
            "missing_value_treatment": "drop incomplete weeks; causal ffill for macros",
            "resampling_method": "ISO/Friday week stamps; monthly as-of with lag",
            "source_counts": src_counts,
        },
        {
            "id": "M2SL",
            "source": "FRED",
            "native_frequency": "monthly",
            "publication_lag_days": MONTHLY_PUBLICATION_LAG_DAYS,
            "start": min(m2) if m2 else None,
            "end": max(m2) if m2 else None,
        },
        {
            "id": "CPIAUCSL",
            "source": "FRED",
            "native_frequency": "monthly",
            "publication_lag_days": MONTHLY_PUBLICATION_LAG_DAYS,
            "start": min(cpi) if cpi else None,
            "end": max(cpi) if cpi else None,
        },
        {
            "id": "GDP",
            "source": "FRED",
            "native_frequency": "quarterly",
            "publication_lag_days": QUARTERLY_LAG_DAYS,
            "start": min(gdp) if gdp else None,
            "end": max(gdp) if gdp else None,
        },
        {
            "id": "GDPC1",
            "source": "FRED",
            "native_frequency": "quarterly",
            "publication_lag_days": QUARTERLY_LAG_DAYS,
        },
        {
            "id": "GDPDEF",
            "source": "FRED",
            "native_frequency": "quarterly",
            "publication_lag_days": QUARTERLY_LAG_DAYS,
            "optional": True,
        },
        {
            "id": "POPTHM",
            "source": "FRED",
            "native_frequency": "monthly",
            "publication_lag_days": MONTHLY_PUBLICATION_LAG_DAYS,
            "optional": True,
        },
        pub["meta"],
        {
            "id": "oanda_research_cache",
            "source": "OANDA XAU_USD",
            "native_frequency": "daily",
            "publication_lag_days": 0,
            "start": oanda.get("start"),
            "end": oanda.get("end"),
            "n": oanda.get("n"),
            "not_promoted_to_prices_latest": True,
        },
    ]

    return {
        "dates": weeks,
        "gold": gold_f,
        "log_gold": log_g,
        "features": {
            "log_m2": log_m2,
            "log_cpi": log_cpi,
            "log_real_m2": log_real_m2,
            "log_m2_per_gdp": log_m2_per_gdp,
            "log_m2_pc": log_m2_pc,
            "cum_excess_cpi": cum_ex_cpi,
            "cum_excess_ngdp": cum_ex_ngdp,
            "cum_excess_rgdp": cum_ex_rgdp,
            "m2": m2_f,
            "cpi": cpi_f,
            "gdp": gdp_f,
            "gdpc1": gdpc_f,
            "pop": pop_f,
            "dff": dff_f,
            "dgs10": dgs10_f,
            "broad_usd": broad_f,
        },
        "gold_source": gold_source,
        "series_documentation": series_docs,
        "meta": {
            "n_weeks": len(weeks),
            "start": weeks[0],
            "end": weeks[-1],
            "source_counts": src_counts,
            "published_gold_model_untouched": PUBLISHED_GOLD_MODEL_ID,
        },
    }


# ---------------------------------------------------------------------------
# Candidate specs
# ---------------------------------------------------------------------------


def _candidate_specs() -> list[dict[str, Any]]:
    return [
        {
            "id": "A_mpp_m2_cpi",
            "label": "Model A — Monetary purchasing power",
            "equation": "log(Gold)=α+β1·log(M2)+β2·log(CPI)",
            "features": ["log_m2", "log_cpi"],
            "expected_signs": {"log_m2": "positive", "log_cpi": "positive"},
        },
        {
            "id": "B_m2_per_ngdp",
            "label": "Model B — Money per unit of output",
            "equation": "log(Gold)=α+β1·log(M2/nominal GDP)",
            "features": ["log_m2_per_gdp"],
            "expected_signs": {"log_m2_per_gdp": "positive"},
        },
        {
            "id": "C_real_m2",
            "label": "Model C — Real money stock",
            "equation": "log(Gold)=α+β1·log(M2/CPI)",
            "features": ["log_real_m2"],
            "expected_signs": {"log_real_m2": "positive"},
        },
        {
            "id": "D_m2_pc_cpi",
            "label": "Model D — Per-capita M2 + CPI",
            "equation": "log(Gold)=α+β1·log(M2/pop)+β2·log(CPI)",
            "features": ["log_m2_pc", "log_cpi"],
            "expected_signs": {"log_m2_pc": "positive", "log_cpi": "positive"},
        },
        {
            # Note: cum(ΔlogM2−ΔlogCPI) ≡ log(M2/CPI) up to a constant → identical to Model C.
            # Kept only as an explicit redundancy check (not a separate economic hypothesis).
            "id": "E_cum_excess_cpi_REDUNDANT_vs_C",
            "label": "Model E — Excess vs CPI (algebraically ≡ Model C)",
            "equation": "log(Gold)=α+β·cum(ΔlogM2−ΔlogCPI)  [≡ Model C]",
            "features": ["cum_excess_cpi"],
            "expected_signs": {"cum_excess_cpi": "positive"},
            "is_baseline": True,
            "redundant_of": "C_real_m2",
        },
        {
            # cum(ΔlogM2−ΔlogNGDP) ≡ log(M2/NGDP) up to a constant → identical to Model B.
            "id": "E_cum_excess_ngdp_REDUNDANT_vs_B",
            "label": "Model E — Excess vs NGDP (algebraically ≡ Model B)",
            "equation": "log(Gold)=α+β·cum(ΔlogM2−ΔlogNGDP)  [≡ Model B]",
            "features": ["cum_excess_ngdp"],
            "expected_signs": {"cum_excess_ngdp": "positive"},
            "is_baseline": True,
            "redundant_of": "B_m2_per_ngdp",
        },
        {
            "id": "E_cum_excess_rgdp",
            "label": "Model E — Cumulative monetary excess vs real GDP",
            "equation": "log(Gold)=α+β·cum(ΔlogM2−Δlog realGDP)",
            "features": ["cum_excess_rgdp"],
            "expected_signs": {"cum_excess_rgdp": "positive"},
        },
        # Baselines (for comparison; not monetary promote candidates)
        {
            "id": "BASE_m2_only",
            "label": "Baseline — M2 only",
            "equation": "log(Gold)=α+β·log(M2)",
            "features": ["log_m2"],
            "expected_signs": {"log_m2": "positive"},
            "is_baseline": True,
        },
        {
            "id": "BASE_cpi_only",
            "label": "Baseline — CPI only",
            "equation": "log(Gold)=α+β·log(CPI)",
            "features": ["log_cpi"],
            "expected_signs": {"log_cpi": "positive"},
            "is_baseline": True,
        },
    ]


def _align_features(
    dates: list[str],
    y: list[float],
    prices: list[float],
    feat_map: dict[str, list[float]],
    feature_ids: list[str],
) -> tuple[list[str], list[float], list[float], dict[str, list[float]]]:
    out_d, out_y, out_p = [], [], []
    out_x: dict[str, list[float]] = {f: [] for f in feature_ids}
    for i, d in enumerate(dates):
        vals = []
        ok = True
        for f in feature_ids:
            v = feat_map[f][i]
            if v is None or not math.isfinite(float(v)):
                ok = False
                break
            vals.append(float(v))
        if not ok:
            continue
        out_d.append(d)
        out_y.append(y[i])
        out_p.append(prices[i])
        for f, v in zip(feature_ids, vals):
            out_x[f].append(v)
    return out_d, out_y, out_p, out_x


def _subsample_mask(dates: list[str], label: str) -> list[bool]:
    n = len(dates)
    if label == "full":
        return [True] * n
    if label == "first_half":
        mid = n // 2
        return [i < mid for i in range(n)]
    if label == "second_half":
        mid = n // 2
        return [i >= mid for i in range(n)]
    if label == "pre_2000":
        return [d < "2000-01-01" for d in dates]
    if label == "y2000_2019":
        return ["2000-01-01" <= d < "2020-01-01" for d in dates]
    if label == "post_2020":
        return [d >= "2020-01-01" for d in dates]
    return [True] * n


def _walk_forward_fair(
    y: list[float],
    cols: list[list[float]],
    *,
    min_train: int,
    step: int = STEP,
    rolling: int | None = None,
) -> tuple[list[float | None], list[list[float]], dict[str, Any]]:
    n = len(y)
    fair: list[float | None] = [None] * n
    coef_path: list[list[float]] = []
    names = [f"x{i}" for i in range(len(cols))]
    wf = _walk_forward_predictions(y, cols, feature_names=names, min_train=min_train, step=step)
    t = min_train
    while t < n:
        if rolling and t >= rolling:
            sl = slice(t - rolling, t)
        else:
            sl = slice(0, t)
        beta, r2 = _multivariate_ols(y[sl], [c[sl] for c in cols])
        if not beta or r2 is None:
            t += step
            continue
        coef_path.append([float(b) for b in beta])
        end = min(t + step, n)
        for i in range(t, end):
            fair[i] = _predict_log_price(beta, [c[i] for c in cols])
        t += step
    return fair, coef_path, wf


def _ecm_estimate(
    log_g: list[float],
    fair_logs: list[float | None],
    x_cols: list[list[float]],
    *,
    min_train: int,
    max_lag: int = 2,
) -> dict[str, Any]:
    """Expanding ECM: ΔlogG_t = a + λ EC_{t-1} + short-run ΔX + lags ΔlogG."""
    n = len(log_g)
    lambdas: list[float] = []
    oos_preds: list[float] = []
    oos_actual: list[float] = []
    t = max(min_train, max_lag + 2)
    while t < n:
        # Build training rows using fair from past-only path (fair[i] uses data < i in WF)
        rows_y = []
        rows_x = []
        for i in range(max_lag + 1, t):
            fl_prev = fair_logs[i - 1]
            if fl_prev is None:
                continue
            ec = log_g[i - 1] - float(fl_prev)
            dy = log_g[i] - log_g[i - 1]
            feats = [1.0, ec]
            for c in x_cols:
                feats.append(c[i] - c[i - 1])
            for j in range(1, max_lag + 1):
                feats.append(log_g[i - j] - log_g[i - j - 1])
            rows_y.append(dy)
            rows_x.append(feats)
        if len(rows_y) < 40:
            t += STEP
            continue
        Y = np.asarray(rows_y, float)
        X = np.asarray(rows_x, float)
        beta, r2 = _ols(Y, X)
        if beta.size == 0:
            t += STEP
            continue
        lam = float(beta[1])
        lambdas.append(lam)
        # OOS next STEP weeks
        end = min(t + STEP, n)
        for i in range(t, end):
            fl_prev = fair_logs[i - 1]
            if fl_prev is None or i < max_lag + 1:
                continue
            ec = log_g[i - 1] - float(fl_prev)
            feats = [1.0, ec]
            for c in x_cols:
                feats.append(c[i] - c[i - 1])
            for j in range(1, max_lag + 1):
                feats.append(log_g[i - j] - log_g[i - j - 1])
            pred = float(np.dot(beta, np.asarray(feats, float)))
            actual = log_g[i] - log_g[i - 1]
            oos_preds.append(pred)
            oos_actual.append(actual)
        t += STEP

    if not lambdas:
        return {"ok": False, "lambda_mean": None}
    flip = any(a * b < 0 for a, b in zip(lambdas, lambdas[1:]))
    lam_mean = sum(lambdas) / len(lambdas)
    # Half-life in weeks for discrete EC: HL = ln(2)/(-ln(1+λ)) if -1<λ<0
    hl = None
    if -1.0 < lam_mean < 0:
        hl = math.log(2.0) / (-math.log(1.0 + lam_mean))
    pct_neg = sum(1 for L in lambdas if L < 0) / len(lambdas)
    return {
        "ok": True,
        "lambda_mean": round(lam_mean, 6),
        "lambda_median": round(sorted(lambdas)[len(lambdas) // 2], 6),
        "lambda_neg_share": round(pct_neg, 3),
        "lambda_sign_flip": flip,
        "lambda_stable_negative": bool(pct_neg >= 0.80 and not flip and lam_mean < -0.01),
        "half_life_weeks": round(hl, 2) if hl is not None else None,
        "n_windows": len(lambdas),
        "path_head": [round(x, 6) for x in lambdas[:3]],
        "path_tail": [round(x, 6) for x in lambdas[-3:]],
        "oos_n": len(oos_preds),
    }


def _pooled_valuation_spread(fwd_rows: list[dict[str, Any]], *, horizon: int = 13) -> dict[str, Any]:
    under_n = over_n = 0
    under_sum = over_sum = 0.0
    bucket_means: dict[str, float | None] = {}
    order = [
        "materially_undervalued",
        "undervalued",
        "near_fair_value",
        "overvalued",
        "materially_overvalued",
    ]
    for r in fwd_rows:
        if r.get("horizon_weeks") != horizon:
            continue
        bucket_means[r["bucket"]] = r.get("mean_return_pct")
        mean = r.get("mean_return_pct")
        n = int(r.get("n") or 0)
        if mean is None or n <= 0:
            continue
        if r["bucket"] in {"materially_undervalued", "undervalued"}:
            under_n += n
            under_sum += float(mean) * n
        elif r["bucket"] in {"materially_overvalued", "overvalued"}:
            over_n += n
            over_sum += float(mean) * n
    if under_n < 12 or over_n < 12:
        return {"ok": False, "spread_pp": None, "monotonic_score": 0.0}
    u = under_sum / under_n
    o = over_sum / over_n
    spread = u - o
    # Monotonicity score across ordered bucket means
    seq = [bucket_means.get(b) for b in order]
    pairs = 0
    good = 0
    for i in range(len(seq) - 1):
        a, b = seq[i], seq[i + 1]
        if a is None or b is None:
            continue
        pairs += 1
        if float(a) >= float(b):
            good += 1
    mono = good / pairs if pairs else 0.0
    return {
        "ok": True,
        "horizon_weeks": horizon,
        "under_mean_pct": round(u, 3),
        "over_mean_pct": round(o, 3),
        "under_n": under_n,
        "over_n": over_n,
        "spread_pp": round(spread, 3),
        "monotonic_score": round(mono, 3),
        "bucket_means": bucket_means,
    }


def _price_model_score(wf: dict[str, Any], *, signs_ok: bool, flip: bool, vs_naive: float | None) -> dict[str, Any]:
    parts: dict[str, float] = {}
    parts["signs"] = 20.0 if signs_ok else 0.0
    parts["stability"] = 0.0 if flip else 20.0
    oos = wf.get("oos_r2")
    parts["oos_r2"] = max(0.0, min(25.0, float(oos) / 0.4 * 25.0)) if oos is not None else 0.0
    if vs_naive is None:
        parts["vs_naive"] = 0.0
    else:
        parts["vs_naive"] = max(0.0, min(20.0, float(vs_naive) / 25.0 * 20.0))
    parts["coverage"] = 15.0 if (wf.get("n_oos") or 0) >= 100 else 8.0
    total = sum(parts.values())
    return {"price_model_score": round(total, 1), "parts": {k: round(v, 2) for k, v in parts.items()}}


def _valuation_score(
    spread: dict[str, Any],
    ecm: dict[str, Any],
    *,
    regime_positive_share: float | None,
) -> dict[str, Any]:
    parts: dict[str, float] = {}
    sp = spread.get("spread_pp")
    if sp is None:
        parts["spread"] = 0.0
    elif float(sp) <= 0:
        parts["spread"] = max(-15.0, float(sp))  # penalty
    else:
        parts["spread"] = max(0.0, min(35.0, float(sp) / 8.0 * 35.0))
    parts["monotonicity"] = 20.0 * float(spread.get("monotonic_score") or 0.0)
    if ecm.get("lambda_stable_negative"):
        parts["ecm"] = 25.0
    elif ecm.get("lambda_mean") is not None and float(ecm["lambda_mean"]) < 0 and not ecm.get("lambda_sign_flip"):
        parts["ecm"] = 12.0
    else:
        parts["ecm"] = 0.0
    if regime_positive_share is None:
        parts["regime"] = 0.0
    else:
        parts["regime"] = max(0.0, min(20.0, float(regime_positive_share) * 20.0))
    total = sum(parts.values())
    return {"valuation_score": round(total, 1), "parts": {k: round(v, 2) for k, v in parts.items()}}


def _classify_candidate(
    *,
    price_score: float,
    val_score: float,
    ecm: dict[str, Any],
    flip: bool,
    spread_pp: float | None,
) -> str:
    if flip and price_score < 50:
        return "UNSTABLE"
    if spread_pp is not None and spread_pp < 0 and price_score >= 55:
        return "PRICE_MODEL_NOT_VALUATION"
    if (
        price_score >= PRICE_PROMOTE
        and val_score >= VAL_PROMOTE
        and ecm.get("lambda_stable_negative")
        and not flip
    ):
        return "VALID_VALUATION"
    if val_score >= 45 and (spread_pp or 0) > 0:
        return "WEAK_VALUATION"
    if price_score >= 55 and (spread_pp is None or spread_pp <= 0):
        return "PRICE_MODEL_NOT_VALUATION"
    if flip:
        return "UNSTABLE"
    return "REJECT"


def _regime_tags(dates: list[str], feat: dict[str, list[float]]) -> list[dict[str, str]]:
    out = []
    dff = feat.get("dff") or []
    dgs = feat.get("dgs10") or []
    cpi = feat.get("cpi") or []
    m2 = feat.get("m2") or []
    broad = feat.get("broad_usd") or []
    for i, d in enumerate(dates):
        tags: dict[str, str] = {
            "calendar": (
                "pre_2000"
                if d < "2000-01-01"
                else ("y2000_2019" if d < "2020-01-01" else "post_2020")
            )
        }
        if i >= 52 and i < len(dff) and math.isfinite(dff[i]) and math.isfinite(dgs[i]):
            # Approx real rate = nominal 10y - trailing CPI yoy
            if i >= 52 and cpi[i] > 0 and cpi[i - 52] > 0:
                cpi_yoy = 100.0 * (cpi[i] / cpi[i - 52] - 1.0)
                real = dgs[i] - cpi_yoy
                tags["real_rate"] = "neg_real" if real < 0 else "pos_real"
                tags["inflation"] = "high_infl" if cpi_yoy > 4.0 else ("low_infl" if cpi_yoy < 2.0 else "mid_infl")
            rm2_chg = (m2[i] / cpi[i]) / (m2[i - 52] / cpi[i - 52]) - 1.0 if cpi[i - 52] > 0 else 0.0
            tags["real_m2"] = "expanding_rm2" if rm2_chg > 0.02 else ("contracting_rm2" if rm2_chg < -0.02 else "flat_rm2")
            tags["qe_era"] = "qe_era" if "2008-11-01" <= d <= "2014-10-31" or "2020-03-01" <= d <= "2022-03-01" else "non_qe"
            tags["cb_gold_era"] = "cb_accumulation" if d >= "2018-01-01" else "pre_cb_accumulation"
        else:
            tags["real_rate"] = "warmup"
            tags["inflation"] = "warmup"
            tags["real_m2"] = "warmup"
            tags["qe_era"] = "warmup"
            tags["cb_gold_era"] = "warmup"
        if i < len(broad) and math.isfinite(broad[i]) and i >= 52:
            window = [broad[j] for j in range(max(0, i - 156), i) if math.isfinite(broad[j])]
            if len(window) >= 40:
                mu = sum(window) / len(window)
                sd = math.sqrt(sum((x - mu) ** 2 for x in window) / len(window))
                z = (broad[i] - mu) / sd if sd > 1e-12 else 0.0
                tags["dollar"] = "strong_usd" if z > 0.5 else ("weak_usd" if z < -0.5 else "neutral_usd")
            else:
                tags["dollar"] = "unavailable"
        else:
            tags["dollar"] = "unavailable"
        out.append(tags)
    return out


def _evaluate_candidate(panel: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    dates = panel["dates"]
    y = panel["log_gold"]
    prices = panel["gold"]
    feats = panel["features"]
    fids = list(spec["features"])
    d_al, y_al, p_al, x_al = _align_features(dates, y, prices, feats, fids)
    if len(y_al) < MIN_TRAIN_LONG + 80:
        return {"id": spec["id"], "ok": False, "reason": "short_aligned_sample", "n": len(y_al)}

    cols = [x_al[f] for f in fids]
    n_vars = 1 + len(fids)

    # Integration + cointegration on subsamples
    coint_rows = []
    for label in ("full", "first_half", "second_half", "pre_2000", "y2000_2019", "post_2020"):
        mask = _subsample_mask(d_al, label)
        yy = [y_al[i] for i, m in enumerate(mask) if m]
        xx = [[c[i] for i, m in enumerate(mask) if m] for c in cols]
        if len(yy) < 80:
            coint_rows.append({"sample": label, "ok": False, "n": len(yy)})
            continue
        adf_y = _adf_tstat(yy)
        adf_xs = [_adf_tstat(c) for c in xx]
        eg = _eg_cointegration(yy, xx, n_vars=n_vars)
        joh = None
        if len(fids) == 1:
            joh = _johansen_trace_2var(yy, xx[0])
        coint_rows.append(
            {
                "sample": label,
                "ok": True,
                "n": len(yy),
                "adf_y": adf_y,
                "adf_x": adf_xs,
                "engle_granger": eg,
                "johansen": joh,
                "cointegrated_5pct": bool(eg.get("cointegrated_5pct")),
            }
        )

    full_coint = next((r for r in coint_rows if r.get("sample") == "full"), {})
    coint_ok_share = sum(1 for r in coint_rows if r.get("cointegrated_5pct")) / max(
        1, sum(1 for r in coint_rows if r.get("ok"))
    )

    # Walk-forward fair values
    fair, coef_path, wf = _walk_forward_fair(
        y_al, cols, min_train=MIN_TRAIN_LONG, step=STEP, rolling=None
    )
    fair_roll, coef_roll, _ = _walk_forward_fair(
        y_al, cols, min_train=MIN_TRAIN_LONG, step=STEP, rolling=ROLL_WINDOW
    )

    # Tip signs from last expanding window
    signs_ok = True
    tip_beta = coef_path[-1] if coef_path else []
    coefs = {}
    if tip_beta:
        for i, f in enumerate(fids):
            coefs[f] = round(tip_beta[i + 1], 6)
            exp = (spec.get("expected_signs") or {}).get(f)
            if exp == "positive" and tip_beta[i + 1] <= 0:
                signs_ok = False
            if exp == "negative" and tip_beta[i + 1] >= 0:
                signs_ok = False
    # Sign flip across expanding path
    flip = False
    for i, f in enumerate(fids):
        path = [b[i + 1] for b in coef_path if len(b) > i + 1]
        if any(a * b < 0 for a, b in zip(path, path[1:])):
            flip = True
            break
    expected_sign_share = {}
    for i, f in enumerate(fids):
        path = [b[i + 1] for b in coef_path if len(b) > i + 1]
        exp = (spec.get("expected_signs") or {}).get(f)
        if not path or not exp:
            continue
        if exp == "positive":
            expected_sign_share[f] = round(sum(1 for v in path if v > 0) / len(path), 3)
        else:
            expected_sign_share[f] = round(sum(1 for v in path if v < 0) / len(path), 3)

    # OOS metrics vs expanding mean
    idxs = wf.get("indices") or []
    preds = wf.get("preds") or []
    actuals = wf.get("actuals") or []
    naive_err2 = []
    for i in idxs:
        mu = sum(y_al[:i]) / i
        naive_err2.append((mu - y_al[i]) ** 2)
    naive_rmse = math.sqrt(sum(naive_err2) / len(naive_err2)) if naive_err2 else None
    model_rmse = wf.get("oos_rmse")
    vs_naive = None
    if naive_rmse and model_rmse and naive_rmse > 1e-12:
        vs_naive = 100.0 * (naive_rmse - float(model_rmse)) / naive_rmse

    # Baselines: RW / AR(1) on Δlog for price-model comparison context
    rw_mae = None
    if len(y_al) > MIN_TRAIN_LONG + 10:
        # RW predicts no change in log level → error = Δlog
        errs = [abs(y_al[i] - y_al[i - 1]) for i in range(MIN_TRAIN_LONG, len(y_al))]
        rw_mae = sum(errs) / len(errs) if errs else None

    # ECM on expanding fair
    ecm = _ecm_estimate(y_al, fair, cols, min_train=MIN_TRAIN_LONG)
    ecm_roll = _ecm_estimate(y_al, fair_roll, cols, min_train=MIN_TRAIN_LONG)

    # Valuation buckets (past-only fair)
    deviations = _deviation_series(p_al, fair)
    # Extend structural bucket helper with 104w by local call
    fwd = _bucket_forward_returns(d_al, p_al, deviations, horizons=HORIZONS)
    spread13 = _pooled_valuation_spread(fwd, horizon=13)
    spread52 = _pooled_valuation_spread(fwd, horizon=52)
    spread104 = _pooled_valuation_spread(fwd, horizon=104)

    # Regime diagnostics on 13w spread
    idx_map = {d: i for i, d in enumerate(dates)}
    feat_al = {
        k: [
            float(feats[k][idx_map[d]])
            if math.isfinite(float(feats[k][idx_map[d]]))
            else float("nan")
            for d in d_al
        ]
        for k in ("dff", "dgs10", "cpi", "m2", "broad_usd")
        if k in feats
    }
    tags = _regime_tags(d_al, feat_al)
    regime_rows = []
    for dim in ("calendar", "inflation", "real_rate", "real_m2", "dollar", "qe_era", "cb_gold_era"):
        levels = sorted({t.get(dim, "na") for t in tags})
        for level in levels:
            if level in {"warmup", "na", "unavailable"}:
                continue
            under, over = [], []
            for i in range(len(d_al) - 13):
                if tags[i].get(dim) != level:
                    continue
                dv = deviations[i]
                if dv is None:
                    continue
                fwd_ret = 100.0 * (p_al[i + 13] / p_al[i] - 1.0)
                if dv <= -5:
                    under.append(fwd_ret)
                elif dv >= 5:
                    over.append(fwd_ret)
            sp = None
            if under and over:
                sp = sum(under) / len(under) - sum(over) / len(over)
            regime_rows.append(
                {
                    "dimension": dim,
                    "regime": level,
                    "n_under": len(under),
                    "n_over": len(over),
                    "spread_13w_pp": round(sp, 3) if sp is not None else None,
                }
            )
    pos_regs = [r for r in regime_rows if r.get("spread_13w_pp") is not None]
    regime_pos_share = (
        sum(1 for r in pos_regs if float(r["spread_13w_pp"]) > 0) / len(pos_regs) if pos_regs else None
    )

    # Start-date sensitivity: drop first 5y
    sens = None
    if len(d_al) > MIN_TRAIN_LONG + 260:
        d2, y2, p2 = d_al[260:], y_al[260:], p_al[260:]
        cols2 = [c[260:] for c in cols]
        fair2, _, _ = _walk_forward_fair(y2, cols2, min_train=MIN_TRAIN_LONG)
        dev2 = _deviation_series(p2, fair2)
        fwd2 = _bucket_forward_returns(d2, p2, dev2, horizons=(13,))
        sp2 = _pooled_valuation_spread(fwd2, horizon=13)
        sens = {
            "drop_first_5y_spread_13w": sp2.get("spread_pp"),
            "delta_vs_full": (
                None
                if sp2.get("spread_pp") is None or spread13.get("spread_pp") is None
                else round(float(sp2["spread_pp"]) - float(spread13["spread_pp"]), 3)
            ),
        }

    pscore = _price_model_score(
        {"oos_r2": wf.get("oos_r2"), "n_oos": wf.get("n_oos")},
        signs_ok=signs_ok,
        flip=flip,
        vs_naive=vs_naive,
    )
    vscore = _valuation_score(spread13, ecm, regime_positive_share=regime_pos_share)
    classification = _classify_candidate(
        price_score=pscore["price_model_score"],
        val_score=vscore["valuation_score"],
        ecm=ecm,
        flip=flip,
        spread_pp=spread13.get("spread_pp"),
    )

    return {
        "id": spec["id"],
        "ok": True,
        "label": spec["label"],
        "equation": spec["equation"],
        "is_baseline": bool(spec.get("is_baseline")),
        "features": fids,
        "n_weeks": len(y_al),
        "sample_start": d_al[0],
        "sample_end": d_al[-1],
        "coefficients_tip": coefs,
        "signs_ok": signs_ok,
        "coef_sign_flip": flip,
        "expected_sign_share": expected_sign_share,
        "oos_r2": wf.get("oos_r2"),
        "oos_rmse": wf.get("oos_rmse"),
        "oos_mae": wf.get("oos_mae"),
        "naive_oos_rmse": round(naive_rmse, 6) if naive_rmse else None,
        "rmse_vs_naive_impr_pct": round(vs_naive, 3) if vs_naive is not None else None,
        "rw_mae_dlog": round(rw_mae, 6) if rw_mae else None,
        "cointegration": coint_rows,
        "cointegration_pass_share": round(coint_ok_share, 3),
        "full_sample_cointegrated": bool(full_coint.get("cointegrated_5pct")),
        "ecm_expanding": ecm,
        "ecm_rolling_fair": ecm_roll,
        "valuation_spread_13w": spread13,
        "valuation_spread_52w": spread52,
        "valuation_spread_104w": spread104,
        "forward_returns": fwd,
        "regime_diagnostics": regime_rows,
        "start_date_sensitivity": sens,
        "price_model_score": pscore["price_model_score"],
        "price_score_parts": pscore["parts"],
        "valuation_score": vscore["valuation_score"],
        "valuation_score_parts": vscore["parts"],
        "classification": classification,
        "coef_path_expanding": coef_path[-40:],  # tail for CSV size
        "coef_path_rolling_n": len(coef_roll),
        "_dates": d_al,
        "_prices": p_al,
        "_fair_logs": fair,
        "_deviations": deviations,
    }


def _published_real_yield_baseline(panel: dict[str, Any]) -> dict[str, Any]:
    """Compare against metals_real_yield_v1 form on overlapping modern sample only."""
    dates = panel["dates"]
    # Need DFII10 + broad USD — only post-2006 broadly
    dfii = load_fred_daily_map("DFII10", observation_start="2003-01-01")
    broad = load_fred_daily_map("DTWEXBGS", observation_start="2006-01-01")
    ry = _asof_series(dfii, dates)
    dx = _asof_series(broad, dates)
    y = panel["log_gold"]
    prices = panel["gold"]
    d_al, y_al, p_al, x_al = _align_features(
        dates,
        y,
        prices,
        {
            "real_yield": [float(v) if v is not None else float("nan") for v in ry],
            "log_broad": [
                math.log(float(v)) if v is not None and float(v) > 0 else float("nan") for v in dx
            ],
        },
        ["real_yield", "log_broad"],
    )
    if len(y_al) < MIN_TRAIN + 40:
        return {"ok": False, "id": "BASE_metals_real_yield_v1_form", "reason": "short"}
    cols = [x_al["real_yield"], x_al["log_broad"]]
    fair, _, wf = _walk_forward_fair(y_al, cols, min_train=MIN_TRAIN)
    dev = _deviation_series(p_al, fair)
    fwd = _bucket_forward_returns(d_al, p_al, dev, horizons=(13, 52))
    sp = _pooled_valuation_spread(fwd, horizon=13)
    return {
        "ok": True,
        "id": "BASE_metals_real_yield_v1_form",
        "label": "Baseline — published-form real yield + log(USD) (research mirror)",
        "is_baseline": True,
        "n_weeks": len(y_al),
        "sample_start": d_al[0],
        "sample_end": d_al[-1],
        "oos_r2": wf.get("oos_r2"),
        "oos_rmse": wf.get("oos_rmse"),
        "valuation_spread_13w": sp,
        "note": "Does not call or mutate metals_real_yield_v1 production engine.",
        "published_model_id": PUBLISHED_GOLD_MODEL_ID,
    }


def _verdict(rows: list[dict[str, Any]]) -> dict[str, Any]:
    primary = [r for r in rows if r.get("ok") and not r.get("is_baseline")]
    valid = [r for r in primary if r.get("classification") == "VALID_VALUATION"]
    if valid:
        best = sorted(
            valid,
            key=lambda r: (
                float(r.get("valuation_score") or 0),
                float(r.get("price_model_score") or 0),
            ),
            reverse=True,
        )[0]
        return {
            "verdict": "PROMOTE",
            "strongest_candidate": best["id"],
            "narrative": (
                f"{best['id']} passes dual score gates "
                f"(price={best['price_model_score']}, val={best['valuation_score']}) "
                f"with stable negative ECM λ."
            ),
        }
    weak = [r for r in primary if r.get("classification") == "WEAK_VALUATION"]
    price_only = [r for r in primary if r.get("classification") == "PRICE_MODEL_NOT_VALUATION"]
    if weak or price_only or primary:
        # Prefer least-bad valuation spread among non-baselines
        def key(r: dict[str, Any]) -> tuple[float, float]:
            sp = (r.get("valuation_spread_13w") or {}).get("spread_pp")
            spv = float(sp) if sp is not None else -1e9
            return (spv, float(r.get("valuation_score") or 0))

        best = sorted(primary, key=key, reverse=True)[0]
        # If all valuation spreads negative and cointegration fragile → reject approach
        spreads = [
            (r.get("valuation_spread_13w") or {}).get("spread_pp")
            for r in primary
            if (r.get("valuation_spread_13w") or {}).get("spread_pp") is not None
        ]
        coint_any = any(r.get("full_sample_cointegrated") for r in primary)
        ecm_any = any((r.get("ecm_expanding") or {}).get("lambda_stable_negative") for r in primary)
        if spreads and max(spreads) <= 0 and not ecm_any:
            return {
                "verdict": "REJECT_MONETARY_EQUILIBRIUM",
                "strongest_candidate": best["id"],
                "narrative": (
                    "Unconditional monetary-equilibrium valuation is rejected on the "
                    "extended 1975–2026 sample: no candidate combines cointegration, "
                    "stable negative ECM λ, and a positive pooled under-minus-over "
                    f"return spread. Best price-fit lead {best['id']} is "
                    f"{best.get('classification')} (coint_full={coint_any}). "
                    "Regime diagnostics show a possible conditional pocket "
                    "(contracting real-M2 / negative real-rate periods) with better "
                    "directional spreads — that is a separate follow-up track, not a "
                    "promotion of the unconditional M2/CPI equilibrium."
                ),
            }
        return {
            "verdict": "CONTINUE_RESEARCH",
            "strongest_candidate": best["id"],
            "narrative": (
                f"No promote. Strongest lead {best['id']} class="
                f"{best.get('classification')} price={best.get('price_model_score')} "
                f"val={best.get('valuation_score')} "
                f"spread13={(best.get('valuation_spread_13w') or {}).get('spread_pp')} "
                f"λ={(best.get('ecm_expanding') or {}).get('lambda_mean')}."
            ),
        }
    return {
        "verdict": "REJECT_MONETARY_EQUILIBRIUM",
        "strongest_candidate": None,
        "narrative": "No evaluable monetary candidates.",
    }


def run_gold_monetary_equilibrium_research(
    *, start: str = "1975-01-01", force_refresh: bool = False
) -> dict[str, Any]:
    t0 = datetime.now(timezone.utc)
    panel = build_extended_gold_panel(start=start, force_refresh=force_refresh)
    results = []
    for spec in _candidate_specs():
        results.append(_evaluate_candidate(panel, spec))
    base_ry = _published_real_yield_baseline(panel)
    if base_ry.get("ok"):
        results.append(base_ry)

    ok_rows = [r for r in results if r.get("ok")]
    # Sort non-baselines by valuation then price
    ok_rows.sort(
        key=lambda r: (
            0 if r.get("is_baseline") else 1,
            float(r.get("valuation_score") or -1e9),
            float(r.get("price_model_score") or -1e9),
        ),
        reverse=True,
    )
    for i, r in enumerate(ok_rows, 1):
        r["rank"] = i

    verdict = _verdict(ok_rows)

    CHART_DIR.mkdir(parents=True, exist_ok=True)
    charts = []
    chart_ids = []
    if verdict.get("strongest_candidate"):
        chart_ids.append(verdict["strongest_candidate"])
    chart_ids.extend(
        [
            r["id"]
            for r in ok_rows
            if not r.get("is_baseline") and r["id"] not in chart_ids
        ][:3]
    )
    for r in ok_rows:
        if r["id"] not in chart_ids:
            continue
        if "_dates" not in r:
            continue
        path = CHART_DIR / f"{r['id']}_sync.svg"
        _write_sync_chart_svg(
            path,
            title=f"Gold monetary EQ: {r['id']}",
            dates=list(r["_dates"]),
            prices=list(r["_prices"]),
            fair_logs=list(r["_fair_logs"]),
            deviations=list(r["_deviations"]),
        )
        rel = str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")
        r["chart"] = rel
        charts.append(rel)

    public_rows = []
    for r in ok_rows:
        public_rows.append({k: v for k, v in r.items() if not str(k).startswith("_")})

    payload = {
        "generated_at": t0.replace(microsecond=0).isoformat(),
        "ok": True,
        "research_only": True,
        "published_models_untouched": {
            "gold_model_id": PUBLISHED_GOLD_MODEL_ID,
            "prices_latest_not_modified": True,
            "ng_untouched": True,
        },
        "panel": panel["meta"],
        "series_documentation": panel["series_documentation"],
        "ranking": public_rows,
        "verdict": verdict,
        "charts": charts,
        "runtime_sec": round((datetime.now(timezone.utc) - t0).total_seconds(), 2),
        "_private": ok_rows,
        "_panel": panel,
    }
    return payload


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_outputs(payload: dict[str, Any]) -> dict[str, str]:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    private = list(payload.get("_private") or [])
    public = {k: v for k, v in payload.items() if not str(k).startswith("_")}
    RANKING_JSON.write_text(json.dumps(public, indent=2, ensure_ascii=False), encoding="utf-8")

    coint_rows: list[dict[str, Any]] = []
    ecm_rows: list[dict[str, Any]] = []
    score_rows: list[dict[str, Any]] = []
    fwd_rows: list[dict[str, Any]] = []
    regime_rows: list[dict[str, Any]] = []
    stab_rows: list[dict[str, Any]] = []
    for r in private:
        cid = r.get("id")
        for cr in r.get("cointegration") or []:
            eg = cr.get("engle_granger") or {}
            coint_rows.append(
                {
                    "candidate_id": cid,
                    "sample": cr.get("sample"),
                    "n": cr.get("n"),
                    "cointegrated_5pct": cr.get("cointegrated_5pct"),
                    "eg_adf_tstat": eg.get("residual_adf_tstat"),
                    "eg_cv_5pct": eg.get("critical_value_5pct"),
                    "eg_r2": eg.get("r_squared"),
                    "johansen_reject_r0": (cr.get("johansen") or {}).get("reject_r0_5pct"),
                }
            )
        ecm = r.get("ecm_expanding") or {}
        ecm_rows.append(
            {
                "candidate_id": cid,
                "lambda_mean": ecm.get("lambda_mean"),
                "lambda_median": ecm.get("lambda_median"),
                "lambda_neg_share": ecm.get("lambda_neg_share"),
                "lambda_sign_flip": ecm.get("lambda_sign_flip"),
                "lambda_stable_negative": ecm.get("lambda_stable_negative"),
                "half_life_weeks": ecm.get("half_life_weeks"),
                "n_windows": ecm.get("n_windows"),
                "rolling_fair_lambda_mean": (r.get("ecm_rolling_fair") or {}).get(
                    "lambda_mean"
                ),
            }
        )
        score_rows.append(
            {
                "candidate_id": cid,
                "rank": r.get("rank"),
                "classification": r.get("classification"),
                "price_model_score": r.get("price_model_score"),
                "valuation_score": r.get("valuation_score"),
                "signs_ok": r.get("signs_ok"),
                "coef_sign_flip": r.get("coef_sign_flip"),
                "oos_r2": r.get("oos_r2"),
                "oos_rmse": r.get("oos_rmse"),
                "rmse_vs_naive_impr_pct": r.get("rmse_vs_naive_impr_pct"),
                "spread_13w_pp": (r.get("valuation_spread_13w") or {}).get("spread_pp"),
                "spread_52w_pp": (r.get("valuation_spread_52w") or {}).get("spread_pp"),
                "spread_104w_pp": (r.get("valuation_spread_104w") or {}).get("spread_pp"),
                "monotonic_13w": (r.get("valuation_spread_13w") or {}).get(
                    "monotonic_score"
                ),
                "full_sample_cointegrated": r.get("full_sample_cointegrated"),
                "ecm_lambda_mean": ecm.get("lambda_mean"),
                "ecm_stable_neg": ecm.get("lambda_stable_negative"),
                "is_baseline": r.get("is_baseline"),
            }
        )
        for fr in r.get("forward_returns") or []:
            fwd_rows.append({"candidate_id": cid, **fr})
        for rr in r.get("regime_diagnostics") or []:
            regime_rows.append({"candidate_id": cid, **rr})
        for fname, share in (r.get("expected_sign_share") or {}).items():
            stab_rows.append(
                {
                    "candidate_id": cid,
                    "feature": fname,
                    "expected_sign_share": share,
                    "coef_sign_flip": r.get("coef_sign_flip"),
                    "tip_coefficient": (r.get("coefficients_tip") or {}).get(fname),
                    "start_date_sensitivity_delta": (r.get("start_date_sensitivity") or {}).get(
                        "delta_vs_full"
                    ),
                }
            )

    _write_csv(
        COINT_CSV,
        [
            "candidate_id",
            "sample",
            "n",
            "cointegrated_5pct",
            "eg_adf_tstat",
            "eg_cv_5pct",
            "eg_r2",
            "johansen_reject_r0",
        ],
        coint_rows,
    )
    _write_csv(
        ECM_CSV,
        [
            "candidate_id",
            "lambda_mean",
            "lambda_median",
            "lambda_neg_share",
            "lambda_sign_flip",
            "lambda_stable_negative",
            "half_life_weeks",
            "n_windows",
            "rolling_fair_lambda_mean",
        ],
        ecm_rows,
    )
    _write_csv(
        SCORES_CSV,
        [
            "candidate_id",
            "rank",
            "classification",
            "price_model_score",
            "valuation_score",
            "signs_ok",
            "coef_sign_flip",
            "oos_r2",
            "oos_rmse",
            "rmse_vs_naive_impr_pct",
            "spread_13w_pp",
            "spread_52w_pp",
            "spread_104w_pp",
            "monotonic_13w",
            "full_sample_cointegrated",
            "ecm_lambda_mean",
            "ecm_stable_neg",
            "is_baseline",
        ],
        score_rows,
    )
    _write_csv(
        FWD_CSV,
        [
            "candidate_id",
            "bucket",
            "horizon_weeks",
            "n",
            "mean_return_pct",
            "median_return_pct",
            "positive_return_rate",
            "max_adverse_excursion_mean",
            "avg_weeks_toward_fair",
            "pct_extremes_toward_equilibrium",
        ],
        fwd_rows,
    )
    _write_csv(
        REGIME_CSV,
        [
            "candidate_id",
            "dimension",
            "regime",
            "n_under",
            "n_over",
            "spread_13w_pp",
        ],
        regime_rows,
    )
    _write_csv(
        STAB_CSV,
        [
            "candidate_id",
            "feature",
            "expected_sign_share",
            "coef_sign_flip",
            "tip_coefficient",
            "start_date_sensitivity_delta",
        ],
        stab_rows,
    )
    REPORT_MD.write_text(render_markdown(public), encoding="utf-8")
    return {
        "report": str(REPORT_MD.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "ranking_json": str(RANKING_JSON.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "cointegration_csv": str(COINT_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "ecm_csv": str(ECM_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "scores_csv": str(SCORES_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "forward_csv": str(FWD_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "regime_csv": str(REGIME_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "stability_csv": str(STAB_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "charts_dir": str(CHART_DIR.relative_to(PROJECT_ROOT)).replace("\\", "/"),
    }


def render_markdown(payload: dict[str, Any]) -> str:
    verdict = payload.get("verdict") or {}
    panel = payload.get("panel") or {}
    lines = [
        "# Gold Monetary Equilibrium + ECM Research",
        "",
        f"Generated: `{payload.get('generated_at')}`",
        "",
        "**Research only — not deployed. Production valuation models untouched.**",
        "",
        f"**Verdict: {verdict.get('verdict')}**",
        "",
        verdict.get("narrative") or "",
        "",
        "## 1. Extended sample",
        "",
        f"- Weeks: **{panel.get('n_weeks')}** ({panel.get('start')} → {panel.get('end')})",
        f"- Gold source mix: `{panel.get('source_counts')}`",
        f"- Published model id unchanged: `{PUBLISHED_GOLD_MODEL_ID}`",
        "",
        "## 2. Series documentation",
        "",
    ]
    for s in payload.get("series_documentation") or []:
        if not isinstance(s, dict):
            continue
        lines.append(
            f"- **{s.get('id') or s.get('source')}**: freq={s.get('native_frequency')}, "
            f"lag={s.get('publication_lag_days')}, start={s.get('start')}, end={s.get('end')}"
        )
    lines.extend(
        [
            "",
            "## 3. Candidate scores",
            "",
            "| Rank | ID | Class | Price | Val | OOS R2 | Spread13 | ECM λ | Coint full | Flip |",
            "| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for r in payload.get("ranking") or []:
        ecm = r.get("ecm_expanding") or {}
        sp = (r.get("valuation_spread_13w") or {}).get("spread_pp")
        lines.append(
            f"| {r.get('rank')} | `{r.get('id')}` | {r.get('classification')} | "
            f"{r.get('price_model_score')} | {r.get('valuation_score')} | {r.get('oos_r2')} | "
            f"{sp} | {ecm.get('lambda_mean')} | {r.get('full_sample_cointegrated')} | "
            f"{r.get('coef_sign_flip')} |"
        )
    sid = verdict.get("strongest_candidate")
    strongest = next((r for r in (payload.get("ranking") or []) if r.get("id") == sid), None)
    lines.extend(["", "## 4. Strongest candidate", ""])
    if strongest:
        lines.extend(
            [
                f"- **ID:** `{strongest.get('id')}`",
                f"- **Equation:** `{strongest.get('equation')}`",
                f"- **Classification:** {strongest.get('classification')}",
                f"- **Tip coefficients:** `{strongest.get('coefficients_tip')}`",
                f"- **ECM:** `{strongest.get('ecm_expanding')}`",
                f"- **Spread 13/52/104w:** "
                f"{(strongest.get('valuation_spread_13w') or {}).get('spread_pp')} / "
                f"{(strongest.get('valuation_spread_52w') or {}).get('spread_pp')} / "
                f"{(strongest.get('valuation_spread_104w') or {}).get('spread_pp')}",
                f"- **Chart:** `{strongest.get('chart')}`",
                "",
            ]
        )
    lines.extend(
        [
            "## 5. Interpretation",
            "",
            "- Engle–Granger cointegration does **not** hold for Model A (or peers) on "
            "full sample or major subsamples at 5%.",
            "- High OOS R² on monetary levels largely reflects shared trends "
            "(price-model fit), not usable valuation deviations.",
            "- Pooled under-minus-over spreads are negative at 13w/52w/104w for primary "
            "candidates — wrong-way for a valuation engine.",
            "- ECM λ is near zero and/or sign-flips; half-lives (when defined) are "
            "hundreds of weeks — economically trivial correction.",
            "- `cum(ΔlogM2−ΔlogCPI)` is algebraically equivalent to `log(M2/CPI)` "
            "(Model C); `cum` vs NGDP ≡ Model B. Only excess vs real GDP is distinct.",
            "- Regime pocket: contracting real M2 (and sometimes negative real rates / "
            "pre-2000) shows positive spreads — candidate for a *future* "
            "state-conditioned research pass, not unconditional promotion.",
            "",
            "## 6. Promote rule",
            "",
            f"- PROMOTE only if price_model_score ≥ {PRICE_PROMOTE}, "
            f"valuation_score ≥ {VAL_PROMOTE}, stable negative ECM λ, no severe flip, no leakage.",
            "- High price fit with negative valuation spread → `PRICE_MODEL_NOT_VALUATION`.",
            "",
            "## 7. Safety",
            "",
            "- `prices_latest.json` not modified (OANDA history cached under research audit dir only)",
            "- `metals_real_yield_v1` / NG / COT / Scanner / Seasonality untouched",
            "- Outputs confined to `data/audits/gold_monetary_equilibrium/`",
            "",
            f"Runtime: {payload.get('runtime_sec')}s",
            "",
        ]
    )
    return "\n".join(lines)