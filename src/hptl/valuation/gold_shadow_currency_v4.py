"""Gold V4 — Fixed-Form Shadow Currency Engine (research only).

FV_t = k_t × (M_global,t / G_above,t) × exp(-β_t × Y_real,t) × (DXY_bench,t / DXY_t)

Estimates only k_t and β_t (sign-constrained). No feature search, COT, ETF,
physicals, NG variables, or production changes.

Does NOT modify metals_real_yield_v1, NG valuation, COT, Scanner, Seasonality,
canonical price stores, or production endpoints.
"""

from __future__ import annotations

import csv
import io
import json
import math
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
from scipy.optimize import lsq_linear

from hptl.config import PROJECT_ROOT
from hptl.fx.fx_macro_history import load_fred_daily_map
from hptl.valuation.gold_focused_macro_valuation import (
    _build_gold_weekly,
    _forward_bucket_stats,
    _pooled_spread,
)
from hptl.valuation.gold_global_liquidity_valuation import (
    TROY_OZ_PER_TONNE,
    _annual_to_weekly_carry,
    _build_above_ground_annual,
    _improved_real_yield,
    _month_end_iso,
)
from hptl.valuation.gold_macro_tier1_discovery import _asof_series, _load_dx_daily
from hptl.valuation.gold_structural_valuation_research import (
    _classify_deviation,
    _finite_ffill,
)
from hptl.valuation.metals_valuation_v1 import MODEL_ID as PUBLISHED_GOLD_MODEL_ID

AUDIT_DIR = PROJECT_ROOT / "data" / "audits" / "gold_shadow_currency_v4"
CHART_DIR = AUDIT_DIR / "charts"
CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "gold_shadow_currency_v4"
REPORT_MD = AUDIT_DIR / "gold_shadow_currency_report.md"
HISTORY_CSV = AUDIT_DIR / "gold_shadow_currency_history.csv"
PARAM_CSV = AUDIT_DIR / "gold_shadow_currency_parameters.csv"
FWD_CSV = AUDIT_DIR / "gold_shadow_currency_forward_returns.csv"
EPISODE_CSV = AUDIT_DIR / "gold_shadow_currency_episodes.csv"
RANK_JSON = AUDIT_DIR / "gold_shadow_currency_ranking.json"

MODEL_ID = "gold_shadow_currency_v4"
HORIZONS = (13, 26, 52, 104)
CALIB_MONTHS = 15 * 12  # 15-year rolling window
MIN_CALIB_OBS = 120
REFIT_MONTHS = 3  # quarterly parameter refresh
DXY_BENCH_WEEKS = 520  # trailing 10y geometric mean
M2_PUB_LAG_DAYS = 45

# External claims (NOT used as model controls; reported for comparison only).
EXTERNAL_CLAIMS = {"k_range_claim": (0.02, 0.05), "beta_claim_high": 18.5, "beta_claim_low": (0.05, 0.08)}

# China M2 (CNY, absolute) year-end / selected prints after FRED IFS ends (2019-08).
# Sources: PBoC / NBS public money-supply releases (research bootstrap).
CHINA_M2_CNY_EXTENSION: dict[str, float] = {
    "2019-12-31": 198_648_800_000_000.0,
    "2020-12-31": 218_679_900_000_000.0,
    "2021-12-31": 238_289_900_000_000.0,
    "2022-12-31": 266_432_100_000_000.0,
    "2023-12-31": 292_270_000_000_000.0,
    "2024-06-30": 305_300_000_000_000.0,
    "2024-12-31": 320_500_000_000_000.0,
    "2025-06-30": 330_800_000_000_000.0,
    "2025-12-31": 352_600_000_000_000.0,
    "2026-06-30": 356_710_000_000_000.0,
}


# ---------------------------------------------------------------------------
# Dates / HTTP
# ---------------------------------------------------------------------------


def _parse_iso(d: str) -> date:
    return date.fromisoformat(str(d)[:10])


def _add_days(iso: str, days: int) -> str:
    return (_parse_iso(iso) + timedelta(days=days)).isoformat()


def _http_get(url: str, timeout: int = 60) -> bytes:
    req = urllib.request.Request(
        url, headers={"User-Agent": "HPTL-research/1.0", "Accept": "text/csv,application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _month_ends(start: str, end: str) -> list[str]:
    d0 = _parse_iso(start).replace(day=1)
    d1 = _parse_iso(end)
    out: list[str] = []
    y, m = d0.year, d0.month
    while True:
        if m == 12:
            me = date(y, 12, 31)
            y, m = y + 1, 1
        else:
            me = date(y, m + 1, 1) - timedelta(days=1)
            m += 1
        if me > d1:
            break
        if me >= _parse_iso(start):
            out.append(me.isoformat())
    return out


# ---------------------------------------------------------------------------
# Global M2 (USD)
# ---------------------------------------------------------------------------


def _cache_json(path: Path, builder: Any) -> dict[str, Any]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    doc = builder()
    path.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    return doc


def _load_ecb_m2_eur() -> dict[str, float]:
    """Euro-area M2 stocks (EUR). ECB BSI series; values are millions of EUR."""

    def build() -> dict[str, Any]:
        url = (
            "https://data-api.ecb.europa.eu/service/data/BSI/"
            "M.U2.Y.V.M20.X.1.U2.2300.Z01.E?format=csvdata&startPeriod=1999-01"
        )
        raw = _http_get(url).decode("utf-8", "replace")
        series: dict[str, float] = {}
        for row in csv.DictReader(io.StringIO(raw)):
            tp = (row.get("TIME_PERIOD") or "").strip()
            ov = row.get("OBS_VALUE")
            if not tp or ov in (None, ""):
                continue
            # OBS in millions of EUR → absolute EUR
            series[_month_end_iso(tp)] = float(ov) * 1_000_000.0
        return {
            "source": "ECB BSI M.U2.Y.V.M20.X.1.U2.2300.Z01.E",
            "unit": "EUR absolute (from millions)",
            "series": series,
            "fetched_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }

    doc = _cache_json(CACHE_DIR / "ecb_m2_eur.json", build)
    return {str(k): float(v) for k, v in (doc.get("series") or {}).items()}


def _load_japan_m2_jpy() -> dict[str, float]:
    """Japan M2 in JPY. Prefer long FRED OECD series, splice ECB RTD for tip."""

    def build() -> dict[str, Any]:
        from hptl.macro import fred_client

        series: dict[str, float] = {}
        # OECD/FRED broad money Japan (national currency units)
        try:
            df = fred_client.get_series_df("MABMM301JPM189S", "1980-01-01")
            for _, row in df.iterrows():
                d = str(row["date"])[:10]
                series[d] = float(row["value"])
        except Exception:
            pass
        # ECB RTD (UNIT_MULT=9 → billions JPY)
        url = (
            "https://data-api.ecb.europa.eu/service/data/RTD/"
            "M.JP.Y.M_M2.J?format=csvdata&startPeriod=2000-01"
        )
        try:
            raw = _http_get(url).decode("utf-8", "replace")
            for row in csv.DictReader(io.StringIO(raw)):
                tp = (row.get("TIME_PERIOD") or "").strip()
                ov = row.get("OBS_VALUE")
                if not tp or ov in (None, ""):
                    continue
                series[_month_end_iso(tp)] = float(ov) * 1_000_000_000.0
        except Exception:
            pass
        return {
            "source": "FRED MABMM301JPM189S + ECB RTD M.JP.Y.M_M2.J",
            "unit": "JPY absolute",
            "series": series,
            "fetched_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }

    doc = _cache_json(CACHE_DIR / "japan_m2_jpy.json", build)
    return {str(k): float(v) for k, v in (doc.get("series") or {}).items()}


def _load_china_m2_cny() -> dict[str, float]:
    """China M2 in CNY. FRED IFS through ~2019, then documented PBoC extension."""

    def build() -> dict[str, Any]:
        from hptl.macro import fred_client

        series: dict[str, float] = {}
        try:
            df = fred_client.get_series_df("MYAGM2CNM189N", "1998-01-01")
            for _, row in df.iterrows():
                d = str(row["date"])[:10]
                series[d] = float(row["value"])
        except Exception:
            pass
        for d, v in CHINA_M2_CNY_EXTENSION.items():
            series[d] = float(v)
        return {
            "source": "FRED MYAGM2CNM189N + PBoC/NBS documented extension (research)",
            "unit": "CNY absolute",
            "series": series,
            "extension_note": (
                "FRED IFS China M2 ends Aug-2019; later prints are documented "
                "public PBoC money-supply levels with publication lag applied at use."
            ),
            "fetched_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }

    doc = _cache_json(CACHE_DIR / "china_m2_cny.json", build)
    return {str(k): float(v) for k, v in (doc.get("series") or {}).items()}


def _fx_asof(fx_daily: dict[str, float], obs_d: str) -> float | None:
    fx = None
    for d, v in sorted(fx_daily.items()):
        if d <= obs_d:
            fx = float(v)
        else:
            break
    return fx


def build_global_m2_usd_monthly() -> tuple[dict[str, float], dict[str, Any]]:
    """US+EA+JP+CN M2 converted to USD with past-only FX and publication lag."""
    us = load_fred_daily_map("M2SL", observation_start="1980-01-01")  # billions USD
    ea = _load_ecb_m2_eur()
    jp = _load_japan_m2_jpy()
    cn = _load_china_m2_cny()
    dex_eu = load_fred_daily_map("DEXUSEU", observation_start="1990-01-01")
    dex_jp = load_fred_daily_map("DEXJPUS", observation_start="1990-01-01")
    dex_cn = load_fred_daily_map("DEXCHUS", observation_start="1990-01-01")

    # Normalize US to month-end absolute USD
    us_abs: dict[str, float] = {}
    for d, v in us.items():
        us_abs[d] = float(v) * 1_000_000_000.0

    # Component usable maps after lag
    def lag_map(raw: dict[str, float], lag: int) -> dict[str, float]:
        return {_add_days(d, lag): float(v) for d, v in raw.items()}

    us_u = lag_map(us_abs, M2_PUB_LAG_DAYS)
    ea_usd: dict[str, float] = {}
    for obs, eur in ea.items():
        fx = _fx_asof(dex_eu, obs)
        if fx is None or fx <= 0:
            continue
        ea_usd[_add_days(obs, M2_PUB_LAG_DAYS)] = eur * fx
    jp_usd: dict[str, float] = {}
    for obs, jpy in jp.items():
        fx = _fx_asof(dex_jp, obs)
        if fx is None or fx <= 0:
            continue
        jp_usd[_add_days(obs, M2_PUB_LAG_DAYS)] = jpy / fx
    cn_usd: dict[str, float] = {}
    for obs, cny in cn.items():
        fx = _fx_asof(dex_cn, obs)
        if fx is None or fx <= 0:
            continue
        cn_usd[_add_days(obs, M2_PUB_LAG_DAYS)] = cny / fx

    # Union of month-ends across components
    all_dates = sorted(set(us_u) | set(ea_usd) | set(jp_usd) | set(cn_usd))
    out: dict[str, float] = {}
    last = {"us": None, "ea": None, "jp": None, "cn": None}
    for d in all_dates:
        if d in us_u:
            last["us"] = us_u[d]
        if d in ea_usd:
            last["ea"] = ea_usd[d]
        if d in jp_usd:
            last["jp"] = jp_usd[d]
        if d in cn_usd:
            last["cn"] = cn_usd[d]
        if any(last[k] is None for k in last):
            continue
        out[d] = float(last["us"]) + float(last["ea"]) + float(last["jp"]) + float(last["cn"])

    tip = next(reversed(list(out.items())), (None, None))
    meta = {
        "components": ["US M2SL", "EA ECB M2", "Japan M2", "China M2"],
        "fx": {"EUR": "DEXUSEU", "JPY": "DEXJPUS", "CNY": "DEXCHUS"},
        "publication_lag_days": M2_PUB_LAG_DAYS,
        "fx_rule": "contemporaneous FX as-of national M2 observation date (past-only)",
        "n": len(out),
        "start": next(iter(out), None),
        "end": tip[0],
        "tip_usd_tn": round(float(tip[1]) / 1e12, 3) if tip[1] else None,
        "tip_parts_usd_tn": {
            "us": round(float(last["us"]) / 1e12, 3) if last["us"] else None,
            "ea": round(float(last["ea"]) / 1e12, 3) if last["ea"] else None,
            "jp": round(float(last["jp"]) / 1e12, 3) if last["jp"] else None,
            "cn": round(float(last["cn"]) / 1e12, 3) if last["cn"] else None,
        },
    }
    return out, meta


# ---------------------------------------------------------------------------
# Panel
# ---------------------------------------------------------------------------


def build_shadow_panel(*, start: str = "2000-01-01") -> dict[str, Any]:
    """Weekly gold panel with causally aligned shadow-currency inputs."""
    dates, prices, gold_meta = _build_gold_weekly(start=start)
    m2_map, m2_meta = build_global_m2_usd_monthly()
    ag_annual = _build_above_ground_annual()
    ag = _annual_to_weekly_carry(ag_annual, dates)
    m2 = _finite_ffill(_asof_series(m2_map, dates))
    real_pct, real_meta = _improved_real_yield(dates)  # percent units from DFII10
    dx = _load_dx_daily()
    dxy_raw = _finite_ffill(_asof_series(dx, dates))

    # Real yield as decimal
    real_dec: list[float | None] = [
        (float(v) / 100.0) if v is not None and math.isfinite(float(v)) else None
        for v in real_pct
    ]

    # DXY levels as floats for geom mean
    dxy_vals = [float(v) if v is not None else float("nan") for v in dxy_raw]
    dxy_bench: list[float | None] = []
    for i in range(len(dates)):
        # exclude current point: use history through i (inclusive of i via past prints)
        # Spec: past-only trailing 10y geom mean — use values up to i inclusive as available.
        if i + 1 < DXY_BENCH_WEEKS:
            dxy_bench.append(None)
            continue
        chunk = dxy_vals[i + 1 - DXY_BENCH_WEEKS : i + 1]
        if any(not math.isfinite(v) or v <= 0 for v in chunk):
            dxy_bench.append(None)
        else:
            dxy_bench.append(math.exp(sum(math.log(v) for v in chunk) / len(chunk)))

    keep: list[int] = []
    for i in range(len(dates)):
        if (
            m2[i] is not None
            and ag[i] is not None
            and float(ag[i]) > 0  # type: ignore[arg-type]
            and real_dec[i] is not None
            and dxy_raw[i] is not None
            and float(dxy_raw[i]) > 0  # type: ignore[arg-type]
            and dxy_bench[i] is not None
            and prices[i] > 0
        ):
            keep.append(i)

    d2 = [dates[i] for i in keep]
    p2 = [prices[i] for i in keep]
    m2_2 = [float(m2[i]) for i in keep]  # type: ignore[arg-type]
    ag_2 = [float(ag[i]) for i in keep]  # type: ignore[arg-type]
    ry_2 = [float(real_dec[i]) for i in keep]  # type: ignore[arg-type]
    dxy_2 = [float(dxy_raw[i]) for i in keep]  # type: ignore[arg-type]
    bench_2 = [float(dxy_bench[i]) for i in keep]  # type: ignore[arg-type]
    mon_val = [m2_2[j] / ag_2[j] for j in range(len(d2))]
    dxy_factor = [bench_2[j] / dxy_2[j] for j in range(len(d2))]

    return {
        "dates": d2,
        "prices": p2,
        "log_gold": [math.log(p) for p in p2],
        "m2_usd": m2_2,
        "above_ground_oz": ag_2,
        "monetary_value_per_ounce": mon_val,
        "real_yield_decimal": ry_2,
        "dxy": dxy_2,
        "dxy_benchmark": bench_2,
        "dxy_factor": dxy_factor,
        "meta": {
            "gold": gold_meta,
            "global_m2": m2_meta,
            "above_ground": {
                "source": "WGC above-ground stock + USGS mine-production backcast",
                "includes": (
                    "jewellery, bars & coins (incl. ETFs), official holdings, other fabricated"
                ),
                "unit": "troy ounces",
            },
            "real_yield": {**real_meta, "model_units": "decimal (percent/100)"},
            "dxy_benchmark": {
                "definition": "past-only trailing 10-year geometric mean of DXY",
                "window_weeks": DXY_BENCH_WEEKS,
            },
            "n": len(d2),
            "start": d2[0] if d2 else None,
            "end": d2[-1] if d2 else None,
        },
    }


# ---------------------------------------------------------------------------
# Fair value formula + calibration
# ---------------------------------------------------------------------------


def fair_value_components(
    *,
    k: float,
    beta: float,
    monetary_value_per_ounce: float,
    real_yield_decimal: float,
    dxy_benchmark: float,
    dxy: float,
) -> dict[str, float]:
    """Exact shadow-currency decomposition (all inputs scalars)."""
    if k <= 0 or monetary_value_per_ounce <= 0 or dxy <= 0 or dxy_benchmark <= 0:
        raise ValueError("inputs must be positive; k>0")
    base = k * monetary_value_per_ounce
    yield_factor = math.exp(-beta * real_yield_decimal)
    dxy_factor = dxy_benchmark / dxy
    fv = base * yield_factor * dxy_factor
    if not math.isfinite(fv) or fv <= 0:
        raise ValueError("fair value must be finite and positive")
    return {
        "k": k,
        "beta": beta,
        "monetary_value_per_ounce": monetary_value_per_ounce,
        "base_value": base,
        "yield_factor": yield_factor,
        "dxy_factor": dxy_factor,
        "fair_value": fv,
    }


def _calibrate_k_beta(
    y_adj: list[float],
    real_dec: list[float],
) -> tuple[float, float, dict[str, Any]]:
    """Fit y_adj = log(k) - beta * Y_real, with k>0 and beta>=0."""
    n = len(y_adj)
    if n < 8:
        return float("nan"), float("nan"), {"ok": False, "reason": "n<8"}
    # X = [1, -Y_real]; coef = [log(k), beta]; beta >= 0; log(k) free
    X = np.column_stack([np.ones(n), -np.asarray(real_dec, float)])
    yy = np.asarray(y_adj, float)
    lo = np.array([-np.inf, 0.0])
    hi = np.array([np.inf, np.inf])
    pinned_beta = False
    try:
        res = lsq_linear(X, yy, bounds=(lo, hi), method="bvls", max_iter=400)
        log_k = float(res.x[0])
        beta = float(res.x[1])
        pinned_beta = abs(beta) < 1e-12
    except Exception:
        coef, _, _, _ = np.linalg.lstsq(X, yy, rcond=None)
        log_k = float(coef[0])
        beta = max(0.0, float(coef[1]))
        pinned_beta = float(coef[1]) < 0
    k = math.exp(log_k)
    yhat = log_k - beta * np.asarray(real_dec, float)
    ss_res = float(np.sum((yy - yhat) ** 2))
    ss_tot = float(np.sum((yy - yy.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else None
    return k, beta, {
        "ok": True,
        "n": n,
        "r2": round(r2, 4) if r2 is not None else None,
        "pinned_beta_at_zero": pinned_beta,
        "log_k": log_k,
    }


def _month_index(dates: list[str]) -> list[int]:
    """Indices of last observation in each calendar month."""
    last: dict[str, int] = {}
    for i, d in enumerate(dates):
        last[d[:7]] = i
    return [last[k] for k in sorted(last)]


def _run_calibration(
    panel: dict[str, Any],
    *,
    mode: str,
    beta_fixed: float | None = None,
) -> dict[str, Any]:
    """mode: 'rolling' | 'expanding' | 'rolling_k_fixed_beta'."""
    dates = panel["dates"]
    prices = panel["prices"]
    log_g = panel["log_gold"]
    mon = panel["monetary_value_per_ounce"]
    ry = panel["real_yield_decimal"]
    dxy_f = panel["dxy_factor"]
    dxy = panel["dxy"]
    bench = panel["dxy_benchmark"]

    # Adjusted dependent variable for calibration months
    y_adj = [
        log_g[i] - math.log(mon[i]) - math.log(dxy_f[i]) for i in range(len(dates))
    ]
    month_idx = _month_index(dates)
    n_m = len(month_idx)

    fair: list[float | None] = [None] * len(dates)
    history: list[dict[str, Any]] = []
    param_rows: list[dict[str, Any]] = []
    k_path: list[float] = []
    beta_path: list[float] = []
    pinned = 0
    pred_logs: list[float] = []
    actual_logs: list[float] = []

    # First apply month requires MIN_CALIB_OBS prior months (through t-1).
    t = MIN_CALIB_OBS
    while t < n_m:
        train_end = t  # exclusive — params use data through month t-1 only
        if mode in {"rolling", "rolling_k_fixed_beta"}:
            train_start = max(0, train_end - CALIB_MONTHS)
        else:
            train_start = 0
        train_months = month_idx[train_start:train_end]
        if len(train_months) < MIN_CALIB_OBS:
            t += 1
            continue

        y_tr = [y_adj[i] for i in train_months]
        ry_tr = [ry[i] for i in train_months]
        if beta_fixed is None:
            k, beta, meta = _calibrate_k_beta(y_tr, ry_tr)
        else:
            beta = float(beta_fixed)
            k = math.exp(
                sum(y_tr[j] + beta * ry_tr[j] for j in range(len(y_tr))) / len(y_tr)
            )
            meta = {
                "ok": True,
                "n": len(y_tr),
                "r2": None,
                "pinned_beta_at_zero": False,
                "fixed_beta": True,
            }
        if not meta.get("ok") or not math.isfinite(k) or k <= 0 or not math.isfinite(beta):
            t += 1
            continue
        if meta.get("pinned_beta_at_zero"):
            pinned += 1
        k_path.append(k)
        beta_path.append(beta)
        param_rows.append(
            {
                "param_date": dates[month_idx[train_end - 1]],
                "mode": mode,
                "n_train_months": len(train_months),
                "k": round(k, 8),
                "beta": round(beta, 6),
                "r2": meta.get("r2"),
                "pinned_beta_at_zero": bool(meta.get("pinned_beta_at_zero")),
            }
        )

        apply_end = min(t + REFIT_MONTHS, n_m)
        week_start = month_idx[t - 1] + 1  # first week of apply month t
        week_end = month_idx[apply_end - 1]  # last week of final apply month
        for i in range(week_start, week_end + 1):
            comps = fair_value_components(
                k=k,
                beta=beta,
                monetary_value_per_ounce=mon[i],
                real_yield_decimal=ry[i],
                dxy_benchmark=bench[i],
                dxy=dxy[i],
            )
            fv = comps["fair_value"]
            fair[i] = fv
            dev = 100.0 * (prices[i] / fv - 1.0)
            history.append(
                {
                    "date": dates[i],
                    "gold_price": round(prices[i], 3),
                    "fair_value": round(fv, 3),
                    "deviation_pct": round(dev, 3),
                    "bucket": _classify_deviation(dev),
                    "premium_discount": (
                        "Premium" if dev > 0 else "Discount" if dev < 0 else "Fair"
                    ),
                    "k": round(k, 8),
                    "beta": round(beta, 6),
                    "monetary_value_per_ounce": round(mon[i], 6),
                    "base_value": round(comps["base_value"], 3),
                    "yield_factor": round(comps["yield_factor"], 8),
                    "dxy_factor": round(comps["dxy_factor"], 8),
                    "real_yield_decimal": round(ry[i], 6),
                    "dxy": round(dxy[i], 4),
                    "dxy_benchmark": round(bench[i], 4),
                    "m2_usd": round(panel["m2_usd"][i], 3),
                    "above_ground_oz": round(panel["above_ground_oz"][i], 3),
                    "mode": mode,
                }
            )
            pred_logs.append(math.log(fv))
            actual_logs.append(log_g[i])
        t = apply_end

    oos: dict[str, Any] = {"n_oos": len(pred_logs)}
    if len(pred_logs) >= 20:
        err2 = [(p - a) ** 2 for p, a in zip(pred_logs, actual_logs)]
        mae = sum(abs(p - a) for p, a in zip(pred_logs, actual_logs)) / len(pred_logs)
        rmse = math.sqrt(sum(err2) / len(err2))
        mean_a = sum(actual_logs) / len(actual_logs)
        ss_tot = sum((a - mean_a) ** 2 for a in actual_logs)
        oos_r2 = 1.0 - sum(err2) / ss_tot if ss_tot > 0 else None
        oos.update(
            {
                "oos_r2": round(oos_r2, 4) if oos_r2 is not None else None,
                "oos_rmse": round(rmse, 6),
                "oos_mae": round(mae, 6),
            }
        )

    deviations = [None] * len(dates)
    for i, fv in enumerate(fair):
        if fv is None:
            continue
        deviations[i] = 100.0 * (prices[i] / fv - 1.0)

    # Parameter diagnostics
    k_diag = _param_diagnostics(k_path, name="k")
    b_diag = _param_diagnostics(beta_path, name="beta")
    k_trend = _k_trend_absorbs_price(k_path, param_rows, history)

    return {
        "mode": mode,
        "fair": fair,
        "deviations": deviations,
        "history": history,
        "parameters": param_rows,
        "oos": oos,
        "k_diagnostics": k_diag,
        "beta_diagnostics": b_diag,
        "pinned_beta_share": round(pinned / max(1, len(param_rows)), 3),
        "k_absorbs_trend": k_trend,
        "tip": history[-1] if history else None,
        "current_k": k_path[-1] if k_path else None,
        "current_beta": beta_path[-1] if beta_path else None,
    }


def _param_diagnostics(path: list[float], *, name: str) -> dict[str, Any]:
    if not path:
        return {"name": name, "ok": False}
    rets = [path[i] / path[i - 1] - 1.0 for i in range(1, len(path)) if path[i - 1] > 0]
    vol = float(np.std(rets)) if rets else 0.0
    return {
        "name": name,
        "ok": True,
        "n": len(path),
        "tip": round(path[-1], 8),
        "min": round(min(path), 8),
        "max": round(max(path), 8),
        "mean": round(sum(path) / len(path), 8),
        "instability_vol": round(vol, 6),
        "range_ratio": round(max(path) / min(path), 4) if min(path) > 0 else None,
    }


def _k_trend_absorbs_price(
    k_path: list[float], param_rows: list[dict[str, Any]], history: list[dict[str, Any]]
) -> dict[str, Any]:
    """Reject signal if k must continually rise to follow gold."""
    if len(k_path) < 8:
        return {"ok": False}
    # Fraction of windows where k increased
    up = sum(1 for i in range(1, len(k_path)) if k_path[i] > k_path[i - 1])
    up_share = up / (len(k_path) - 1)
    # Correlation of k with contemporaneous gold (param dates)
    ks = []
    ps = []
    price_by_date = {h["date"]: h["gold_price"] for h in history}
    for row in param_rows:
        d = row["param_date"]
        # nearest history price on/after param date
        px = price_by_date.get(d)
        if px is None:
            continue
        ks.append(float(row["k"]))
        ps.append(float(px))
    corr = None
    if len(ks) >= 8:
        mx, my = sum(ks) / len(ks), sum(ps) / len(ps)
        num = sum((a - mx) * (b - my) for a, b in zip(ks, ps))
        den = math.sqrt(sum((a - mx) ** 2 for a in ks) * sum((b - my) ** 2 for b in ps))
        corr = num / den if den > 0 else 0.0
    # Strong monotone rise + high corr with price ⇒ k absorbs trend
    absorbs = up_share >= 0.75 and corr is not None and corr >= 0.85
    return {
        "ok": True,
        "k_up_share": round(up_share, 3),
        "corr_k_gold": round(corr, 4) if corr is not None else None,
        "absorbs_trend": absorbs,
        "k_start": round(k_path[0], 8),
        "k_end": round(k_path[-1], 8),
        "k_multiple": round(k_path[-1] / k_path[0], 4) if k_path[0] > 0 else None,
    }


def _error_correction(dates: list[str], prices: list[float], deviations: list[float | None]) -> dict[str, Any]:
    pairs = []
    for i in range(len(prices) - 13):
        d = deviations[i]
        if d is None:
            continue
        fwd = 100.0 * (prices[i + 13] / prices[i] - 1.0)
        pairs.append((d, fwd))
    if len(pairs) < 40:
        return {"ok": False, "n": len(pairs)}
    xs = [-p[0] for p in pairs]
    ys = [p[1] for p in pairs]
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = math.sqrt(sum((x - mx) ** 2 for x in xs) * sum((y - my) ** 2 for y in ys))
    corr = num / den if den > 0 else 0.0
    return {
        "ok": True,
        "n": len(pairs),
        "corr_cheapness_vs_fwd13": round(corr, 4),
        "error_correction": corr > 0.05,
        "wrong_way": corr < -0.05,
    }


def _window_start_sensitivity(panel: dict[str, Any]) -> list[dict[str, Any]]:
    """Refit rolling model with alternate panel start years."""
    rows = []
    for start_year in (2000, 2003, 2006, 2010):
        # Slice panel
        idx = [i for i, d in enumerate(panel["dates"]) if d >= f"{start_year}-01-01"]
        if len(idx) < MIN_CALIB_OBS + 40:
            rows.append({"start_year": start_year, "ok": False})
            continue
        sub = {k: ([panel[k][i] for i in idx] if isinstance(panel[k], list) else panel[k]) for k in panel}
        # meta not needed
        eng = _run_calibration(sub, mode="rolling")
        tip = eng.get("tip") or {}
        rows.append(
            {
                "start_year": start_year,
                "ok": True,
                "n": len(idx),
                "tip_k": eng.get("current_k"),
                "tip_beta": eng.get("current_beta"),
                "tip_fair": tip.get("fair_value"),
                "tip_deviation_pct": tip.get("deviation_pct"),
                "oos_r2": (eng.get("oos") or {}).get("oos_r2"),
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Boundary tests
# ---------------------------------------------------------------------------


def mathematical_boundary_tests(
    *,
    k: float,
    beta: float,
    monetary_value_per_ounce: float,
    dxy_benchmark: float,
    dxy: float,
) -> dict[str, Any]:
    """Structural formula tests.

    Yield-direction tests use a strictly positive beta reference so the
    functional form is validated even when calibrated beta pins at 0.
    """
    beta_struct = float(beta) if float(beta) > 1e-9 else 10.0
    base_args = dict(
        k=k,
        beta=beta_struct,
        monetary_value_per_ounce=monetary_value_per_ounce,
        dxy_benchmark=dxy_benchmark,
        dxy=dxy,
    )
    fv0 = fair_value_components(**base_args, real_yield_decimal=0.0)["fair_value"]
    fv3 = fair_value_components(**base_args, real_yield_decimal=0.03)["fair_value"]
    fvn = fair_value_components(**base_args, real_yield_decimal=-0.01)["fair_value"]
    fv_liq = fair_value_components(**base_args, real_yield_decimal=0.01)["fair_value"]
    fv_liq2 = fair_value_components(
        k=k,
        beta=beta_struct,
        monetary_value_per_ounce=(monetary_value_per_ounce * 1.1) / 1.1,
        real_yield_decimal=0.01,
        dxy_benchmark=dxy_benchmark,
        dxy=dxy,
    )["fair_value"]
    fv_mid = fair_value_components(**base_args, real_yield_decimal=0.01)["fair_value"]
    fv_hi_dxy = fair_value_components(
        k=k,
        beta=beta_struct,
        monetary_value_per_ounce=monetary_value_per_ounce,
        real_yield_decimal=0.01,
        dxy_benchmark=dxy_benchmark,
        dxy=dxy * 1.1,
    )["fair_value"]
    fv_lo_dxy = fair_value_components(
        k=k,
        beta=beta_struct,
        monetary_value_per_ounce=monetary_value_per_ounce,
        real_yield_decimal=0.01,
        dxy_benchmark=dxy_benchmark,
        dxy=dxy * 0.9,
    )["fair_value"]
    recon = fair_value_components(**base_args, real_yield_decimal=0.015)
    # Also reconcile at the *calibrated* beta (may be 0).
    recon_cal = fair_value_components(
        k=k,
        beta=max(0.0, float(beta)),
        monetary_value_per_ounce=monetary_value_per_ounce,
        real_yield_decimal=0.015,
        dxy_benchmark=dxy_benchmark,
        dxy=dxy,
    )
    manual = (
        recon_cal["k"]
        * recon_cal["monetary_value_per_ounce"]
        * recon_cal["yield_factor"]
        * recon_cal["dxy_factor"]
    )
    struct_pass = all(
        [
            fv3 < fv0 and fv3 > 0 and math.isfinite(fv3),
            fvn > fv0 and math.isfinite(fvn),
            abs(fv_liq - fv_liq2) < 1e-9,
            fv_hi_dxy < fv_mid < fv_lo_dxy,
            abs(manual - recon_cal["fair_value"]) < 1e-9,
            recon["fair_value"] > 0,
        ]
    )
    return {
        "beta_used_for_yield_direction_tests": beta_struct,
        "calibrated_beta": float(beta),
        "calibrated_beta_degenerate": float(beta) <= 1e-12,
        "real_yield_compression": {
            "pass": fv3 < fv0 and fv3 > 0 and math.isfinite(fv3),
            "fv_yield_0": fv0,
            "fv_yield_3pct": fv3,
        },
        "negative_yield_premium": {
            "pass": fvn > fv0 and math.isfinite(fvn),
            "fv_yield_0": fv0,
            "fv_yield_minus_1pct": fvn,
        },
        "liquidity_invariance": {
            "pass": abs(fv_liq - fv_liq2) < 1e-9,
            "fv": fv_liq,
            "fv_scaled_m_and_g": fv_liq2,
        },
        "dxy_direction": {
            "pass": fv_hi_dxy < fv_mid < fv_lo_dxy,
            "fv_high_dxy": fv_hi_dxy,
            "fv_mid": fv_mid,
            "fv_low_dxy": fv_lo_dxy,
        },
        "scaling_reconciliation": {
            "pass": abs(manual - recon_cal["fair_value"]) < 1e-9,
            "fair_value": recon_cal["fair_value"],
            "product": manual,
        },
        "all_pass": struct_pass,
    }


# ---------------------------------------------------------------------------
# Charts / episodes / verdict
# ---------------------------------------------------------------------------


def _svg_lines(
    path: Path,
    *,
    title: str,
    series: list[tuple[str, dict[str, float]]],
    colors: dict[str, str],
    zero_line: bool = False,
) -> str:
    w, h = 1200, 420
    pad_l, pad_r, pad_t = 55, 20, 36
    plot_w = w - pad_l - pad_r
    plot_h = h - 80
    if len(series) < 5:
        path.write_text(
            f"<svg xmlns='http://www.w3.org/2000/svg' width='{w}' height='{h}'>"
            f"<text x='20' y='40' fill='#94a3b8'>Insufficient data</text></svg>",
            encoding="utf-8",
        )
        return str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")
    keys = list(colors)
    allv = [float(s[1][k]) for s in series for k in keys if k in s[1]]
    vmin, vmax = min(allv), max(allv)
    if abs(vmax - vmin) < 1e-12:
        vmax = vmin + 1.0

    def x_of(i: int) -> float:
        return pad_l + (i / max(1, len(series) - 1)) * plot_w

    def y_of(v: float) -> float:
        return pad_t + (1 - (v - vmin) / (vmax - vmin)) * plot_h

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" '
        f'style="background:#0b1220;font-family:Segoe UI,Arial,sans-serif">',
        f'<text x="{pad_l}" y="22" fill="#e2e8f0" font-size="15">{title}</text>',
    ]
    if zero_line and vmin < 0 < vmax:
        parts.append(
            f'<line x1="{pad_l}" y1="{y_of(0):.1f}" x2="{w - pad_r}" y2="{y_of(0):.1f}" '
            f'stroke="#475569" stroke-dasharray="4 3"/>'
        )
    lx = pad_l
    for k, col in colors.items():
        pts = " ".join(
            f"{x_of(i):.1f},{y_of(float(s[1][k])):.1f}"
            for i, s in enumerate(series)
            if k in s[1]
        )
        parts.append(f'<polyline fill="none" stroke="{col}" stroke-width="1.6" points="{pts}"/>')
        parts.append(f'<text x="{lx}" y="{h - 14}" fill="{col}" font-size="11">{k}</text>')
        lx += 150
    parts.append(
        f'<text x="{pad_l}" y="{h - 30}" fill="#64748b" font-size="10">'
        f"{series[0][0]} → {series[-1][0]} · n={len(series)}</text>"
    )
    parts.append("</svg>")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(parts), encoding="utf-8")
    return str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")


def _write_charts(main: dict[str, Any]) -> list[str]:
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    hist = main.get("history") or []
    if len(hist) < 10:
        return []
    paths = []
    pairs = [
        (
            r["date"],
            {
                "Gold": float(r["gold_price"]),
                "Fair value": float(r["fair_value"]),
                "Deviation %": float(r["deviation_pct"]),
                "Mon. value/oz": float(r["monetary_value_per_ounce"]),
                "k": float(r["k"]),
                "beta": float(r["beta"]),
                "Yield factor": float(r["yield_factor"]),
                "DXY factor": float(r["dxy_factor"]),
            },
        )
        for r in hist
    ]
    paths.append(
        _svg_lines(
            CHART_DIR / "gold_price_fair_value.svg",
            title="Gold price vs shadow-currency fair value",
            series=[(d, {"Gold": v["Gold"], "Fair value": v["Fair value"]}) for d, v in pairs],
            colors={"Gold": "#38bdf8", "Fair value": "#f472b6"},
        )
    )
    paths.append(
        _svg_lines(
            CHART_DIR / "premium_discount.svg",
            title="Premium / discount (%)",
            series=[(d, {"Deviation %": v["Deviation %"]}) for d, v in pairs],
            colors={"Deviation %": "#a3e635"},
            zero_line=True,
        )
    )
    paths.append(
        _svg_lines(
            CHART_DIR / "monetary_value_per_ounce.svg",
            title="Monetary value per ounce (Global M2 / above-ground oz)",
            series=[(d, {"Mon. value/oz": v["Mon. value/oz"]}) for d, v in pairs],
            colors={"Mon. value/oz": "#34d399"},
        )
    )
    paths.append(
        _svg_lines(
            CHART_DIR / "k_calibration.svg",
            title="Calibrated k",
            series=[(d, {"k": v["k"]}) for d, v in pairs],
            colors={"k": "#fbbf24"},
        )
    )
    paths.append(
        _svg_lines(
            CHART_DIR / "beta_calibration.svg",
            title="Calibrated beta",
            series=[(d, {"beta": v["beta"]}) for d, v in pairs],
            colors={"beta": "#a78bfa"},
        )
    )
    paths.append(
        _svg_lines(
            CHART_DIR / "yield_factor.svg",
            title="Real-yield factor exp(-β·Y)",
            series=[(d, {"Yield factor": v["Yield factor"]}) for d, v in pairs],
            colors={"Yield factor": "#fb7185"},
        )
    )
    paths.append(
        _svg_lines(
            CHART_DIR / "dxy_factor.svg",
            title="DXY factor (benchmark / DXY)",
            series=[(d, {"DXY factor": v["DXY factor"]}) for d, v in pairs],
            colors={"DXY factor": "#38bdf8"},
        )
    )
    return paths


def _write_episodes(history: list[dict[str, Any]], path: Path) -> None:
    if not history:
        path.write_text("bucket,start,end,n_weeks,mean_deviation_pct\n", encoding="utf-8")
        return
    rows = []
    cur_b = history[0]["bucket"]
    start = end = history[0]["date"]
    acc = [float(history[0]["deviation_pct"])]
    for r in history[1:]:
        if r["bucket"] != cur_b:
            rows.append(
                {
                    "bucket": cur_b,
                    "start": start,
                    "end": end,
                    "n_weeks": len(acc),
                    "mean_deviation_pct": round(sum(acc) / len(acc), 3),
                }
            )
            cur_b = r["bucket"]
            start = end = r["date"]
            acc = [float(r["deviation_pct"])]
        else:
            end = r["date"]
            acc.append(float(r["deviation_pct"]))
    rows.append(
        {
            "bucket": cur_b,
            "start": start,
            "end": end,
            "n_weeks": len(acc),
            "mean_deviation_pct": round(sum(acc) / len(acc), 3),
        }
    )
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(
            fh, fieldnames=["bucket", "start", "end", "n_weeks", "mean_deviation_pct"]
        )
        w.writeheader()
        w.writerows(rows)


def _score(eng: dict[str, Any], spread13: dict[str, Any], ec: dict[str, Any]) -> float:
    score = 0.0
    sp = spread13.get("spread_pp")
    if sp is not None and float(sp) > 0:
        score += min(40.0, float(sp) / 5.0 * 40.0)
    if ec.get("error_correction"):
        score += 20
    if ec.get("wrong_way"):
        score -= 30
    if not (eng.get("k_absorbs_trend") or {}).get("absorbs_trend"):
        score += 15
    else:
        score -= 25
    oos_r2 = (eng.get("oos") or {}).get("oos_r2")
    if oos_r2 is not None and float(oos_r2) > 0.6 and (sp is None or float(sp) <= 0):
        score -= 20
    if eng.get("current_k") and eng.get("current_beta") is not None:
        if float(eng["current_k"]) > 0 and float(eng["current_beta"]) >= 0:
            score += 10
    return round(score, 2)


def _classify_verdict(
    main: dict[str, Any],
    spread13: dict[str, Any],
    spread52: dict[str, Any],
    ec: dict[str, Any],
    bounds: dict[str, Any],
) -> dict[str, Any]:
    sp13 = spread13.get("spread_pp")
    sp52 = spread52.get("spread_pp")
    oos_r2 = (main.get("oos") or {}).get("oos_r2")
    absorbs = bool((main.get("k_absorbs_trend") or {}).get("absorbs_trend"))
    tip = main.get("tip") or {}

    if not bounds.get("all_pass"):
        return {
            "verdict": "REJECT",
            "narrative": "Mathematical boundary tests failed; formula identity or sign structure broken.",
        }
    pinned_share = float(main.get("pinned_beta_share") or 0.0)
    tip_beta = main.get("current_beta")
    if absorbs:
        return {
            "verdict": "REJECT",
            "narrative": (
                "Calibrated k continually rises with Gold "
                f"(up_share={(main.get('k_absorbs_trend') or {}).get('k_up_share')}, "
                f"corr={(main.get('k_absorbs_trend') or {}).get('corr_k_gold')}); "
                "k is absorbing the long-term price trend rather than anchoring a stable shadow value."
            ),
        }
    if tip_beta is not None and float(tip_beta) <= 1e-12 and pinned_share >= 0.5:
        return {
            "verdict": "REJECT",
            "narrative": (
                f"Real-yield coefficient is inactive (tip beta={tip_beta}, "
                f"pinned_share={pinned_share}). The engine collapses to "
                "k×(M2/G)×DXY_factor and cannot implement the required yield adjustment."
            ),
        }
    if (
        ec.get("wrong_way")
        or (
            oos_r2 is not None
            and float(oos_r2) >= 0.45
            and (sp13 is None or float(sp13) <= 0)
        )
        or (sp13 is not None and float(sp13) <= 0 and not ec.get("error_correction"))
    ):
        tip_note = ""
        if tip_beta is not None and float(tip_beta) <= 1e-12:
            tip_note = (
                f" Tip beta is pinned at 0 (yield factor inert); "
                f"historical beta mean={(main.get('beta_diagnostics') or {}).get('mean')}."
            )
        return {
            "verdict": "PRICE_MODEL_NOT_VALUATION",
            "narrative": (
                f"Shadow-currency FV tracks a monetary baseline (oos_r2={oos_r2}, "
                f"k≈{main.get('current_k')}) but fails valuation: "
                f"spread13={sp13}pp, ec_corr={ec.get('corr_cheapness_vs_fwd13')}, "
                f"wrong_way={ec.get('wrong_way')}. "
                f"Undervalued does not outperform overvalued.{tip_note}"
            ),
        }

    promote = (
        sp13 is not None
        and float(sp13) > 2.0
        and sp52 is not None
        and float(sp52) > 0
        and ec.get("error_correction")
        and not absorbs
        and bounds.get("all_pass")
        and int(spread13.get("under_n") or 0) >= 20
    )
    if promote:
        return {
            "verdict": "PROMOTE",
            "narrative": (
                f"Fixed-form shadow currency FV shows valuation edge "
                f"(spread13={sp13}pp, spread52={sp52}pp, "
                f"k={main.get('current_k')}, beta={main.get('current_beta')})."
            ),
        }

    caveats = []
    if sp13 is None or float(sp13) <= 2.0:
        caveats.append(f"spread13={sp13}")
    if not ec.get("error_correction"):
        caveats.append("weak error correction")
    caveats.append(
        f"tip_fv={tip.get('fair_value')}, tip_dev={tip.get('deviation_pct')}%"
    )
    return {
        "verdict": "USEFUL_BUT_RESEARCH",
        "narrative": (
            "Research-only shadow-currency engine is inspectable and mathematically coherent, "
            f"but not promotion-ready: {'; '.join(caveats)}."
        ),
    }


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_gold_shadow_currency_v4(*, start: str = "2000-01-01") -> dict[str, Any]:
    t0 = datetime.now(timezone.utc)
    panel = build_shadow_panel(start=start)
    if panel["meta"]["n"] < MIN_CALIB_OBS + 40:
        return {
            "ok": False,
            "error": f"Insufficient panel n={panel['meta']['n']}",
            "research_only": True,
            "meta": panel["meta"],
        }

    # Test A — main rolling
    eng_a = _run_calibration(panel, mode="rolling")
    # Test B — expanding
    eng_b = _run_calibration(panel, mode="expanding")
    # Median beta from completed rolling windows (past-only for Test C)
    betas = [float(r["beta"]) for r in eng_a["parameters"]]
    median_beta = float(np.median(betas)) if betas else 0.0
    eng_c = _run_calibration(panel, mode="rolling_k_fixed_beta", beta_fixed=median_beta)

    main = eng_a
    dates = panel["dates"]
    prices = panel["prices"]
    fwd = _forward_bucket_stats(dates, prices, main["deviations"], horizons=HORIZONS)
    spread13 = _pooled_spread(fwd, horizon=13)
    spread52 = _pooled_spread(fwd, horizon=52)
    spread104 = _pooled_spread(fwd, horizon=104)
    ec = _error_correction(dates, prices, main["deviations"])

    tip = main.get("tip") or {}
    bounds = mathematical_boundary_tests(
        k=float(main["current_k"] or tip.get("k") or 0.01),
        beta=float(main["current_beta"] if main["current_beta"] is not None else tip.get("beta") or 0.0),
        monetary_value_per_ounce=float(tip.get("monetary_value_per_ounce") or panel["monetary_value_per_ounce"][-1]),
        dxy_benchmark=float(tip.get("dxy_benchmark") or panel["dxy_benchmark"][-1]),
        dxy=float(tip.get("dxy") or panel["dxy"][-1]),
    )
    sensitivity = _window_start_sensitivity(panel)
    charts = _write_charts(main)

    ranking = []
    for label, eng in [
        ("A_rolling_15y", eng_a),
        ("B_expanding", eng_b),
        ("C_rolling_k_fixed_beta", eng_c),
    ]:
        d_tmp = eng["deviations"]
        fwd_i = _forward_bucket_stats(dates, prices, d_tmp, horizons=(13, 52))
        sp13_i = _pooled_spread(fwd_i, horizon=13)
        ec_i = _error_correction(dates, prices, d_tmp)
        ranking.append(
            {
                "id": label,
                "score": _score(eng, sp13_i, ec_i),
                "current_k": eng.get("current_k"),
                "current_beta": eng.get("current_beta"),
                "oos_r2": (eng.get("oos") or {}).get("oos_r2"),
                "spread13_pp": sp13_i.get("spread_pp"),
                "spread52_pp": _pooled_spread(fwd_i, horizon=52).get("spread_pp"),
                "ec_corr": ec_i.get("corr_cheapness_vs_fwd13"),
                "k_absorbs_trend": (eng.get("k_absorbs_trend") or {}).get("absorbs_trend"),
                "pinned_beta_share": eng.get("pinned_beta_share"),
                "tip_fair": (eng.get("tip") or {}).get("fair_value"),
                "tip_deviation_pct": (eng.get("tip") or {}).get("deviation_pct"),
            }
        )
    ranking.sort(key=lambda r: float(r.get("score") or -1e9), reverse=True)

    verdict = _classify_verdict(main, spread13, spread52, ec, bounds)

    tip_card = None
    if tip:
        tip_card = {
            "date": tip["date"],
            "k": tip["k"],
            "beta": tip["beta"],
            "monetary_value_per_ounce": tip["monetary_value_per_ounce"],
            "base_value": tip["base_value"],
            "yield_factor": tip["yield_factor"],
            "dxy_factor": tip["dxy_factor"],
            "fair_value": tip["fair_value"],
            "market_price": tip["gold_price"],
            "deviation_pct": tip["deviation_pct"],
            "premium_discount": tip["premium_discount"],
            "bucket": tip["bucket"],
            "identity": "FV = k × (M2/G) × exp(-β·Y) × (DXYb/DXY)",
        }

    payload = {
        "generated_at": t0.replace(microsecond=0).isoformat(),
        "ok": True,
        "research_only": True,
        "model_id": MODEL_ID,
        "published_models_untouched": {
            "gold_model_id": PUBLISHED_GOLD_MODEL_ID,
            "prices_latest_not_modified": True,
            "ng_untouched": True,
        },
        "equation": (
            "FV = k × (M_global_USD / G_above_oz) × exp(-β × Y_real_decimal) × (DXY_bench / DXY); "
            "DXY_bench = past-only trailing 10y geometric mean; "
            "k,β from rolling 15y monthly calibration, quarterly refresh, past-only"
        ),
        "external_claims_not_used": EXTERNAL_CLAIMS,
        "panel": panel["meta"],
        "tip": tip_card,
        "parameters": {
            "current_k": main.get("current_k"),
            "current_beta": main.get("current_beta"),
            "k_diagnostics": main.get("k_diagnostics"),
            "beta_diagnostics": main.get("beta_diagnostics"),
            "pinned_beta_share": main.get("pinned_beta_share"),
            "k_absorbs_trend": main.get("k_absorbs_trend"),
            "median_beta_for_test_c": median_beta,
        },
        "boundary_tests": bounds,
        "window_start_sensitivity": sensitivity,
        "oos": main.get("oos"),
        "spread_13w": spread13,
        "spread_52w": spread52,
        "spread_104w": spread104,
        "error_correction": ec,
        "forward_returns": fwd,
        "ranking": ranking,
        "verdict": verdict,
        "charts": charts,
        "runtime_sec": round((datetime.now(timezone.utc) - t0).total_seconds(), 2),
        "_history": main["history"],
        "_parameters": main["parameters"],
        "_eng_b": {k: v for k, v in eng_b.items() if not str(k).startswith("_") and k not in {"fair", "deviations", "history"}},
        "_eng_c": {k: v for k, v in eng_c.items() if not str(k).startswith("_") and k not in {"fair", "deviations", "history"}},
    }
    return payload


def render_markdown(payload: dict[str, Any]) -> str:
    v = payload.get("verdict") or {}
    tip = payload.get("tip") or {}
    p = payload.get("parameters") or {}
    panel = payload.get("panel") or {}
    lines = [
        "# Gold V4 — Fixed-Form Shadow Currency Engine",
        "",
        f"Generated: `{payload.get('generated_at')}`",
        "",
        f"**Model:** `{payload.get('model_id')}`",
        "",
        "**Research only — not deployed.**",
        "",
        f"**Verdict: {v.get('verdict')}**",
        "",
        v.get("narrative") or "",
        "",
        "## Equation",
        "",
        f"`{payload.get('equation')}`",
        "",
        f"External claims (not used as controls): `{payload.get('external_claims_not_used')}`",
        "",
        "## Panel",
        "",
        f"- Weeks: **{panel.get('n')}** ({panel.get('start')} → {panel.get('end')})",
        f"- Global M2 tip: `{(panel.get('global_m2') or {}).get('tip_usd_tn')}` tn USD "
        f"`{(panel.get('global_m2') or {}).get('tip_parts_usd_tn')}`",
        f"- Real yield units: decimal",
        "",
        "## Tip card",
        "",
    ]
    if tip:
        lines.extend(
            [
                "```",
                f"k                          {tip.get('k')}",
                f"Monetary value / oz        {tip.get('monetary_value_per_ounce')}",
                f"Base value (k×M/G)         {tip.get('base_value')}",
                f"beta                       {tip.get('beta')}",
                f"Yield factor               {tip.get('yield_factor')}",
                f"DXY factor                 {tip.get('dxy_factor')}",
                f"Fair value                 {tip.get('fair_value')}",
                f"Market price               {tip.get('market_price')}",
                f"{tip.get('premium_discount'):<26}{tip.get('deviation_pct')}%",
                "```",
                "",
                f"Bucket: **{tip.get('bucket')}**",
            ]
        )
    lines.extend(
        [
            "",
            "## Parameters",
            "",
            f"- Current k: **{p.get('current_k')}**",
            f"- Current beta: **{p.get('current_beta')}**",
            f"- k diagnostics: `{p.get('k_diagnostics')}`",
            f"- beta diagnostics: `{p.get('beta_diagnostics')}`",
            f"- Pinned beta share: `{p.get('pinned_beta_share')}`",
            f"- k absorbs trend?: `{p.get('k_absorbs_trend')}`",
            f"- Test C median beta: `{p.get('median_beta_for_test_c')}`",
            "",
            "## Boundary tests",
            "",
            f"`{payload.get('boundary_tests')}`",
            "",
            "## Controlled sensitivity ranking",
            "",
            "| ID | Score | k | beta | Spread13 | OOS R² | EC | k absorbs |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for r in payload.get("ranking") or []:
        lines.append(
            f"| {r.get('id')} | {r.get('score')} | {r.get('current_k')} | {r.get('current_beta')} | "
            f"{r.get('spread13_pp')} | {r.get('oos_r2')} | {r.get('ec_corr')} | {r.get('k_absorbs_trend')} |"
        )
    lines.extend(
        [
            "",
            "## Valuation spreads",
            "",
            f"- 13w: `{payload.get('spread_13w')}`",
            f"- 52w: `{payload.get('spread_52w')}`",
            f"- 104w: `{payload.get('spread_104w')}`",
            f"- Error correction: `{payload.get('error_correction')}`",
            "",
            "### Forward returns",
            "",
            "| Bucket | H | n | Episodes | Mean % | Median % | Hit | MAE |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for fr in payload.get("forward_returns") or []:
        lines.append(
            f"| {fr.get('bucket')} | {fr.get('horizon_weeks')} | {fr.get('n')} | "
            f"{fr.get('n_episodes')} | {fr.get('mean_return_pct')} | "
            f"{fr.get('median_return_pct')} | {fr.get('positive_return_rate')} | "
            f"{fr.get('max_adverse_excursion_mean')} |"
        )
    lines.extend(["", "## Window-start sensitivity", ""])
    for s in payload.get("window_start_sensitivity") or []:
        lines.append(f"- `{s}`")
    lines.extend(["", "## Charts", ""])
    for c in payload.get("charts") or []:
        lines.append(f"- `{c}`")
    lines.extend(
        [
            "",
            "## Safety",
            "",
            f"- Published Gold model untouched: `{PUBLISHED_GOLD_MODEL_ID}`",
            "- NG / COT / Scanner / Seasonality / production endpoints / price stores untouched",
            "- Outputs only under `data/audits/gold_shadow_currency_v4/`",
            "",
            f"Runtime: {payload.get('runtime_sec')}s",
            "",
        ]
    )
    return "\n".join(lines)


def write_outputs(payload: dict[str, Any]) -> dict[str, str]:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    public = {k: v for k, v in payload.items() if not str(k).startswith("_")}
    RANK_JSON.write_text(json.dumps(public, indent=2, ensure_ascii=False), encoding="utf-8")
    REPORT_MD.write_text(render_markdown(public), encoding="utf-8")

    history = list(payload.get("_history") or [])
    params = list(payload.get("_parameters") or [])
    fwd = list(payload.get("forward_returns") or [])

    if history:
        with HISTORY_CSV.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(history[0].keys()))
            w.writeheader()
            w.writerows(history)
    if params:
        with PARAM_CSV.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(params[0].keys()))
            w.writeheader()
            w.writerows(params)
    with FWD_CSV.open("w", newline="", encoding="utf-8") as fh:
        fields = [
            "bucket",
            "horizon_weeks",
            "n",
            "n_episodes",
            "mean_return_pct",
            "median_return_pct",
            "positive_return_rate",
            "max_adverse_excursion_mean",
        ]
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in fwd:
            w.writerow(row)
    _write_episodes(history, EPISODE_CSV)

    return {
        "report": str(REPORT_MD.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "history_csv": str(HISTORY_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "parameters_csv": str(PARAM_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "forward_csv": str(FWD_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "episodes_csv": str(EPISODE_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "ranking_json": str(RANK_JSON.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "charts_dir": str(CHART_DIR.relative_to(PROJECT_ROOT)).replace("\\", "/"),
    }
