"""Gold Valuation V3 — Global Liquidity and Real-Yield Fair Value (research only).

Core:
  log(Gold) = α − β1·Real10Y + β2·log(Global CB Assets / Above-Ground Gold oz)
              [+ β3·Official Gold Reserve Share] [− β4·DXY]

Models A/B/C only. Expanding walk-forward, past-only transforms, sign constraints.

Does NOT modify NG valuation, metals_real_yield_v1, COT, Scanner, Seasonality,
production endpoints, or canonical price stores. Does not reopen M2/CPI research.
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
    STEP,
    _build_gold_weekly,
    _forward_bucket_stats,
    _pooled_spread,
    _real_yield_series,
)
from hptl.valuation.gold_macro_tier1_discovery import _asof_series, _load_dx_daily
from hptl.valuation.gold_structural_valuation_research import (
    MONTHLY_PUBLICATION_LAG_DAYS,
    _asof_with_lag,
    _classify_deviation,
    _finite_ffill,
)
from hptl.valuation.metals_valuation_v1 import MODEL_ID as PUBLISHED_GOLD_MODEL_ID

AUDIT_DIR = PROJECT_ROOT / "data" / "audits" / "gold_global_liquidity_valuation"
CHART_DIR = AUDIT_DIR / "charts"
CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "gold_liquidity"
REPORT_MD = AUDIT_DIR / "gold_global_liquidity_report.md"
RANK_CSV = AUDIT_DIR / "gold_model_ranking.csv"
HISTORY_CSV = AUDIT_DIR / "gold_fair_value_history.csv"
CONTRIB_CSV = AUDIT_DIR / "gold_driver_contributions.csv"
FWD_CSV = AUDIT_DIR / "gold_forward_returns.csv"
EPISODE_CSV = AUDIT_DIR / "gold_valuation_episodes.csv"
COEF_CSV = AUDIT_DIR / "gold_coefficients.csv"
JSON_OUT = AUDIT_DIR / "gold_global_liquidity_valuation.json"

MODEL_ID = "gold_global_liquidity_valuation_v3"
TROY_OZ_PER_TONNE = 32150.7465
HORIZONS = (13, 26, 52, 104)
MIN_TRAIN_WEEKS = 520  # 10y where data permits
BIS_PUB_LAG_DAYS = {
    "US": 10,  # H.4.1 weekly → usable ~1 week after month-end print in BIS
    "XM": 14,  # ECB weekly / monthly BIS
    "JP": 35,  # BoJ monthly
    "CN": 45,  # PBoC monthly
}
# Local-currency unit multiplier on BIS CBTA (UNIT_MULT=9 → billions).
BIS_UNIT_MULT = 1_000_000_000.0

FEATURE_LABELS = {
    "real10y": "Real Yield",
    "log_liq_per_oz": "Global Liquidity / Gold oz",
    "reserve_share": "Official Gold Reserve Share",
    "dxy": "DXY",
}

# World Gold Council above-ground stock (end-year tonnes). Includes jewellery,
# bars/coins (incl. ETFs), official holdings, and other fabricated gold.
# Source: WGC Goldhub "Above-ground stock" / Gold Demand Trends year-end tables.
WGC_ABOVE_GROUND_TONNES: dict[int, float] = {
    2010: 168245.7,
    2011: 171145.0,
    2012: 174056.9,
    2013: 177196.3,
    2014: 180573.0,
    2015: 183948.2,
    2016: 188028.1,
    2017: 191540.0,
    2018: 194703.9,
    2019: 198315.1,
    2020: 201762.6,
    2021: 205330.0,
    2022: 208949.4,
    2023: 212660.9,
    2024: 216265.4,
    2025: 219891.0,
}

# Approximate world mine production (tonnes) used only to backcast stock before
# WGC 2010. USGS Mineral Commodity Summaries / GFMS order-of-magnitude.
USGS_MINE_PROD_TONNES: dict[int, float] = {
    1990: 2180.0,
    1991: 2160.0,
    1992: 2270.0,
    1993: 2290.0,
    1994: 2300.0,
    1995: 2270.0,
    1996: 2350.0,
    1997: 2470.0,
    1998: 2540.0,
    1999: 2570.0,
    2000: 2590.0,
    2001: 2600.0,
    2002: 2550.0,
    2003: 2590.0,
    2004: 2470.0,
    2005: 2520.0,
    2006: 2480.0,
    2007: 2470.0,
    2008: 2410.0,
    2009: 2570.0,
    2010: 2740.0,
}

# Official monetary gold holdings (tonnes), WGC / IMF IFS order of magnitude.
# Used with gold price + world FX reserves to form reserve share.
OFFICIAL_GOLD_TONNES: dict[int, float] = {
    1999: 33470.0,
    2000: 33060.0,
    2001: 32640.0,
    2002: 32280.0,
    2003: 31920.0,
    2004: 31580.0,
    2005: 30950.0,
    2006: 30420.0,
    2007: 29980.0,
    2008: 29870.0,
    2009: 30120.0,
    2010: 30535.0,
    2011: 31120.0,
    2012: 31680.0,
    2013: 31840.0,
    2014: 32010.0,
    2015: 32740.0,
    2016: 33280.0,
    2017: 33620.0,
    2018: 33910.0,
    2019: 34870.0,
    2020: 35280.0,
    2021: 35640.0,
    2022: 35980.0,
    2023: 36420.0,
    2024: 36840.0,
    2025: 37200.0,
}

# World foreign-exchange reserves excluding gold (USD trillions), IMF IFS /
# COFER / Annual Report published year-end totals (documented research series).
WORLD_FX_RESERVES_USD_TN: dict[int, float] = {
    1999: 1.78,
    2000: 1.93,
    2001: 2.05,
    2002: 2.41,
    2003: 3.02,
    2004: 3.75,
    2005: 4.32,
    2006: 5.04,
    2007: 6.40,
    2008: 6.75,
    2009: 8.16,
    2010: 9.26,
    2011: 10.20,
    2012: 10.95,
    2013: 11.65,
    2014: 11.60,
    2015: 10.90,
    2016: 10.75,
    2017: 11.40,
    2018: 11.45,
    2019: 11.75,
    2020: 12.70,
    2021: 12.85,
    2022: 12.05,
    2023: 12.35,
    2024: 12.40,
    2025: 12.90,
}

ANNUAL_SERIES_PUB_LAG_DAYS = 120  # year-end print usable ~Apr following year


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_iso(d: str) -> date:
    return date.fromisoformat(str(d)[:10])


def _add_days(iso: str, days: int) -> str:
    return (_parse_iso(iso) + timedelta(days=days)).isoformat()


def _month_end_iso(ym: str) -> str:
    """Convert 'YYYY-MM' to month-end ISO date."""
    y, m = int(ym[:4]), int(ym[5:7])
    if m == 12:
        return date(y, 12, 31).isoformat()
    return (date(y, m + 1, 1) - timedelta(days=1)).isoformat()


def _http_get(url: str, timeout: int = 60) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "HPTL-research/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _load_bis_cbta_monthly(ref_area: str, currency: str) -> dict[str, float]:
    """BIS WS_CBTA monthly total assets in local currency (absolute units).

    Values published with UNIT_MULT=9 (billions); converted to full currency units.
    Cached under data/cache/gold_liquidity for deterministic re-runs.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE_DIR / f"bis_cbta_M_{ref_area}_{currency}.json"
    if cache_path.exists():
        doc = json.loads(cache_path.read_text(encoding="utf-8"))
        return {str(k): float(v) for k, v in (doc.get("series") or {}).items()}

    key = f"M.{ref_area}.B.XDC.{currency}.N"
    url = f"https://stats.bis.org/api/v1/data/WS_CBTA/{key}?format=csv"
    raw = _http_get(url).decode("utf-8", "replace")
    # Lightweight CSV parse (avoid hard pandas dependency in path logic).
    import csv as _csv

    reader = _csv.DictReader(io.StringIO(raw))
    series: dict[str, float] = {}
    for row in reader:
        tp = (row.get("TIME_PERIOD") or "").strip()
        ov = row.get("OBS_VALUE")
        if not tp or ov in (None, ""):
            continue
        try:
            val_bn = float(ov)
        except ValueError:
            continue
        series[_month_end_iso(tp)] = val_bn * BIS_UNIT_MULT

    payload = {
        "source": "BIS WS_CBTA",
        "key": key,
        "unit": f"{currency} (absolute, from UNIT_MULT=9 billions)",
        "n": len(series),
        "series": series,
        "fetched_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    cache_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return series


def _build_above_ground_annual() -> dict[str, float]:
    """Annual end-year above-ground gold stock in troy ounces (lagged usable date)."""
    # Backcast from WGC 2010 using mine production (stock grows by mine output).
    stock: dict[int, float] = dict(WGC_ABOVE_GROUND_TONNES)
    y = 2010
    while y > 1990:
        prod = USGS_MINE_PROD_TONNES.get(y, 2500.0)
        prev = stock[y] - prod
        if prev < 100_000.0:
            break
        stock[y - 1] = prev
        y -= 1

    # Carry 2025 forward for in-year weeks (no future interpolation of 2026).
    out: dict[str, float] = {}
    for year, tonnes in sorted(stock.items()):
        # Publication lag: year-end estimate usable ~Apr of following year.
        usable = date(year + 1, 4, 30).isoformat()
        out[usable] = float(tonnes) * TROY_OZ_PER_TONNE
    return out


def _annual_to_weekly_carry(
    annual_usable: dict[str, float], dates: list[str]
) -> list[float | None]:
    """As-of join with carry-forward; no interpolation using future prints."""
    return _finite_ffill(_asof_series(annual_usable, dates))


def _build_reserve_share_weekly(
    dates: list[str], gold_prices: list[float]
) -> tuple[list[float | None], dict[str, Any]]:
    """Official gold value / (official gold value + world FX reserves excl gold)."""
    gold_tonnes_usable: dict[str, float] = {}
    for year, tonnes in OFFICIAL_GOLD_TONNES.items():
        gold_tonnes_usable[date(year + 1, 4, 30).isoformat()] = float(tonnes)

    fx_usable: dict[str, float] = {}
    for year, tn in WORLD_FX_RESERVES_USD_TN.items():
        fx_usable[date(year + 1, 4, 30).isoformat()] = float(tn) * 1e12

    tonnes_w = _annual_to_weekly_carry(gold_tonnes_usable, dates)
    fx_w = _annual_to_weekly_carry(fx_usable, dates)

    out: list[float | None] = []
    n_ok = 0
    for i, px in enumerate(gold_prices):
        t = tonnes_w[i]
        fx = fx_w[i]
        if t is None or fx is None or px is None or px <= 0 or fx <= 0:
            out.append(None)
            continue
        gold_val = float(t) * TROY_OZ_PER_TONNE * float(px)
        share = gold_val / (gold_val + float(fx))
        out.append(share)
        n_ok += 1

    meta = {
        "definition": (
            "official_gold_tonnes × oz/t × gold_price / "
            "(same + world_FX_reserves_excl_gold_USD)"
        ),
        "official_gold_source": "WGC / IMF IFS documented annual tonnes (research cache)",
        "fx_reserves_source": (
            "IMF IFS / COFER / Annual Report year-end FX reserves excl gold "
            "(documented research series, USD)"
        ),
        "publication_lag_days_approx": ANNUAL_SERIES_PUB_LAG_DAYS,
        "engineering": "annual print → Apr+1 usable date → weekly as-of carry-forward",
        "n_finite": n_ok,
        "includes_jewellery_etc_in_above_ground": True,
        "note": (
            "Long annual history from 1999; not truncated to post-2022 CB purchases. "
            "Short purchase series is NOT used as the structural reserve term."
        ),
    }
    return out, meta


def _convert_local_to_usd(
    local_monthly: dict[str, float],
    fx_daily: dict[str, float],
    *,
    fx_mode: str,
    pub_lag_days: int,
) -> dict[str, float]:
    """Convert local CB assets to USD using contemporaneous past-only FX.

    fx_mode:
      - 'eur_usd': local EUR × DEXUSEU
      - 'usd_per_local': local / DEXJPUS or DEXCHUS (USD per 1 local? wait JP/CN are local per USD)
    FRED DEXJPUS = Yen per USD; DEXCHUS = Yuan per USD; DEXUSEU = USD per EUR.
    """
    out: dict[str, float] = {}
    for obs_d, local_val in sorted(local_monthly.items()):
        usable = _add_days(obs_d, pub_lag_days)
        # FX as-of observation date (not usable date) — no future FX.
        fx = None
        for d, v in sorted(fx_daily.items()):
            if d <= obs_d:
                fx = float(v)
            else:
                break
        if fx is None or fx <= 0:
            continue
        if fx_mode == "eur_usd":
            usd = local_val * fx
        elif fx_mode == "local_per_usd":
            usd = local_val / fx
        else:
            usd = local_val
        out[usable] = usd
    return out


def build_global_cb_assets_usd(dates: list[str]) -> tuple[list[float | None], dict[str, Any]]:
    """Sum Fed+ECB+BoJ+PBoC total assets in USD with publication lags + past FX."""
    us = _load_bis_cbta_monthly("US", "USD")
    xm = _load_bis_cbta_monthly("XM", "EUR")
    jp = _load_bis_cbta_monthly("JP", "JPY")
    cn = _load_bis_cbta_monthly("CN", "CNY")

    dex_eu = load_fred_daily_map("DEXUSEU", observation_start="1990-01-01")
    dex_jp = load_fred_daily_map("DEXJPUS", observation_start="1990-01-01")
    dex_cn = load_fred_daily_map("DEXCHUS", observation_start="1990-01-01")

    # US already USD — apply publication lag only (no FX).
    us_usd = {_add_days(d, BIS_PUB_LAG_DAYS["US"]): v for d, v in us.items()}

    xm_usd = _convert_local_to_usd(
        xm, dex_eu, fx_mode="eur_usd", pub_lag_days=BIS_PUB_LAG_DAYS["XM"]
    )
    jp_usd = _convert_local_to_usd(
        jp, dex_jp, fx_mode="local_per_usd", pub_lag_days=BIS_PUB_LAG_DAYS["JP"]
    )
    cn_usd = _convert_local_to_usd(
        cn, dex_cn, fx_mode="local_per_usd", pub_lag_days=BIS_PUB_LAG_DAYS["CN"]
    )

    us_w = _finite_ffill(_asof_series(us_usd, dates))
    xm_w = _finite_ffill(_asof_series(xm_usd, dates))
    jp_w = _finite_ffill(_asof_series(jp_usd, dates))
    cn_w = _finite_ffill(_asof_series(cn_usd, dates))

    out: list[float | None] = []
    for i in range(len(dates)):
        parts = [us_w[i], xm_w[i], jp_w[i], cn_w[i]]
        if any(p is None for p in parts):
            out.append(None)
        else:
            out.append(float(us_w[i]) + float(xm_w[i]) + float(jp_w[i]) + float(cn_w[i]))  # type: ignore[arg-type]

    tip_parts = {}
    for name, series in [("fed", us_w), ("ecb", xm_w), ("boj", jp_w), ("pboc", cn_w)]:
        last = next((series[i] for i in range(len(series) - 1, -1, -1) if series[i] is not None), None)
        tip_parts[name] = round(float(last) / 1e12, 3) if last is not None else None

    meta = {
        "components": ["Federal Reserve", "ECB", "Bank of Japan", "People's Bank of China"],
        "source": "BIS WS_CBTA monthly (BIS-spliced), FRED FX for USD conversion",
        "fx_series": {"EUR": "DEXUSEU", "JPY": "DEXJPUS", "CNY": "DEXCHUS"},
        "publication_lags_days": BIS_PUB_LAG_DAYS,
        "fx_rule": "contemporaneous FX as-of balance-sheet observation date (past-only)",
        "vintage_note": (
            "BIS current vintage used (point-in-time vintage not available via free API); "
            "publication lags applied; no future FX."
        ),
        "tip_assets_usd_tn": tip_parts,
        "n_finite": sum(1 for v in out if v is not None),
    }
    return out, meta


def _improved_real_yield(dates: list[str]) -> tuple[list[float | None], dict[str, Any]]:
    """DFII10 primary; else DGS10 − T10YIE; else DGS10 − CPI YoY."""
    from hptl.valuation.gold_focused_macro_valuation import _cpi_yoy

    dfii = load_fred_daily_map("DFII10", observation_start="2000-01-01")
    dgs10 = load_fred_daily_map("DGS10", observation_start="1970-01-01")
    t10yie = load_fred_daily_map("T10YIE", observation_start="2000-01-01")
    cpi = load_fred_daily_map("CPIAUCSL", observation_start="1970-01-01")

    tips = _asof_series(dfii, dates)
    n10 = _asof_series(dgs10, dates)
    be = _asof_series(t10yie, dates)
    cpi_w = _finite_ffill(_asof_with_lag(cpi, dates, lag_days=MONTHLY_PUBLICATION_LAG_DAYS))
    cpi_f = [float(v) if v is not None else float("nan") for v in cpi_w]
    yoy = _cpi_yoy(cpi_f)

    out: list[float | None] = []
    counts = {"dfii10": 0, "dgs10_minus_t10yie": 0, "dgs10_minus_cpi_yoy": 0}
    for i in range(len(dates)):
        if tips[i] is not None and math.isfinite(float(tips[i])):
            out.append(float(tips[i]))
            counts["dfii10"] += 1
        elif n10[i] is not None and be[i] is not None:
            out.append(float(n10[i]) - float(be[i]))  # type: ignore[arg-type]
            counts["dgs10_minus_t10yie"] += 1
        elif n10[i] is not None and yoy[i] is not None:
            out.append(float(n10[i]) - float(yoy[i]))
            counts["dgs10_minus_cpi_yoy"] += 1
        else:
            out.append(None)
    return out, {
        "primary": "DFII10",
        "proxy_preferred": "DGS10 - T10YIE (long-term inflation expectation)",
        "proxy_fallback": "DGS10 - CPI_YoY",
        "counts": counts,
        "no_policy_rate": True,
    }


def build_liquidity_panel(*, start: str = "2000-01-01") -> dict[str, Any]:
    dates, prices, gold_meta = _build_gold_weekly(start=start)
    real_w, real_meta = _improved_real_yield(dates)
    # Fallback helper still available if needed
    _ = _real_yield_series

    cb_usd, cb_meta = build_global_cb_assets_usd(dates)
    ag_annual = _build_above_ground_annual()
    ag_oz = _annual_to_weekly_carry(ag_annual, dates)
    reserve_w, reserve_meta = _build_reserve_share_weekly(dates, prices)

    dx = _load_dx_daily()
    dxy = _finite_ffill(_asof_series(dx, dates))

    log_liq: list[float | None] = []
    liq_ratio: list[float | None] = []
    for i in range(len(dates)):
        a = cb_usd[i]
        g = ag_oz[i]
        if a is None or g is None or float(g) <= 0 or float(a) <= 0:
            log_liq.append(None)
            liq_ratio.append(None)
        else:
            ratio = float(a) / float(g)
            liq_ratio.append(ratio)
            log_liq.append(math.log(ratio))

    keep = []
    for i in range(len(dates)):
        if (
            real_w[i] is not None
            and log_liq[i] is not None
            and dxy[i] is not None
            and prices[i] > 0
        ):
            keep.append(i)

    # Align core (Model A needs real + log_liq; reserve optional)
    d2 = [dates[i] for i in keep]
    p2 = [prices[i] for i in keep]
    return {
        "dates": d2,
        "prices": p2,
        "log_gold": [math.log(p) for p in p2],
        "raw": {
            "real10y": [float(real_w[i]) for i in keep],  # type: ignore[arg-type]
            "log_liq_per_oz": [float(log_liq[i]) for i in keep],  # type: ignore[arg-type]
            "liq_per_oz": [float(liq_ratio[i]) for i in keep],  # type: ignore[arg-type]
            "reserve_share": [
                float(reserve_w[i]) if reserve_w[i] is not None else float("nan") for i in keep
            ],
            "dxy": [float(dxy[i]) for i in keep],  # type: ignore[arg-type]
            "global_cb_assets_usd": [float(cb_usd[i]) for i in keep],  # type: ignore[arg-type]
            "above_ground_oz": [float(ag_oz[i]) for i in keep],  # type: ignore[arg-type]
        },
        "meta": {
            "gold": gold_meta,
            "real_yield": real_meta,
            "global_cb": cb_meta,
            "above_ground": {
                "source": "World Gold Council above-ground stock (2010–2025)",
                "backcast": "USGS/GFMS-order mine production subtracted pre-2010",
                "includes": (
                    "jewellery, bars & coins (incl. gold-backed ETFs), "
                    "central-bank holdings, other fabricated gold"
                ),
                "publication_lag": "year-end estimate usable from 30 Apr of following year",
                "engineering": "carry-forward most recent available; no future interpolation",
                "n_annual_prints": len(ag_annual),
            },
            "reserve_share": reserve_meta,
            "start_requested": start,
            "n_core": len(d2),
            "core_start": d2[0] if d2 else None,
            "core_end": d2[-1] if d2 else None,
        },
    }


# ---------------------------------------------------------------------------
# Estimation
# ---------------------------------------------------------------------------


def _sign_bounds(names: list[str]) -> tuple[np.ndarray, np.ndarray]:
    lo: list[float] = []
    hi: list[float] = []
    for f in names:
        if f in {"real10y", "dxy"}:
            lo.append(-np.inf)
            hi.append(0.0)
        elif f in {"log_liq_per_oz", "reserve_share"}:
            lo.append(0.0)
            hi.append(np.inf)
        else:
            lo.append(-np.inf)
            hi.append(np.inf)
    return np.asarray(lo, float), np.asarray(hi, float)


def _constrained_level_fit(
    y: list[float], cols: list[list[float]], names: list[str]
) -> tuple[float, list[float], float | None]:
    """Expanding level OLS: log(Gold)=α+β'X with free α and sign-constrained β."""
    if len(y) < len(names) + 12:
        return 0.0, [], None
    X_feat = np.column_stack([np.asarray(c, float) for c in cols])
    X = np.column_stack([np.ones(len(y)), X_feat])
    yy = np.asarray(y, float)
    lo_b, hi_b = _sign_bounds(names)
    lo = np.concatenate([[-np.inf], lo_b])
    hi = np.concatenate([[np.inf], hi_b])
    try:
        res = lsq_linear(X, yy, bounds=(lo, hi), method="bvls", max_iter=500)
        coef = [float(v) for v in res.x]
    except Exception:
        coef_arr, _, _, _ = np.linalg.lstsq(X, yy, rcond=None)
        coef = [float(v) for v in coef_arr]
    alpha = coef[0]
    beta = coef[1:]
    yhat = X @ np.asarray(coef)
    ss_res = float(np.sum((yy - yhat) ** 2))
    ss_tot = float(np.sum((yy - yy.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else None
    return alpha, beta, r2


def _log_contributions(
    *,
    alpha: float,
    slopes: list[float],
    names: list[str],
    feats: list[float],
    spot: float,
) -> dict[str, Any]:
    log_cs = [slopes[i] * feats[i] for i in range(len(names))]
    log_fair = alpha + sum(log_cs)
    fair = math.exp(log_fair)
    # Dollar attribution: share of (fair − 1) scaled from log parts including alpha,
    # with driver dollars reconciling to fair − exp(0) relative to intercept-only.
    base = math.exp(alpha)
    gap = fair - base
    sum_log = sum(log_cs)
    rows = []
    for i, name in enumerate(names):
        lc = log_cs[i]
        dollar = 0.0 if abs(sum_log) < 1e-15 else gap * (lc / sum_log)
        rows.append(
            {
                "feature": name,
                "label": FEATURE_LABELS.get(name, name),
                "coefficient": round(slopes[i], 6),
                "transformed_input": round(feats[i], 6),
                "log_contribution": round(lc, 8),
                "dollar_contribution": round(dollar, 2),
            }
        )
    recon_log = alpha + sum(r["log_contribution"] for r in rows)
    row_sum = sum(r["dollar_contribution"] for r in rows)
    if abs(row_sum) > 1e-9 and abs(gap - row_sum) > 0.05:
        scale = gap / row_sum
        for r in rows:
            r["dollar_contribution"] = round(r["dollar_contribution"] * scale, 2)
    dev = 100.0 * (spot - fair) / fair if fair > 0 else None
    return {
        "alpha": round(alpha, 8),
        "log_fair": round(log_fair, 8),
        "recon_log": round(recon_log, 8),
        "base_fair_value": round(base, 3),
        "drivers": rows,
        "net_macro_effect_usd": round(fair - base, 2),
        "fair_value": round(fair, 3),
        "market_price": round(spot, 3),
        "deviation_pct": round(dev, 3) if dev is not None else None,
        "premium_discount": (
            "Premium" if dev is not None and dev > 0 else
            "Discount" if dev is not None and dev < 0 else "Fair"
        ),
        "bucket": _classify_deviation(dev) if dev is not None else None,
    }


def _walk_forward(
    dates: list[str],
    prices: list[float],
    y: list[float],
    cols: list[list[float]],
    names: list[str],
    *,
    min_train: int,
    step: int = STEP,
) -> dict[str, Any]:
    """Expanding walk-forward level model with quarterly coefficient refresh."""
    n = len(y)
    fair_logs: list[float | None] = [None] * n
    history: list[dict[str, Any]] = []
    coef_rows: list[dict[str, Any]] = []
    contrib_rows: list[dict[str, Any]] = []
    preds: list[float] = []
    actuals: list[float] = []
    slope_paths: dict[str, list[float]] = {f: [] for f in names}
    sign_violations = 0
    inert_core_windows = 0

    t = min_train
    while t < n:
        y_fit = list(y[:t])
        cols_fit = [c[:t] for c in cols]
        alpha, slopes, r2 = _constrained_level_fit(y_fit, cols_fit, names)
        if not slopes or r2 is None:
            t += step
            continue
        lo, hi = _sign_bounds(names)
        for s, l, h in zip(slopes, lo, hi):
            if s < l - 1e-9 or s > h + 1e-9:
                sign_violations += 1
        if abs(slopes[names.index("real10y")]) < 1e-9 and abs(
            slopes[names.index("log_liq_per_oz")]
        ) < 1e-9:
            inert_core_windows += 1
        for f, s in zip(names, slopes):
            slope_paths[f].append(s)
        coef_rows.append(
            {
                "train_end": dates[t - 1],
                "n_train": t,
                "alpha": round(alpha, 6),
                "r2_in_sample": round(r2, 4),
                **{f: round(s, 6) for f, s in zip(names, slopes)},
            }
        )
        end = min(t + step, n)
        for i in range(t, end):
            feats = [c[i] for c in cols]
            contrib = _log_contributions(
                alpha=alpha, slopes=slopes, names=names, feats=feats, spot=prices[i]
            )
            fair_logs[i] = contrib["log_fair"]
            preds.append(float(contrib["log_fair"]))
            actuals.append(y[i])
            history.append(
                {
                    "date": dates[i],
                    "gold_price": round(prices[i], 3),
                    "fair_value": contrib["fair_value"],
                    "deviation_pct": contrib["deviation_pct"],
                    "premium_discount": contrib["premium_discount"],
                    "bucket": contrib["bucket"],
                    "alpha_intercept": contrib["alpha"],
                    "net_contribution_usd": contrib["net_macro_effect_usd"],
                    **{f"coef_{f}": round(s, 6) for f, s in zip(names, slopes)},
                    **{
                        f"log_{f}": next(
                            d["log_contribution"] for d in contrib["drivers"] if d["feature"] == f
                        )
                        for f in names
                    },
                    **{
                        f"usd_{f}": next(
                            d["dollar_contribution"] for d in contrib["drivers"] if d["feature"] == f
                        )
                        for f in names
                    },
                }
            )
            for d in contrib["drivers"]:
                contrib_rows.append(
                    {
                        "date": dates[i],
                        "feature": d["feature"],
                        "label": d["label"],
                        "log_contribution": d["log_contribution"],
                        "dollar_contribution": d["dollar_contribution"],
                        "coefficient": d["coefficient"],
                        "transformed_input": d["transformed_input"],
                        "fair_value": contrib["fair_value"],
                        "market_price": contrib["market_price"],
                        "deviation_pct": contrib["deviation_pct"],
                        "alpha_intercept": contrib["alpha"],
                    }
                )
        t += step

    oos: dict[str, Any] = {"n_oos": len(preds)}
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

    stability = {}
    for f, path in slope_paths.items():
        if not path:
            continue
        if f in {"real10y", "dxy"}:
            ok_share = sum(1 for v in path if v <= 1e-12) / len(path)
            active_share = sum(1 for v in path if v < -1e-9) / len(path)
        else:
            ok_share = sum(1 for v in path if v >= -1e-12) / len(path)
            active_share = sum(1 for v in path if v > 1e-9) / len(path)
        flips = any(a * b < -1e-12 for a, b in zip(path, path[1:]))
        stability[f] = {
            "n_windows": len(path),
            "expected_sign_share": round(ok_share, 3),
            "active_nonzero_share": round(active_share, 3),
            "sign_flip": flips,
            "mean": round(sum(path) / len(path), 6),
            "tip": round(path[-1], 6),
        }

    return {
        "fair_logs": fair_logs,
        "history": history,
        "coefficients": coef_rows,
        "contributions": contrib_rows,
        "oos": oos,
        "stability": stability,
        "sign_violations": sign_violations,
        "inert_core_windows": inert_core_windows,
        "tip": history[-1] if history else None,
    }


def _model_specs() -> list[dict[str, Any]]:
    return [
        {
            "id": "A_structural_core",
            "label": "Model A — Structural Core",
            "features": ["real10y", "log_liq_per_oz"],
            "requires_reserve": False,
        },
        {
            "id": "B_plus_reserve_share",
            "label": "Model B — Structural Core + Reserve Share",
            "features": ["real10y", "log_liq_per_oz", "reserve_share"],
            "requires_reserve": True,
        },
        {
            "id": "C_plus_dxy_overlay",
            "label": "Model C — Structural Core + Reserve Share + DXY",
            "features": ["real10y", "log_liq_per_oz", "reserve_share", "dxy"],
            "requires_reserve": True,
        },
    ]


def _align_for_model(
    panel: dict[str, Any], names: list[str], *, require_reserve: bool
) -> tuple[list[str], list[float], list[float], dict[str, list[float]]]:
    dates = panel["dates"]
    prices = panel["prices"]
    y = panel["log_gold"]
    raw = panel["raw"]
    d_al: list[str] = []
    p_al: list[float] = []
    y_al: list[float] = []
    x_al: dict[str, list[float]] = {f: [] for f in names}
    for i in range(len(dates)):
        vals = []
        ok = True
        for f in names:
            v = raw[f][i]
            if v is None or not math.isfinite(float(v)):
                ok = False
                break
            vals.append(float(v))
        if require_reserve and (
            not math.isfinite(float(raw["reserve_share"][i]))
        ):
            ok = False
        if not ok:
            continue
        d_al.append(dates[i])
        p_al.append(prices[i])
        y_al.append(y[i])
        for f, v in zip(names, vals):
            x_al[f].append(v)
    return d_al, p_al, y_al, x_al


def _time_trend_placebo(
    dates: list[str], prices: list[float], y: list[float], real: list[float], min_train: int
) -> dict[str, Any]:
    """Replace log liquidity with a pure time trend; compare valuation spread."""
    t = [(i / max(1, len(dates) - 1)) for i in range(len(dates))]
    eng = _walk_forward(
        dates, prices, y, [real, t], ["real10y", "log_liq_per_oz"], min_train=min_train
    )
    deviations = []
    for fl, px in zip(eng["fair_logs"], prices):
        if fl is None:
            deviations.append(None)
        else:
            fair = math.exp(fl)
            deviations.append(100.0 * (px / fair - 1.0) if fair > 0 else None)
    fwd = _forward_bucket_stats(dates, prices, deviations, horizons=(13, 52))
    return {
        "spread_13w": _pooled_spread(fwd, horizon=13),
        "spread_52w": _pooled_spread(fwd, horizon=52),
        "oos": eng["oos"],
        "note": "Placebo: log_liq replaced by linear time index (same sign constraints).",
    }


def _error_correction_check(
    dates: list[str], prices: list[float], deviations: list[float | None]
) -> dict[str, Any]:
    """Simple past-only check: does cheapness predict positive subsequent returns?"""
    pairs = []
    for i in range(len(prices) - 13):
        d = deviations[i]
        if d is None:
            continue
        fwd = 100.0 * (prices[i + 13] / prices[i] - 1.0)
        pairs.append((d, fwd))
    if len(pairs) < 40:
        return {"ok": False, "n": len(pairs)}
    # Correlation of (-deviation) with forward return — positive => error correction
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


def _era_notes(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    eras = [
        ("2001–2011 bull", "2001-01-01", "2011-12-31"),
        ("2011 peak zone", "2011-01-01", "2011-12-31"),
        ("2013 decline", "2013-01-01", "2013-12-31"),
        ("2020 monetary expansion", "2020-01-01", "2020-12-31"),
        ("2022–2026 CB accumulation", "2022-01-01", "2026-12-31"),
        ("Late-1970s inflation", "1975-01-01", "1979-12-31"),
        ("1980–2000 bear", "1980-01-01", "2000-12-31"),
    ]
    out = []
    for label, a, b in eras:
        rows = [r for r in history if a <= r["date"] <= b]
        if not rows:
            out.append({"era": label, "available": False, "note": "Outside common sample"})
            continue
        devs = [float(r["deviation_pct"]) for r in rows if r.get("deviation_pct") is not None]
        out.append(
            {
                "era": label,
                "available": True,
                "n": len(rows),
                "start": rows[0]["date"],
                "end": rows[-1]["date"],
                "mean_deviation_pct": round(sum(devs) / len(devs), 2) if devs else None,
                "tip_fair": rows[-1].get("fair_value"),
                "tip_price": rows[-1].get("gold_price"),
            }
        )
    return out


def _score_model(row: dict[str, Any]) -> float:
    score = 0.0
    if row.get("signs_ok"):
        score += 20
    sp13 = (row.get("spread_13w") or {}).get("spread_pp")
    sp52 = (row.get("spread_52w") or {}).get("spread_pp")
    if sp13 is not None and float(sp13) > 0:
        score += min(35.0, float(sp13) / 5.0 * 35.0)
    if sp52 is not None and float(sp52) > 0:
        score += 15
    ec = row.get("error_correction") or {}
    if ec.get("error_correction"):
        score += 15
    if ec.get("wrong_way"):
        score -= 25
    oos_r2 = (row.get("oos") or {}).get("oos_r2")
    if oos_r2 is not None and float(oos_r2) > 0.7 and (
        sp13 is None or float(sp13) <= 0
    ):
        score -= 20  # price fit without valuation edge
    if row.get("n_weeks", 0) >= 520:
        score += 10
    return round(score, 2)


def _classify_verdict(best: dict[str, Any], placebo: dict[str, Any]) -> dict[str, Any]:
    sp13 = (best.get("spread_13w") or {}).get("spread_pp")
    sp52 = (best.get("spread_52w") or {}).get("spread_pp")
    oos_r2 = (best.get("oos") or {}).get("oos_r2")
    ec = best.get("error_correction") or {}
    signs_ok = bool(best.get("signs_ok"))
    under_n = int((best.get("spread_13w") or {}).get("under_n") or 0)
    placebo_sp = (placebo.get("spread_13w") or {}).get("spread_pp")
    stab = best.get("stability") or {}
    real_active = float((stab.get("real10y") or {}).get("active_nonzero_share") or 0)
    liq_active = float((stab.get("log_liq_per_oz") or {}).get("active_nonzero_share") or 0)

    # Strong OOS price fit without meaningful valuation edge / wrong-way EC
    price_not_val = (
        oos_r2 is not None
        and float(oos_r2) >= 0.55
        and (
            sp13 is None
            or float(sp13) <= 1.0
            or not ec.get("error_correction")
            or ec.get("wrong_way")
        )
    )
    if price_not_val or ec.get("wrong_way"):
        return {
            "verdict": "PRICE_MODEL_NOT_VALUATION",
            "narrative": (
                f"Best model `{best.get('id')}` fails the valuation test: "
                f"under−over spread13={sp13}pp (want >0), "
                f"error-correction corr={ec.get('corr_cheapness_vs_fwd13')} "
                f"(wrong_way={ec.get('wrong_way')}), oos_r2={oos_r2}. "
                f"Fair-value residuals do not identify subsequent cheap→rich mean reversion; "
                f"Models B/C raise price fit via reserve share/DXY but keep negative spreads."
            ),
        }

    if not signs_ok:
        return {
            "verdict": "REJECT",
            "narrative": "Constrained coefficient signs not stably satisfied.",
        }
    if real_active < 0.5 or liq_active < 0.5:
        return {
            "verdict": "REJECT",
            "narrative": (
                "Core structural coefficients are repeatedly inert at the sign bound "
                f"(real active={real_active}, liquidity active={liq_active})."
            ),
        }
    if (
        sp13 is not None
        and placebo_sp is not None
        and float(sp13) <= float(placebo_sp) + 0.5
    ):
        return {
            "verdict": "REJECT",
            "narrative": (
                "Above-ground / liquidity term adds no clear valuation information "
                f"beyond a time-trend placebo (spread13={sp13} vs placebo={placebo_sp})."
            ),
        }

    promote = (
        signs_ok
        and sp13 is not None
        and float(sp13) > 2.0
        and sp52 is not None
        and float(sp52) > 0
        and ec.get("error_correction")
        and under_n >= 20
        and int(best.get("n_weeks") or 0) >= 520
        and not ec.get("wrong_way")
        and real_active >= 0.7
        and liq_active >= 0.7
    )
    if promote:
        return {
            "verdict": "PROMOTE",
            "narrative": (
                f"`{best.get('id')}` shows usable valuation edge: "
                f"spread13={sp13}pp, spread52={sp52}pp, "
                f"error-correction corr={ec.get('corr_cheapness_vs_fwd13')}."
            ),
        }

    caveats = []
    if sp13 is None or float(sp13) <= 2.0:
        caveats.append(f"spread13={sp13}")
    if under_n < 20:
        caveats.append(f"few undervalued obs ({under_n})")
    if not ec.get("error_correction"):
        caveats.append("weak error correction")
    if int(best.get("n_weeks") or 0) < 520:
        caveats.append("sample shorter than 10y walk-forward ideal")
    caveats.append(
        "common sample starts ~2000 (ECB+PBoC); late-1970s / 1980–2000 eras unavailable"
    )
    return {
        "verdict": "USEFUL_BUT_RESEARCH",
        "narrative": (
            f"Research-only global-liquidity FV (`{best.get('id')}`) is inspectable "
            f"(tip_fv={best.get('tip_fair')}, tip_dev={best.get('tip_deviation_pct')}%, "
            f"spread13={sp13}). Not promotion-ready: {'; '.join(caveats)}."
        ),
    }


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------


def _svg_polyline(
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

    keys = list(colors.keys())
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
            f'<line x1="{pad_l}" y1="{y_of(0):.1f}" x2="{w-pad_r}" y2="{y_of(0):.1f}" '
            f'stroke="#475569" stroke-dasharray="4 3"/>'
        )
    lx = pad_l
    for k, col in colors.items():
        pts = " ".join(
            f"{x_of(i):.1f},{y_of(float(s[1][k])):.1f}"
            for i, s in enumerate(series)
            if k in s[1]
        )
        parts.append(
            f'<polyline fill="none" stroke="{col}" stroke-width="1.6" points="{pts}"/>'
        )
        parts.append(f'<text x="{lx}" y="{h-14}" fill="{col}" font-size="11">{k}</text>')
        lx += 140
    parts.append(
        f'<text x="{pad_l}" y="{h-30}" fill="#64748b" font-size="10">'
        f'{series[0][0]} → {series[-1][0]} · n={len(series)}</text>'
    )
    parts.append("</svg>")
    path.write_text("\n".join(parts), encoding="utf-8")
    return str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")


def _write_charts(best: dict[str, Any], panel: dict[str, Any]) -> list[str]:
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    paths: list[str] = []
    dates = best["_dates"]
    prices = best["_prices"]
    fair_logs = best["_fair_logs"]
    deviations = best["_deviations"]
    raw_idx = {d: i for i, d in enumerate(panel["dates"])}

    pairs = []
    for d, px, fl, dv in zip(dates, prices, fair_logs, deviations):
        if fl is None or dv is None:
            continue
        i = raw_idx.get(d)
        pairs.append(
            (
                d,
                {
                    "Gold": px,
                    "Fair value": math.exp(fl),
                    "Deviation %": dv,
                    "Real yield": panel["raw"]["real10y"][i] if i is not None else 0.0,
                    "Liq/oz": panel["raw"]["liq_per_oz"][i] if i is not None else 0.0,
                    "Reserve share": (
                        panel["raw"]["reserve_share"][i]
                        if i is not None and math.isfinite(panel["raw"]["reserve_share"][i])
                        else 0.0
                    ),
                },
            )
        )
    if len(pairs) < 10:
        return paths

    paths.append(
        _svg_polyline(
            CHART_DIR / "gold_price_fair_value.svg",
            title="Gold price vs global-liquidity fair value",
            series=[(d, {"Gold": v["Gold"], "Fair value": v["Fair value"]}) for d, v in pairs],
            colors={"Gold": "#38bdf8", "Fair value": "#f472b6"},
        )
    )
    paths.append(
        _svg_polyline(
            CHART_DIR / "gold_valuation_deviation.svg",
            title="Valuation deviation %",
            series=[(d, {"Deviation %": v["Deviation %"]}) for d, v in pairs],
            colors={"Deviation %": "#a3e635"},
            zero_line=True,
        )
    )
    paths.append(
        _svg_polyline(
            CHART_DIR / "real_yield.svg",
            title="US 10Y real yield (DFII10 / proxy)",
            series=[(d, {"Real yield": v["Real yield"]}) for d, v in pairs],
            colors={"Real yield": "#fbbf24"},
            zero_line=True,
        )
    )
    paths.append(
        _svg_polyline(
            CHART_DIR / "liquidity_per_gold_ounce.svg",
            title="Global CB assets / above-ground gold ounce (USD)",
            series=[(d, {"Liq/oz": v["Liq/oz"]}) for d, v in pairs],
            colors={"Liq/oz": "#34d399"},
        )
    )
    paths.append(
        _svg_polyline(
            CHART_DIR / "official_gold_reserve_share.svg",
            title="Official gold reserve share (value)",
            series=[(d, {"Reserve share": v["Reserve share"]}) for d, v in pairs],
            colors={"Reserve share": "#a78bfa"},
        )
    )

    hist = best.get("_history") or []
    if hist:
        names = best["features"]
        recent = hist[-min(160, len(hist)) :]
        series = []
        for r in recent:
            series.append(
                (
                    r["date"],
                    {FEATURE_LABELS.get(f, f): float(r.get(f"usd_{f}") or 0.0) for f in names},
                )
            )
        colors = {
            "Real Yield": "#fbbf24",
            "Global Liquidity / Gold oz": "#34d399",
            "Official Gold Reserve Share": "#a78bfa",
            "DXY": "#38bdf8",
        }
        use_colors = {k: colors[k] for k in series[0][1].keys() if k in colors}
        paths.append(
            _svg_polyline(
                CHART_DIR / "driver_contributions.svg",
                title="Driver contributions ($/oz)",
                series=series,
                colors=use_colors,
                zero_line=True,
            )
        )
    return paths


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_gold_global_liquidity_valuation(*, start: str = "2000-01-01") -> dict[str, Any]:
    t0 = datetime.now(timezone.utc)
    panel = build_liquidity_panel(start=start)
    if panel["meta"]["n_core"] < 200:
        return {
            "ok": False,
            "error": f"Insufficient core weeks n={panel['meta']['n_core']}",
            "research_only": True,
            "meta": panel["meta"],
        }

    results: list[dict[str, Any]] = []
    for spec in _model_specs():
        names = list(spec["features"])
        d_al, p_al, y_al, x_al = _align_for_model(
            panel, names, require_reserve=bool(spec["requires_reserve"])
        )
        if len(y_al) < 180:
            results.append(
                {
                    "id": spec["id"],
                    "ok": False,
                    "label": spec["label"],
                    "error": f"Insufficient aligned weeks n={len(y_al)}",
                }
            )
            continue

        min_train = (
            MIN_TRAIN_WEEKS
            if len(y_al) >= MIN_TRAIN_WEEKS + 52
            else max(156, len(y_al) // 3)
        )
        cols = [x_al[f] for f in names]
        eng = _walk_forward(
            d_al, p_al, y_al, cols, names, min_train=min_train, step=STEP
        )
        deviations = []
        for fl, px in zip(eng["fair_logs"], p_al):
            if fl is None:
                deviations.append(None)
            else:
                fair = math.exp(fl)
                deviations.append(100.0 * (px / fair - 1.0) if fair > 0 else None)

        fwd = _forward_bucket_stats(d_al, p_al, deviations, horizons=HORIZONS)
        spread13 = _pooled_spread(fwd, horizon=13)
        spread52 = _pooled_spread(fwd, horizon=52)
        spread104 = _pooled_spread(fwd, horizon=104)
        ec = _error_correction_check(d_al, p_al, deviations)

        stab = eng["stability"]
        signs_ok = all(
            float((stab.get(f) or {}).get("expected_sign_share") or 0) >= 0.85 for f in names
        ) and eng["sign_violations"] == 0

        tip = eng.get("tip")
        row = {
            "id": spec["id"],
            "ok": True,
            "label": spec["label"],
            "features": names,
            "n_weeks": len(y_al),
            "sample_start": d_al[0],
            "sample_end": d_al[-1],
            "min_train": min_train,
            "oos": eng["oos"],
            "stability": stab,
            "signs_ok": signs_ok,
            "sign_violations": eng["sign_violations"],
            "spread_13w": spread13,
            "spread_52w": spread52,
            "spread_104w": spread104,
            "error_correction": ec,
            "forward_returns": fwd,
            "tip_fair": tip.get("fair_value") if tip else None,
            "tip_price": tip.get("gold_price") if tip else None,
            "tip_deviation_pct": tip.get("deviation_pct") if tip else None,
            "tip_bucket": tip.get("bucket") if tip else None,
            "tip_premium_discount": tip.get("premium_discount") if tip else None,
            "era_coverage": _era_notes(eng["history"]),
            "_dates": d_al,
            "_prices": p_al,
            "_fair_logs": eng["fair_logs"],
            "_deviations": deviations,
            "_history": eng["history"],
            "_coefficients": eng["coefficients"],
            "_contributions": eng["contributions"],
        }
        row["score"] = _score_model(row)
        results.append(row)

    ok_rows = [r for r in results if r.get("ok")]
    if not ok_rows:
        return {
            "ok": False,
            "error": "All models failed",
            "research_only": True,
            "results": results,
        }

    ok_rows.sort(key=lambda r: float(r.get("score") or -1e9), reverse=True)
    best = ok_rows[0]

    # Placebo vs Model A / best core features
    a_row = next((r for r in ok_rows if r["id"] == "A_structural_core"), best)
    placebo = _time_trend_placebo(
        a_row["_dates"],
        a_row["_prices"],
        [math.log(p) for p in a_row["_prices"]],
        [
            panel["raw"]["real10y"][panel["dates"].index(d)]
            if d in panel["dates"]
            else 0.0
            for d in a_row["_dates"]
        ],
        min_train=int(a_row["min_train"]),
    )

    verdict = _classify_verdict(best, placebo)
    charts = _write_charts(best, panel)

    # Tip contribution card from best model
    tip_card = None
    if best.get("_history"):
        last = best["_history"][-1]
        tip_card = {
            "date": last["date"],
            "model_id": best["id"],
            "drivers_usd": {
                FEATURE_LABELS.get(f, f): last.get(f"usd_{f}") for f in best["features"]
            },
            "drivers_log": {
                FEATURE_LABELS.get(f, f): last.get(f"log_{f}") for f in best["features"]
            },
            "intercept_alpha": last.get("alpha_intercept"),
            "net_contribution_usd": last.get("net_contribution_usd"),
            "fair_value": last.get("fair_value"),
            "market_price": last.get("gold_price"),
            "deviation_pct": last.get("deviation_pct"),
            "premium_discount": last.get("premium_discount"),
            "bucket": last.get("bucket"),
            "coefficients": {
                FEATURE_LABELS.get(f, f): last.get(f"coef_{f}") for f in best["features"]
            },
        }

    ranking = []
    for i, r in enumerate(ok_rows):
        ranking.append(
            {
                "rank": i + 1,
                "id": r["id"],
                "label": r["label"],
                "score": r["score"],
                "signs_ok": r["signs_ok"],
                "n_weeks": r["n_weeks"],
                "sample_start": r["sample_start"],
                "sample_end": r["sample_end"],
                "oos_r2": (r.get("oos") or {}).get("oos_r2"),
                "spread13_pp": (r.get("spread_13w") or {}).get("spread_pp"),
                "spread52_pp": (r.get("spread_52w") or {}).get("spread_pp"),
                "spread104_pp": (r.get("spread_104w") or {}).get("spread_pp"),
                "error_correction_corr": (r.get("error_correction") or {}).get(
                    "corr_cheapness_vs_fwd13"
                ),
                "tip_fair": r.get("tip_fair"),
                "tip_deviation_pct": r.get("tip_deviation_pct"),
            }
        )

    payload = {
        "generated_at": t0.replace(microsecond=0).isoformat(),
        "ok": True,
        "research_only": True,
        "model_id": MODEL_ID,
        "published_models_untouched": {
            "gold_model_id": PUBLISHED_GOLD_MODEL_ID,
            "prices_latest_not_modified": True,
            "ng_untouched": True,
            "m2_cpi_not_reopened": True,
        },
        "equation": (
            "log(Gold)=α − β1·Real10Y + β2·log(GlobalCB_USD/AboveGroundOz) "
            "[+ β3·OfficialGoldReserveShare] [− β4·DXY]; "
            "expanding walk-forward α,β with sign constraints; fair=exp(·)"
        ),
        "panel": panel["meta"],
        "ranking": ranking,
        "best_model_id": best["id"],
        "tip": tip_card,
        "oos": best["oos"],
        "stability": best["stability"],
        "spread_13w": best["spread_13w"],
        "spread_52w": best["spread_52w"],
        "spread_104w": best["spread_104w"],
        "error_correction": best["error_correction"],
        "time_trend_placebo": placebo,
        "era_coverage": best["era_coverage"],
        "forward_returns": best["forward_returns"],
        "verdict": verdict,
        "charts": charts,
        "runtime_sec": round((datetime.now(timezone.utc) - t0).total_seconds(), 2),
        "_best": best,
        "_all_results": results,
        "_panel": panel,
    }
    return payload


def render_markdown(payload: dict[str, Any]) -> str:
    v = payload.get("verdict") or {}
    tip = payload.get("tip") or {}
    panel = payload.get("panel") or {}
    lines = [
        "# Gold Valuation V3 — Global Liquidity & Real-Yield Fair Value",
        "",
        f"Generated: `{payload.get('generated_at')}`",
        "",
        f"**Model family:** `{payload.get('model_id')}`",
        "",
        "**Research only — not deployed. M2/CPI branch not reopened.**",
        "",
        f"**Verdict: {v.get('verdict')}**",
        "",
        v.get("narrative") or "",
        "",
        "## Equation",
        "",
        f"`{payload.get('equation')}`",
        "",
        f"**Best model:** `{payload.get('best_model_id')}`",
        "",
        "## Panel / inputs",
        "",
        f"- Core weeks: **{panel.get('n_core')}** "
        f"({panel.get('core_start')} → {panel.get('core_end')})",
        f"- Real yield: `{(panel.get('real_yield') or {})}`",
        f"- Global CB: `{(panel.get('global_cb') or {}).get('tip_assets_usd_tn')}`",
        f"- Above-ground: `{(panel.get('above_ground') or {})}`",
        f"- Reserve share: `{(panel.get('reserve_share') or {}).get('definition')}`",
        "",
        "## Model ranking",
        "",
        "| Rank | ID | Score | Signs | Spread13 | Spread52 | Spread104 | OOS R² | EC corr |",
        "| ---: | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for r in payload.get("ranking") or []:
        lines.append(
            f"| {r.get('rank')} | {r.get('id')} | {r.get('score')} | {r.get('signs_ok')} | "
            f"{r.get('spread13_pp')} | {r.get('spread52_pp')} | {r.get('spread104_pp')} | "
            f"{r.get('oos_r2')} | {r.get('error_correction_corr')} |"
        )

    lines.extend(["", "## Tip card (best model)", ""])
    if tip:
        lines.append("```")
        for label, usd in (tip.get("drivers_usd") or {}).items():
            sign = "+" if (usd or 0) >= 0 else ""
            lines.append(f"{label:<32}{sign}{usd}")
        lines.append("--------------------------------")
        net = tip.get("net_contribution_usd")
        sign = "+" if (net or 0) >= 0 else ""
        lines.append(f"{'Net contribution':<32}{sign}{net}")
        lines.append(f"{'Intercept α (log)':<32}{tip.get('intercept_alpha')}")
        lines.append(f"{'Fair value':<32}{tip.get('fair_value')}")
        lines.append(f"{'Current price':<32}{tip.get('market_price')}")
        lines.append(f"{tip.get('premium_discount'):<32}{tip.get('deviation_pct')}%")
        lines.append("```")
        lines.append("")
        lines.append(f"Bucket: **{tip.get('bucket')}**")
        lines.append(f"Coefficients: `{tip.get('coefficients')}`")

    lines.extend(
        [
            "",
            "## Valuation test (best model)",
            "",
            f"- Spread 13w: `{(payload.get('spread_13w') or {})}`",
            f"- Spread 52w: `{(payload.get('spread_52w') or {})}`",
            f"- Spread 104w: `{(payload.get('spread_104w') or {})}`",
            f"- Error correction: `{(payload.get('error_correction') or {})}`",
            f"- Time-trend placebo: `{(payload.get('time_trend_placebo') or {})}`",
            "",
            "### Forward returns by bucket",
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

    lines.extend(["", "## Historical eras", ""])
    for era in payload.get("era_coverage") or []:
        if era.get("available"):
            lines.append(
                f"- **{era['era']}**: n={era.get('n')}, mean_dev={era.get('mean_deviation_pct')}% "
                f"({era.get('start')} → {era.get('end')})"
            )
        else:
            lines.append(f"- **{era['era']}**: {era.get('note')}")

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
            "- Outputs only under `data/audits/gold_global_liquidity_valuation/`",
            "",
            f"Runtime: {payload.get('runtime_sec')}s",
            "",
        ]
    )
    return "\n".join(lines)


def _write_episodes(history: list[dict[str, Any]], path: Path) -> None:
    """Contiguous valuation-bucket episodes."""
    rows: list[dict[str, Any]] = []
    if not history:
        path.write_text("bucket,start,end,n_weeks,mean_deviation_pct\n", encoding="utf-8")
        return
    cur_b = history[0].get("bucket")
    start = history[0]["date"]
    end = history[0]["date"]
    acc = [float(history[0]["deviation_pct"])]
    for r in history[1:]:
        b = r.get("bucket")
        if b != cur_b:
            rows.append(
                {
                    "bucket": cur_b,
                    "start": start,
                    "end": end,
                    "n_weeks": len(acc),
                    "mean_deviation_pct": round(sum(acc) / len(acc), 3),
                }
            )
            cur_b = b
            start = r["date"]
            end = r["date"]
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
            fh,
            fieldnames=["bucket", "start", "end", "n_weeks", "mean_deviation_pct"],
        )
        w.writeheader()
        w.writerows(rows)


def write_outputs(payload: dict[str, Any]) -> dict[str, str]:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    public = {k: v for k, v in payload.items() if not str(k).startswith("_")}
    JSON_OUT.write_text(json.dumps(public, indent=2, ensure_ascii=False), encoding="utf-8")
    REPORT_MD.write_text(render_markdown(public), encoding="utf-8")

    best = payload.get("_best") or {}
    history = list(best.get("_history") or [])
    coefs = list(best.get("_coefficients") or [])
    contribs = list(best.get("_contributions") or [])
    fwd = list(payload.get("forward_returns") or [])

    with RANK_CSV.open("w", newline="", encoding="utf-8") as fh:
        fields = [
            "rank",
            "id",
            "label",
            "score",
            "signs_ok",
            "n_weeks",
            "sample_start",
            "sample_end",
            "oos_r2",
            "spread13_pp",
            "spread52_pp",
            "spread104_pp",
            "error_correction_corr",
            "tip_fair",
            "tip_deviation_pct",
        ]
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in payload.get("ranking") or []:
            w.writerow(row)

    if history:
        with HISTORY_CSV.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(history[0].keys()))
            w.writeheader()
            w.writerows(history)
    if coefs:
        with COEF_CSV.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(coefs[0].keys()))
            w.writeheader()
            w.writerows(coefs)
    if contribs:
        with CONTRIB_CSV.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(contribs[0].keys()))
            w.writeheader()
            w.writerows(contribs)

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
        "ranking_csv": str(RANK_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "history_csv": str(HISTORY_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "contributions_csv": str(CONTRIB_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "forward_csv": str(FWD_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "episodes_csv": str(EPISODE_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "coefficients_csv": str(COEF_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "json": str(JSON_OUT.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "charts_dir": str(CHART_DIR.relative_to(PROJECT_ROOT)).replace("\\", "/"),
    }
