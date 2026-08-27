"""Gold Valuation V5 — Supply/Demand Market-Clearing Engine (research only).

WGC-style equilibrium:
  estimate sector demand/supply → imbalance at current price →
  implied Δlog P* = Imbalance / net_elasticity →
  FV = P × exp(Δlog P*)

Staged build: Stage1 aggregates → Stage2 jewellery/tech → Stage3 bar-coin/ETF.

Does NOT recreate GRAM, use COT, shadow-currency/M2-per-oz, or modify production.
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
from hptl.valuation.gold_focused_macro_valuation import (
    _build_gold_weekly,
    _forward_bucket_stats,
    _pooled_spread,
)
from hptl.valuation.gold_global_liquidity_valuation import _improved_real_yield
from hptl.valuation.gold_macro_tier1_discovery import _asof_series, _load_dx_daily
from hptl.valuation.gold_structural_valuation_research import (
    _classify_deviation,
    _finite_ffill,
)
from hptl.valuation.metals_valuation_v1 import MODEL_ID as PUBLISHED_GOLD_MODEL_ID

AUDIT_DIR = PROJECT_ROOT / "data" / "audits" / "gold_market_clearing_valuation"
CHART_DIR = AUDIT_DIR / "charts"
CACHE_PATH = PROJECT_ROOT / "data" / "cache" / "gold_market_clearing" / "wgc_gdt_sectors.json"

REPORT_MD = AUDIT_DIR / "gold_market_clearing_report.md"
SECTOR_EQ_CSV = AUDIT_DIR / "gold_sector_equations.csv"
SECTOR_FC_CSV = AUDIT_DIR / "gold_sector_forecasts.csv"
IMBALANCE_CSV = AUDIT_DIR / "gold_supply_demand_imbalance.csv"
ELAS_CSV = AUDIT_DIR / "gold_elasticities.csv"
HISTORY_CSV = AUDIT_DIR / "gold_fair_value_history.csv"
FWD_CSV = AUDIT_DIR / "gold_forward_returns.csv"
EPISODE_CSV = AUDIT_DIR / "gold_valuation_episodes.csv"
MISSING_Q_CSV = AUDIT_DIR / "gold_missing_quarters_audit.csv"
RECON_MD = AUDIT_DIR / "gold_market_clearing_reconciliation.md"
JSON_OUT = AUDIT_DIR / "gold_market_clearing_ranking.json"

MODEL_ID = "gold_market_clearing_valuation_v5"
HORIZONS = (13, 26, 52, 104)
GDT_PUB_LAG_DAYS = 75
MIN_TRAIN_Q = 8
DELTA_LOG_BOUND = 0.50  # ± ~65% price move cap; report hits
VIX_SERIES = "VIXCLS"
INDPRO_SERIES = "INDPRO"
GDP_SERIES = "GDPC1"


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------


def _parse_iso(d: str) -> date:
    return date.fromisoformat(str(d)[:10])


def _add_days(iso: str, days: int) -> str:
    return (_parse_iso(iso) + timedelta(days=days)).isoformat()


def load_gdt_sectors(*, force_bootstrap: bool = False) -> dict[str, Any]:
    """Load cached WGC GDT quarterly sector tonnes; optionally bootstrap."""
    if force_bootstrap or not CACHE_PATH.exists():
        import runpy

        runpy.run_path(
            str(PROJECT_ROOT / "scripts" / "_bootstrap_wgc_gdt_sectors.py"),
            run_name="__main__",
        )
    doc = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    return doc


def _quarter_end_dates(start: str, end: str) -> list[str]:
    out: list[str] = []
    y, m = _parse_iso(start).year, ((_parse_iso(start).month - 1) // 3) * 3 + 3
    if m > 12:
        y += 1
        m = 3
    d1 = _parse_iso(end)
    while True:
        me = (date(y, m, 1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        # month-end of quarter month
        if m in (3, 6, 9, 12):
            me = date(y, m, 1)
            # last day of month m
            if m == 12:
                me = date(y, 12, 31)
            else:
                me = date(y, m + 1, 1) - timedelta(days=1)
            if me >= _parse_iso(start) and me <= d1:
                out.append(me.isoformat())
        if m == 12:
            y += 1
            m = 3
        else:
            m += 3
        if date(y, min(m, 12), 1) > d1:
            break
    return out


def _avg_in_quarter(
    daily_or_weekly: dict[str, float],
    q_end: str,
    *,
    require_positive: bool = False,
) -> float | None:
    """Average observations within the calendar quarter ending at q_end.

    Real yields may be negative — never require positivity for rate series.
    """
    qe = _parse_iso(q_end)
    q = (qe.month - 1) // 3
    qs = date(qe.year, q * 3 + 1, 1)
    vals = []
    for d, v in daily_or_weekly.items():
        dd = _parse_iso(d)
        if not (qs <= dd <= qe) or v is None:
            continue
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(fv):
            continue
        if require_positive and fv <= 0:
            continue
        vals.append(fv)
    if not vals:
        return None
    return sum(vals) / len(vals)


def _asof_last(daily_or_weekly: dict[str, float], asof: str) -> float | None:
    """Last finite observation on or before asof (no future leakage)."""
    last = None
    cutoff = _parse_iso(asof)
    for d, v in daily_or_weekly.items():
        try:
            dd = _parse_iso(d)
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if dd <= cutoff and math.isfinite(fv):
            if last is None or dd >= last[0]:
                last = (dd, fv)
    return None if last is None else last[1]


def _yoy(series: list[float | None], lag: int = 4) -> list[float | None]:
    out: list[float | None] = [None] * len(series)
    for i in range(len(series)):
        if i < lag:
            continue
        a, b = series[i], series[i - lag]
        if a is None or b is None or b == 0:
            continue
        out[i] = float(a) / float(b) - 1.0
    return out


def _qoq(series: list[float | None]) -> list[float | None]:
    out: list[float | None] = [None] * len(series)
    for i in range(1, len(series)):
        a, b = series[i], series[i - 1]
        if a is None or b is None or b == 0:
            continue
        out[i] = float(a) / float(b) - 1.0
    return out


def build_quarterly_panel() -> dict[str, Any]:
    gdt = load_gdt_sectors(force_bootstrap=False)
    series = gdt.get("series") or {}
    # Common dates across core sectors
    keys_core = ["mine", "recycling", "jewellery", "technology", "bar_coin", "etf", "cb"]
    date_sets = [set((series.get(k) or {}).keys()) for k in keys_core if series.get(k)]
    if not date_sets:
        return {"ok": False, "error": "No GDT sector data", "dates": []}
    dates = sorted(set.intersection(*date_sets))
    if len(dates) < MIN_TRAIN_Q + 2:
        # fall back to union with ffill within available
        dates = sorted(set.union(*date_sets))

    # Gold weekly → quarterly average
    weeks, prices, gold_meta = _build_gold_weekly(start="2000-01-01")
    gold_map = {d: p for d, p in zip(weeks, prices)}
    dx = _load_dx_daily()
    real_w, real_meta = _improved_real_yield(weeks)
    real_map = {d: v for d, v in zip(weeks, real_w) if v is not None}
    vix = load_fred_daily_map(VIX_SERIES, observation_start="1990-01-01")
    indpro = load_fred_daily_map(INDPRO_SERIES, observation_start="1990-01-01")
    gdp = load_fred_daily_map(GDP_SERIES, observation_start="1990-01-01")

    # Publication-lagged usable sector dates + exclusion audit
    usable_dates = []
    raw_rows = []
    missing_quarter_audit: list[dict[str, Any]] = []
    real_map_f = {k: float(v) for k, v in real_map.items()}
    for d in dates:
        usable = _add_days(d, GDT_PUB_LAG_DAYS)
        row = {"obs_date": d, "usable_date": usable}
        reasons: list[str] = []
        for k in keys_core + ["hedging"]:
            v = (series.get(k) or {}).get(d)
            if v is None and k != "hedging":
                reasons.append(f"missing_{k}")
                break
            row[k] = float(v) if v is not None else 0.0
        # Prices must be positive; real yields may be negative (do not drop).
        g = _avg_in_quarter(gold_map, d, require_positive=True)
        dxy = _avg_in_quarter(dx, d, require_positive=True)
        if dxy is None:
            dxy = _asof_last(dx, d)
        ry = _avg_in_quarter(real_map_f, d, require_positive=False)
        if ry is None:
            ry = _asof_last(real_map_f, d)
        vx = _avg_in_quarter(vix, d, require_positive=False)
        if vx is None:
            vx = _asof_last(vix, d)
        ip = _asof_last(indpro, d)
        gd = _asof_last(gdp, d)
        if g is None:
            reasons.append("missing_gold_avg")
        if dxy is None:
            reasons.append("missing_dxy")
        if ry is None:
            reasons.append("missing_real_yield")
        if ip is None:
            reasons.append("missing_indpro")
        if gd is None:
            reasons.append("missing_gdp")
        if reasons:
            missing_quarter_audit.append(
                {
                    "quarter": d,
                    "missing_field": ";".join(reasons),
                    "reason_excluded": "sector_or_macro_alignment",
                    "unavoidable": True,
                }
            )
            continue
        otc_raw = (series.get("otc_other") or {}).get(d)
        row.update(
            {
                "gold_price": g,
                "log_gold": math.log(g),
                "dxy": float(dxy),
                "real_yield": float(ry),  # percent (may be negative)
                "real_yield_decimal": float(ry) / 100.0,
                "vix": float(vx) if vx is not None else 20.0,
                "indpro": float(ip),
                "gdp": float(gd),
                "otc_other": float(otc_raw) if otc_raw is not None else None,
            }
        )
        usable_dates.append(usable)
        raw_rows.append(row)

    if len(raw_rows) < MIN_TRAIN_Q + 2:
        return {
            "ok": False,
            "error": f"Insufficient aligned quarters n={len(raw_rows)}",
            "dates": [],
            "meta": {"gdt_counts": gdt.get("counts"), "gold": gold_meta},
        }

    # Derived macros
    gdp_lvl = [r["gdp"] for r in raw_rows]
    ip_lvl = [r["indpro"] for r in raw_rows]
    gdp_yoy = _yoy(gdp_lvl, 4)
    ip_yoy = _yoy(ip_lvl, 4)
    # If sample < 5y, use qoq annualized proxy
    for i, r in enumerate(raw_rows):
        r["gdp_growth"] = gdp_yoy[i] if gdp_yoy[i] is not None else (
            ((gdp_lvl[i] / gdp_lvl[i - 1]) ** 4 - 1.0) if i > 0 else None
        )
        r["indpro_growth"] = ip_yoy[i] if ip_yoy[i] is not None else (
            ((ip_lvl[i] / ip_lvl[i - 1]) ** 4 - 1.0) if i > 0 else None
        )
        r["dlog_gold"] = (
            math.log(raw_rows[i]["gold_price"] / raw_rows[i - 1]["gold_price"])
            if i > 0
            else 0.0
        )
        r["fabrication"] = r["jewellery"] + r["technology"]
        r["investment"] = r["bar_coin"] + r["etf"]
        r["total_demand"] = r["fabrication"] + r["investment"] + r["cb"]
        r["total_supply"] = r["mine"] + r["recycling"] + r["hedging"]

    # Drop rows still missing growth if needed — allow first row with 0 growth
    for i, r in enumerate(raw_rows):
        if r["gdp_growth"] is None:
            r["gdp_growth"] = 0.0
        if r["indpro_growth"] is None:
            r["indpro_growth"] = 0.0
        if not math.isfinite(r["vix"]):
            r["vix"] = 20.0

    return {
        "ok": True,
        "dates": [r["obs_date"] for r in raw_rows],
        "usable_dates": [r["usable_date"] for r in raw_rows],
        "rows": raw_rows,
        "missing_quarter_audit": missing_quarter_audit,
        "meta": {
            "source": gdt.get("source"),
            "gdt_counts": gdt.get("counts"),
            "gdt_quarters_loaded": gdt.get("n_quarters"),
            "n_quarters": len(raw_rows),
            "n_excluded": len(missing_quarter_audit),
            "start": raw_rows[0]["obs_date"],
            "end": raw_rows[-1]["obs_date"],
            "publication_lag_days": GDT_PUB_LAG_DAYS,
            "min_train_quarters": MIN_TRAIN_Q,
            "gold": gold_meta,
            "real_yield": real_meta,
            "note": (
                "Panel built from official WGC GDT XLSX cache. "
                "Real yields may be negative and are retained. "
                f"Walk-forward OOS starts after {MIN_TRAIN_Q} training quarters."
            ),
        },
    }


# ---------------------------------------------------------------------------
# Sector equations
# ---------------------------------------------------------------------------


def _stage_specs(stage: int) -> dict[str, Any]:
    """Return demand/supply sector specs for the stage."""
    # Each sector: name, side, features, price_feature, sign_bounds per feature
    # features exclude intercept; price feature name for elasticity extraction
    if stage == 1:
        demand = [
            {
                "id": "fabrication",
                "label": "Fabrication (jewellery+tech)",
                "y": "fabrication",
                "features": ["gdp_growth", "log_gold"],
                "signs": {"gdp_growth": ">=0", "log_gold": "<=0"},
                "price_feature": "log_gold",
            },
            {
                "id": "investment",
                "label": "Investment (bar/coin+ETF)",
                "y": "investment",
                "features": ["real_yield", "dxy", "vix", "log_gold"],
                "signs": {
                    "real_yield": "<=0",
                    "dxy": "<=0",
                    "vix": ">=0",
                    # Demand price response must not be positive or net elasticity collapses
                    "log_gold": "<=0",
                },
                "price_feature": "log_gold",
            },
            {
                "id": "cb",
                "label": "Central-bank demand",
                "y": "cb",
                "features": [],  # reported / lagged level
                "signs": {},
                "price_feature": None,
                "exogenous": True,
            },
        ]
        supply = [
            {
                "id": "mine",
                "label": "Mine production",
                "y": "mine",
                "features": ["mine_lag", "log_gold_lag"],
                "signs": {"mine_lag": ">=0", "log_gold_lag": ">=0"},
                "price_feature": None,  # lagged price — slow; not in spot elasticity
                "spot_price_elastic": False,
            },
            {
                "id": "recycling",
                "label": "Recycling",
                "y": "recycling",
                "features": ["log_gold", "dlog_gold", "gdp_growth"],
                "signs": {"log_gold": ">=0", "dlog_gold": ">=0", "gdp_growth": "<=0"},
                "price_feature": "log_gold",
            },
        ]
    elif stage == 2:
        demand = [
            {
                "id": "jewellery",
                "label": "Jewellery",
                "y": "jewellery",
                "features": ["gdp_growth", "log_gold"],
                "signs": {"gdp_growth": ">=0", "log_gold": "<=0"},
                "price_feature": "log_gold",
            },
            {
                "id": "technology",
                "label": "Technology",
                "y": "technology",
                "features": ["indpro_growth", "log_gold"],
                "signs": {"indpro_growth": ">=0", "log_gold": "<=0"},
                "price_feature": "log_gold",
            },
            {
                "id": "investment",
                "label": "Investment (bar/coin+ETF)",
                "y": "investment",
                "features": ["real_yield", "dxy", "vix", "log_gold"],
                "signs": {
                    "real_yield": "<=0",
                    "dxy": "<=0",
                    "vix": ">=0",
                    "log_gold": "<=0",
                },
                "price_feature": "log_gold",
            },
            {
                "id": "cb",
                "label": "Central-bank demand",
                "y": "cb",
                "features": [],
                "signs": {},
                "price_feature": None,
                "exogenous": True,
            },
        ]
        supply = [
            {
                "id": "mine",
                "label": "Mine production",
                "y": "mine",
                "features": ["mine_lag", "log_gold_lag"],
                "signs": {"mine_lag": ">=0", "log_gold_lag": ">=0"},
                "price_feature": None,
                "spot_price_elastic": False,
            },
            {
                "id": "recycling",
                "label": "Recycling",
                "y": "recycling",
                "features": ["log_gold", "dlog_gold", "gdp_growth"],
                "signs": {"log_gold": ">=0", "dlog_gold": ">=0", "gdp_growth": "<=0"},
                "price_feature": "log_gold",
            },
        ]
    else:  # stage 3
        demand = [
            {
                "id": "jewellery",
                "label": "Jewellery",
                "y": "jewellery",
                "features": ["gdp_growth", "log_gold"],
                "signs": {"gdp_growth": ">=0", "log_gold": "<=0"},
                "price_feature": "log_gold",
            },
            {
                "id": "technology",
                "label": "Technology",
                "y": "technology",
                "features": ["indpro_growth", "log_gold"],
                "signs": {"indpro_growth": ">=0", "log_gold": "<=0"},
                "price_feature": "log_gold",
            },
            {
                "id": "bar_coin",
                "label": "Bar and coin",
                "y": "bar_coin",
                "features": ["real_yield", "dxy", "vix", "log_gold"],
                "signs": {
                    "real_yield": "<=0",
                    "dxy": "<=0",
                    "vix": ">=0",
                    "log_gold": "<=0",
                },
                "price_feature": "log_gold",
            },
            {
                "id": "etf",
                "label": "ETF / institutional",
                "y": "etf",
                "features": ["real_yield", "dxy", "vix", "dlog_gold"],
                "signs": {
                    "real_yield": "<=0",
                    "dxy": "<=0",
                    "vix": ">=0",
                    "dlog_gold": "free",
                },
                "price_feature": None,  # responds to changes, not level — spot elas≈0
                "spot_price_elastic": False,
            },
            {
                "id": "cb",
                "label": "Central-bank demand",
                "y": "cb",
                "features": [],
                "signs": {},
                "price_feature": None,
                "exogenous": True,
            },
        ]
        supply = [
            {
                "id": "mine",
                "label": "Mine production",
                "y": "mine",
                "features": ["mine_lag", "log_gold_lag"],
                "signs": {"mine_lag": ">=0", "log_gold_lag": ">=0"},
                "price_feature": None,
                "spot_price_elastic": False,
            },
            {
                "id": "recycling",
                "label": "Recycling",
                "y": "recycling",
                "features": ["log_gold", "dlog_gold", "gdp_growth"],
                "signs": {"log_gold": ">=0", "dlog_gold": ">=0", "gdp_growth": "<=0"},
                "price_feature": "log_gold",
            },
        ]
    return {"demand": demand, "supply": supply}


def _enrich_row_features(rows: list[dict[str, Any]]) -> None:
    for i, r in enumerate(rows):
        r["mine_lag"] = rows[i - 1]["mine"] if i > 0 else r["mine"]
        r["log_gold_lag"] = rows[i - 1]["log_gold"] if i > 0 else r["log_gold"]


def _sign_bounds(features: list[str], signs: dict[str, str]) -> tuple[np.ndarray, np.ndarray]:
    lo = [-np.inf]  # intercept
    hi = [np.inf]
    for f in features:
        s = signs.get(f, "free")
        if s == ">=0":
            lo.append(0.0)
            hi.append(np.inf)
        elif s == "<=0":
            lo.append(-np.inf)
            hi.append(0.0)
        else:
            lo.append(-np.inf)
            hi.append(np.inf)
    return np.asarray(lo, float), np.asarray(hi, float)


def _fit_sector(
    rows: list[dict[str, Any]],
    spec: dict[str, Any],
) -> dict[str, Any]:
    """Fit one sector on past rows only."""
    y_name = spec["y"]
    feats = list(spec.get("features") or [])
    if spec.get("exogenous"):
        # Use last observed value as forecast (carry)
        last = float(rows[-1][y_name])
        return {
            "id": spec["id"],
            "ok": True,
            "exogenous": True,
            "alpha": last,
            "beta": {},
            "price_elasticity": 0.0,
            "r2": None,
            "n": len(rows),
            "signs_ok": True,
        }

    y = np.asarray([float(r[y_name]) for r in rows], float)
    if not feats:
        alpha = float(np.mean(y))
        return {
            "id": spec["id"],
            "ok": True,
            "alpha": alpha,
            "beta": {},
            "price_elasticity": 0.0,
            "r2": None,
            "n": len(rows),
            "signs_ok": True,
        }

    X = np.column_stack(
        [np.ones(len(rows))] + [[float(r[f]) for r in rows] for f in feats]
    )
    lo, hi = _sign_bounds(feats, spec.get("signs") or {})
    try:
        res = lsq_linear(X, y, bounds=(lo, hi), method="bvls", max_iter=400)
        coef = [float(c) for c in res.x]
    except Exception:
        coef_arr, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
        coef = [float(c) for c in coef_arr]
    alpha = coef[0]
    beta = {f: coef[i + 1] for i, f in enumerate(feats)}
    yhat = X @ np.asarray(coef)
    ss_res = float(np.sum((y - yhat) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else None
    # Sign check
    signs_ok = True
    for f, s in (spec.get("signs") or {}).items():
        b = beta.get(f)
        if b is None:
            continue
        if s == ">=0" and b < -1e-9:
            signs_ok = False
        if s == "<=0" and b > 1e-9:
            signs_ok = False
    pf = spec.get("price_feature")
    elas = float(beta[pf]) if pf and pf in beta else 0.0
    if spec.get("spot_price_elastic") is False:
        elas = 0.0
    return {
        "id": spec["id"],
        "ok": True,
        "alpha": alpha,
        "beta": beta,
        "price_elasticity": elas,
        "r2": round(r2, 4) if r2 is not None else None,
        "n": len(rows),
        "signs_ok": signs_ok,
        "features": feats,
    }


def _predict_sector(fit: dict[str, Any], row: dict[str, Any], *, log_gold_override: float | None = None) -> float:
    if fit.get("exogenous"):
        return float(fit["alpha"])
    lg = float(row["log_gold"] if log_gold_override is None else log_gold_override)
    # local row copy for features
    pred = float(fit["alpha"])
    for f, b in (fit.get("beta") or {}).items():
        if f == "log_gold":
            pred += b * lg
        else:
            pred += b * float(row[f])
    return pred


# ---------------------------------------------------------------------------
# Market clearing
# ---------------------------------------------------------------------------


def solve_market_clearing(
    *,
    demand_fits: list[dict[str, Any]],
    supply_fits: list[dict[str, Any]],
    demand_specs: list[dict[str, Any]],
    supply_specs: list[dict[str, Any]],
    row: dict[str, Any],
) -> dict[str, Any]:
    """Compute imbalance, net elasticity, implied FV at one date.

    Identities (when solver_status == OK):
      imbalance = D0 - S0
      net_elasticity = supply_elas - demand_elas
      raw_delta_log_price = imbalance / net_elasticity
      fair_value = price * exp(raw_delta_log_price)
      deviation_pct = 100 * (price - fair_value) / fair_value

    Never publish current price as fair value after a failed solve.
    Bound hits are diagnostic only — not legitimate published fair values.
    """
    price = float(row["gold_price"])
    log_p = math.log(price)

    d_parts = {}
    for fit, spec in zip(demand_fits, demand_specs):
        d_parts[spec["id"]] = _predict_sector(fit, row)
    s_parts = {}
    for fit, spec in zip(supply_fits, supply_specs):
        s_parts[spec["id"]] = _predict_sector(fit, row)

    d0 = sum(d_parts.values())
    s0 = sum(s_parts.values())
    imbalance = d0 - s0

    demand_elas = sum(float(f.get("price_elasticity") or 0.0) for f in demand_fits)
    supply_elas = sum(float(f.get("price_elasticity") or 0.0) for f in supply_fits)
    # dD/dlogP <= 0 typically; dS/dlogP >= 0; net = S_elas - D_elas > 0
    net_elas = supply_elas - demand_elas

    raw_delta: float | None = None
    bounded_delta: float | None = None
    raw_fv: float | None = None
    displayed_fv: float | None = None
    bound_hit = False
    solve_ok = False
    solver_status = "OK"

    if not math.isfinite(net_elas) or abs(net_elas) <= 1e-6:
        solver_status = "SOLVER_INVALID"
        solve_ok = False
        bound_hit = True
    elif net_elas <= 0:
        # Wrong-signed elasticities → clearing equation not economically usable
        solver_status = "SOLVER_INVALID"
        solve_ok = False
        bound_hit = True
        raw_delta = imbalance / net_elas if abs(net_elas) > 1e-12 else None
        if raw_delta is not None and math.isfinite(raw_delta):
            raw_fv = price * math.exp(raw_delta)
            bounded_delta = math.copysign(min(abs(raw_delta), DELTA_LOG_BOUND), raw_delta)
    else:
        raw_delta = imbalance / net_elas
        # Guard overflow while still recording diagnostic raw_delta
        try:
            if abs(raw_delta) > 50:
                raw_fv = None
            else:
                raw_fv = price * math.exp(raw_delta)
        except OverflowError:
            raw_fv = None
        if abs(raw_delta) > DELTA_LOG_BOUND:
            bound_hit = True
            bounded_delta = math.copysign(DELTA_LOG_BOUND, raw_delta)
            solver_status = "BOUND_HIT_INVALID"
            solve_ok = False
            # Do NOT publish clipped FV as legitimate valuation
            displayed_fv = None
        else:
            bounded_delta = raw_delta
            displayed_fv = raw_fv
            solve_ok = True
            solver_status = "OK"

    fv = displayed_fv
    dev = (
        100.0 * (price - fv) / fv
        if fv is not None and fv > 0 and math.isfinite(fv)
        else None
    )
    raw_dev = (
        100.0 * (price - raw_fv) / raw_fv
        if raw_fv is not None and raw_fv > 0 and math.isfinite(raw_fv)
        else None
    )

    # Anti-circularity diagnostic (lagged log-gold) — not used as published FV
    row_lag = dict(row)
    row_lag["log_gold"] = float(row.get("log_gold_lag") or log_p)
    d_lag = sum(_predict_sector(f, row_lag) for f in demand_fits)
    s_lag = sum(_predict_sector(f, row_lag) for f in supply_fits)
    imb_lag = d_lag - s_lag
    fv_lag_price = None
    if net_elas > 1e-6:
        delta_lag = imb_lag / net_elas
        try:
            if abs(delta_lag) <= 50:
                fv_lag_price = price * math.exp(delta_lag)
        except OverflowError:
            fv_lag_price = None

    d_npe = sum(
        _predict_sector(
            {
                **f,
                "beta": {
                    k: (0.0 if k == "log_gold" else v)
                    for k, v in (f.get("beta") or {}).items()
                },
                "price_elasticity": 0.0,
            },
            row,
        )
        if not f.get("exogenous")
        else _predict_sector(f, row)
        for f in demand_fits
    )
    s_npe = sum(
        _predict_sector(
            {
                **f,
                "beta": {
                    k: (0.0 if k == "log_gold" else v)
                    for k, v in (f.get("beta") or {}).items()
                },
                "price_elasticity": 0.0,
            },
            row,
        )
        if not f.get("exogenous")
        else _predict_sector(f, row)
        for f in supply_fits
    )
    price_term_demand_share = min(
        1.0, abs(d0 - d_npe) / max(1.0, abs(d0), abs(d_npe))
    )
    price_term_supply_share = min(
        1.0, abs(s0 - s_npe) / max(1.0, abs(s0), abs(s_npe))
    )

    identity_ok = (
        solve_ok
        and fv is not None
        and raw_delta is not None
        and abs(fv - price * math.exp(raw_delta)) < 1e-6
        and abs((d0 - s0) - imbalance) < 1e-6
    )

    return {
        "gold_price": round(price, 3),
        "reference_price": round(price, 3),
        "fair_value": round(fv, 3) if fv is not None else None,
        "raw_fair_value": round(raw_fv, 3) if raw_fv is not None else None,
        "displayed_fair_value": round(fv, 3) if fv is not None else None,
        "deviation_pct": round(dev, 3) if dev is not None else None,
        "raw_deviation_pct": round(raw_dev, 3) if raw_dev is not None else None,
        "bucket": _classify_deviation(dev) if dev is not None else None,
        "premium_discount": (
            "Premium"
            if dev is not None and dev > 0
            else "Discount"
            if dev is not None and dev < 0
            else "Fair"
            if dev is not None
            else None
        ),
        "D0": round(d0, 3),
        "S0": round(s0, 3),
        "total_demand": round(d0, 3),
        "total_supply": round(s0, 3),
        "imbalance": round(imbalance, 3),
        "demand_elasticity": round(demand_elas, 6),
        "supply_elasticity": round(supply_elas, 6),
        "net_elasticity": round(net_elas, 6),
        "raw_delta_log_price": round(raw_delta, 6) if raw_delta is not None else None,
        "bounded_delta_log_price": (
            round(bounded_delta, 6) if bounded_delta is not None else None
        ),
        "delta_log_price": round(raw_delta, 6) if (solve_ok and raw_delta is not None) else None,
        "bound_hit": bound_hit,
        "solve_ok": solve_ok,
        "solver_status": solver_status,
        "demand_parts": {k: round(v, 3) for k, v in d_parts.items()},
        "supply_parts": {k: round(v, 3) for k, v in s_parts.items()},
        "price_term_demand_share": round(price_term_demand_share, 4),
        "price_term_supply_share": round(price_term_supply_share, 4),
        "fv_with_lagged_log_gold_in_eqs": (
            round(fv_lag_price, 3) if fv_lag_price is not None else None
        ),
        "identity_check": identity_ok,
        "delta_log_bound": DELTA_LOG_BOUND,
    }


def _walk_forward_stage(panel: dict[str, Any], stage: int) -> dict[str, Any]:
    rows = list(panel["rows"])
    _enrich_row_features(rows)
    specs = _stage_specs(stage)
    d_specs = specs["demand"]
    s_specs = specs["supply"]

    history: list[dict[str, Any]] = []
    eq_rows: list[dict[str, Any]] = []
    fc_rows: list[dict[str, Any]] = []
    imb_rows: list[dict[str, Any]] = []
    elas_rows: list[dict[str, Any]] = []
    bound_hits = 0
    sign_ok_windows = 0
    windows = 0
    fc_errors: dict[str, list[float]] = {}

    for t in range(MIN_TRAIN_Q, len(rows)):
        train = rows[:t]  # through t-1
        test = rows[t]
        d_fits = [_fit_sector(train, sp) for sp in d_specs]
        s_fits = [_fit_sector(train, sp) for sp in s_specs]
        windows += 1
        if all(f.get("signs_ok") for f in d_fits + s_fits):
            sign_ok_windows += 1

        for fit, sp in zip(d_fits + s_fits, d_specs + s_specs):
            eq_rows.append(
                {
                    "stage": stage,
                    "train_end": train[-1]["obs_date"],
                    "sector": sp["id"],
                    "side": "demand" if sp in d_specs else "supply",
                    "n": fit.get("n"),
                    "r2": fit.get("r2"),
                    "signs_ok": fit.get("signs_ok"),
                    "price_elasticity": fit.get("price_elasticity"),
                    "alpha": round(float(fit.get("alpha") or 0.0), 6),
                    **{f"b_{k}": round(v, 6) for k, v in (fit.get("beta") or {}).items()},
                }
            )

        # Forecast errors vs realized sector volumes
        for fit, sp in zip(d_fits + s_fits, d_specs + s_specs):
            yhat = _predict_sector(fit, test)
            ytrue = float(test[sp["y"]])
            err = yhat - ytrue
            fc_errors.setdefault(sp["id"], []).append(err)
            fc_rows.append(
                {
                    "stage": stage,
                    "date": test["obs_date"],
                    "sector": sp["id"],
                    "forecast": round(yhat, 3),
                    "actual": round(ytrue, 3),
                    "error": round(err, 3),
                }
            )

        sol = solve_market_clearing(
            demand_fits=d_fits,
            supply_fits=s_fits,
            demand_specs=d_specs,
            supply_specs=s_specs,
            row=test,
        )
        if sol["bound_hit"]:
            bound_hits += 1
        elas_rows.append(
            {
                "stage": stage,
                "date": test["obs_date"],
                "demand_elasticity": sol["demand_elasticity"],
                "supply_elasticity": sol["supply_elasticity"],
                "net_elasticity": sol["net_elasticity"],
                "solve_ok": sol["solve_ok"],
                "bound_hit": sol["bound_hit"],
            }
        )
        imb_rows.append(
            {
                "stage": stage,
                "date": test["obs_date"],
                "D0": sol["D0"],
                "S0": sol["S0"],
                "imbalance": sol["imbalance"],
                "delta_log_price": sol["delta_log_price"],
                "fair_value": sol["fair_value"],
                "gold_price": sol["gold_price"],
            }
        )
        tip_card = {
            **{f"demand_{k}": v for k, v in sol["demand_parts"].items()},
            **{f"supply_{k}": v for k, v in sol["supply_parts"].items()},
            "net_imbalance": sol["imbalance"],
            "implied_dlog_price": sol.get("raw_delta_log_price"),
            "observed_jewellery": test.get("jewellery"),
            "observed_technology": test.get("technology"),
            "observed_bar_coin": test.get("bar_coin"),
            "observed_etf": test.get("etf"),
            "observed_cb": test.get("cb"),
            "observed_mine": test.get("mine"),
            "observed_recycling": test.get("recycling"),
            "observed_hedging": test.get("hedging"),
            "observed_otc_other": test.get("otc_other"),
            "publication_date": test.get("usable_date"),
        }
        history.append(
            {
                "stage": stage,
                "date": test["obs_date"],
                "usable_date": test["usable_date"],
                "publication_date": test.get("usable_date"),
                "market_price": sol.get("gold_price"),
                **sol,
                **tip_card,
            }
        )

    # Forecast RMSE
    fc_rmse = {
        k: round(math.sqrt(sum(e * e for e in v) / len(v)), 3)
        for k, v in fc_errors.items()
        if v
    }

    return {
        "stage": stage,
        "history": history,
        "equations": eq_rows,
        "forecasts": fc_rows,
        "imbalances": imb_rows,
        "elasticities": elas_rows,
        "bound_hit_share": round(bound_hits / max(1, windows), 3),
        "sign_ok_share": round(sign_ok_windows / max(1, windows), 3),
        "forecast_rmse": fc_rmse,
        "n_oos": len(history),
        "tip": history[-1] if history else None,
    }


def _quarterly_to_weekly_fv(
    history: list[dict[str, Any]],
    week_dates: list[str],
    week_prices: list[float],
) -> tuple[list[float | None], list[float | None]]:
    """Carry quarterly FV forward as stale-aware slow-moving valuation.

    Only OK solves participate — never carry clipped / invalid FV.
    """
    q = sorted(
        [
            (h["usable_date"], h["fair_value"], h["deviation_pct"])
            for h in history
            if h.get("solve_ok") and h.get("fair_value") is not None
        ],
        key=lambda x: x[0],
    )
    fvs: list[float | None] = []
    devs: list[float | None] = []
    j = -1
    for d, px in zip(week_dates, week_prices):
        while j + 1 < len(q) and q[j + 1][0] <= d:
            j += 1
        if j < 0:
            fvs.append(None)
            devs.append(None)
        else:
            fv = float(q[j][1])
            fvs.append(fv)
            # recompute deviation vs current week price (stale FV)
            devs.append(100.0 * (px - fv) / fv if fv > 0 else None)
    return fvs, devs


def _score_stage(eng: dict[str, Any], spread13: dict[str, Any], ec: dict[str, Any]) -> float:
    score = 0.0
    if float(eng.get("sign_ok_share") or 0) >= 0.7:
        score += 20
    sp = spread13.get("spread_pp")
    if sp is not None and float(sp) > 0:
        score += min(40.0, float(sp) / 5.0 * 40.0)
    if ec.get("error_correction"):
        score += 20
    if ec.get("wrong_way"):
        score -= 30
    if float(eng.get("bound_hit_share") or 0) > 0.4:
        score -= 15
    if int(eng.get("n_oos") or 0) >= 8:
        score += 10
    return round(score, 2)


def _error_correction(prices: list[float], deviations: list[float | None]) -> dict[str, Any]:
    pairs = []
    for i in range(len(prices) - 13):
        d = deviations[i]
        if d is None:
            continue
        pairs.append((d, 100.0 * (prices[i + 13] / prices[i] - 1.0)))
    if len(pairs) < 20:
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


def _price_identity_leakage(history: list[dict[str, Any]]) -> dict[str, Any]:
    usable = [
        h
        for h in history
        if h.get("fair_value") is not None and h.get("gold_price") is not None and h.get("solve_ok")
    ]
    if len(usable) < 4:
        return {"ok": False, "n_valid": len(usable)}
    # Corr(FV, P) and share of FV changes matching P changes
    fvs = [float(h["fair_value"]) for h in usable]
    ps = [float(h["gold_price"]) for h in usable]
    mf, mp = sum(fvs) / len(fvs), sum(ps) / len(ps)
    num = sum((a - mf) * (b - mp) for a, b in zip(fvs, ps))
    den = math.sqrt(sum((a - mf) ** 2 for a in fvs) * sum((b - mp) ** 2 for b in ps))
    corr = num / den if den > 0 else 0.0
    dlog_fv = [math.log(fvs[i] / fvs[i - 1]) for i in range(1, len(fvs)) if fvs[i - 1] > 0]
    dlog_p = [math.log(ps[i] / ps[i - 1]) for i in range(1, len(ps)) if ps[i - 1] > 0]
    if len(dlog_fv) >= 3:
        m1, m2 = sum(dlog_fv) / len(dlog_fv), sum(dlog_p) / len(dlog_p)
        num2 = sum((a - m1) * (b - m2) for a, b in zip(dlog_fv, dlog_p))
        den2 = math.sqrt(sum((a - m1) ** 2 for a in dlog_fv) * sum((b - m2) ** 2 for b in dlog_p))
        corr_chg = num2 / den2 if den2 > 0 else 0.0
    else:
        corr_chg = None
    price_share = np.mean([float(h.get("price_term_demand_share") or 0) for h in usable])
    return {
        "ok": True,
        "n_valid": len(usable),
        "corr_fv_price_level": round(corr, 4),
        "corr_fv_price_changes": round(corr_chg, 4) if corr_chg is not None else None,
        "mean_price_term_demand_share": round(float(price_share), 4),
        "identity_leakage": corr > 0.98 and (corr_chg is not None and corr_chg > 0.95),
    }


def _classify_verdict(
    best: dict[str, Any],
    spread13: dict[str, Any],
    ec: dict[str, Any],
    leakage: dict[str, Any],
    panel_meta: dict[str, Any],
) -> dict[str, Any]:
    sp = spread13.get("spread_pp")
    n_q = int(panel_meta.get("n_quarters") or 0)
    if leakage.get("identity_leakage"):
        return {
            "verdict": "PRICE_MODEL_NOT_VALUATION",
            "narrative": (
                "Fair value mechanically tracks the current Gold price "
                f"(corr_level={leakage.get('corr_fv_price_level')}, "
                f"corr_chg={leakage.get('corr_fv_price_changes')})."
            ),
        }
    if float(best.get("sign_ok_share") or 0) < 0.5:
        return {
            "verdict": "REJECT",
            "narrative": (
                f"Sector sign constraints unstable (sign_ok_share={best.get('sign_ok_share')})."
            ),
        }
    if n_q < 24 and (sp is None or float(sp) <= 0):
        # Short sample + no valuation edge
        if ec.get("wrong_way") or (sp is not None and float(sp) <= 0):
            return {
                "verdict": "PRICE_MODEL_NOT_VALUATION",
                "narrative": (
                    f"Stage {best.get('stage')} clearing engine runs on a short GDT sample "
                    f"(n_quarters={n_q}). Under−over spread13={sp}, "
                    f"ec_corr={ec.get('corr_cheapness_vs_fwd13')}, "
                    f"wrong_way={ec.get('wrong_way')}. Not a validated valuation signal."
                ),
            }
    if ec.get("wrong_way") or (
        sp is not None and float(sp) <= 0 and not ec.get("error_correction")
    ):
        return {
            "verdict": "PRICE_MODEL_NOT_VALUATION",
            "narrative": (
                f"Best stage {best.get('stage')} fails valuation test: "
                f"spread13={sp}, ec_corr={ec.get('corr_cheapness_vs_fwd13')}."
            ),
        }
    promote = (
        sp is not None
        and float(sp) > 2.0
        and ec.get("error_correction")
        and float(best.get("sign_ok_share") or 0) >= 0.8
        and n_q >= 24
        and not leakage.get("identity_leakage")
    )
    if promote:
        return {
            "verdict": "PROMOTE",
            "narrative": (
                f"Stage {best.get('stage')} market-clearing FV shows valuation edge "
                f"(spread13={sp}pp)."
            ),
        }
    return {
        "verdict": "USEFUL_BUT_RESEARCH",
        "narrative": (
            f"Research-only market-clearing engine (best stage {best.get('stage')}) "
            f"is inspectable on WGC GDT sectors (n_quarters={n_q}, spread13={sp}). "
            "Not promotion-ready: short parseable GDT history and/or weak valuation edge."
        ),
    }


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------


def _svg(
    path: Path,
    *,
    title: str,
    series: list[tuple[str, dict[str, float]]],
    colors: dict[str, str],
    zero_line: bool = False,
) -> str:
    w, h = 1200, 420
    pad_l, pad_r, pad_t = 55, 20, 36
    if len(series) < 3:
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
    plot_w = w - pad_l - pad_r
    plot_h = h - 80

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
            f"{x_of(i):.1f},{y_of(float(s[1][k])):.1f}" for i, s in enumerate(series) if k in s[1]
        )
        parts.append(f'<polyline fill="none" stroke="{col}" stroke-width="1.6" points="{pts}"/>')
        parts.append(f'<text x="{lx}" y="{h-14}" fill="{col}" font-size="11">{k}</text>')
        lx += 140
    parts.append("</svg>")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(parts), encoding="utf-8")
    return str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")


def _write_charts(best_hist: list[dict[str, Any]]) -> list[str]:
    best_hist = [
        h
        for h in best_hist
        if h.get("fair_value") is not None
        and h.get("deviation_pct") is not None
        and h.get("solve_ok")
    ]
    if len(best_hist) < 3:
        return []
    paths = []
    pairs = [
        (
            h["date"],
            {
                "Gold": float(h["gold_price"]),
                "Fair value": float(h["fair_value"]),
                "Deviation %": float(h["deviation_pct"]),
                "Imbalance": float(h["imbalance"]),
                "D0": float(h["D0"]),
                "S0": float(h["S0"]),
                "Net elas": float(h["net_elasticity"]),
            },
        )
        for h in best_hist
    ]
    paths.append(
        _svg(
            CHART_DIR / "gold_price_fair_value.svg",
            title="Gold price vs market-clearing fair value",
            series=[(d, {"Gold": v["Gold"], "Fair value": v["Fair value"]}) for d, v in pairs],
            colors={"Gold": "#38bdf8", "Fair value": "#f472b6"},
        )
    )
    paths.append(
        _svg(
            CHART_DIR / "premium_discount.svg",
            title="Premium / discount (%)",
            series=[(d, {"Deviation %": v["Deviation %"]}) for d, v in pairs],
            colors={"Deviation %": "#a3e635"},
            zero_line=True,
        )
    )
    paths.append(
        _svg(
            CHART_DIR / "supply_demand_imbalance.svg",
            title="Estimated demand, supply, and imbalance (tonnes)",
            series=[(d, {"D0": v["D0"], "S0": v["S0"], "Imbalance": v["Imbalance"]}) for d, v in pairs],
            colors={"D0": "#34d399", "S0": "#fbbf24", "Imbalance": "#fb7185"},
            zero_line=True,
        )
    )
    paths.append(
        _svg(
            CHART_DIR / "net_elasticity.svg",
            title="Net price elasticity (tonnes per Δlog P)",
            series=[(d, {"Net elas": v["Net elas"]}) for d, v in pairs],
            colors={"Net elas": "#a78bfa"},
        )
    )
    return paths


def _write_episodes(history: list[dict[str, Any]], path: Path) -> None:
    if not history:
        path.write_text("bucket,start,end,n,mean_deviation_pct\n", encoding="utf-8")
        return
    rows = []
    cur = history[0]["bucket"]
    start = end = history[0]["date"]
    acc = [float(history[0]["deviation_pct"])]
    for h in history[1:]:
        if h["bucket"] != cur:
            rows.append(
                {
                    "bucket": cur,
                    "start": start,
                    "end": end,
                    "n": len(acc),
                    "mean_deviation_pct": round(sum(acc) / len(acc), 3),
                }
            )
            cur = h["bucket"]
            start = end = h["date"]
            acc = [float(h["deviation_pct"])]
        else:
            end = h["date"]
            acc.append(float(h["deviation_pct"]))
    rows.append(
        {
            "bucket": cur,
            "start": start,
            "end": end,
            "n": len(acc),
            "mean_deviation_pct": round(sum(acc) / len(acc), 3),
        }
    )
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["bucket", "start", "end", "n", "mean_deviation_pct"])
        w.writeheader()
        w.writerows(rows)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_gold_market_clearing_valuation() -> dict[str, Any]:
    t0 = datetime.now(timezone.utc)
    panel = build_quarterly_panel()
    if not panel.get("ok"):
        return {
            "ok": False,
            "error": panel.get("error"),
            "research_only": True,
            "meta": panel.get("meta"),
        }

    stage_engines = []
    for stage in (1, 2, 3):
        eng = _walk_forward_stage(panel, stage)
        stage_engines.append(eng)

    # Evaluate each stage on weekly carry for forward returns
    weeks, wprices, _ = _build_gold_weekly(start="2015-01-01")
    ranking = []
    for eng in stage_engines:
        fvs, devs = _quarterly_to_weekly_fv(eng["history"], weeks, wprices)
        # Align to weeks with FV
        d_al, p_al, v_al = [], [], []
        for d, p, v in zip(weeks, wprices, devs):
            if v is None:
                continue
            d_al.append(d)
            p_al.append(p)
            v_al.append(v)
        fwd = _forward_bucket_stats(d_al, p_al, v_al, horizons=HORIZONS) if d_al else []
        sp13 = _pooled_spread(fwd, horizon=13) if fwd else {"ok": False, "spread_pp": None}
        sp52 = _pooled_spread(fwd, horizon=52) if fwd else {"ok": False, "spread_pp": None}
        ec = _error_correction(p_al, v_al) if d_al else {"ok": False}
        leakage = _price_identity_leakage(eng["history"])
        score = _score_stage(eng, sp13, ec)
        ranking.append(
            {
                "stage": eng["stage"],
                "score": score,
                "n_oos_quarters": eng["n_oos"],
                "sign_ok_share": eng["sign_ok_share"],
                "bound_hit_share": eng["bound_hit_share"],
                "forecast_rmse": eng["forecast_rmse"],
                "spread13_pp": sp13.get("spread_pp"),
                "spread52_pp": sp52.get("spread_pp"),
                "ec_corr": ec.get("corr_cheapness_vs_fwd13"),
                "wrong_way": ec.get("wrong_way"),
                "identity_leakage": leakage.get("identity_leakage"),
                "corr_fv_price": leakage.get("corr_fv_price_level"),
                "tip_fair": (eng.get("tip") or {}).get("fair_value"),
                "tip_deviation_pct": (eng.get("tip") or {}).get("deviation_pct"),
                "_eng": eng,
                "_fwd": fwd,
                "_spread13": sp13,
                "_spread52": sp52,
                "_ec": ec,
                "_leakage": leakage,
                "_weekly_devs": v_al,
                "_weekly_prices": p_al,
                "_weekly_dates": d_al,
            }
        )

    # Prefer stages whose latest tip is a valid solve; else highest score with MODEL INVALID tip
    def _tip_ok(r: dict[str, Any]) -> bool:
        tip0 = (r.get("_eng") or {}).get("tip") or {}
        return bool(tip0.get("solve_ok") and tip0.get("fair_value") is not None)

    ranking.sort(
        key=lambda r: (
            1 if _tip_ok(r) else 0,
            float(r.get("score") or -1e9),
            # Prefer disaggregated stage 3 when scores tie
            int(r.get("stage") or 0),
        ),
        reverse=True,
    )
    best = ranking[0]
    best_eng = best["_eng"]
    charts = _write_charts([h for h in best_eng["history"] if h.get("solve_ok")])
    verdict = _classify_verdict(
        best_eng, best["_spread13"], best["_ec"], best["_leakage"], panel["meta"]
    )

    tip = best_eng.get("tip") or {}
    tip_card = None
    if tip:
        tip_card = {
            "date": tip.get("date"),
            "stage": best["stage"],
            "jewellery_or_fabrication": tip.get("demand_jewellery") or tip.get("demand_fabrication"),
            "technology": tip.get("demand_technology"),
            "bar_coin": tip.get("demand_bar_coin") or tip.get("observed_bar_coin"),
            "etf_investment": tip.get("demand_etf") or tip.get("observed_etf"),
            "investment_aggregate": tip.get("demand_investment"),
            "central_bank": tip.get("demand_cb"),
            "mine_supply": tip.get("supply_mine"),
            "recycling_supply": tip.get("supply_recycling"),
            "producer_hedging": tip.get("supply_hedging") or tip.get("observed_hedging"),
            "otc_other": tip.get("observed_otc_other"),
            "total_demand": tip.get("total_demand") or tip.get("D0"),
            "total_supply": tip.get("total_supply") or tip.get("S0"),
            "net_imbalance_tonnes": tip.get("imbalance"),
            "demand_elasticity": tip.get("demand_elasticity"),
            "supply_elasticity": tip.get("supply_elasticity"),
            "net_elasticity": tip.get("net_elasticity"),
            "raw_delta_log_price": tip.get("raw_delta_log_price"),
            "bounded_delta_log_price": tip.get("bounded_delta_log_price"),
            "implied_dlog_price": tip.get("raw_delta_log_price"),
            "raw_fair_value": tip.get("raw_fair_value"),
            "fair_value": tip.get("fair_value"),
            "market_price": tip.get("gold_price"),
            "deviation_pct": tip.get("deviation_pct"),
            "premium_discount": tip.get("premium_discount"),
            "bucket": tip.get("bucket"),
            "bound_hit": tip.get("bound_hit"),
            "solve_ok": tip.get("solve_ok"),
            "solver_status": tip.get("solver_status"),
            "publication_date": tip.get("publication_date") or tip.get("usable_date"),
        }

    public_ranking = [
        {k: v for k, v in r.items() if not str(k).startswith("_")} for r in ranking
    ]

    payload = {
        "generated_at": t0.replace(microsecond=0).isoformat(),
        "ok": True,
        "research_only": True,
        "model_id": MODEL_ID,
        "published_models_untouched": {
            "gold_model_id": PUBLISHED_GOLD_MODEL_ID,
            "prices_latest_not_modified": True,
            "ng_untouched": True,
            "no_cot": True,
            "no_shadow_currency": True,
            "no_gram": True,
        },
        "equation": (
            "D0=Σ demand_sectors(P,X); S0=Σ supply_sectors(P,Z); "
            "Imbalance=D0-S0; net_elas=S_elas-D_elas; "
            "ΔlogP*=Imbalance/net_elas; FV=P·exp(ΔlogP*)"
        ),
        "panel": panel["meta"],
        "missing_quarter_audit": panel.get("missing_quarter_audit") or [],
        "best_stage": best["stage"],
        "ranking": public_ranking,
        "tip": tip_card,
        "spread_13w": best["_spread13"],
        "spread_52w": best["_spread52"],
        "spread_104w": _pooled_spread(best["_fwd"], horizon=104) if best["_fwd"] else {},
        "error_correction": best["_ec"],
        "price_identity_leakage": best["_leakage"],
        "forward_returns": best["_fwd"],
        "bound_hit_share": best_eng.get("bound_hit_share"),
        "sign_ok_share": best_eng.get("sign_ok_share"),
        "forecast_rmse": best_eng.get("forecast_rmse"),
        "verdict": verdict,
        "charts": charts,
        "runtime_sec": round((datetime.now(timezone.utc) - t0).total_seconds(), 2),
        "_best_history": best_eng["history"],
        "_all_equations": [e for eng in stage_engines for e in eng["equations"]],
        "_all_forecasts": [e for eng in stage_engines for e in eng["forecasts"]],
        "_all_imbalances": [e for eng in stage_engines for e in eng["imbalances"]],
        "_all_elasticities": [e for eng in stage_engines for e in eng["elasticities"]],
        "_stage_engines": stage_engines,
    }
    return payload


def render_markdown(payload: dict[str, Any]) -> str:
    v = payload.get("verdict") or {}
    tip = payload.get("tip") or {}
    panel = payload.get("panel") or {}
    lines = [
        "# Gold Valuation V5 — Supply/Demand Market-Clearing Engine",
        "",
        f"Generated: `{payload.get('generated_at')}`",
        "",
        f"**Model:** `{payload.get('model_id')}`",
        "",
        "**Research only — not deployed. Not GRAM. No COT. No shadow-currency.**",
        "",
        f"**Verdict: {v.get('verdict')}**",
        "",
        v.get("narrative") or "",
        "",
        "## Equation",
        "",
        f"`{payload.get('equation')}`",
        "",
        f"**Best stage:** {payload.get('best_stage')}",
        "",
        "## Panel",
        "",
        f"- Quarters: **{panel.get('n_quarters')}** ({panel.get('start')} → {panel.get('end')})",
        f"- GDT lag: {panel.get('publication_lag_days')} days",
        f"- Note: {panel.get('note')}",
        f"- Counts: `{panel.get('gdt_counts')}`",
        "",
        "## Stage ranking",
        "",
        "| Stage | Score | OOS Q | Signs | Bounds | Spread13 | EC | Leakage |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for r in payload.get("ranking") or []:
        lines.append(
            f"| {r.get('stage')} | {r.get('score')} | {r.get('n_oos_quarters')} | "
            f"{r.get('sign_ok_share')} | {r.get('bound_hit_share')} | {r.get('spread13_pp')} | "
            f"{r.get('ec_corr')} | {r.get('identity_leakage')} |"
        )
    lines.extend(["", "## Tip card", ""])
    if tip:
        lines.append("```")
        for k in [
            "solver_status",
            "jewellery_or_fabrication",
            "technology",
            "bar_coin",
            "etf_investment",
            "investment_aggregate",
            "central_bank",
            "mine_supply",
            "recycling_supply",
            "net_imbalance_tonnes",
            "raw_delta_log_price",
            "raw_fair_value",
            "fair_value",
            "market_price",
            "deviation_pct",
            "premium_discount",
        ]:
            if tip.get(k) is not None:
                lines.append(f"{k:<28}{tip.get(k)}")
        lines.append("```")
        lines.append(f"Bucket: **{tip.get('bucket') or 'MODEL INVALID'}**")
        lines.append(f"Solver: **{tip.get('solver_status') or 'UNKNOWN'}**")
    lines.extend(
        [
            "",
            "## Validation",
            "",
            f"- Spread 13w: `{payload.get('spread_13w')}`",
            f"- Spread 52w: `{payload.get('spread_52w')}`",
            f"- Spread 104w: `{payload.get('spread_104w')}`",
            f"- Error correction: `{payload.get('error_correction')}`",
            f"- Price identity leakage: `{payload.get('price_identity_leakage')}`",
            f"- Forecast RMSE: `{payload.get('forecast_rmse')}`",
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
            "- Outputs only under `data/audits/gold_market_clearing_valuation/`",
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
    JSON_OUT.write_text(json.dumps(public, indent=2, ensure_ascii=False), encoding="utf-8")
    REPORT_MD.write_text(render_markdown(public), encoding="utf-8")

    def _write(path: Path, rows: list[dict[str, Any]]) -> None:
        if not rows:
            path.write_text("", encoding="utf-8")
            return
        # Union keys
        fields: list[str] = []
        seen = set()
        for r in rows:
            for k in r:
                if k not in seen:
                    seen.add(k)
                    fields.append(k)
        with path.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)

    hist = list(payload.get("_best_history") or [])
    _write(HISTORY_CSV, hist)
    _write(SECTOR_EQ_CSV, list(payload.get("_all_equations") or []))
    _write(SECTOR_FC_CSV, list(payload.get("_all_forecasts") or []))
    _write(IMBALANCE_CSV, list(payload.get("_all_imbalances") or []))
    _write(ELAS_CSV, list(payload.get("_all_elasticities") or []))
    _write(FWD_CSV, list(payload.get("forward_returns") or []))
    _write(MISSING_Q_CSV, list(payload.get("missing_quarter_audit") or []))
    _write_episodes([h for h in hist if h.get("solve_ok")], EPISODE_CSV)
    RECON_MD.write_text(_render_reconciliation(hist), encoding="utf-8")

    return {
        "report": str(REPORT_MD.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "history_csv": str(HISTORY_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "sector_equations_csv": str(SECTOR_EQ_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "sector_forecasts_csv": str(SECTOR_FC_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "imbalance_csv": str(IMBALANCE_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "elasticities_csv": str(ELAS_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "forward_csv": str(FWD_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "episodes_csv": str(EPISODE_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "missing_quarters_csv": str(MISSING_Q_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "reconciliation_md": str(RECON_MD.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "json": str(JSON_OUT.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "charts_dir": str(CHART_DIR.relative_to(PROJECT_ROOT)).replace("\\", "/"),
    }


def _render_reconciliation(history: list[dict[str, Any]]) -> str:
    """Phase-8 style identity proof for key dates."""
    if not history:
        return "# Gold Market-Clearing Reconciliation\n\nNo history.\n"
    by_q = {str(h.get("date"))[:10]: h for h in history}
    targets = []
    dates = sorted(by_q)
    if dates:
        targets.append(("earliest", dates[0]))
    for label, needle in [
        ("2013_peak_era", "2013-06-30"),
        ("2020", "2020-06-30"),
        ("2022", "2022-06-30"),
        ("latest", dates[-1] if dates else None),
    ]:
        if not needle:
            continue
        # nearest available
        if needle in by_q:
            targets.append((label, needle))
        else:
            near = [d for d in dates if d[:4] == needle[:4]]
            if near:
                targets.append((label, near[len(near) // 2]))

    lines = [
        "# Gold Market-Clearing Reconciliation",
        "",
        "Identities checked per observation:",
        "",
        "- `imbalance = total_demand - total_supply`",
        "- `raw_delta_log_price = imbalance / net_elasticity` (when net_elas > 0)",
        "- `raw_fair_value = reference_price × exp(raw_delta_log_price)`",
        "- `displayed deviation = 100 × (price - fair_value) / fair_value` (OK solves only)",
        "",
    ]
    for label, q in targets:
        h = by_q.get(q)
        if not h:
            continue
        d0 = float(h.get("total_demand") or h.get("D0") or 0)
        s0 = float(h.get("total_supply") or h.get("S0") or 0)
        imb = float(h.get("imbalance") or 0)
        net = h.get("net_elasticity")
        raw_d = h.get("raw_delta_log_price")
        px = float(h.get("gold_price") or h.get("reference_price") or 0)
        raw_fv = h.get("raw_fair_value")
        fv = h.get("fair_value")
        dev = h.get("deviation_pct")
        status = h.get("solver_status")
        imb_ok = abs((d0 - s0) - imb) < 0.05
        delta_ok = True
        fv_ok = True
        dev_ok = True
        if status == "OK" and net not in (None, "") and float(net) > 1e-6 and raw_d is not None:
            delta_ok = abs(float(raw_d) - imb / float(net)) < 1e-4
            if raw_fv is not None:
                fv_ok = abs(float(raw_fv) - px * math.exp(float(raw_d))) < 0.05
            if fv is not None and dev is not None and float(fv) > 0:
                calc_dev = 100.0 * (px - float(fv)) / float(fv)
                dev_ok = abs(calc_dev - float(dev)) < 0.01
                bucket_ok = h.get("bucket") == _classify_deviation(float(dev))
            else:
                bucket_ok = False
        else:
            bucket_ok = h.get("bucket") is None
            delta_ok = status != "OK"  # not required when invalid
            fv_ok = fv is None
            dev_ok = dev is None
        lines.extend(
            [
                f"## {label} (`{q}`)",
                "",
                f"- solver_status: `{status}`",
                f"- demand={d0:.3f} supply={s0:.3f} imbalance={imb:.3f} · identity={imb_ok}",
                f"- net_elasticity={net} raw_delta={raw_d} · delta_identity={delta_ok}",
                f"- price={px} raw_fv={raw_fv} fair_value={fv} · fv_identity={fv_ok}",
                f"- deviation_pct={dev} bucket={h.get('bucket')} · dev_identity={dev_ok} bucket_ok={bucket_ok}",
                f"- **PASS={all([imb_ok, delta_ok, fv_ok, dev_ok, bucket_ok])}**",
                "",
            ]
        )
    return "\n".join(lines)
