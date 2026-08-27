"""Gold Valuation — Phase 2 Macro + Physical Discovery (research only).

Transform-aware discovery: each driver is evaluated under multiple
point-in-time-safe transforms before combination. Raw levels are NOT forced.

Does NOT modify published Gold valuation, NG, COT, Stage 4, Scanner,
Inspector, Seasonality, or dashboard wiring.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.fx.fx_macro_history import load_fred_daily_map
from hptl.valuation.energy_natural_gas_valuation_v1 import _multivariate_ols
from hptl.valuation.gold_macro_tier1_discovery import (
    CORR_REDUNDANT,
    VIF_HIGH,
    _align,
    _asof_series,
    _load_dx_daily,
    _pearson,
    _vif,
)
from hptl.valuation.metals_institutional_drivers import (
    _load_cache_series,
    _weekly_from_daily,
)

CB_CACHE_REL = "data/cache/metals_drivers/wgc_cb_gold_net_purchases.json"


def _load_cache_doc(rel_path: str) -> dict[str, Any]:
    path = PROJECT_ROOT / rel_path
    if not path.exists():
        return {}
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return doc if isinstance(doc, dict) else {}


def _load_monthly_cb() -> list[tuple[str, float]]:
    daily = _load_cache_series(CB_CACHE_REL)
    return sorted((d, v) for d, v in daily.items())


def _engineer_monthly_cb(
    monthly: list[tuple[str, float]], engineer: str
) -> dict[str, float]:
    dates = [d for d, _ in monthly]
    values = [v for _, v in monthly]
    out: dict[str, float] = {}
    for i, d in enumerate(dates):
        if engineer == "level":
            out[d] = values[i]
        elif engineer == "roll12" and i >= 11:
            out[d] = sum(values[i - 11 : i + 1])
        elif engineer == "lag1" and i >= 1:
            out[d] = values[i - 1]
        elif engineer == "yoy" and i >= 12:
            out[d] = values[i] - values[i - 12]
    return out
from hptl.valuation.metals_valuation_v1 import (
    MODEL_ID as PUBLISHED_MODEL_ID,
    REAL_YIELD_SERIES,
    _build_weekly_panel,
)
from hptl.valuation.ng_driver_validation_phase2_production import (
    DM_ALPHA,
    MIN_TRAIN,
    STEP,
    _diebold_mariano_pvalue,
    _eval_model,
)
from hptl.valuation.ng_driver_validation_phase3_lng import _regime_stability

AUDIT_DIR = PROJECT_ROOT / "data" / "audits" / "gold_phase2_macro_physical"
JSON_OUT = AUDIT_DIR / "gold_phase2_macro_physical_research.json"
MD_OUT = AUDIT_DIR / "gold_phase2_macro_physical_research.md"

ETF_CACHE_REL = "data/cache/metals_drivers/gold_etf_holdings.json"
ETF_FLOWS_CACHE_REL = "data/cache/metals_drivers/gold_etf_flows.json"

# Past-only transforms — no full-sample z-score leakage.
RATE_TRANSFORMS = ("level", "chg_4w", "chg_12w", "yoy_pp", "zscore_156_past", "trend_dev_104")
DOLLAR_TRANSFORMS = ("log_level", "chg_4w", "chg_12w", "yoy_pct", "zscore_156_past")
ETF_TRANSFORMS = ("log_holdings", "holdings_level", "flow_4w", "flow_12w", "yoy_pct", "zscore_156_past")
CB_TRANSFORMS = ("level", "roll12", "yoy", "lag1")


def _finite(xs: list[float | None]) -> list[float]:
    return [float(v) for v in xs if v is not None and math.isfinite(float(v))]


def _zscore_past(xs: list[float], window: int = 156) -> list[float | None]:
    n = len(xs)
    out: list[float | None] = [None] * n
    for i in range(n):
        if i < window:
            continue
        hist = xs[i - window : i]
        mu = sum(hist) / len(hist)
        var = sum((v - mu) ** 2 for v in hist) / len(hist)
        sd = math.sqrt(var) if var > 1e-18 else None
        if sd is None:
            continue
        out[i] = (xs[i] - mu) / sd
    return out


def _trend_dev(xs: list[float], window: int = 104) -> list[float | None]:
    n = len(xs)
    out: list[float | None] = [None] * n
    for i in range(n):
        if i < window:
            continue
        pts = [(float(j), float(xs[j])) for j in range(i - window, i)]
        nn = float(len(pts))
        sx = sum(p[0] for p in pts)
        sy = sum(p[1] for p in pts)
        sxx = sum(p[0] * p[0] for p in pts)
        sxy = sum(p[0] * p[1] for p in pts)
        den = nn * sxx - sx * sx
        if abs(den) < 1e-12:
            continue
        slope = (nn * sxy - sx * sy) / den
        intercept = (sy - slope * sx) / nn
        out[i] = float(xs[i]) - (intercept + slope * float(i))
    return out


def _transforms_from_level(
    level: list[float],
    *,
    kind: str,
) -> dict[str, list[float | None]]:
    """Build PIT-safe transforms. kind in {rate, dollar, etf}."""
    n = len(level)
    chg4: list[float | None] = [None] * n
    chg12: list[float | None] = [None] * n
    yoy: list[float | None] = [None] * n
    for i in range(n):
        if i >= 4:
            chg4[i] = level[i] - level[i - 4]
        if i >= 12:
            chg12[i] = level[i] - level[i - 12]
        if i >= 52 and abs(level[i - 52]) > 1e-12:
            if kind == "rate":
                yoy[i] = level[i] - level[i - 52]  # pp
            else:
                yoy[i] = 100.0 * (level[i] - level[i - 52]) / abs(level[i - 52])

    out: dict[str, list[float | None]] = {
        "chg_4w": chg4,
        "chg_12w": chg12,
        "zscore_156_past": _zscore_past(level, 156),
    }
    if kind == "rate":
        out["level"] = list(level)  # type: ignore[assignment]
        out["yoy_pp"] = yoy
        out["trend_dev_104"] = _trend_dev(level, 104)
    elif kind == "dollar":
        out["log_level"] = [math.log(v) if v > 0 else None for v in level]
        out["yoy_pct"] = yoy
    elif kind == "etf":
        out["holdings_level"] = list(level)  # type: ignore[assignment]
        out["log_holdings"] = [math.log(v) if v > 0 else None for v in level]
        out["yoy_pct"] = yoy
        out["flow_4w"] = chg4
        out["flow_12w"] = chg12
    return out


def _score_transform(model: dict[str, Any], feature: str, expected: str) -> dict[str, Any]:
    coef = (model.get("coefficients") or {}).get(feature)
    stab = (model.get("coefficient_stability") or {}).get(feature) or {}
    sign_ok = coef is not None and (
        (expected == "negative" and coef < 0) or (expected == "positive" and coef > 0)
    )
    flip = bool(stab.get("sign_flip"))
    rmse = model.get("oos_rmse")
    # Lexicographic preference: sign → stability → lower RMSE (RMSE never overrides bad economics)
    rank = (
        0 if sign_ok else 1,
        0 if not flip else 1,
        float(rmse) if rmse is not None else 999.0,
    )
    return {
        "sign_ok": sign_ok,
        "sign_flip": flip,
        "coef": coef,
        "oos_rmse": rmse,
        "oos_mae": model.get("oos_mae"),
        "oos_r2": model.get("oos_r2"),
        "p_value": (model.get("p_values") or {}).get(feature),
        "rank_key": rank,
    }


def _audit_series(
    *,
    driver_id: str,
    label: str,
    category: str,
    symbol: str,
    provider: str,
    frequency: str,
    release_cadence: str,
    revisions: str,
    pit: str,
    dates: list[str],
    series: list[float | None],
    available: bool,
    omit_reason: str | None = None,
) -> dict[str, Any]:
    finite = [(dates[i], series[i]) for i in range(len(dates)) if series[i] is not None]
    return {
        "driver_id": driver_id,
        "label": label,
        "category": category,
        "symbol": symbol,
        "provider": provider,
        "frequency": frequency,
        "release_cadence": release_cadence,
        "revisions_policy": revisions,
        "point_in_time_safety": pit,
        "available": available,
        "omit_reason": omit_reason,
        "n_aligned": len(finite),
        "first_aligned": finite[0][0] if finite else None,
        "last_aligned": finite[-1][0] if finite else None,
        "current_value": finite[-1][1] if finite else None,
        "missing_on_panel": len(dates) - len(finite),
        "history_coverage": (
            f"{finite[0][0]} → {finite[-1][0]} (n={len(finite)})" if finite else "none"
        ),
    }


def run_gold_phase2_discovery(*, as_of_week: str | None = None) -> dict[str, Any]:
    t0 = datetime.now(timezone.utc)
    generated_at = t0.replace(microsecond=0).isoformat()

    panel = _build_weekly_panel("Gold")
    if as_of_week:
        panel = [o for o in panel if o.date <= str(as_of_week)[:10]]
    dates = [o.date for o in panel]
    y_all = [math.log(o.price) for o in panel]

    # --- Load raw series ---
    dgs2 = load_fred_daily_map("DGS2")
    dgs30 = load_fred_daily_map("DGS30")
    dfii = load_fred_daily_map(REAL_YIELD_SERIES)
    dx = _load_dx_daily()

    etf_doc = _load_cache_doc(ETF_CACHE_REL)
    etf_hold_daily = _load_cache_series(ETF_CACHE_REL)
    etf_flow_daily = _load_cache_series(ETF_FLOWS_CACHE_REL)

    cb_doc = _load_cache_doc(CB_CACHE_REL)
    cb_monthly = _load_monthly_cb()
    cb_available = len(cb_monthly) >= 24

    # Mine production proxy (config mentions SA proxy — not global mine output)
    mine_daily = load_fred_daily_map("ZAFPROINDMISMEI")
    mine_ok = len(mine_daily) >= 52

    def _require_level(asof: list[float | None]) -> list[float] | None:
        if any(v is None for v in asof):
            # forward fill gaps of small size from asof already; if any None, drop driver
            if sum(1 for v in asof if v is None) > len(asof) * 0.05:
                return None
        out: list[float] = []
        last = None
        for v in asof:
            if v is not None:
                last = float(v)
            if last is None:
                return None
            out.append(last)
        return out

    s2 = _require_level(_asof_series(dgs2, dates))
    s30 = _require_level(_asof_series(dgs30, dates))
    sry = _require_level(_asof_series(dfii, dates))
    sdx = _require_level(_asof_series(dx, dates))
    setf = _require_level(_asof_series(etf_hold_daily, dates)) if etf_hold_daily else None

    audits: list[dict[str, Any]] = []
    audits.append(
        _audit_series(
            driver_id="us_2y_yield",
            label="US 2-Year Treasury Yield",
            category="Macro",
            symbol="DGS2",
            provider="FRED",
            frequency="Daily",
            release_cadence="Daily Treasury CM via FRED",
            revisions="Market yield; low delayed revision risk",
            pit="Weekly as-of join; transforms past-only",
            dates=dates,
            series=_asof_series(dgs2, dates),
            available=bool(s2),
        )
    )
    audits.append(
        _audit_series(
            driver_id="us_30y_yield",
            label="US 30-Year Treasury Yield",
            category="Macro",
            symbol="DGS30",
            provider="FRED",
            frequency="Daily",
            release_cadence="Daily Treasury CM via FRED",
            revisions="Market yield; low delayed revision risk",
            pit="Weekly as-of join; transforms past-only",
            dates=dates,
            series=_asof_series(dgs30, dates),
            available=bool(s30),
        )
    )
    audits.append(
        _audit_series(
            driver_id="us_10y_real_yield",
            label="US 10-Year Real Yield",
            category="Macro",
            symbol="DFII10",
            provider="FRED",
            frequency="Daily",
            release_cadence="Daily TIPS real yield via FRED",
            revisions="Market yield; low delayed revision risk",
            pit="Weekly as-of join; transforms past-only",
            dates=dates,
            series=_asof_series(dfii, dates),
            available=bool(sry),
        )
    )
    audits.append(
        _audit_series(
            driver_id="ice_dxy",
            label="ICE DXY / DX futures",
            category="Macro",
            symbol="DX",
            provider="Price store / ICE DX",
            frequency="Daily",
            release_cadence="Futures session closes",
            revisions="Futures closes; no EIA-style revisions",
            pit="Weekly as-of join; Broad USD excluded a priori when DXY present",
            dates=dates,
            series=_asof_series(dx, dates),
            available=bool(sdx),
        )
    )

    # ETF audit — prefer flows cache; else derive flows from holdings
    etf_series_for_audit = _asof_series(etf_hold_daily, dates) if etf_hold_daily else [None] * len(dates)
    audits.append(
        _audit_series(
            driver_id="etf_demand",
            label="Gold ETF holdings / flows (GLD)",
            category="Demand",
            symbol="GLD shares outstanding",
            provider=str(etf_doc.get("source_name") or "State Street Global Advisors"),
            frequency=str(etf_doc.get("frequency") or "daily"),
            release_cadence="Daily NAV history (SSGA); flows derived as Δ holdings when native flows absent",
            revisions="Shares outstanding restated rarely; treated as as-of",
            pit="Weekly as-of; flow transforms use past differences only",
            dates=dates,
            series=etf_series_for_audit,
            available=bool(setf),
            omit_reason=None if setf else "ETF holdings cache unavailable",
        )
    )

    cb_asof: list[float | None] = [None] * len(dates)
    if cb_available:
        # default audit on roll12 engineered monthly → weekly
        eng = _engineer_monthly_cb(cb_monthly, "roll12")
        cb_weekly = _weekly_from_daily(eng, dates)
        cb_asof = [cb_weekly.get(d) for d in dates]
    audits.append(
        _audit_series(
            driver_id="cb_net_purchases",
            label="Central-bank net gold purchases",
            category="Demand",
            symbol="WGC / IMF IFS CB net (tonnes)",
            provider="World Gold Council (manual/authenticated) → local cache",
            frequency=str(cb_doc.get("frequency") or "monthly"),
            release_cadence=(
                "WGC monthly files ~2 months in arrears; Goldhub download currently 403 "
                "without login. Local cache required."
            ),
            revisions="IMF IFS / WGC can revise late reporters",
            pit="Monthly engineered then as-of to Friday weeks; prefer roll12 over noisy level",
            dates=dates,
            series=cb_asof,
            available=cb_available,
            omit_reason=(
                None
                if cb_available
                else (
                    "Cache missing and WGC xlsx download returns 403. "
                    "Place series at data/cache/metals_drivers/wgc_cb_gold_net_purchases.json "
                    "or data/manual/metals/gold_cb_purchases.csv then re-run."
                )
            ),
        )
    )

    mine_asof = _asof_series(mine_daily, dates) if mine_ok else [None] * len(dates)
    audits.append(
        _audit_series(
            driver_id="mine_production",
            label="Global mine production",
            category="Supply",
            symbol="(no reliable global weekly/monthly series in HPTL)",
            provider="WGC / USGS (annual) — not wired",
            frequency="Typically annual",
            release_cadence="Annual / lagging",
            revisions="Annual restatements common",
            pit="N/A — omitted",
            dates=dates,
            series=mine_asof,
            available=False,
            omit_reason=(
                "No reliable global mine-production history with weekly/monthly cadence "
                "and PIT-safe join is available in-repo. FRED ZAFPROINDMISMEI "
                f"{'loaded but is SA industrial proxy, not global mine output — omitted' if mine_ok else 'unavailable'}. "
                "Recycling supply: not available — omitted."
            ),
        )
    )
    audits.append(
        {
            "driver_id": "recycling_supply",
            "label": "Recycling supply",
            "category": "Supply",
            "symbol": "n/a",
            "available": False,
            "history_coverage": "none",
            "omit_reason": "No reliable recycling series with sufficient history in HPTL — omitted.",
        }
    )

    # --- Per-driver transform tournaments (univariate on log price) ---
    driver_specs: list[dict[str, Any]] = []
    if s2:
        driver_specs.append(
            {
                "id": "us_2y_yield",
                "category": "Macro",
                "expected_sign": "negative",
                "transforms": _transforms_from_level(s2, kind="rate"),
                "transform_ids": RATE_TRANSFORMS,
            }
        )
    if s30:
        driver_specs.append(
            {
                "id": "us_30y_yield",
                "category": "Macro",
                "expected_sign": "negative",
                "transforms": _transforms_from_level(s30, kind="rate"),
                "transform_ids": RATE_TRANSFORMS,
            }
        )
    if sry:
        driver_specs.append(
            {
                "id": "us_10y_real_yield",
                "category": "Macro",
                "expected_sign": "negative",
                "transforms": _transforms_from_level(sry, kind="rate"),
                "transform_ids": RATE_TRANSFORMS,
            }
        )
    if sdx:
        driver_specs.append(
            {
                "id": "ice_dxy",
                "category": "Macro",
                "expected_sign": "negative",
                "transforms": _transforms_from_level(sdx, kind="dollar"),
                "transform_ids": DOLLAR_TRANSFORMS,
            }
        )
    if setf:
        etf_tf = _transforms_from_level(setf, kind="etf")
        # If native flows exist, prefer them for flow_* keys
        if etf_flow_daily:
            flow_lvl = _require_level(_asof_series(etf_flow_daily, dates))
            if flow_lvl:
                etf_tf["flow_4w"] = _transforms_from_level(flow_lvl, kind="etf")["chg_4w"]
                etf_tf["flow_12w"] = _transforms_from_level(flow_lvl, kind="etf")["chg_12w"]
        driver_specs.append(
            {
                "id": "etf_demand",
                "category": "Demand",
                "expected_sign": "positive",
                "transforms": etf_tf,
                "transform_ids": ETF_TRANSFORMS,
            }
        )
    if cb_available:
        cb_tf: dict[str, list[float | None]] = {}
        for eng in CB_TRANSFORMS:
            eng_map = _engineer_monthly_cb(cb_monthly, eng if eng != "level" else "level")
            weekly = _weekly_from_daily(eng_map, dates)
            col = [weekly.get(d) for d in dates]
            cb_tf[eng] = col  # type: ignore[assignment]
        driver_specs.append(
            {
                "id": "cb_net_purchases",
                "category": "Demand",
                "expected_sign": "positive",
                "transforms": cb_tf,
                "transform_ids": CB_TRANSFORMS,
            }
        )

    transform_tournaments: list[dict[str, Any]] = []
    selected_features: dict[str, list[float | None]] = {}
    selected_meta: dict[str, dict[str, Any]] = {}

    for spec in driver_specs:
        candidates: list[dict[str, Any]] = []
        for tid in spec["transform_ids"]:
            series = spec["transforms"].get(tid)
            if not series:
                continue
            feat = f"{spec['id']}__{tid}"
            d_a, y_a, x_a = _align(dates, y_all, {feat: series}, [feat])
            if len(y_a) < MIN_TRAIN + 40:
                candidates.append(
                    {"transform": tid, "ok": False, "reason": f"short history n={len(y_a)}"}
                )
                continue
            m = _eval_model(
                name=f"uni_{feat}",
                dates=d_a,
                y=y_a,
                feature_names=[feat],
                cols=[x_a[feat]],
                expected_signs={feat: spec["expected_sign"]},
            )
            sc = _score_transform(m, feat, spec["expected_sign"])
            candidates.append(
                {
                    "transform": tid,
                    "ok": True,
                    "feature": feat,
                    "n_aligned": len(y_a),
                    **{k: v for k, v in sc.items() if k != "rank_key"},
                    "rank_key": list(sc["rank_key"]),
                }
            )
        ok_c = [c for c in candidates if c.get("ok")]
        ok_c.sort(key=lambda c: tuple(c["rank_key"]))
        # Require sensible sign AND no walk-forward sign flip to enter combined model.
        # RMSE is only a tie-breaker among economically valid transforms.
        stable_ok = [
            c for c in ok_c if c.get("sign_ok") and not c.get("sign_flip")
        ]
        chosen = stable_ok[0] if stable_ok else None
        best_econ = next((c for c in ok_c if c.get("sign_ok")), None)

        transform_tournaments.append(
            {
                "driver_id": spec["id"],
                "category": spec["category"],
                "expected_sign": spec["expected_sign"],
                "candidates": [
                    {k: v for k, v in c.items() if k != "rank_key"} for c in candidates
                ],
                "best_transform": (chosen or best_econ or {}).get("transform")
                if (chosen or best_econ)
                else None,
                "best_feature": (chosen or {}).get("feature"),
                "selected_for_combined": bool(chosen),
                "selection_note": (
                    f"Selected {chosen['transform']} (sign OK, no flip; RMSE secondary)."
                    if chosen
                    else (
                        f"Best economic transform {best_econ['transform']} still sign-flips "
                        "in walk-forward — excluded from combined model."
                        if best_econ
                        else "No transform clears economic-sign gate — excluded from combined model."
                    )
                ),
            }
        )
        if chosen:
            tid = chosen["transform"]
            feat = chosen["feature"]
            selected_features[feat] = spec["transforms"][tid]
            selected_meta[feat] = {
                "driver_id": spec["id"],
                "category": spec["category"],
                "transform": tid,
                "expected_sign": spec["expected_sign"],
                "univariate": {k: v for k, v in chosen.items() if k != "rank_key"},
            }

    # Prefer panel real_yield/dxy from WeeklyObs for published alignment
    pub_ry = [o.real_yield for o in panel]
    pub_dxy = [math.log(o.dxy) for o in panel]
    d_p, y_p, x_p = _align(
        dates,
        y_all,
        {"real_yield": pub_ry, "log_dxy": pub_dxy},  # type: ignore[arg-type]
        ["real_yield", "log_dxy"],
    )
    published = _eval_model(
        name="published_metals_real_yield_v1",
        dates=d_p,
        y=y_p,
        feature_names=["real_yield", "log_dxy"],
        cols=[x_p["real_yield"], x_p["log_dxy"]],
        expected_signs={"real_yield": "negative", "log_dxy": "negative"},
    )

    feat_ids = list(selected_features.keys())
    if len(feat_ids) < 1:
        payload = {
            "generated_at": generated_at,
            "ok": True,
            "research_only": True,
            "economic_status": "NO_SELECTABLE_DRIVERS",
            "suitable_for_gold_v2": False,
            "dataset_audit": audits,
            "transform_tournaments": transform_tournaments,
            "plain_english": (
                "No driver transform cleared economic-sign gates. "
                "Cannot propose a combined Gold V2. Published model unchanged."
            ),
            "runtime_seconds": round((datetime.now(timezone.utc) - t0).total_seconds(), 2),
        }
        return json.loads(json.dumps(payload, default=str))

    d_c, y_c, x_c = _align(dates, y_all, selected_features, feat_ids)
    cols_c = [x_c[f] for f in feat_ids]
    expected = {f: selected_meta[f]["expected_sign"] for f in feat_ids}

    combined = _eval_model(
        name="phase2_combined_selected_transforms",
        dates=d_c,
        y=y_c,
        feature_names=feat_ids,
        cols=cols_c,
        expected_signs=expected,
    )

    vifs = {
        f: (round(v, 3) if v is not None else None)
        for f, v in ((f, _vif(cols_c, i)) for i, f in enumerate(feat_ids))
    }
    pairwise = []
    for i, a in enumerate(feat_ids):
        for b in feat_ids[i + 1 :]:
            corr = _pearson(x_c[a], x_c[b])
            pairwise.append(
                {
                    "a": a,
                    "b": b,
                    "corr": round(corr, 4) if corr is not None else None,
                    "redundant": bool(corr is not None and abs(corr) >= CORR_REDUNDANT),
                }
            )

    # Drop-one contribution
    full_rmse = combined.get("oos_rmse")
    drop_one: dict[str, Any] = {}
    for f in feat_ids:
        keep = [x for x in feat_ids if x != f]
        if not keep:
            drop_one[f] = {"oos_contrib_rmse_pct": None}
            continue
        m = _eval_model(
            name=f"drop_{f}",
            dates=d_c,
            y=y_c,
            feature_names=keep,
            cols=[x_c[x] for x in keep],
            expected_signs={x: expected[x] for x in keep},
        )
        rmse = m.get("oos_rmse")
        contrib = None
        if full_rmse and rmse is not None and full_rmse > 0:
            contrib = 100.0 * (rmse - full_rmse) / full_rmse
        drop_one[f] = {
            "oos_rmse_without": rmse,
            "oos_contrib_rmse_pct": round(contrib, 2) if contrib is not None else None,
        }

    # Redundancy prune among rate cluster / high corr
    weaker: set[str] = set()
    redundant_drops: list[dict[str, Any]] = []
    coefs = combined.get("coefficients") or {}
    for pair in pairwise:
        if not pair.get("redundant"):
            continue
        a, b = pair["a"], pair["b"]
        sign_a = (coefs.get(a) or 0) < 0 if expected[a] == "negative" else (coefs.get(a) or 0) > 0
        sign_b = (coefs.get(b) or 0) < 0 if expected[b] == "negative" else (coefs.get(b) or 0) > 0
        ca = drop_one.get(a, {}).get("oos_contrib_rmse_pct") or -999
        cb = drop_one.get(b, {}).get("oos_contrib_rmse_pct") or -999
        ta = abs(float((combined.get("t_stats") or {}).get(a) or 0))
        tb = abs(float((combined.get("t_stats") or {}).get(b) or 0))
        score_a = (1 if sign_a else 0, ca, ta)
        score_b = (1 if sign_b else 0, cb, tb)
        keep, drop = (a, b) if score_a >= score_b else (b, a)
        weaker.add(drop)
        redundant_drops.append(
            {"pair": [a, b], "corr": pair["corr"], "keep": keep, "drop": drop}
        )

    retained = [f for f in feat_ids if f not in weaker]
    # Sequential VIF prune
    while len(retained) > 2:
        cols_r = [x_c[f] for f in retained]
        vif_r = {f: _vif(cols_r, i) for i, f in enumerate(retained)}
        worst = max(retained, key=lambda f: vif_r.get(f) or 0.0)
        if (vif_r.get(worst) or 0) < VIF_HIGH:
            break
        # Prefer dropping a rate twin before dollar/demand
        rate_like = [f for f in retained if "yield" in f]
        drop_f = rate_like[0] if len(rate_like) > 1 else worst
        weaker.add(drop_f)
        redundant_drops.append(
            {
                "pair": [drop_f],
                "corr": None,
                "keep": [f for f in retained if f != drop_f],
                "drop": drop_f,
                "reason": f"VIF prune (VIF={vif_r.get(drop_f)})",
            }
        )
        retained = [f for f in retained if f != drop_f]

    # Re-fit retained; require all signs OK else reject suitability
    best = _eval_model(
        name="phase2_best_retained",
        dates=d_c,
        y=y_c,
        feature_names=retained,
        cols=[x_c[f] for f in retained],
        expected_signs={f: expected[f] for f in retained},
    )
    best_coefs = best.get("coefficients") or {}
    all_signs_ok = all(
        (
            (expected[f] == "negative" and (best_coefs.get(f) or 0) < 0)
            or (expected[f] == "positive" and (best_coefs.get(f) or 0) > 0)
        )
        for f in retained
    )
    any_flip = any(
        bool(((best.get("coefficient_stability") or {}).get(f) or {}).get("sign_flip"))
        for f in retained
    )

    # Ranking table for all candidate drivers (including omitted)
    ranking: list[dict[str, Any]] = []
    rank_ids = [
        ("us_2y_yield", "Macro", "negative"),
        ("us_30y_yield", "Macro", "negative"),
        ("us_10y_real_yield", "Macro", "negative"),
        ("ice_dxy", "Macro", "negative"),
        ("cb_net_purchases", "Demand", "positive"),
        ("etf_demand", "Demand", "positive"),
        ("mine_production", "Supply", "negative"),
    ]
    for did, cat, exp in rank_ids:
        tour = next((t for t in transform_tournaments if t["driver_id"] == did), None)
        audit = next((a for a in audits if a.get("driver_id") == did), {})
        feat = next((f for f in retained if f.startswith(did + "__")), None)
        if not audit.get("available") and did in ("cb_net_purchases", "mine_production"):
            ranking.append(
                {
                    "driver": did,
                    "category": cat,
                    "sign": "n/a",
                    "stable": "n/a",
                    "independent": "n/a",
                    "recommendation": "Reject",
                    "reason": audit.get("omit_reason") or "unavailable",
                    "best_transform": None,
                }
            )
            continue
        if tour and not tour.get("selected_for_combined"):
            ranking.append(
                {
                    "driver": did,
                    "category": cat,
                    "sign": "✗",
                    "stable": "✗",
                    "independent": "✗",
                    "recommendation": "Reject",
                    "reason": tour.get("selection_note"),
                    "best_transform": tour.get("best_transform"),
                }
            )
            continue
        if feat is None:
            # selected then pruned
            dropped = next((f for f in feat_ids if f.startswith(did + "__")), None)
            ranking.append(
                {
                    "driver": did,
                    "category": cat,
                    "sign": "✓" if dropped and selected_meta.get(dropped, {}).get("univariate", {}).get("sign_ok") else "✗",
                    "stable": "✗",
                    "independent": "✗",
                    "recommendation": "Reject",
                    "reason": "Removed for redundancy / VIF after selection",
                    "best_transform": selected_meta.get(dropped or "", {}).get("transform"),
                }
            )
            continue
        coef = best_coefs.get(feat)
        sign_ok = coef is not None and (
            (exp == "negative" and coef < 0) or (exp == "positive" and coef > 0)
        )
        stab = (best.get("coefficient_stability") or {}).get(feat) or {}
        stable = not bool(stab.get("sign_flip"))
        contrib = drop_one.get(feat, {}).get("oos_contrib_rmse_pct")
        independent = bool(contrib is not None and contrib > 0) and (vifs.get(feat) or 0) < VIF_HIGH
        if sign_ok and stable and independent:
            rec = "Promote"
            reason = "Clears sign, stability, independence, and walk-forward contribution gates."
        elif sign_ok and stable:
            rec = "Keep Experimental"
            reason = "Economically sensible but weak independence / contribution or high VIF."
        else:
            rec = "Reject"
            reason = "Fails sign or stability in retained combined model."
        ranking.append(
            {
                "driver": did,
                "category": cat,
                "sign": "✓" if sign_ok else "✗",
                "stable": "✓" if stable else "✗",
                "independent": "✓" if independent else "✗",
                "recommendation": rec,
                "reason": reason,
                "best_transform": selected_meta[feat]["transform"],
                "coefficient": coef,
                "vif": vifs.get(feat),
                "oos_contrib_rmse_pct": contrib,
            }
        )

    promote_feats = [
        f
        for f in retained
        if any(
            r["driver"] == selected_meta[f]["driver_id"] and r["recommendation"] == "Promote"
            for r in ranking
        )
    ]

    # If not all signs OK in best, suitability fails
    suitable = bool(all_signs_ok and not any_flip and len(promote_feats) >= 2)

    # DM kitchen vs published on overlapping indices within combined sample
    pub_on = _eval_model(
        name="published_on_phase2_sample",
        dates=d_c,
        y=y_c,
        feature_names=["real_yield", "log_dxy"],
        cols=[
            [float(pub_ry[dates.index(d)]) for d in d_c],
            [float(pub_dxy[dates.index(d)]) for d in d_c],
        ],
        expected_signs={"real_yield": "negative", "log_dxy": "negative"},
    )
    idx_p = set(pub_on.get("_indices") or [])
    idx_b = set(best.get("_indices") or [])
    common = sorted(idx_p & idx_b)
    map_p = {
        i: e
        for i, e in zip(pub_on.get("_indices") or [], pub_on.get("_squared_errors") or [])
    }
    map_b = {
        i: e for i, e in zip(best.get("_indices") or [], best.get("_squared_errors") or [])
    }
    se_p = [map_p[i] for i in common]
    se_b = [map_b[i] for i in common]
    dm = _diebold_mariano_pvalue(se_p, se_b)
    dm["interprets"] = (
        "Positive mean_loss_diff means Phase-2 retained model has lower MSE than published."
    )

    coef_path: list[float] = []
    t = MIN_TRAIN
    n = len(y_c)
    while t < n:
        beta, r2 = _multivariate_ols(y_c[:t], [x_c[f][:t] for f in retained])
        if beta and len(beta) >= 2:
            coef_path.append(float(beta[1]))
        t += STEP
    regime = _regime_stability(
        dates=d_c, indices=common, se_v2=se_p, se_cand=se_b, coef_path=coef_path
    )

    intercept = best.get("intercept")
    terms = [f"{intercept:.6f}" if intercept is not None else "β0"]
    for f in retained:
        b = best_coefs.get(f)
        if b is None:
            terms.append(f"β·{f}")
        else:
            terms.append(f"{'+' if b >= 0 else '-'} {abs(b):.6f}·{f}")
    equation = "log(Gold) = " + " ".join(terms)

    interpretations = []
    for f in retained:
        meta = selected_meta[f]
        coef = best_coefs.get(f)
        interpretations.append(
            {
                "feature": f,
                "driver": meta["driver_id"],
                "transform": meta["transform"],
                "coefficient": coef,
                "interpretation": (
                    f"{meta['driver_id']} via {meta['transform']}: "
                    f"expected {meta['expected_sign']}; fitted β={coef}. "
                    + (
                        "Higher rates/stronger USD → lower gold."
                        if meta["expected_sign"] == "negative"
                        else "Stronger structural demand → higher gold."
                    )
                ),
            }
        )

    rejected = [
        r
        for r in ranking
        if r["recommendation"] == "Reject"
    ]

    if suitable:
        plain = (
            "Phase-2 retained transform-aware model clears economic signs and stability "
            "with ≥2 Promote drivers. Still research-only — do not publish until a "
            "separate wiring decision."
        )
        economic_status = "CANDIDATE_V2_RESEARCH"
    elif all_signs_ok and not any_flip:
        plain = (
            "Combined transform-aware model has sensible signs but lacks enough "
            "Promote-grade independent drivers (or CB/supply still missing). "
            "Keep Experimental — not suitable to publish as Gold V2."
        )
        economic_status = "EXPERIMENTAL_INCOMPLETE"
        suitable = False
    else:
        plain = (
            "Combined model fails economic-sign or stability gates after transform "
            "selection and redundancy pruning. Reject as Gold V2. "
            "Published metals_real_yield_v1 unchanged. "
            + (
                "Central-bank demand series is unavailable (WGC 403 / missing cache) — "
                "re-run after ingesting CB data."
                if not cb_available
                else ""
            )
        )
        economic_status = "REJECT_NOT_SUITABLE_FOR_V2"

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    payload = {
        "generated_at": generated_at,
        "ok": True,
        "phase": "gold_phase2_macro_physical_discovery",
        "research_only": True,
        "published_model_untouched": True,
        "published_model_id": PUBLISHED_MODEL_ID,
        "philosophy": (
            "Transform-aware selection: do not force every driver into a raw linear level. "
            "Economic sign and coefficient stability outrank RMSE."
        ),
        "panel": {
            "n": len(panel),
            "start": dates[0],
            "end": dates[-1],
            "n_combined_aligned": len(y_c),
        },
        "dataset_audit": audits,
        "transform_tournaments": transform_tournaments,
        "variables_removed_redundancy": redundant_drops,
        "selected_features": selected_meta,
        "full_combined_before_prune": {
            k: v for k, v in combined.items() if not k.startswith("_")
        },
        "vif_full": vifs,
        "pairwise": pairwise,
        "drop_one_oos": drop_one,
        "best_combined_model": {k: v for k, v in best.items() if not k.startswith("_")},
        "variables_retained": retained,
        "final_equation": equation,
        "economic_interpretations": interpretations,
        "driver_ranking": ranking,
        "variables_rejected": rejected,
        "published_baseline": {k: v for k, v in published.items() if not k.startswith("_")},
        "published_on_phase2_sample": {
            k: v for k, v in pub_on.items() if not k.startswith("_")
        },
        "diebold_mariano_vs_published": dm,
        "regime_stability_vs_published": {
            k: v for k, v in regime.items() if k != "halves"
        }
        | {"halves": regime.get("halves")},
        "all_signs_ok": all_signs_ok,
        "any_sign_flip": any_flip,
        "economic_status": economic_status,
        "suitable_for_gold_v2": suitable,
        "plain_english": plain,
        "broad_usd_policy": "Excluded a priori when ICE DXY is present (no dual-dollar).",
        "runtime_seconds": round(elapsed, 2),
        "files": {"json": str(JSON_OUT), "markdown": str(MD_OUT)},
        "walk_forward": {"min_train": MIN_TRAIN, "step": STEP, "dm_alpha": DM_ALPHA},
    }
    return json.loads(json.dumps(payload, default=str))


def render_markdown(payload: dict[str, Any]) -> str:
    pub = payload.get("published_on_phase2_sample") or {}
    best = payload.get("best_combined_model") or {}
    dm = payload.get("diebold_mariano_vs_published") or {}
    lines = [
        "# Gold Valuation — Phase 2 Macro + Physical Research Report",
        "",
        f"Generated: `{payload.get('generated_at')}`",
        "",
        "**Research only. No published valuation changes. No dashboard wiring.**",
        "",
        f"Philosophy: {payload.get('philosophy')}",
        f"Economic status: **{payload.get('economic_status')}**",
        f"Suitable for Gold V2 publish: **{payload.get('suitable_for_gold_v2')}**",
        "",
        "## 1. Dataset audit",
        "",
        "| Driver | Category | Symbol | Available | Coverage | Omit reason |",
        "|---|---|---|---|---|---|",
    ]
    for a in payload.get("dataset_audit") or []:
        if a.get("driver_id") == "recycling_supply" and "label=" in a:
            pass
        lines.append(
            f"| {a.get('label') or a.get('driver_id')} | {a.get('category')} | "
            f"`{a.get('symbol')}` | {a.get('available')} | {a.get('history_coverage') or '—'} | "
            f"{(a.get('omit_reason') or '—')[:80]} |"
        )

    lines += [
        "",
        f"Broad USD policy: {payload.get('broad_usd_policy')}",
        "",
        "## 2. Transform tournaments (not forced to levels)",
        "",
    ]
    for t in payload.get("transform_tournaments") or []:
        lines.append(
            f"### {t.get('driver_id')} ({t.get('category')}) — expect {t.get('expected_sign')}"
        )
        lines.append("")
        lines.append(
            f"- Selected: `{t.get('best_transform')}` → combined={t.get('selected_for_combined')}"
        )
        lines.append(f"- {t.get('selection_note')}")
        lines.append("")
        lines.append("| Transform | Sign OK | Flip | OOS RMSE | OOS R² | Coef |")
        lines.append("|---|:---:|:---:|---:|---:|---:|")
        for c in t.get("candidates") or []:
            if not c.get("ok"):
                lines.append(f"| {c.get('transform')} | — | — | — | — | {c.get('reason')} |")
                continue
            lines.append(
                f"| {c.get('transform')} | {c.get('sign_ok')} | {c.get('sign_flip')} | "
                f"{c.get('oos_rmse')} | {c.get('oos_r2')} | {c.get('coef')} |"
            )
        lines.append("")

    lines += [
        "## 3. Best combined research model",
        "",
        f"- Retained: `{payload.get('variables_retained')}`",
        f"- Equation: `{payload.get('final_equation')}`",
        f"- Redundancy removals: `{json.dumps(payload.get('variables_removed_redundancy') or [], default=str)}`",
        "",
        "### Walk-forward metrics",
        "",
        "| Model | N OOS | RMSE | MAE | OOS R² |",
        "|---|---:|---:|---:|---:|",
        f"| Published | {pub.get('n_oos')} | {pub.get('oos_rmse')} | {pub.get('oos_mae')} | {pub.get('oos_r2')} |",
        f"| Phase-2 retained | {best.get('n_oos')} | {best.get('oos_rmse')} | {best.get('oos_mae')} | {best.get('oos_r2')} |",
        "",
        f"DM vs published: p={dm.get('p_value_one_sided')} mean_diff={dm.get('mean_loss_diff')}",
        "",
        "## 4. Driver ranking",
        "",
        "| Driver | Category | Sign | Stable | Independent | Recommendation |",
        "|---|---|:---:|:---:|:---:|---|",
    ]
    for r in payload.get("driver_ranking") or []:
        lines.append(
            f"| {r.get('driver')} | {r.get('category')} | {r.get('sign')} | "
            f"{r.get('stable')} | {r.get('independent')} | {r.get('recommendation')} |"
        )

    lines += ["", "### Rejected / reasons", ""]
    for r in payload.get("variables_rejected") or []:
        lines.append(
            f"- **{r.get('driver')}**: {r.get('reason')} "
            f"(transform={r.get('best_transform')})"
        )

    lines += [
        "",
        "## 5. Economic interpretation (retained)",
        "",
    ]
    for i in payload.get("economic_interpretations") or []:
        lines.append(f"- {i.get('interpretation')}")

    lines += [
        "",
        "## 6. V2 suitability",
        "",
        payload.get("plain_english") or "",
        "",
        f"Published model remains `{payload.get('published_model_id')}` "
        f"(untouched={payload.get('published_model_untouched')}).",
        "",
        f"Runtime: {payload.get('runtime_seconds')}s",
        "",
    ]
    return "\n".join(lines) + "\n"


def write_phase2_outputs(payload: dict[str, Any]) -> dict[str, Path]:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    JSON_OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    MD_OUT.write_text(render_markdown(payload), encoding="utf-8")
    return {"json": JSON_OUT, "markdown": MD_OUT}


__all__ = [
    "run_gold_phase2_discovery",
    "write_phase2_outputs",
    "render_markdown",
]
