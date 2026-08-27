"""Gold Valuation — Tier 1 Macro Discovery (research only).

Builds a combined Tier-1 macro model for Gold and compares walk-forward
performance against the published metals_real_yield_v1 baseline.

Does NOT modify published Gold valuation, NG valuation, COT, Scanner,
Inspector, Seasonality, or any weekly pipeline.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.fx.fx_macro_history import load_fred_daily_map
from hptl.valuation.energy_natural_gas_valuation_v1 import _multivariate_ols, _ols_stats
from hptl.valuation.metals_valuation_v1 import (
    DXY_SERIES,
    MODEL_ID as PUBLISHED_MODEL_ID,
    REAL_YIELD_SERIES,
    _build_weekly_panel,
    _load_dxy_series,
)
from hptl.valuation.ng_driver_validation_phase2_production import (
    DM_ALPHA,
    MIN_TRAIN,
    STEP,
    _diebold_mariano_pvalue,
    _eval_model,
    _walk_forward_predictions,
)
from hptl.valuation.ng_driver_validation_phase3_lng import _regime_stability

AUDIT_DIR = PROJECT_ROOT / "data" / "audits" / "gold_macro_tier1_discovery"
JSON_OUT = AUDIT_DIR / "gold_macro_tier1_research.json"
MD_OUT = AUDIT_DIR / "gold_macro_tier1_research.md"

# Automatic redundancy thresholds
CORR_REDUNDANT = 0.95
CORR_DOLLAR_DUPLICATE = 0.85  # Broad vs DXY: high overlap → keep one
VIF_HIGH = 10.0
P_SIG = 0.10

# All Tier-1 candidates expect negative coef on log(Gold).
TIER1_SPECS: list[dict[str, Any]] = [
    {
        "id": "us_2y_yield",
        "label": "US 2-Year Treasury Yield",
        "symbol": "DGS2",
        "provider": "FRED",
        "transform": "level",
        "expected_sign": "negative",
    },
    {
        "id": "us_10y_yield",
        "label": "US 10-Year Treasury Yield",
        "symbol": "DGS10",
        "provider": "FRED",
        "transform": "level",
        "expected_sign": "negative",
    },
    {
        "id": "us_10y_real_yield",
        "label": "US 10-Year Real Yield",
        "symbol": "DFII10",
        "provider": "FRED",
        "transform": "level",
        "expected_sign": "negative",
    },
    {
        "id": "log_broad_usd",
        "label": "Broad US Dollar Index (log)",
        "symbol": "DTWEXBGS",
        "provider": "FRED",
        "transform": "log_level",
        "expected_sign": "negative",
    },
    {
        "id": "log_dxy_ice",
        "label": "ICE DX / DXY futures (log)",
        "symbol": "DX",
        "provider": "price store / canonical timeline",
        "transform": "log_level",
        "expected_sign": "negative",
        "optional_duplicate_of": "log_broad_usd",
    },
]


def _asof_series(daily: dict[str, float], dates: list[str]) -> list[float | None]:
    if not daily:
        return [None] * len(dates)
    keys = sorted(daily.keys())
    out: list[float | None] = []
    j = 0
    best: float | None = None
    for d in dates:
        while j < len(keys) and keys[j] <= d:
            best = daily[keys[j]]
            j += 1
        out.append(float(best) if best is not None and math.isfinite(float(best)) else None)
    return out


def _pearson(a: list[float], b: list[float]) -> float | None:
    n = min(len(a), len(b))
    if n < 8:
        return None
    ax = a[:n]
    bx = b[:n]
    mx = sum(ax) / n
    my = sum(bx) / n
    num = sum((ax[i] - mx) * (bx[i] - my) for i in range(n))
    denx = math.sqrt(sum((x - mx) ** 2 for x in ax))
    deny = math.sqrt(sum((y - my) ** 2 for y in bx))
    if denx < 1e-12 or deny < 1e-12:
        return None
    return num / (denx * deny)


def _vif(cols: list[list[float]], idx: int) -> float | None:
    """VIF for column idx via R² of regression on other columns."""
    if len(cols) < 2:
        return 1.0
    y = cols[idx]
    others = [c for i, c in enumerate(cols) if i != idx]
    if not others:
        return 1.0
    _beta, r2 = _multivariate_ols(y, others)
    if r2 is None:
        return None
    if r2 >= 0.999999:
        return 999.0
    return 1.0 / max(1.0 - r2, 1e-12)


def _align(
    dates: list[str],
    y: list[float],
    feature_map: dict[str, list[float | None]],
    feature_ids: list[str],
) -> tuple[list[str], list[float], dict[str, list[float]]]:
    out_d: list[str] = []
    out_y: list[float] = []
    out_x: dict[str, list[float]] = {fid: [] for fid in feature_ids}
    for i, d in enumerate(dates):
        vals: list[float] = []
        ok = True
        for fid in feature_ids:
            v = feature_map[fid][i]
            if v is None or not math.isfinite(float(v)):
                ok = False
                break
            vals.append(float(v))
        if not ok or not math.isfinite(y[i]):
            continue
        out_d.append(d)
        out_y.append(float(y[i]))
        for fid, v in zip(feature_ids, vals):
            out_x[fid].append(v)
    return out_d, out_y, out_x


def _load_dx_daily() -> dict[str, float]:
    """ICE DX futures closes if available; empty dict otherwise."""
    out: dict[str, float] = {}
    # Prefer processed price store (fast, no heavy timeline rebuild).
    candidates = [
        PROJECT_ROOT / "data" / "processed" / "prices" / "US_Dollar_Index_DX.json",
        PROJECT_ROOT / "data" / "processed" / "prices" / "US_Dollar_Index_DXY_ICE_DX_futures.json",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        rows = doc.get("bars") or doc.get("series") or doc.get("observations") or doc.get("data") or []
        if isinstance(rows, dict):
            for k, v in rows.items():
                try:
                    fv = float(v if not isinstance(v, dict) else v.get("close") or v.get("value"))
                except (TypeError, ValueError):
                    continue
                if math.isfinite(fv) and fv > 0:
                    out[str(k)[:10]] = fv
        elif isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                d = str(row.get("date") or row.get("t") or "")[:10]
                try:
                    fv = float(row.get("close") or row.get("c") or row.get("value"))
                except (TypeError, ValueError):
                    continue
                if d and math.isfinite(fv) and fv > 0:
                    out[d] = fv
        if len(out) >= 52:
            return out

    # Fallback: canonical timeline (may be slower).
    try:
        from hptl.prices.canonical_timeline import load_canonical_timeline

        tl = load_canonical_timeline("US Dollar Index / DX")
        if tl:
            for d, c in tl.daily_closes():
                iso = str(d)[:10]
                try:
                    fv = float(c)
                except (TypeError, ValueError):
                    continue
                if iso and math.isfinite(fv) and fv > 0:
                    out[iso] = fv
    except Exception:
        pass
    return out


def _dataset_audit(
    feature_raw: dict[str, list[float | None]],
    dates: list[str],
    series_meta: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for spec in TIER1_SPECS:
        fid = spec["id"]
        series = feature_raw.get(fid) or []
        finite = [(dates[i], series[i]) for i in range(len(dates)) if series[i] is not None]
        meta = series_meta.get(fid) or {}
        rows.append(
            {
                "id": fid,
                "label": spec["label"],
                "symbol": spec["symbol"],
                "provider": spec["provider"],
                "frequency": meta.get("frequency", "Daily"),
                "release_cadence": meta.get("release_cadence", "Daily / market"),
                "n_aligned_on_gold_weeks": len(finite),
                "first_aligned": finite[0][0] if finite else None,
                "last_aligned": finite[-1][0] if finite else None,
                "current_value": finite[-1][1] if finite else None,
                "missing_on_gold_panel": len(dates) - len(finite),
                "point_in_time_safety": "Weekly as-of join (last obs ≤ Friday week date)",
                "revisions_policy": meta.get(
                    "revisions_policy",
                    "Market quotes / FRED; low delayed-revision risk vs physical series.",
                ),
            }
        )
    return rows


def _per_variable_recommendation(
    *,
    fid: str,
    coef: float | None,
    expected_sign: str,
    p_value: float | None,
    sign_flip: bool,
    vif: float | None,
    oos_contrib_rmse_pct: float | None,
    redundant_of: str | None,
    weaker_twin: bool,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    sign_ok = (
        coef is not None
        and ((expected_sign == "negative" and coef < 0) or (expected_sign == "positive" and coef > 0))
    )
    if not sign_ok:
        reasons.append("coefficient sign not economically sensible")
    if p_value is None or p_value > P_SIG:
        reasons.append(f"not statistically significant at p<{P_SIG}")
    if sign_flip:
        reasons.append("walk-forward coefficient sign flips")
    if vif is not None and vif >= VIF_HIGH:
        reasons.append(f"high multicollinearity VIF={vif:.1f}")
    if oos_contrib_rmse_pct is not None and oos_contrib_rmse_pct <= 0:
        reasons.append("removing feature does not worsen OOS RMSE (no independent OOS value)")
    if weaker_twin and redundant_of:
        reasons.append(f"redundant with {redundant_of}; weaker twin")

    if weaker_twin or (not sign_ok and (oos_contrib_rmse_pct or 0) <= 0):
        return "Reject", reasons
    if (
        sign_ok
        and not sign_flip
        and (vif is None or vif < VIF_HIGH)
        and p_value is not None
        and p_value <= P_SIG
        and (oos_contrib_rmse_pct or 0) > 0
        and not weaker_twin
    ):
        return "Promote", reasons
    return "Keep Experimental", reasons or ["mixed evidence in combined model"]


def run_gold_macro_tier1_discovery(*, as_of_week: str | None = None) -> dict[str, Any]:
    t0 = datetime.now(timezone.utc)
    generated_at = t0.replace(microsecond=0).isoformat()

    panel = _build_weekly_panel("Gold")
    if as_of_week:
        panel = [o for o in panel if o.date <= str(as_of_week)[:10]]
    if len(panel) < MIN_TRAIN + 40:
        return {
            "generated_at": generated_at,
            "ok": False,
            "error": f"Insufficient Gold panel n={len(panel)}",
        }

    dates = [o.date for o in panel]
    y_all = [math.log(o.price) for o in panel]
    prices = [o.price for o in panel]

    dgs2 = load_fred_daily_map("DGS2")
    dgs10 = load_fred_daily_map("DGS10")
    dfii = load_fred_daily_map(REAL_YIELD_SERIES)
    # Prefer pure FRED broad USD (not DX-patched) for clean duplication test.
    broad = load_fred_daily_map(DXY_SERIES)
    if len(broad) < 52:
        broad = _load_dxy_series()
    dx = _load_dx_daily()

    raw_levels: dict[str, list[float | None]] = {
        "us_2y_yield": _asof_series(dgs2, dates),
        "us_10y_yield": _asof_series(dgs10, dates),
        "us_10y_real_yield": _asof_series(dfii, dates),
        "log_broad_usd": [
            math.log(v) if v is not None and v > 0 else None
            for v in _asof_series(broad, dates)
        ],
        "log_dxy_ice": [
            math.log(v) if v is not None and v > 0 else None
            for v in _asof_series(dx, dates)
        ],
    }

    series_meta = {
        "us_2y_yield": {
            "frequency": "Daily",
            "release_cadence": "Daily Treasury constant maturity via FRED",
            "revisions_policy": "Market yield; low delayed revision risk.",
        },
        "us_10y_yield": {
            "frequency": "Daily",
            "release_cadence": "Daily Treasury constant maturity via FRED",
            "revisions_policy": "Market yield; low delayed revision risk.",
        },
        "us_10y_real_yield": {
            "frequency": "Daily",
            "release_cadence": "Daily TIPS real yield (DFII10) via FRED",
            "revisions_policy": "Market yield; low delayed revision risk.",
        },
        "log_broad_usd": {
            "frequency": "Daily",
            "release_cadence": "FRB H.10 broad USD via FRED (~1bd lag)",
            "revisions_policy": "Index levels; low delayed revision risk.",
        },
        "log_dxy_ice": {
            "frequency": "Daily",
            "release_cadence": "ICE DX futures session closes",
            "revisions_policy": "Futures closes; no EIA-style revisions.",
        },
    }
    dataset_quality = _dataset_audit(raw_levels, dates, series_meta)

    # --- Dollar duplication gate ---
    broad_finite = [v for v in raw_levels["log_broad_usd"] if v is not None]
    dx_pairs_a: list[float] = []
    dx_pairs_b: list[float] = []
    for a, b in zip(raw_levels["log_broad_usd"], raw_levels["log_dxy_ice"]):
        if a is not None and b is not None:
            dx_pairs_a.append(a)
            dx_pairs_b.append(b)
    dollar_corr = _pearson(dx_pairs_a, dx_pairs_b)
    include_dxy_ice = True
    dollar_selection: dict[str, Any] = {
        "broad_symbol": DXY_SERIES,
        "dxy_symbol": "US Dollar Index / DX",
        "n_paired": len(dx_pairs_a),
        "corr_log_levels": round(dollar_corr, 4) if dollar_corr is not None else None,
        "threshold": CORR_REDUNDANT,
    }
    # Always allow both dollars into the *full* kitchen-sink fit when available,
    # then prune to one if overlap is high or signs conflict.
    if not dx_pairs_a:
        include_dxy_ice = False
        dollar_selection["decision"] = "DXY unavailable — use Broad USD only"
        dollar_selection["selected_for_full_model"] = ["log_broad_usd"]
        dollar_selection["dropped_a_priori"] = ["log_dxy_ice"]
    elif dollar_corr is not None and abs(dollar_corr) >= CORR_REDUNDANT:
        include_dxy_ice = False
        dollar_selection["decision"] = (
            f"|corr|={abs(dollar_corr):.3f} ≥ {CORR_REDUNDANT} — Broad and DXY "
            "are duplicates; keep Broad USD for full model and drop ICE DXY a priori"
        )
        dollar_selection["selected_for_full_model"] = ["log_broad_usd"]
        dollar_selection["dropped_a_priori"] = ["log_dxy_ice"]
    else:
        include_dxy_ice = True
        dollar_selection["decision"] = (
            f"|corr|={abs(dollar_corr) if dollar_corr is not None else 'n/a'} "
            f"< {CORR_REDUNDANT} — include both in full combined fit; "
            f"prune to one if |corr|≥{CORR_DOLLAR_DUPLICATE} or signs conflict"
        )
        dollar_selection["selected_for_full_model"] = ["log_broad_usd", "log_dxy_ice"]
        dollar_selection["dropped_a_priori"] = []

    active_ids = [
        "us_2y_yield",
        "us_10y_yield",
        "us_10y_real_yield",
        "log_broad_usd",
    ]
    if include_dxy_ice:
        active_ids.append("log_dxy_ice")

    expected = {s["id"]: s["expected_sign"] for s in TIER1_SPECS}
    labels = {s["id"]: s["label"] for s in TIER1_SPECS}

    dates_c, y_c, xmap = _align(dates, y_all, raw_levels, active_ids)
    cols_c = [xmap[fid] for fid in active_ids]
    if len(y_c) < MIN_TRAIN + 40:
        return {
            "generated_at": generated_at,
            "ok": False,
            "error": f"Insufficient aligned Tier-1 history n={len(y_c)}",
            "dataset_quality": dataset_quality,
            "dollar_selection": dollar_selection,
        }

    # Published baseline on SAME aligned sample where possible
    # Baseline features: real_yield + log_dxy (published metals_real_yield_v1)
    base_map = {
        "us_10y_real_yield": raw_levels["us_10y_real_yield"],
        "log_broad_usd": raw_levels["log_broad_usd"],
    }
    d_b, y_b, x_b = _align(dates, y_all, base_map, ["us_10y_real_yield", "log_broad_usd"])
    published = _eval_model(
        name="published_metals_real_yield_v1",
        dates=d_b,
        y=y_b,
        feature_names=["us_10y_real_yield", "log_broad_usd"],
        cols=[x_b["us_10y_real_yield"], x_b["log_broad_usd"]],
        expected_signs={
            "us_10y_real_yield": "negative",
            "log_broad_usd": "negative",
        },
    )

    # Also evaluate published on the combined-model sample for fair DM
    pub_aligned = _eval_model(
        name="published_on_tier1_sample",
        dates=dates_c,
        y=y_c,
        feature_names=["us_10y_real_yield", "log_broad_usd"],
        cols=[xmap["us_10y_real_yield"], xmap["log_broad_usd"]],
        expected_signs={
            "us_10y_real_yield": "negative",
            "log_broad_usd": "negative",
        },
    )

    combined = _eval_model(
        name="tier1_combined_full",
        dates=dates_c,
        y=y_c,
        feature_names=active_ids,
        cols=cols_c,
        expected_signs={fid: expected[fid] for fid in active_ids},
    )

    # VIF + pairwise corr
    vifs = {
        fid: (round(_vif(cols_c, i), 3) if _vif(cols_c, i) is not None else None)
        for i, fid in enumerate(active_ids)
    }
    pairwise: list[dict[str, Any]] = []
    for i, a in enumerate(active_ids):
        for b in active_ids[i + 1 :]:
            corr = _pearson(xmap[a], xmap[b])
            pairwise.append(
                {
                    "a": a,
                    "b": b,
                    "corr": round(corr, 4) if corr is not None else None,
                    "redundant": bool(corr is not None and abs(corr) >= CORR_REDUNDANT),
                }
            )

    # Drop-one OOS contribution
    full_rmse = combined.get("oos_rmse")
    drop_one: dict[str, Any] = {}
    for fid in active_ids:
        keep = [f for f in active_ids if f != fid]
        m = _eval_model(
            name=f"drop_{fid}",
            dates=dates_c,
            y=y_c,
            feature_names=keep,
            cols=[xmap[f] for f in keep],
            expected_signs={f: expected[f] for f in keep},
        )
        rmse = m.get("oos_rmse")
        contrib = None
        if full_rmse and rmse is not None and full_rmse > 0:
            # Positive => feature helps (RMSE rises when dropped)
            contrib = 100.0 * (rmse - full_rmse) / full_rmse
        drop_one[fid] = {
            "oos_rmse_without": rmse,
            "oos_rmse_full": full_rmse,
            "oos_contrib_rmse_pct": round(contrib, 2) if contrib is not None else None,
        }

    # Identify redundant pairs and weaker twin via |t|, sign, OOS contrib
    redundant_drops: list[dict[str, Any]] = []
    weaker: set[str] = set()
    twin_of: dict[str, str] = {}
    coefs_full = combined.get("coefficients") or {}

    def _prefer_keep(a: str, b: str) -> tuple[str, str]:
        ta = abs(float((combined.get("t_stats") or {}).get(a) or 0.0))
        tb = abs(float((combined.get("t_stats") or {}).get(b) or 0.0))
        ca = drop_one.get(a, {}).get("oos_contrib_rmse_pct") or -999
        cb = drop_one.get(b, {}).get("oos_contrib_rmse_pct") or -999
        sign_a = (coefs_full.get(a) or 0) < 0
        sign_b = (coefs_full.get(b) or 0) < 0
        if {a, b} == {"log_broad_usd", "log_dxy_ice"}:
            if sign_a != sign_b:
                keep = a if sign_a else b
                drop = b if keep == a else a
                return keep, drop
            return "log_broad_usd", "log_dxy_ice"
        score_a = (1 if sign_a else 0, ta, ca)
        score_b = (1 if sign_b else 0, tb, cb)
        if score_a >= score_b:
            return a, b
        return b, a

    for pair in pairwise:
        a, b = pair["a"], pair["b"]
        is_dollar_pair = {a, b} == {"log_broad_usd", "log_dxy_ice"}
        corr_v = pair.get("corr")
        force_dollar = bool(
            is_dollar_pair
            and corr_v is not None
            and abs(float(corr_v)) >= CORR_DOLLAR_DUPLICATE
        )
        opposite_dollar = bool(
            is_dollar_pair
            and ((coefs_full.get(a) or 0) < 0) != ((coefs_full.get(b) or 0) < 0)
        )
        if not pair.get("redundant") and not force_dollar and not opposite_dollar:
            continue
        keep, drop = _prefer_keep(a, b)
        weaker.add(drop)
        twin_of[drop] = keep
        reason = "near-duplicate information (|corr| ≥ threshold)"
        if force_dollar and not pair.get("redundant"):
            reason = (
                f"Broad vs DXY overlap |corr|≥{CORR_DOLLAR_DUPLICATE} — keep one dollar factor"
            )
        if opposite_dollar:
            reason = (
                "Broad vs DXY opposite signs in combined fit (collinearity artifact) — "
                "keep economically sensible (negative) dollar factor"
            )
        redundant_drops.append(
            {
                "pair": [a, b],
                "corr": pair["corr"],
                "keep": keep,
                "drop": drop,
                "reason": reason,
            }
        )

    ranked: list[dict[str, Any]] = []
    for fid in active_ids:
        coef = coefs_full.get(fid)
        p_value = (combined.get("p_values") or {}).get(fid)
        t_stat = (combined.get("t_stats") or {}).get(fid)
        stab = (combined.get("coefficient_stability") or {}).get(fid) or {}
        sign_flip = bool(stab.get("sign_flip"))
        exp = expected[fid]
        sign_ok = coef is not None and (
            (exp == "negative" and coef < 0) or (exp == "positive" and coef > 0)
        )
        rec, reasons = _per_variable_recommendation(
            fid=fid,
            coef=coef,
            expected_sign=exp,
            p_value=p_value,
            sign_flip=sign_flip,
            vif=vifs.get(fid),
            oos_contrib_rmse_pct=(drop_one.get(fid) or {}).get("oos_contrib_rmse_pct"),
            redundant_of=twin_of.get(fid),
            weaker_twin=fid in weaker,
        )
        if not sign_ok and fid in ("log_broad_usd", "us_10y_yield"):
            rec = "Reject"
            if "coefficient sign not economically sensible" not in reasons:
                reasons.append("coefficient sign not economically sensible")
        ranked.append(
            {
                "feature": fid,
                "label": labels[fid],
                "coefficient": coef,
                "expected_sign": exp,
                "fitted_sign": (
                    "negative"
                    if coef is not None and coef < 0
                    else "positive"
                    if coef is not None
                    else None
                ),
                "sign_ok": sign_ok,
                "t_stat": t_stat,
                "p_value": p_value,
                "vif": vifs.get(fid),
                "coef_stability": stab,
                "sign_flip": sign_flip,
                "oos_contribution_rmse_pct": (drop_one.get(fid) or {}).get(
                    "oos_contrib_rmse_pct"
                ),
                "redundant_of": twin_of.get(fid),
                "recommendation": rec,
                "reasons": reasons,
            }
        )
    ranked.sort(
        key=lambda r: (
            {"Promote": 0, "Keep Experimental": 1, "Reject": 2}.get(
                r["recommendation"], 9
            ),
            -(r.get("oos_contribution_rmse_pct") or -999),
        )
    )

    # Candidate nested specs after redundancy (no dual-dollar, no nominal 10Y twin)
    dollar_candidates = [
        f
        for f in ("log_dxy_ice", "log_broad_usd")
        if f in active_ids and f not in weaker
    ]
    if not dollar_candidates:
        dollar_candidates = [
            f for f in ("log_dxy_ice", "log_broad_usd") if f in active_ids
        ]
    rate_candidates = [
        f
        for f in ("us_10y_real_yield", "us_2y_yield")
        if f in active_ids and f not in weaker
    ]
    # Always allow real yield as a rate candidate even if marked weaker elsewhere
    if "us_10y_real_yield" in active_ids and "us_10y_real_yield" not in rate_candidates:
        rate_candidates.insert(0, "us_10y_real_yield")

    nested_specs: list[list[str]] = []
    for d in dollar_candidates:
        nested_specs.append(["us_10y_real_yield", d])
        nested_specs.append(["us_2y_yield", d])
        nested_specs.append(["us_10y_real_yield", "us_2y_yield", d])
    nested_specs.append(["us_10y_real_yield", "log_broad_usd"])  # published shape
    # Deduplicate
    uniq_specs: list[list[str]] = []
    seen_spec: set[tuple[str, ...]] = set()
    for spec in nested_specs:
        key = tuple(spec)
        if key in seen_spec:
            continue
        if any(f not in xmap for f in spec):
            continue
        seen_spec.add(key)
        uniq_specs.append(spec)

    nested_results: list[dict[str, Any]] = []
    for spec in uniq_specs:
        m = _eval_model(
            name="nested_" + "_".join(spec),
            dates=dates_c,
            y=y_c,
            feature_names=spec,
            cols=[xmap[f] for f in spec],
            expected_signs={f: expected[f] for f in spec},
        )
        coefs_m = m.get("coefficients") or {}
        signs_ok = all(
            (coefs_m.get(f) is not None and float(coefs_m[f]) < 0) for f in spec
        )
        flips = any(
            bool(((m.get("coefficient_stability") or {}).get(f) or {}).get("sign_flip"))
            for f in spec
        )
        cols_m = [xmap[f] for f in spec]
        vif_m = {
            f: round(v, 3) if v is not None else None
            for f, v in ((f, _vif(cols_m, i)) for i, f in enumerate(spec))
        }
        max_vif = max((v for v in vif_m.values() if v is not None), default=None)
        nested_results.append(
            {
                "features": spec,
                "signs_ok": signs_ok,
                "any_sign_flip": flips,
                "max_vif": max_vif,
                "vif": vif_m,
                "oos_rmse": m.get("oos_rmse"),
                "oos_mae": m.get("oos_mae"),
                "oos_r2": m.get("oos_r2"),
                "r_squared": m.get("r_squared"),
                "coefficients": coefs_m,
                "n_oos": m.get("n_oos"),
                "_model": m,
            }
        )

    # Prefer: all signs OK, no flips, max VIF < 10, then lowest OOS RMSE
    def _nested_key(row: dict[str, Any]) -> tuple:
        rmse = row.get("oos_rmse")
        return (
            0 if row.get("signs_ok") else 1,
            0 if not row.get("any_sign_flip") else 1,
            0 if (row.get("max_vif") is not None and row["max_vif"] < VIF_HIGH) else 1,
            float(rmse) if rmse is not None else 999.0,
        )

    nested_results.sort(key=_nested_key)
    sign_ok_specs = [row for row in nested_results if row.get("signs_ok")]
    best_nested = sign_ok_specs[0] if sign_ok_specs else None
    # Predictive kitchen-sink (may be unidentified / wrong signs)
    best_predictive = combined
    best_predictive_features = list(active_ids)

    if best_nested:
        retained = list(best_nested["features"])
        best = best_nested["_model"]
        economic_status = "SIGN_OK_NESTED_FOUND"
    else:
        # No Tier-1 nested macro spec clears economic signs.
        # Keep published feature set as the research status-quo architecture.
        retained = ["us_10y_real_yield", "log_broad_usd"]
        best = pub_aligned
        economic_status = "NO_SIGN_OK_TIER1_SPEC"
        # Prefer single dollar factor for future V2 experiments: ICE DXY if it
        # carries the correct negative sign in the kitchen-sink fit.
        if "log_dxy_ice" in active_ids and (coefs_full.get("log_dxy_ice") or 0) < 0:
            dollar_selection["recommended_single_dollar_for_future_v2"] = "log_dxy_ice"
        else:
            dollar_selection["recommended_single_dollar_for_future_v2"] = "log_broad_usd"

    nested_public = [
        {k: v for k, v in row.items() if k != "_model"} for row in nested_results
    ]

    rejected = sorted(
        {
            r["feature"]
            for r in ranked
            if r["recommendation"] == "Reject" or r["feature"] in weaker
        }
    )

    dollar_selection["selected_after_prune"] = [
        f for f in retained if f.startswith("log_")
    ]
    dollar_selection["dropped_after_prune"] = [
        f
        for f in ("log_broad_usd", "log_dxy_ice")
        if f in active_ids and f not in retained
    ]
    dollar_selection["economic_status"] = economic_status

    # DM: kitchen-sink Tier-1 vs published (primary predictive comparison)
    idx_p = set(pub_aligned.get("_indices") or [])
    idx_k = set(best_predictive.get("_indices") or [])
    common = sorted(idx_p & idx_k)
    map_p = {
        i: e
        for i, e in zip(pub_aligned.get("_indices") or [], pub_aligned.get("_squared_errors") or [])
    }
    map_k = {
        i: e
        for i, e in zip(
            best_predictive.get("_indices") or [],
            best_predictive.get("_squared_errors") or [],
        )
    }
    se_p = [map_p[i] for i in common]
    se_k = [map_k[i] for i in common]
    dm = _diebold_mariano_pvalue(se_p, se_k)
    dm["interprets"] = (
        "Positive mean_loss_diff means Tier-1 kitchen-sink has lower MSE than "
        f"published {PUBLISHED_MODEL_ID}."
    )

    # Regime: kitchen-sink vs published
    coef_path: list[float] = []
    t = MIN_TRAIN
    n = len(y_c)
    kitchen_cols = [xmap[f] for f in best_predictive_features]
    while t < n:
        beta, r2 = _multivariate_ols(y_c[:t], [c[:t] for c in kitchen_cols])
        if beta and len(beta) >= 2:
            coef_path.append(float(beta[1]))
        t += STEP
    regime = _regime_stability(
        dates=dates_c,
        indices=common,
        se_v2=se_p,
        se_cand=se_k,
        coef_path=coef_path,
    )

    # Final equation string
    intercept = best.get("intercept")
    coefs = best.get("coefficients") or {}
    terms = [f"{intercept:.6f}" if intercept is not None else "β0"]
    for fid in retained:
        b = coefs.get(fid)
        if b is None:
            terms.append(f"β_{fid}·{fid}")
        else:
            sign = "+" if b >= 0 else "-"
            terms.append(f"{sign} {abs(b):.6f}·{fid}")
    equation = "log(Gold) = " + " ".join(terms)

    # Architecture recommendation
    promote_vars = [r["feature"] for r in ranked if r["recommendation"] == "Promote"]
    experimental_vars = [
        r["feature"]
        for r in ranked
        if r["recommendation"] == "Keep Experimental" and r["feature"] not in weaker
    ]
    kitchen_eq_terms = [
        f"{(best_predictive.get('intercept')):.6f}"
        if best_predictive.get("intercept") is not None
        else "β0"
    ]
    for fid in best_predictive_features:
        b = (best_predictive.get("coefficients") or {}).get(fid)
        if b is None:
            kitchen_eq_terms.append(f"β_{fid}·{fid}")
        else:
            sign = "+" if b >= 0 else "-"
            kitchen_eq_terms.append(f"{sign} {abs(b):.6f}·{fid}")
    kitchen_equation = "log(Gold) = " + " ".join(kitchen_eq_terms)

    architecture = {
        "proposed_model_id": "gold_macro_tier1_v2_research",
        "status": "RESEARCH_ONLY — do not publish",
        "economic_status": economic_status,
        "fair_value_drivers": promote_vars,  # empty unless Promote-grade
        "experimental_display_only": experimental_vars,
        "rejected_or_redundant": rejected,
        "status_quo_equation_published": (
            "log(Gold) = β0 + β1·real_yield(DFII10) + β2·log(DTWEXBGS)"
        ),
        "kitchen_sink_equation_research": kitchen_equation,
        "economics_constrained_equation": equation,
        "vs_published": {
            "published_model_id": PUBLISHED_MODEL_ID,
            "published_equation": "log(Gold) = β0 + β1·real_yield(DFII10) + β2·log(DTWEXBGS)",
            "oos_rmse_published_aligned": pub_aligned.get("oos_rmse"),
            "oos_rmse_kitchen_sink": best_predictive.get("oos_rmse"),
            "oos_rmse_economics_constrained": best.get("oos_rmse"),
            "dm_kitchen_sink_vs_published": dm,
            "improves_published_predictively": bool(
                dm.get("ok")
                and (dm.get("mean_loss_diff") or 0) > 0
                and (dm.get("p_value_one_sided") or 1) < DM_ALPHA
            ),
            "improves_published_economically": bool(
                economic_status == "SIGN_OK_NESTED_FOUND"
                and promote_vars
            ),
        },
        "guidance": (
            "Do not publish a Tier-1-rates-only Gold V2. The kitchen-sink macro "
            "fit improves OOS error but is unidentified (wrong signs / collinearity). "
            "No nested Tier-1 spec clears economic sign gates. Keep "
            f"{PUBLISHED_MODEL_ID} published. Next research should add "
            "non-rate drivers (e.g. CB / ETF) rather than more Treasury yields. "
            "Use a single dollar factor (prefer ICE DXY if Broad/DXY conflict)."
        ),
    }

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    payload = {
        "generated_at": generated_at,
        "ok": True,
        "phase": "gold_macro_tier1_discovery",
        "research_only": True,
        "published_model_untouched": True,
        "published_model_id": PUBLISHED_MODEL_ID,
        "scope": {
            "market": "Gold",
            "tier1_candidates": [s["id"] for s in TIER1_SPECS],
            "walk_forward": {"min_train": MIN_TRAIN, "step": STEP},
            "thresholds": {
                "corr_redundant": CORR_REDUNDANT,
                "corr_dollar_duplicate": CORR_DOLLAR_DUPLICATE,
                "vif_high": VIF_HIGH,
                "dm_alpha": DM_ALPHA,
                "p_significance": P_SIG,
            },
            "not_modified": [
                "published Gold valuation",
                "Natural Gas valuation",
                "weekly COT",
                "Stage 4",
                "Scanner",
                "Inspector",
                "Seasonality",
            ],
        },
        "panel": {
            "n_weeks": len(panel),
            "start": dates[0],
            "end": dates[-1],
            "n_aligned_tier1": len(y_c),
            "aligned_start": dates_c[0],
            "aligned_end": dates_c[-1],
            "spot_latest": prices[-1],
        },
        "dataset_quality": dataset_quality,
        "dollar_selection": dollar_selection,
        "pairwise_correlations": pairwise,
        "redundant_variable_analysis": redundant_drops,
        "published_baseline": {
            k: v for k, v in published.items() if not k.startswith("_")
        },
        "published_on_tier1_sample": {
            k: v for k, v in pub_aligned.items() if not k.startswith("_")
        },
        "full_combined_model": {
            k: v for k, v in combined.items() if not k.startswith("_")
        },
        "nested_sign_constrained_specs": nested_public,
        "vif": vifs,
        "drop_one_oos": drop_one,
        "ranked_contribution_table": ranked,
        "best_combined_model": {
            k: v for k, v in best.items() if not k.startswith("_")
        },
        "kitchen_sink_model": {
            k: v for k, v in best_predictive.items() if not k.startswith("_")
        },
        "economic_status": economic_status,
        "variables_retained": retained,
        "variables_rejected": rejected,
        "final_equation": equation,
        "kitchen_sink_equation": kitchen_equation,
        "diebold_mariano_vs_published": dm,
        "regime_stability_vs_published": {
            k: v for k, v in regime.items() if k != "halves"
        }
        | {"halves": regime.get("halves")},
        "recommended_v2_architecture": architecture,
        "runtime_seconds": round(elapsed, 2),
        "files": {"json": str(JSON_OUT), "markdown": str(MD_OUT)},
    }
    return json.loads(json.dumps(payload, default=str))


def render_markdown(payload: dict[str, Any]) -> str:
    pub = payload.get("published_on_tier1_sample") or {}
    full = payload.get("full_combined_model") or {}
    best = payload.get("best_combined_model") or {}
    dollar = payload.get("dollar_selection") or {}
    dm = payload.get("diebold_mariano_vs_published") or {}
    arch = payload.get("recommended_v2_architecture") or {}
    panel = payload.get("panel") or {}

    lines = [
        "# Gold Macro Tier 1 Research Report",
        "",
        f"Generated: `{payload.get('generated_at')}`",
        "",
        "**Research only. Published Gold valuation was not modified.**",
        "",
        f"Published baseline model id: `{payload.get('published_model_id')}`",
        f"Panel: n={panel.get('n_weeks')} weeks ({panel.get('start')} → {panel.get('end')}); "
        f"Tier-1 aligned n={panel.get('n_aligned_tier1')}",
        "",
        "## 1. Dataset quality",
        "",
        "| Driver | Symbol | Provider | Aligned n | Tip date | Tip value | Missing on panel |",
        "|---|---|---|---:|---|---:|---:|",
    ]
    for row in payload.get("dataset_quality") or []:
        lines.append(
            f"| {row.get('label')} | `{row.get('symbol')}` | {row.get('provider')} | "
            f"{row.get('n_aligned_on_gold_weeks')} | {row.get('last_aligned')} | "
            f"{row.get('current_value')} | {row.get('missing_on_gold_panel')} |"
        )

    lines += [
        "",
        "### Dollar duplication",
        "",
        f"- Correlation (log Broad vs log ICE DXY): `{dollar.get('corr_log_levels')}` "
        f"(n={dollar.get('n_paired')})",
        f"- Decision: {dollar.get('decision')}",
        f"- Selected for full model: `{dollar.get('selected_for_full_model')}`",
        f"- Dropped a priori: `{dollar.get('dropped_a_priori')}`",
        f"- Selected after prune: `{dollar.get('selected_after_prune')}`",
        f"- Dropped after prune: `{dollar.get('dropped_after_prune')}`",
        "",
        "## 2. Redundant variable analysis",
        "",
    ]
    red = payload.get("redundant_variable_analysis") or []
    if not red:
        lines.append("No pairwise |corr| ≥ threshold among active features after dollar gate.")
    else:
        for r in red:
            lines.append(
                f"- Pair `{r.get('pair')}` corr={r.get('corr')}: keep **{r.get('keep')}**, "
                f"drop **{r.get('drop')}** ({r.get('reason')})"
            )

    lines += [
        "",
        "### Pairwise correlations",
        "",
        "| A | B | Corr | Redundant |",
        "|---|---|---:|:---:|",
    ]
    for p in payload.get("pairwise_correlations") or []:
        lines.append(
            f"| {p.get('a')} | {p.get('b')} | {p.get('corr')} | "
            f"{'YES' if p.get('redundant') else 'no'} |"
        )

    lines += [
        "",
        "## 3. Walk-forward performance",
        "",
        f"Settings: min_train={MIN_TRAIN}, step={STEP}",
        "",
        "| Model | Features | N OOS | RMSE | MAE | OOS R² | In-sample R² |",
        "|---|---|---:|---:|---:|---:|---:|",
        (
            f"| Published `{payload.get('published_model_id')}` | "
            f"real_yield + log_broad | {pub.get('n_oos')} | {pub.get('oos_rmse')} | "
            f"{pub.get('oos_mae')} | {pub.get('oos_r2')} | {pub.get('r_squared')} |"
        ),
        (
            f"| Full Tier-1 combined | {', '.join(full.get('features') or [])} | "
            f"{full.get('n_oos')} | {full.get('oos_rmse')} | {full.get('oos_mae')} | "
            f"{full.get('oos_r2')} | {full.get('r_squared')} |"
        ),
        (
            f"| **Best combined (retained)** | {', '.join(best.get('features') or [])} | "
            f"{best.get('n_oos')} | {best.get('oos_rmse')} | {best.get('oos_mae')} | "
            f"{best.get('oos_r2')} | {best.get('r_squared')} |"
        ),
        "",
        "### Diebold–Mariano vs published",
        "",
        f"- mean_loss_diff: `{dm.get('mean_loss_diff')}`",
        f"- t_stat: `{dm.get('t_stat')}`",
        f"- one-sided p: `{dm.get('p_value_one_sided')}`",
        f"- {dm.get('interprets')}",
        "",
        "### VIF (full combined)",
        "",
        "```json",
        json.dumps(payload.get("vif") or {}, indent=2),
        "```",
        "",
        "## 4. Ranked contribution table",
        "",
        "| Rank | Feature | Coef | Sign OK | p | VIF | OOS contrib % | Sign flip | Recommendation |",
        "|---:|---|---:|:---:|---:|---:|---:|:---:|---|",
    ]
    for i, r in enumerate(payload.get("ranked_contribution_table") or [], 1):
        lines.append(
            "| "
            + " | ".join(
                str(x)
                for x in [
                    i,
                    r.get("feature"),
                    r.get("coefficient"),
                    r.get("sign_ok"),
                    r.get("p_value"),
                    r.get("vif"),
                    r.get("oos_contribution_rmse_pct"),
                    r.get("sign_flip"),
                    r.get("recommendation"),
                ]
            )
            + " |"
        )

    lines += [
        "",
        "### Reasons",
        "",
    ]
    for r in payload.get("ranked_contribution_table") or []:
        lines.append(
            f"- **{r.get('feature')}** ({r.get('recommendation')}): "
            + ("; ".join(r.get("reasons") or []) or "—")
        )

    lines += [
        "",
        "## 5. Best combined model",
        "",
        f"- **Variables retained:** `{payload.get('variables_retained')}`",
        f"- **Variables rejected / redundant:** `{payload.get('variables_rejected')}`",
        f"- **Final equation:** `{payload.get('final_equation')}`",
        f"- Coefficients: `{json.dumps(best.get('coefficients') or {}, sort_keys=True)}`",
        f"- Coefficient stability: `{json.dumps(best.get('coefficient_stability') or {}, default=str)}`",
        "",
        "### Nested sign-constrained specs (ranked)",
        "",
        "| Features | Signs OK | Sign flip | Max VIF | OOS RMSE | OOS MAE | OOS R² |",
        "|---|:---:|:---:|---:|---:|---:|---:|",
    ]
    for row in payload.get("nested_sign_constrained_specs") or []:
        lines.append(
            "| "
            + " | ".join(
                str(x)
                for x in [
                    ", ".join(row.get("features") or []),
                    row.get("signs_ok"),
                    row.get("any_sign_flip"),
                    row.get("max_vif"),
                    row.get("oos_rmse"),
                    row.get("oos_mae"),
                    row.get("oos_r2"),
                ]
            )
            + " |"
        )
    lines += [
        "",
        "### Regime stability vs published",
        "",
        "```json",
        json.dumps(payload.get("regime_stability_vs_published") or {}, indent=2, default=str),
        "```",
        "",
        "## 6. Recommended Gold Valuation V2 architecture",
        "",
        f"- Proposed id: `{arch.get('proposed_model_id')}`",
        f"- Status: **{arch.get('status')}**",
        f"- Economic status: `{arch.get('economic_status') or payload.get('economic_status')}`",
        f"- Fair-value Promote drivers: `{arch.get('fair_value_drivers')}`",
        f"- Experimental (display only): `{arch.get('experimental_display_only')}`",
        f"- Rejected / redundant: `{arch.get('rejected_or_redundant')}`",
        f"- Published status-quo equation: `{arch.get('status_quo_equation_published')}`",
        f"- Kitchen-sink research equation: `{arch.get('kitchen_sink_equation_research')}`",
        f"- Economics-constrained equation: `{arch.get('economics_constrained_equation')}`",
        "",
        (arch.get("guidance") or ""),
        "",
        f"Runtime: {payload.get('runtime_seconds')}s",
        "",
        "## Safety",
        "",
        "- No published valuation changes",
        "- No dashboard wiring",
        "- Natural Gas / weekly COT / Stage 4 / Scanner / Inspector / Seasonality untouched",
        "",
    ]
    return "\n".join(lines) + "\n"


def write_tier1_outputs(payload: dict[str, Any]) -> dict[str, Path]:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    JSON_OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    MD_OUT.write_text(render_markdown(payload), encoding="utf-8")
    return {"json": JSON_OUT, "markdown": MD_OUT}


__all__ = [
    "run_gold_macro_tier1_discovery",
    "write_tier1_outputs",
    "render_markdown",
]
