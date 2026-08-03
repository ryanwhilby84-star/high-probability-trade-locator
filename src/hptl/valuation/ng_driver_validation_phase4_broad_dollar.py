"""Natural Gas Valuation — Macro Validation Phase 4 (Broad US Dollar).

Research-only unless Broad Dollar clears every promotion gate.
Does not modify weekly COT / HPTL_SKIP_VALUATION / published valuation.

Compares:
  A) Storage-only
  B) Storage + Production YoY  (current published v2)
  C) Storage + Production YoY + Broad Dollar transform  (one transform at a time)
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.fx.fx_macro_history import load_fred_daily_map
from hptl.valuation.energy_ng_drivers import (
    _asof_value,
    _load_config,
    _weekly_from_daily,
    build_ng_driver_bundle,
)
from hptl.valuation.energy_natural_gas_valuation_v1 import _multivariate_ols
from hptl.valuation.metals_valuation_v1 import MIN_WEEKS
from hptl.valuation.ng_driver_validation_phase2_production import (
    DM_ALPHA,
    MIN_OOS_RMSE_IMPROVEMENT_PCT,
    MIN_TRAIN,
    STEP,
    _align_finite,
    _build_production_transforms,
    _diebold_mariano_pvalue,
    _eval_model,
)
from hptl.valuation.ng_driver_validation_phase3_lng import (
    _align_triple,
    _build_production_yoy_from_bundle,
    _regime_stability,
)

AUDIT_DIR = PROJECT_ROOT / "data" / "audits" / "ng_driver_validation_phase4_broad_dollar"
JSON_OUT = AUDIT_DIR / "phase4_broad_dollar_validation.json"
MD_OUT = AUDIT_DIR / "phase4_broad_dollar_validation.md"

SERIES_ID = "DTWEXBGS"
SERIES_LABEL = "Nominal Broad U.S. Dollar Index"

# Stronger USD → lower commodity/NG prices → negative coef on log(P).
TRANSFORM_SPECS: list[tuple[str, str, str]] = [
    ("raw_index", "raw Broad USD index level (as-of weekly)", "negative"),
    ("yoy_pct", "year-over-year Broad USD % change", "negative"),
    ("chg_4w", "4-week change in Broad USD", "negative"),
    ("chg_12w", "12-week change in Broad USD", "negative"),
    ("rolling_zscore_156", "trailing 156-week z-score (past-only mean/sd)", "negative"),
    (
        "trend_deviation_104",
        "deviation from trailing 104-week linear trend (past-only)",
        "negative",
    ),
]


def document_broad_dollar_dataset() -> dict[str, Any]:
    cfg = _load_config()
    fred_map = cfg.get("fred_series") or {}
    series_id = str(fred_map.get("dxy_broad") or SERIES_ID)
    daily = load_fred_daily_map(series_id)
    dates = sorted(daily.keys())
    gaps: list[dict[str, Any]] = []
    if len(dates) >= 2:
        prev = datetime.strptime(dates[0], "%Y-%m-%d")
        for ds in dates[1:]:
            cur = datetime.strptime(ds, "%Y-%m-%d")
            delta = (cur - prev).days
            if delta > 5:
                gaps.append(
                    {
                        "from": prev.strftime("%Y-%m-%d"),
                        "to": cur.strftime("%Y-%m-%d"),
                        "calendar_days": delta,
                    }
                )
            prev = cur

    return {
        "driver": "broad_us_dollar_index",
        "series_name": SERIES_LABEL,
        "symbol": series_id,
        "provider": "FRED (Federal Reserve Board H.10 via FRED)",
        "data_source": "FRED API / resilient macro_cache (load_fred_daily_map)",
        "source_url": f"https://fred.stlouisfed.org/series/{series_id}",
        "frequency": "Daily (business days)",
        "release_cadence": (
            "Daily; FRB trade-weighted indexes typically available with about "
            "one business-day lag."
        ),
        "history_available": {
            "n_observations": len(dates),
            "start": dates[0] if dates else None,
            "end": dates[-1] if dates else None,
        },
        "current_observation_date": dates[-1] if dates else None,
        "latest_value": daily[dates[-1]] if dates else None,
        "missing_periods": gaps,
        "missing_period_count": len(gaps),
        "point_in_time_safety": {
            "native_frequency": "daily",
            "safe_for_asof_weekly_join": True,
            "alignment": (
                "Daily Broad USD levels as-of joined to Friday NG weeks "
                "(last observation on or before week date). Transforms use only "
                "past weekly as-of levels — no full-sample z-score."
            ),
        },
        "revisions_policy": (
            "FRB/FRED index levels; occasional restatements possible but material "
            "delayed-revision risk is low versus EIA physical series. Research "
            "treats the downloaded daily values as point-in-time as-of."
        ),
        "alignment_with_ng_history": {
            "ng_panel_uses_weekly_asof": True,
            "sufficient_history_for_walk_forward": len(dates) >= 400,
            "note": (
                "Daily Broad USD covers the post-2016 NG weekly valuation panel. "
                "Safe to align with storage + production YoY history via as-of join."
            ),
        },
        "expected_economic_sign": "negative",
        "economic_rationale": (
            "A stronger Broad USD typically pressures commodity prices, so the "
            "coefficient on log(NG price) should be negative."
        ),
    }


def _trend_deviation(xs: list[float], window: int = 104) -> list[float | None]:
    """Residual of current value vs OLS line fit on prior `window` obs only."""
    n = len(xs)
    out: list[float | None] = [None] * n
    for i in range(n):
        if i < window:
            continue
        pts = [(float(j), float(xs[j])) for j in range(i - window, i)]
        if len(pts) < max(24, window // 3):
            continue
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
        fitted = intercept + slope * float(i)
        out[i] = float(xs[i]) - fitted
    return out


def _rolling_z_past_only(xs: list[float], window: int = 156) -> list[float | None]:
    """Z-score vs mean/sd of prior `window` observations (excludes current)."""
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


def _build_dollar_transforms(
    dates: list[str], level: list[float]
) -> dict[str, list[float | None]]:
    """Individual Broad Dollar transforms — no combinations, no full-sample leakage."""
    base = _build_production_transforms(dates, level)
    # Override rolling z with strict past-only (phase-2 trailing includes current).
    return {
        "raw_index": list(base["raw_level"]),  # type: ignore[arg-type]
        "yoy_pct": base["yoy_pct"],
        "chg_4w": base["chg_4w"],
        "chg_12w": base["chg_12w"],
        "rolling_zscore_156": _rolling_z_past_only(level, 156),
        "trend_deviation_104": _trend_deviation(level, 104),
    }


def _promotion_decision(
    *,
    transform_id: str,
    candidate: dict[str, Any],
    v2_baseline: dict[str, Any],
    dm_vs_v2: dict[str, Any],
    regime: dict[str, Any],
) -> dict[str, Any]:
    reasons: list[str] = []
    gates = {
        "oos_improves_vs_v2_gt_2pct": False,
        "dollar_sign_ok": False,
        "dollar_coef_stable_no_flip": False,
        "no_point_in_time_leakage": True,
        "statistically_meaningful_vs_v2": False,
        "not_driven_by_one_regime": bool(regime.get("not_single_regime")),
    }

    base_rmse = v2_baseline.get("oos_rmse")
    cand_rmse = candidate.get("oos_rmse")
    improvement_pct = None
    if base_rmse and cand_rmse is not None and base_rmse > 0:
        improvement_pct = 100.0 * (base_rmse - cand_rmse) / base_rmse
        gates["oos_improves_vs_v2_gt_2pct"] = (
            improvement_pct >= MIN_OOS_RMSE_IMPROVEMENT_PCT
        )
        if not gates["oos_improves_vs_v2_gt_2pct"]:
            reasons.append(
                f"OOS RMSE improvement vs v2 is {improvement_pct:.2f}% "
                f"(need ≥ {MIN_OOS_RMSE_IMPROVEMENT_PCT:.0f}%)"
            )
    else:
        reasons.append("missing OOS RMSE for v2 comparison")

    dollar_name = next(
        (f for f in (candidate.get("features") or []) if f.startswith("broad_usd__")),
        None,
    )
    if dollar_name:
        coef = (candidate.get("coefficients") or {}).get(dollar_name)
        gates["dollar_sign_ok"] = bool(coef is not None and coef < 0)
        if not gates["dollar_sign_ok"]:
            reasons.append(
                f"Broad Dollar coefficient sign not economically sensible "
                f"(got {coef}; expect negative)"
            )
        stab = (candidate.get("coefficient_stability") or {}).get(dollar_name) or {}
        gates["dollar_coef_stable_no_flip"] = not bool(stab.get("sign_flip"))
        if not gates["dollar_coef_stable_no_flip"]:
            reasons.append("walk-forward Broad Dollar coefficient sign flips")
    else:
        reasons.append("Broad Dollar feature missing")

    p = dm_vs_v2.get("p_value_one_sided")
    gates["statistically_meaningful_vs_v2"] = bool(
        dm_vs_v2.get("ok")
        and p is not None
        and p < DM_ALPHA
        and (dm_vs_v2.get("mean_loss_diff") or 0) > 0
    )
    if not gates["statistically_meaningful_vs_v2"]:
        reasons.append(
            f"OOS MSE improvement vs v2 not statistically meaningful "
            f"(DM one-sided p={p}; need p < {DM_ALPHA})"
        )

    if not gates["not_driven_by_one_regime"]:
        reasons.append(regime.get("reason") or "regime split failed")

    promote = all(gates.values())
    if promote:
        recommendation = "Promote"
        plain = (
            f"Transform '{transform_id}' clears every Broad Dollar promotion gate "
            "versus Storage+Production YoY (v2)."
        )
    else:
        recommendation = "Reject"
        if (
            improvement_pct is not None
            and improvement_pct >= 0
            and gates["dollar_sign_ok"]
            and gates["no_point_in_time_leakage"]
        ):
            recommendation = "Keep Experimental"
        plain = (
            f"Transform '{transform_id}' does not deserve promotion. "
            + ("; ".join(reasons) if reasons else "Promotion gates unmet.")
        )

    return {
        "recommendation": recommendation,
        "promote": promote,
        "gates": gates,
        "oos_rmse_improvement_pct_vs_v2": (
            round(improvement_pct, 2) if improvement_pct is not None else None
        ),
        "reasons": reasons,
        "plain_english": plain,
    }


def run_phase4_broad_dollar_validation(*, as_of_week: str | None = None) -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    dataset = document_broad_dollar_dataset()
    bundle = build_ng_driver_bundle(as_of_week=as_of_week)

    if bundle.n < MIN_WEEKS or "storage_surplus_bcf" not in bundle.features:
        return {
            "generated_at": generated_at,
            "ok": False,
            "error": "Insufficient NG panel or missing storage",
            "broad_dollar_dataset": dataset,
        }

    cfg = _load_config()
    fred_map = cfg.get("fred_series") or {}
    series_id = str(fred_map.get("dxy_broad") or SERIES_ID)
    daily = load_fred_daily_map(series_id)
    weekly_map = _weekly_from_daily(daily, bundle.dates)
    level: list[float] = []
    level_ok = True
    for d in bundle.dates:
        v = weekly_map.get(d)
        if v is None:
            v = _asof_value(daily, d)
        if v is None or not math.isfinite(float(v)):
            level_ok = False
            break
        level.append(float(v))
    if not level_ok or len(level) != bundle.n:
        return {
            "generated_at": generated_at,
            "ok": False,
            "error": "Could not as-of align Broad USD to full NG weekly panel",
            "broad_dollar_dataset": dataset,
        }

    y_all = [math.log(p) for p in bundle.price]
    storage_all = bundle.features["storage_surplus_bcf"]
    prod_yoy_all = _build_production_yoy_from_bundle(bundle)
    dollar_transforms = _build_dollar_transforms(bundle.dates, level)

    d_a, y_a, s_a, _ = _align_finite(bundle.dates, y_all, storage_all, storage_all)
    storage_only = _eval_model(
        name="A_storage",
        dates=d_a,
        y=y_a,
        feature_names=["storage_surplus_bcf"],
        cols=[s_a],
        expected_signs={"storage_surplus_bcf": "negative"},
    )

    d_b, y_b, s_b, p_b = _align_finite(bundle.dates, y_all, storage_all, prod_yoy_all)
    v2_model = _eval_model(
        name="B_storage_production_yoy",
        dates=d_b,
        y=y_b,
        feature_names=["storage_surplus_bcf", "production_yoy_pct"],
        cols=[s_b, p_b],
        expected_signs={
            "storage_surplus_bcf": "negative",
            "production_yoy_pct": "negative",
        },
    )

    candidates: list[dict[str, Any]] = []
    for transform_id, label, exp_sign in TRANSFORM_SPECS:
        series = dollar_transforms[transform_id]
        dates, y, s, p, usd = _align_triple(
            bundle.dates, y_all, storage_all, prod_yoy_all, series
        )
        if len(y) < MIN_TRAIN + 40:
            candidates.append(
                {
                    "transform_id": transform_id,
                    "label": label,
                    "ok": False,
                    "reason": f"insufficient aligned history n={len(y)}",
                }
            )
            continue

        v2_aligned = _eval_model(
            name="B_storage_production_yoy_aligned",
            dates=dates,
            y=y,
            feature_names=["storage_surplus_bcf", "production_yoy_pct"],
            cols=[s, p],
            expected_signs={
                "storage_surplus_bcf": "negative",
                "production_yoy_pct": "negative",
            },
        )
        usd_feat = f"broad_usd__{transform_id}"
        candidate = _eval_model(
            name=f"C_storage_prod_yoy_usd_{transform_id}",
            dates=dates,
            y=y,
            feature_names=["storage_surplus_bcf", "production_yoy_pct", usd_feat],
            cols=[s, p, usd],
            expected_signs={
                "storage_surplus_bcf": "negative",
                "production_yoy_pct": "negative",
                usd_feat: exp_sign,
            },
        )

        idx_v2 = set(v2_aligned.get("_indices") or [])
        idx_c = set(candidate.get("_indices") or [])
        common = sorted(idx_v2 & idx_c)
        map_v2 = {
            i: e
            for i, e in zip(
                v2_aligned.get("_indices") or [],
                v2_aligned.get("_squared_errors") or [],
            )
        }
        map_c = {
            i: e
            for i, e in zip(
                candidate.get("_indices") or [],
                candidate.get("_squared_errors") or [],
            )
        }
        se_v2 = [map_v2[i] for i in common]
        se_c = [map_c[i] for i in common]
        dm = _diebold_mariano_pvalue(se_v2, se_c)
        dm["interprets"] = (
            "Positive mean_loss_diff means Storage+ProdYoY+BroadUSD has lower MSE than v2."
        )

        coef_path: list[float] = []
        t = MIN_TRAIN
        n = len(y)
        while t < n:
            beta, r2 = _multivariate_ols(y[:t], [s[:t], p[:t], usd[:t]])
            if beta and len(beta) >= 4:
                coef_path.append(float(beta[3]))
            t += STEP

        regime = _regime_stability(
            dates=dates,
            indices=common,
            se_v2=se_v2,
            se_cand=se_c,
            coef_path=coef_path,
        )

        decision = _promotion_decision(
            transform_id=transform_id,
            candidate=candidate,
            v2_baseline=v2_aligned,
            dm_vs_v2=dm,
            regime=regime,
        )

        storage_aligned = _eval_model(
            name="A_storage_aligned",
            dates=dates,
            y=y,
            feature_names=["storage_surplus_bcf"],
            cols=[s],
            expected_signs={"storage_surplus_bcf": "negative"},
        )

        slim_c = {k: v for k, v in candidate.items() if not k.startswith("_")}
        slim_v2 = {k: v for k, v in v2_aligned.items() if not k.startswith("_")}
        slim_a = {k: v for k, v in storage_aligned.items() if not k.startswith("_")}

        candidates.append(
            {
                "transform_id": transform_id,
                "label": label,
                "expected_sign": exp_sign,
                "leaky": False,
                "ok": True,
                "n_aligned": len(y),
                "sample_start": dates[0],
                "sample_end": dates[-1],
                "storage_only_aligned": slim_a,
                "v2_storage_prod_yoy_aligned": slim_v2,
                "candidate_storage_prod_yoy_usd": slim_c,
                "diebold_mariano_vs_v2": dm,
                "regime_stability": {
                    k: v for k, v in regime.items() if k != "halves"
                }
                | {"halves": regime.get("halves")},
                "decision": decision,
                "dollar_coef_path_summary": {
                    "n_windows": len(coef_path),
                    "mean": round(sum(coef_path) / len(coef_path), 6) if coef_path else None,
                    "sign_flip": any(a * b < 0 for a, b in zip(coef_path, coef_path[1:]))
                    if len(coef_path) > 1
                    else None,
                },
            }
        )

    ok_cands = [c for c in candidates if c.get("ok")]
    promoted = [c for c in ok_cands if (c.get("decision") or {}).get("promote")]
    if promoted:
        promoted.sort(
            key=lambda c: -(
                (c.get("decision") or {}).get("oos_rmse_improvement_pct_vs_v2") or -999
            )
        )
        best = promoted[0]
        overall = "Promote"
        overall_plain = (
            f"Promote Broad Dollar into fair value only as `{best.get('transform_id')}`. "
            "Published model stays v2 until a separate wiring step."
        )
    else:
        scored = []
        for c in ok_cands:
            impr = (c.get("decision") or {}).get("oos_rmse_improvement_pct_vs_v2")
            if impr is not None:
                scored.append((impr, c))
        scored.sort(key=lambda x: -x[0])
        best = scored[0][1] if scored else (ok_cands[0] if ok_cands else None)
        overall = "Keep Experimental"
        reject_count = sum(
            1
            for c in ok_cands
            if (c.get("decision") or {}).get("recommendation") == "Reject"
        )
        if ok_cands and reject_count == len(ok_cands):
            overall = "Reject"
            overall_plain = (
                "Reject Broad Dollar for fair value. No transform improves "
                "Storage+Production YoY with a stable, economically sensible "
                "coefficient under walk-forward. Keep published model "
                "ng_storage_production_v2."
            )
        else:
            overall_plain = (
                "Broad Dollar remains Experimental. No transform clears all "
                "promotion gates versus v2 (Storage + Production YoY). Keep "
                "published model ng_storage_production_v2."
            )

    published_decision = "Promote" if overall == "Promote" else "Reject"
    if overall == "Keep Experimental":
        published_decision = "Reject"
        overall_plain = (
            "Reject Broad Dollar for promotion into the published fair-value model. "
            + overall_plain
        )

    payload = {
        "generated_at": generated_at,
        "ok": True,
        "phase": "ng_driver_validation_phase4_broad_dollar",
        "scope": {
            "candidate_driver": "Broad US Dollar Index",
            "series_id": series_id,
            "baseline_storage": "storage_surplus_bcf",
            "baseline_v2": ["storage_surplus_bcf", "production_yoy_pct"],
            "not_tested": [
                "Bond yields",
                "Inflation",
                "Liquidity",
                "Weather/HDD/CDD",
                "LNG (already rejected Phase 3)",
            ],
            "walk_forward": {"min_train": MIN_TRAIN, "step": STEP},
            "promotion_thresholds": {
                "min_oos_rmse_improvement_pct_vs_v2": MIN_OOS_RMSE_IMPROVEMENT_PCT,
                "dm_alpha_one_sided": DM_ALPHA,
                "expected_dollar_sign": "negative",
                "no_sign_flip": True,
                "no_leakage": True,
                "not_single_regime": True,
            },
        },
        "broad_dollar_dataset": dataset,
        "storage_only_model": {
            k: v for k, v in storage_only.items() if not k.startswith("_")
        },
        "v2_storage_production_yoy_model": {
            k: v for k, v in v2_model.items() if not k.startswith("_")
        },
        "dollar_transforms_tested": [
            {"id": tid, "label": lab, "expected_sign": sgn}
            for tid, lab, sgn in TRANSFORM_SPECS
        ],
        "candidates": candidates,
        "best_candidate": (
            {
                "transform_id": best.get("transform_id"),
                "label": best.get("label"),
                "decision": best.get("decision"),
                "candidate_metrics": {
                    "oos_rmse": (best.get("candidate_storage_prod_yoy_usd") or {}).get(
                        "oos_rmse"
                    ),
                    "oos_mae": (best.get("candidate_storage_prod_yoy_usd") or {}).get(
                        "oos_mae"
                    ),
                    "oos_r2": (best.get("candidate_storage_prod_yoy_usd") or {}).get(
                        "oos_r2"
                    ),
                    "coefficients": (
                        best.get("candidate_storage_prod_yoy_usd") or {}
                    ).get("coefficients"),
                    "signs_ok": (best.get("candidate_storage_prod_yoy_usd") or {}).get(
                        "signs_ok"
                    ),
                },
                "regime_stability": best.get("regime_stability"),
                "diebold_mariano_vs_v2": best.get("diebold_mariano_vs_v2"),
            }
            if best
            else None
        ),
        "broad_dollar_recommendation": published_decision,
        "research_status": overall,
        "plain_english": overall_plain,
        "published_model_unchanged": published_decision != "Promote",
        "published_model_id": "ng_storage_production_v2",
        "proposed_model_id_if_promoted": "ng_storage_production_usd_v3",
        "note": (
            "Research-only. Weekly COT untouched. No bonds/inflation/liquidity tested."
        ),
    }
    return json.loads(json.dumps(payload, default=str))


def render_markdown(payload: dict[str, Any]) -> str:
    ds = payload.get("broad_dollar_dataset") or {}
    a = payload.get("storage_only_model") or {}
    b = payload.get("v2_storage_production_yoy_model") or {}
    best = payload.get("best_candidate") or {}
    lines = [
        "# Natural Gas Valuation — Macro Validation Phase 4 (Broad US Dollar)",
        "",
        f"Generated: `{payload.get('generated_at')}`",
        "",
        "## Task 1 — Broad Dollar dataset quality",
        "",
        f"- **Series:** {ds.get('series_name')} (`{ds.get('symbol')}`)",
        f"- **Provider:** {ds.get('provider')}",
        f"- **Source:** {ds.get('data_source')}",
        f"- **URL:** {ds.get('source_url')}",
        f"- **Frequency:** {ds.get('frequency')}",
        f"- **Release cadence:** {ds.get('release_cadence')}",
        f"- **History:** n={((ds.get('history_available') or {}).get('n_observations'))} "
        f"from {(ds.get('history_available') or {}).get('start')} to "
        f"{(ds.get('history_available') or {}).get('end')}",
        f"- **Current observation date:** {ds.get('current_observation_date')} "
        f"(value={ds.get('latest_value')})",
        f"- **Missing periods (>5 calendar days):** {ds.get('missing_period_count')}",
        f"- **Point-in-time safety:** {(ds.get('point_in_time_safety') or {}).get('alignment')}",
        f"- **Revisions policy:** {ds.get('revisions_policy')}",
        f"- **NG alignment:** {(ds.get('alignment_with_ng_history') or {}).get('note')}",
        f"- **Expected sign:** {ds.get('expected_economic_sign')} — {ds.get('economic_rationale')}",
        "",
        "## Task 2 — Transformations tested",
        "",
    ]
    for t in payload.get("dollar_transforms_tested") or []:
        lines.append(f"- `{t.get('id')}` — {t.get('label')} (expect {t.get('expected_sign')})")
    lines += [
        "",
        "## Task 3 — Model comparison (identical walk-forward)",
        "",
        "### Storage-only baseline",
        "",
        f"- OOS RMSE={a.get('oos_rmse')} MAE={a.get('oos_mae')} R²={a.get('oos_r2')} "
        f"in-sample R²={a.get('r_squared')}",
        f"- Coefs: `{json.dumps(a.get('coefficients') or {}, sort_keys=True)}`",
        "",
        "### Current published v2 — Storage + Production YoY",
        "",
        f"- OOS RMSE={b.get('oos_rmse')} MAE={b.get('oos_mae')} R²={b.get('oos_r2')} "
        f"in-sample R²={b.get('r_squared')}",
        f"- Coefs: `{json.dumps(b.get('coefficients') or {}, sort_keys=True)}`",
        "",
        "### Candidate — Storage + Production YoY + Broad Dollar",
        "",
        "| Transform | OOS RMSE | OOS MAE | OOS R² | ΔRMSE% vs v2 | USD coef | Sign OK | Sign flip | DM p | Regime OK | Decision |",
        "|---|---:|---:|---:|---:|---:|---|---|---:|---|---|",
    ]
    for c in payload.get("candidates") or []:
        if not c.get("ok"):
            lines.append(
                f"| {c.get('transform_id')} | — | — | — | — | — | — | — | — | — | "
                f"{c.get('reason')} |"
            )
            continue
        sp = c.get("candidate_storage_prod_yoy_usd") or {}
        d = c.get("decision") or {}
        gates = d.get("gates") or {}
        usd_key = next(
            (k for k in (sp.get("coefficients") or {}) if k.startswith("broad_usd__")),
            None,
        )
        coef = (sp.get("coefficients") or {}).get(usd_key) if usd_key else None
        stab = (
            ((sp.get("coefficient_stability") or {}).get(usd_key) or {}) if usd_key else {}
        )
        dm = c.get("diebold_mariano_vs_v2") or {}
        regime = c.get("regime_stability") or {}
        lines.append(
            "| "
            + " | ".join(
                str(x)
                for x in [
                    c.get("transform_id"),
                    sp.get("oos_rmse"),
                    sp.get("oos_mae"),
                    sp.get("oos_r2"),
                    d.get("oos_rmse_improvement_pct_vs_v2"),
                    coef,
                    gates.get("dollar_sign_ok"),
                    stab.get("sign_flip"),
                    dm.get("p_value_one_sided"),
                    regime.get("not_single_regime"),
                    d.get("recommendation"),
                ]
            )
            + " |"
        )

    bm = best.get("candidate_metrics") or {}
    lines += [
        "",
        f"**Best-performing transformation (by ΔRMSE% vs v2):** "
        f"`{best.get('transform_id')}`",
        "",
        "## Coefficient interpretation",
        "",
        (
            "Economically, a stronger Broad USD should pressure NG prices "
            f"(negative β). Best candidate coefficients: "
            f"`{json.dumps(bm.get('coefficients') or {}, sort_keys=True)}`."
        ),
        "",
        "## Regime stability",
        "",
        f"```json\n{json.dumps(best.get('regime_stability') or {}, indent=2, default=str)}\n```",
        "",
        "## Recommendation",
        "",
        f"**{payload.get('broad_dollar_recommendation')}** "
        f"(research status: {payload.get('research_status')})",
        "",
        payload.get("plain_english") or "",
        "",
        f"Published model remains **`{payload.get('published_model_id')}`** "
        f"(unchanged={payload.get('published_model_unchanged')}).",
        "",
        "## Safety",
        "",
        "- Weekly COT / HPTL_SKIP_VALUATION / Stage 4 / Scanner / Inspector / Seasonality untouched",
        "- No bond yields / inflation / liquidity tested in this phase",
        "",
    ]
    return "\n".join(lines) + "\n"


def write_phase4_outputs(payload: dict[str, Any]) -> dict[str, Path]:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    JSON_OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    MD_OUT.write_text(render_markdown(payload), encoding="utf-8")
    return {"json": JSON_OUT, "markdown": MD_OUT}


__all__ = [
    "run_phase4_broad_dollar_validation",
    "document_broad_dollar_dataset",
    "write_phase4_outputs",
    "render_markdown",
]
