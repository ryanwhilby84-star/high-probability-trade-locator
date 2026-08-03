"""Natural Gas Valuation — Driver Validation Phase 3 (LNG Exports).

Research-only unless LNG clears every promotion gate.
Does not modify weekly COT / HPTL_SKIP_VALUATION.

Compares:
  A) Storage-only
  B) Storage + Production YoY  (current published v2)
  C) Storage + Production YoY + LNG transform  (one transform at a time)
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.valuation.energy_ng_drivers import (
    _load_cache_doc,
    _load_cache_series,
    _load_config,
    build_ng_driver_bundle,
)
from hptl.valuation.ng_driver_validation_phase2_production import (
    DM_ALPHA,
    MIN_OOS_RMSE_IMPROVEMENT_PCT,
    MIN_TRAIN,
    STEP,
    _align_finite,
    _build_production_transforms,
    _diebold_mariano_pvalue,
    _eval_model,
    _month_key,
)
from hptl.valuation.metals_valuation_v1 import MIN_WEEKS

AUDIT_DIR = PROJECT_ROOT / "data" / "audits" / "ng_driver_validation_phase3_lng"
JSON_OUT = AUDIT_DIR / "phase3_lng_validation.json"
MD_OUT = AUDIT_DIR / "phase3_lng_validation.md"

# LNG expected sign: higher exports support domestic prices → positive coef on log(P).
TRANSFORM_SPECS: list[tuple[str, str, str]] = [
    ("raw_level", "raw LNG exports (Bcf/d, as-of weekly)", "positive"),
    ("yoy_pct", "year-over-year LNG exports % change", "positive"),
    ("seasonal_deviation", "deviation from prior-year same-month seasonal norm", "positive"),
    ("trailing_zscore_156", "trailing 156-week z-score (past-only)", "positive"),
    ("chg_4w", "4-week change in as-of LNG exports", "positive"),
    ("chg_12w", "12-week change in as-of LNG exports", "positive"),
    (
        "v1_fullsample_zscore",
        "CURRENT V1 feature: full-sample z-score (LEAKY — audit contrast only)",
        "positive",
    ),
]


def document_lng_dataset() -> dict[str, Any]:
    cfg = _load_config()
    cache_rel = (cfg.get("cache_paths") or {}).get(
        "lng_exports", "data/cache/energy_drivers/eia_lng_exports.json"
    )
    doc = _load_cache_doc(cache_rel)
    series = _load_cache_series(cache_rel)
    dates = sorted(series.keys())
    gaps: list[dict[str, Any]] = []
    if len(dates) >= 2:
        prev = datetime.strptime(dates[0], "%Y-%m-%d")
        for ds in dates[1:]:
            cur = datetime.strptime(ds, "%Y-%m-%d")
            months = (cur.year - prev.year) * 12 + (cur.month - prev.month)
            if months > 1:
                gaps.append(
                    {
                        "from": prev.strftime("%Y-%m"),
                        "to": cur.strftime("%Y-%m"),
                        "months_spanned": months,
                    }
                )
            prev = cur

    return {
        "driver": "lng_exports",
        "source": doc.get("official_source") or "EIA dnav hist_xls",
        "source_url": doc.get("source_url")
        or "https://www.eia.gov/dnav/ng/hist_xls/N9133US2m.xls",
        "series_id": doc.get("series_identifier") or "N9133US2",
        "concept": doc.get("concept")
        or "U.S. liquefied natural gas exports (not terminal feedgas)",
        "units": doc.get("units") or "Bcf/d",
        "frequency": doc.get("frequency") or "monthly",
        "release_cadence": (
            "Monthly EIA Natural Gas Monthly / dnav hist_xls republish. "
            "Typical publish lag ~1–3 months after the reference month."
        ),
        "history_available": {
            "n_observations": len(dates),
            "start": dates[0] if dates else None,
            "end": dates[-1] if dates else None,
        },
        "current_observation_date": doc.get("latest_observation_date")
        or (dates[-1] if dates else None),
        "latest_value": series.get(dates[-1]) if dates else None,
        "missing_periods": gaps,
        "missing_period_count": len(gaps),
        "point_in_time_safety": {
            "native_frequency": "monthly",
            "alignment": (
                "YoY/seasonal transforms computed on native monthly dates or "
                "past-only weekly as-of levels; as-of forward-fill onto weekly "
                "price dates. Full-sample z-score is leaky and used only as contrast."
            ),
            "no_future_peers_in_seasonal_norm": True,
        },
        "revisions_policy": (
            "EIA monthly series can be revised in subsequent Natural Gas Monthly "
            "releases. HPTL stores the latest downloaded hist_xls snapshot; it does "
            "not retain a vintage/point-in-time revision archive. Validation treats "
            "the current cache as the working series (standard for this pillar)."
        ),
        "cache_path": cache_rel,
        "last_successful_refresh": doc.get("last_successful_refresh"),
        "status": doc.get("status"),
        "expected_economic_sign": "positive",
        "economic_rationale": (
            "Stronger LNG export volumes tighten the domestic balance and are "
            "typically supportive for Henry Hub / NG prices (positive coefficient)."
        ),
    }


def _build_production_yoy_from_bundle(bundle) -> list[float | None]:
    """Prefer validated monthly YoY feature; else build from level series."""
    yoy = bundle.features.get("production_yoy_pct")
    if yoy and len(yoy) == bundle.n:
        return list(yoy)
    level = bundle.features.get("dry_gas_production_level") or []
    if len(level) != bundle.n:
        return [None] * bundle.n
    transforms = _build_production_transforms(bundle.dates, [float(v) for v in level])
    return transforms["yoy_pct"]


def _align_triple(
    dates: list[str],
    y: list[float],
    storage: list[float],
    production_yoy: list[float | None],
    lng: list[float | None],
) -> tuple[list[str], list[float], list[float], list[float], list[float]]:
    out_d: list[str] = []
    out_y: list[float] = []
    out_s: list[float] = []
    out_p: list[float] = []
    out_l: list[float] = []
    for d, yi, s, p, l in zip(dates, y, storage, production_yoy, lng):
        if p is None or l is None:
            continue
        if not all(math.isfinite(float(v)) for v in (yi, s, p, l)):
            continue
        out_d.append(d)
        out_y.append(float(yi))
        out_s.append(float(s))
        out_p.append(float(p))
        out_l.append(float(l))
    return out_d, out_y, out_s, out_p, out_l


def _regime_stability(
    *,
    dates: list[str],
    indices: list[int],
    se_v2: list[float],
    se_cand: list[float],
    coef_path: list[float],
) -> dict[str, Any]:
    """Split OOS roughly in half by time; require improvement in both halves + stable sign."""
    n = min(len(se_v2), len(se_cand), len(indices))
    if n < 40:
        return {"ok": False, "reason": "insufficient_oos_for_regime_split"}
    mid = n // 2
    halves = {
        "early": (se_v2[:mid], se_cand[:mid], indices[:mid]),
        "late": (se_v2[mid:n], se_cand[mid:n], indices[mid:n]),
    }
    out: dict[str, Any] = {"ok": True, "halves": {}}
    both_improve = True
    for label, (b, c, idx) in halves.items():
        rmse_b = math.sqrt(sum(b) / len(b))
        rmse_c = math.sqrt(sum(c) / len(c))
        impr = 100.0 * (rmse_b - rmse_c) / rmse_b if rmse_b > 0 else None
        improves = impr is not None and impr > 0
        if not improves:
            both_improve = False
        out["halves"][label] = {
            "n": len(b),
            "date_start": dates[idx[0]] if idx else None,
            "date_end": dates[idx[-1]] if idx else None,
            "v2_oos_rmse": round(rmse_b, 6),
            "candidate_oos_rmse": round(rmse_c, 6),
            "improvement_pct": round(impr, 2) if impr is not None else None,
            "improves": improves,
        }
    # Coefficient path halves
    if len(coef_path) >= 4:
        m = len(coef_path) // 2
        early_sign = coef_path[0] > 0
        late_sign = coef_path[-1] > 0
        early_mean = sum(coef_path[:m]) / m
        late_mean = sum(coef_path[m:]) / (len(coef_path) - m)
        same_sign = (early_mean > 0) == (late_mean > 0)
        out["coefficient_halves"] = {
            "early_mean": round(early_mean, 6),
            "late_mean": round(late_mean, 6),
            "same_sign": same_sign,
            "tip_sign_positive": late_sign,
        }
    else:
        same_sign = True
        out["coefficient_halves"] = {"same_sign": None, "reason": "short_coef_path"}

    out["both_halves_improve"] = both_improve
    out["not_single_regime"] = bool(both_improve and same_sign)
    if not out["not_single_regime"]:
        out["ok"] = False
        out["reason"] = "improvement_or_sign_concentrated_in_one_regime"
    return out


def _promotion_decision(
    *,
    transform_id: str,
    leaky: bool,
    candidate: dict[str, Any],
    v2_baseline: dict[str, Any],
    dm_vs_v2: dict[str, Any],
    regime: dict[str, Any],
) -> dict[str, Any]:
    reasons: list[str] = []
    gates = {
        "oos_improves_vs_v2_gt_2pct": False,
        "lng_sign_ok": False,
        "lng_coef_stable_no_flip": False,
        "no_point_in_time_leakage": not leaky,
        "statistically_meaningful_vs_v2": False,
        "not_driven_by_one_regime": bool(regime.get("not_single_regime")),
    }

    base_rmse = v2_baseline.get("oos_rmse")
    cand_rmse = candidate.get("oos_rmse")
    improvement_pct = None
    if base_rmse and cand_rmse is not None and base_rmse > 0:
        improvement_pct = 100.0 * (base_rmse - cand_rmse) / base_rmse
        gates["oos_improves_vs_v2_gt_2pct"] = improvement_pct >= MIN_OOS_RMSE_IMPROVEMENT_PCT
        if not gates["oos_improves_vs_v2_gt_2pct"]:
            reasons.append(
                f"OOS RMSE improvement vs v2 is {improvement_pct:.2f}% "
                f"(need ≥ {MIN_OOS_RMSE_IMPROVEMENT_PCT:.0f}%)"
            )
    else:
        reasons.append("missing OOS RMSE for v2 comparison")

    lng_name = next(
        (f for f in (candidate.get("features") or []) if f.startswith("lng__")),
        None,
    )
    if lng_name:
        coef = (candidate.get("coefficients") or {}).get(lng_name)
        gates["lng_sign_ok"] = bool(coef is not None and coef > 0)
        if not gates["lng_sign_ok"]:
            reasons.append(
                f"LNG coefficient sign not economically sensible (got {coef}; expect positive)"
            )
        stab = (candidate.get("coefficient_stability") or {}).get(lng_name) or {}
        gates["lng_coef_stable_no_flip"] = not bool(stab.get("sign_flip"))
        if not gates["lng_coef_stable_no_flip"]:
            reasons.append("walk-forward LNG coefficient sign flips")
    else:
        reasons.append("LNG feature missing")

    if leaky:
        reasons.append("transform uses full-sample information (point-in-time leakage)")

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
            f"Transform '{transform_id}' clears every LNG promotion gate versus "
            "Storage+Production YoY (v2)."
        )
    else:
        harmful = (
            improvement_pct is not None
            and improvement_pct < 0
            and not gates["lng_sign_ok"]
            and not gates["lng_coef_stable_no_flip"]
        )
        recommendation = "Reject" if harmful or leaky else "Keep Experimental"
        if recommendation == "Reject" and not harmful and leaky:
            recommendation = "Keep Experimental"
        # Explicit reject when economics + OOS both fail hard
        if (
            improvement_pct is not None
            and improvement_pct < 0
            and not gates["lng_sign_ok"]
        ):
            recommendation = "Reject"
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


def run_phase3_lng_validation(*, as_of_week: str | None = None) -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    dataset = document_lng_dataset()
    bundle = build_ng_driver_bundle(as_of_week=as_of_week)

    if bundle.n < MIN_WEEKS or "storage_surplus_bcf" not in bundle.features:
        return {
            "generated_at": generated_at,
            "ok": False,
            "error": "Insufficient NG panel or missing storage",
            "lng_dataset": dataset,
        }

    lng_level = bundle.features.get("lng_exports_level")
    if not lng_level or len(lng_level) != bundle.n:
        return {
            "generated_at": generated_at,
            "ok": False,
            "error": "Missing lng_exports_level in driver bundle",
            "lng_dataset": dataset,
        }

    y_all = [math.log(p) for p in bundle.price]
    storage_all = bundle.features["storage_surplus_bcf"]
    prod_yoy_all = _build_production_yoy_from_bundle(bundle)
    lng_transforms = _build_production_transforms(
        bundle.dates, [float(v) for v in lng_level]
    )

    # --- A: Storage-only (full storage panel) ---
    d_a, y_a, s_a, _ = _align_finite(bundle.dates, y_all, storage_all, storage_all)
    storage_only = _eval_model(
        name="A_storage",
        dates=d_a,
        y=y_a,
        feature_names=["storage_surplus_bcf"],
        cols=[s_a],
        expected_signs={"storage_surplus_bcf": "negative"},
    )

    # --- B: v2 Storage + Production YoY (aligned where prod YoY exists) ---
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
        series = lng_transforms[transform_id]
        dates, y, s, p, l = _align_triple(
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

        # Re-fit v2 on the SAME aligned sample for nested comparison.
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
        lng_feat = f"lng__{transform_id}"
        candidate = _eval_model(
            name=f"C_storage_prod_yoy_lng_{transform_id}",
            dates=dates,
            y=y,
            feature_names=["storage_surplus_bcf", "production_yoy_pct", lng_feat],
            cols=[s, p, l],
            expected_signs={
                "storage_surplus_bcf": "negative",
                "production_yoy_pct": "negative",
                lng_feat: exp_sign,
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
            "Positive mean_loss_diff means Storage+ProdYoY+LNG has lower MSE than v2."
        )

        lng_stab = (candidate.get("coefficient_stability") or {}).get(lng_feat) or {}
        # Recover coef path from stability head/tail is incomplete; re-walk for regime.
        from hptl.valuation.ng_driver_validation_phase2_production import (
            _walk_forward_predictions,
        )

        wf_full = _walk_forward_predictions(
            y, [s, p, l], feature_names=["storage_surplus_bcf", "production_yoy_pct", lng_feat]
        )
        coef_path_meta = (wf_full.get("coefficient_stability") or {}).get(lng_feat) or {}
        # Rebuild approximate path from stored head/tail only if needed — use DM indices.
        # For regime, use paired errors + sign of mean early/late from stability means.
        # Re-run short coef path extraction:
        coef_path: list[float] = []
        t = MIN_TRAIN
        n = len(y)
        while t < n:
            from hptl.valuation.energy_natural_gas_valuation_v1 import _multivariate_ols

            beta, r2 = _multivariate_ols(y[:t], [s[:t], p[:t], l[:t]])
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

        leaky = transform_id == "v1_fullsample_zscore"
        decision = _promotion_decision(
            transform_id=transform_id,
            leaky=leaky,
            candidate=candidate,
            v2_baseline=v2_aligned,
            dm_vs_v2=dm,
            regime=regime,
        )

        # Also report vs storage-only on this sample
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
                "leaky": leaky,
                "ok": True,
                "n_aligned": len(y),
                "sample_start": dates[0],
                "sample_end": dates[-1],
                "storage_only_aligned": slim_a,
                "v2_storage_prod_yoy_aligned": slim_v2,
                "candidate_storage_prod_yoy_lng": slim_c,
                "diebold_mariano_vs_v2": dm,
                "regime_stability": {
                    k: v for k, v in regime.items() if k != "halves"
                }
                | {"halves": regime.get("halves")},
                "decision": decision,
                "lng_coef_path_summary": {
                    "n_windows": len(coef_path),
                    "mean": round(sum(coef_path) / len(coef_path), 6) if coef_path else None,
                    "sign_flip": any(a * b < 0 for a, b in zip(coef_path, coef_path[1:]))
                    if len(coef_path) > 1
                    else None,
                    **coef_path_meta,
                },
            }
        )

    non_leaky = [c for c in candidates if c.get("ok") and not c.get("leaky")]
    promoted = [c for c in non_leaky if (c.get("decision") or {}).get("promote")]
    if promoted:
        promoted.sort(
            key=lambda c: -(
                (c.get("decision") or {}).get("oos_rmse_improvement_pct_vs_v2") or -999
            )
        )
        best = promoted[0]
        overall = "Promote"
        overall_plain = (
            f"Promote LNG into fair value only as `{best.get('transform_id')}`. "
            "Create ng_storage_production_lng_v3. Published model stays v2 until wiring."
        )
    else:
        scored = []
        for c in non_leaky:
            impr = (c.get("decision") or {}).get("oos_rmse_improvement_pct_vs_v2")
            if impr is not None:
                scored.append((impr, c))
        scored.sort(key=lambda x: -x[0])
        best = scored[0][1] if scored else (non_leaky[0] if non_leaky else None)
        reject_count = sum(
            1
            for c in non_leaky
            if (c.get("decision") or {}).get("recommendation") == "Reject"
        )
        if non_leaky and reject_count == len(non_leaky):
            overall = "Reject"
            overall_plain = (
                "Reject LNG for fair value. No transform improves Storage+Production YoY "
                "with a stable, economically sensible coefficient under walk-forward. "
                "Keep published model ng_storage_production_v2."
            )
        else:
            overall = "Keep Experimental"
            overall_plain = (
                "LNG remains Experimental. No transform clears all promotion gates versus "
                "v2 (Storage + Production YoY). Keep published model ng_storage_production_v2."
            )

    # Force overall Reject wording if best is clearly harmful — user asked Promote/Reject.
    # Map Keep Experimental → Reject for deliverable when nothing promotes (per "Otherwise Remain Experimental"
    # but deliverable says Promote/Reject — we'll report recommendation as Reject for published-model purposes
    # when not Promote, while noting experimental display status).
    published_decision = "Promote" if overall == "Promote" else "Reject"
    if overall == "Keep Experimental":
        published_decision = "Reject"
        overall_plain = (
            "Reject LNG for promotion into the published fair-value model. "
            + overall_plain
        )

    payload = {
        "generated_at": generated_at,
        "ok": True,
        "phase": "ng_driver_validation_phase3_lng",
        "scope": {
            "candidate_driver": "US LNG Exports",
            "series_id": "N9133US2",
            "baseline_storage": "storage_surplus_bcf",
            "baseline_v2": ["storage_surplus_bcf", "production_yoy_pct"],
            "not_tested": ["Weather/HDD/CDD", "Broad USD", "Inflation", "Bonds", "Seasonality"],
            "walk_forward": {"min_train": MIN_TRAIN, "step": STEP},
            "promotion_thresholds": {
                "min_oos_rmse_improvement_pct_vs_v2": MIN_OOS_RMSE_IMPROVEMENT_PCT,
                "dm_alpha_one_sided": DM_ALPHA,
                "expected_lng_sign": "positive",
                "no_sign_flip": True,
                "no_leakage": True,
                "not_single_regime": True,
            },
        },
        "lng_dataset": dataset,
        "storage_only_model": {
            k: v for k, v in storage_only.items() if not k.startswith("_")
        },
        "v2_storage_production_yoy_model": {
            k: v for k, v in v2_model.items() if not k.startswith("_")
        },
        "lng_transforms_tested": [
            {"id": tid, "label": lab, "expected_sign": sgn}
            for tid, lab, sgn in TRANSFORM_SPECS
        ],
        "candidates": candidates,
        "best_non_leaky_candidate": (
            {
                "transform_id": best.get("transform_id"),
                "label": best.get("label"),
                "decision": best.get("decision"),
                "candidate_metrics": {
                    "oos_rmse": (best.get("candidate_storage_prod_yoy_lng") or {}).get(
                        "oos_rmse"
                    ),
                    "oos_mae": (best.get("candidate_storage_prod_yoy_lng") or {}).get(
                        "oos_mae"
                    ),
                    "oos_r2": (best.get("candidate_storage_prod_yoy_lng") or {}).get(
                        "oos_r2"
                    ),
                    "coefficients": (best.get("candidate_storage_prod_yoy_lng") or {}).get(
                        "coefficients"
                    ),
                    "signs_ok": (best.get("candidate_storage_prod_yoy_lng") or {}).get(
                        "signs_ok"
                    ),
                },
            }
            if best
            else None
        ),
        "lng_recommendation": published_decision,
        "research_status": overall,
        "plain_english": overall_plain,
        "published_model_unchanged": published_decision != "Promote",
        "published_model_id": "ng_storage_production_v2",
        "proposed_model_id_if_promoted": "ng_storage_production_lng_v3",
        "note": (
            "Research-only unless recommendation is Promote and a separate wiring "
            "step creates ng_storage_production_lng_v3. Weekly COT untouched."
        ),
    }
    # Strip private keys from nested candidates
    payload = json.loads(json.dumps(payload, default=str))
    return payload


def render_markdown(payload: dict[str, Any]) -> str:
    ds = payload.get("lng_dataset") or {}
    a = payload.get("storage_only_model") or {}
    b = payload.get("v2_storage_production_yoy_model") or {}
    lines = [
        "# Natural Gas Valuation — Driver Validation Phase 3 (LNG Exports)",
        "",
        f"Generated: `{payload.get('generated_at')}`",
        "",
        "## Task 1 — LNG dataset quality",
        "",
        f"- **Source:** {ds.get('source')} (`{ds.get('series_id')}`)",
        f"- **URL:** {ds.get('source_url')}",
        f"- **Frequency:** {ds.get('frequency')} · Units: {ds.get('units')}",
        f"- **Release cadence:** {ds.get('release_cadence')}",
        f"- **History:** n={((ds.get('history_available') or {}).get('n_observations'))} "
        f"from {(ds.get('history_available') or {}).get('start')} to "
        f"{(ds.get('history_available') or {}).get('end')}",
        f"- **Current observation date:** {ds.get('current_observation_date')} "
        f"(value={ds.get('latest_value')})",
        f"- **Missing periods:** {ds.get('missing_period_count')}",
        f"- **Point-in-time safety:** {(ds.get('point_in_time_safety') or {}).get('alignment')}",
        f"- **Revisions policy:** {ds.get('revisions_policy')}",
        f"- **Expected sign:** {ds.get('expected_economic_sign')} — {ds.get('economic_rationale')}",
        "",
        "## Task 3 — Model comparison",
        "",
        "### Storage-only baseline",
        "",
        f"- OOS RMSE={a.get('oos_rmse')} MAE={a.get('oos_mae')} R²={a.get('oos_r2')} "
        f"in-sample R²={a.get('r_squared')}",
        f"- Coefs: `{json.dumps(a.get('coefficients') or {}, sort_keys=True)}`",
        "",
        "### v2 Storage + Production YoY",
        "",
        f"- OOS RMSE={b.get('oos_rmse')} MAE={b.get('oos_mae')} R²={b.get('oos_r2')} "
        f"in-sample R²={b.get('r_squared')}",
        f"- Coefs: `{json.dumps(b.get('coefficients') or {}, sort_keys=True)}`",
        "",
        "### Storage + Production YoY + LNG (one transform at a time)",
        "",
        "| Transform | OOS RMSE | OOS MAE | OOS R² | ΔRMSE% vs v2 | LNG coef | Sign OK | Sign flip | DM p | Regime OK | Leaky | Decision |",
        "|---|---:|---:|---:|---:|---:|---|---|---:|---|---|---|",
    ]
    for c in payload.get("candidates") or []:
        if not c.get("ok"):
            lines.append(
                f"| {c.get('transform_id')} | — | — | — | — | — | — | — | — | — | "
                f"{c.get('leaky')} | {c.get('reason')} |"
            )
            continue
        sp = c.get("candidate_storage_prod_yoy_lng") or {}
        d = c.get("decision") or {}
        gates = d.get("gates") or {}
        lng_key = next(
            (k for k in (sp.get("coefficients") or {}) if k.startswith("lng__")),
            None,
        )
        coef = (sp.get("coefficients") or {}).get(lng_key) if lng_key else None
        stab = ((sp.get("coefficient_stability") or {}).get(lng_key) or {}) if lng_key else {}
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
                    gates.get("lng_sign_ok"),
                    stab.get("sign_flip"),
                    dm.get("p_value_one_sided"),
                    regime.get("not_single_regime"),
                    c.get("leaky"),
                    d.get("recommendation"),
                ]
            )
            + " |"
        )

    lines += [
        "",
        "## Recommendation",
        "",
        f"**{payload.get('lng_recommendation')}** "
        f"(research status: {payload.get('research_status')})",
        "",
        payload.get("plain_english") or "",
        "",
        f"Published model remains **`{payload.get('published_model_id')}`** "
        f"(unchanged={payload.get('published_model_unchanged')}).",
        "",
        "## Safety",
        "",
        "- Weekly COT / HPTL_SKIP_VALUATION untouched",
        "- No Weather / USD / Inflation / Bonds / Seasonality testing",
        "",
    ]
    return "\n".join(lines) + "\n"


def write_phase3_outputs(payload: dict[str, Any]) -> dict[str, Path]:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    JSON_OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    MD_OUT.write_text(render_markdown(payload), encoding="utf-8")
    return {"json": JSON_OUT, "markdown": MD_OUT}
