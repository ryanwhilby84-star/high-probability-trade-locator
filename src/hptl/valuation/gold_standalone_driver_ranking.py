"""Gold Valuation — Standalone Driver Ranking Gate (research only).

Ranks every Tier-1 candidate with a *standalone* walk-forward score
**before** any combination testing. Weak variables are rejected so they
never seed overfitting combinations.

Does NOT modify published Gold valuation, NG, COT, Stage 4, Scanner,
Inspector, Seasonality, or dashboard wiring.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.fx.fx_macro_history import load_fred_daily_map
from hptl.valuation.energy_natural_gas_valuation_v1 import (
    _multivariate_ols,
    _predict_log_price,
)
from hptl.valuation.gold_macro_tier1_discovery import (
    _align,
    _asof_series,
    _load_dx_daily,
)
from hptl.valuation.gold_phase2_macro_physical_discovery import _transforms_from_level
from hptl.valuation.metals_valuation_v1 import (
    DXY_SERIES,
    MODEL_ID as PUBLISHED_MODEL_ID,
    REAL_YIELD_SERIES,
    _build_weekly_panel,
    _load_dxy_series,
)
from hptl.valuation.ng_driver_validation_phase2_production import (
    MIN_TRAIN,
    STEP,
    _eval_model,
)

AUDIT_DIR = PROJECT_ROOT / "data" / "audits" / "gold_standalone_driver_ranking"
JSON_OUT = AUDIT_DIR / "gold_standalone_driver_ranking.json"
MD_OUT = AUDIT_DIR / "gold_standalone_driver_ranking.md"

# Decision thresholds for the ranking table the research lead requested.
SCORE_KEEP = 70.0
SCORE_MAYBE = 50.0

# Expected economic signs on log(Gold).
CANDIDATES: list[dict[str, Any]] = [
    {
        "id": "us_10y_real_yield",
        "label": "US Real 10-Year Yield",
        "symbol": REAL_YIELD_SERIES,  # DFII10
        "provider": "FRED",
        "tier": 1,
        "kind": "rate",
        "expected_sign": "negative",
        "rationale": "Primary opportunity cost of holding gold.",
    },
    {
        "id": "dxy_ice",
        "label": "DXY (ICE DX futures)",
        "symbol": "DX",
        "provider": "price store / ICE",
        "tier": 1,
        "kind": "dollar",
        "expected_sign": "negative",
        "rationale": "Gold priced globally in dollars; stronger USD -> weaker gold.",
    },
    {
        "id": "broad_usd",
        "label": "Broad US Dollar Index",
        "symbol": DXY_SERIES,  # DTWEXBGS
        "provider": "FRED",
        "tier": 1,
        "kind": "dollar",
        "expected_sign": "negative",
        "rationale": "Alternate dollar factor (trade-weighted). Competing twin of ICE DXY.",
    },
    {
        "id": "us_10y_yield",
        "label": "US 10-Year Treasury Yield",
        "symbol": "DGS10",
        "provider": "FRED",
        "tier": 1,
        "kind": "rate",
        "expected_sign": "negative",
        "rationale": "Nominal rate environment; may add info beyond real yields.",
    },
    {
        "id": "breakeven_10y",
        "label": "Inflation Expectations (10Y Breakeven)",
        "symbol": "T10YIE",
        "provider": "FRED",
        "tier": 1,
        "kind": "rate",
        "expected_sign": "positive",
        "rationale": "Gold as inflation hedge — test before combining.",
    },
    {
        "id": "fed_funds",
        "label": "Federal Funds Rate",
        "symbol": "DFF",
        "provider": "FRED",
        "tier": 1,
        "kind": "rate",
        "expected_sign": "negative",
        "rationale": "Monetary policy regime; may explain long-duration valuation shifts.",
    },
]


def _coef_sign_flip(path: list[float]) -> bool:
    if len(path) < 4:
        return False
    signs = [1 if v > 0 else (-1 if v < 0 else 0) for v in path if v != 0]
    if not signs:
        return False
    return min(signs) < 0 < max(signs)


def _extreme_mean_reversion(
    dates: list[str],
    prices: list[float],
    fair_logs: list[float | None],
    *,
    horizon: int = 12,
) -> dict[str, Any]:
    """Quintile study: undervalued (neg deviation) should earn higher forward returns."""
    rows: list[tuple[float, float]] = []
    n = len(prices)
    for i in range(n - horizon):
        fl = fair_logs[i]
        if fl is None or not math.isfinite(fl) or fl <= 0:
            continue
        fair = math.exp(fl)
        if fair <= 0 or prices[i] <= 0:
            continue
        dev = 100.0 * (prices[i] / fair - 1.0)
        fwd = 100.0 * (prices[i + horizon] / prices[i] - 1.0)
        if math.isfinite(dev) and math.isfinite(fwd):
            rows.append((dev, fwd))
    if len(rows) < 40:
        return {"ok": False, "n": len(rows), "quality_score": 0.0}
    rows.sort(key=lambda r: r[0])
    q = max(1, len(rows) // 5)
    under = rows[:q]
    over = rows[-q:]
    under_mean = sum(r[1] for r in under) / len(under)
    over_mean = sum(r[1] for r in over) / len(over)
    spread = under_mean - over_mean  # want positive (undervalued outperforms)
    # Map spread to 0..10: +8pp → full marks, ≤0 → 0
    quality = max(0.0, min(10.0, (spread / 8.0) * 10.0))
    return {
        "ok": True,
        "n": len(rows),
        "horizon_weeks": horizon,
        "undervalued_mean_fwd_pct": round(under_mean, 3),
        "overvalued_mean_fwd_pct": round(over_mean, 3),
        "spread_under_minus_over_pp": round(spread, 3),
        "quality_score": round(quality, 2),
    }


def _standalone_score(eval_row: dict[str, Any], extremes: dict[str, Any]) -> dict[str, Any]:
    """Compose 0–100 score. In-sample R² alone cannot dominate."""
    parts: dict[str, float] = {}
    # Sign (full-sample OLS)
    parts["economic_sign"] = 25.0 if eval_row.get("signs_ok") else 0.0

    # Walk-forward OOS R² (0..30)
    oos_r2 = eval_row.get("oos_r2")
    if oos_r2 is None or not math.isfinite(float(oos_r2)):
        parts["oos_r2"] = 0.0
    else:
        parts["oos_r2"] = max(0.0, min(30.0, float(oos_r2) / 0.35 * 30.0))

    # OOS RMSE vs train-mean naive (0..20)
    # Lower RMSE better; use relative improvement vs naive if present.
    naive = eval_row.get("naive_oos_rmse")
    oos_rmse = eval_row.get("oos_rmse")
    if (
        naive is not None
        and oos_rmse is not None
        and float(naive) > 1e-12
        and math.isfinite(float(oos_rmse))
    ):
        impr = (float(naive) - float(oos_rmse)) / float(naive)
        parts["vs_naive"] = max(0.0, min(20.0, impr * 40.0))  # 50% better → 20
    else:
        parts["vs_naive"] = 0.0

    # Coefficient stability (0..15)
    flip = bool(eval_row.get("coef_sign_flip"))
    parts["stability"] = 0.0 if flip else 15.0

    # Extreme mean-reversion (0..10)
    parts["extremes"] = float(extremes.get("quality_score") or 0.0)

    total = sum(parts.values())
    return {
        "standalone_score": round(total, 1),
        "score_parts": {k: round(v, 2) for k, v in parts.items()},
        "max_possible": 100.0,
    }


def _keep_decision(score: float, signs_ok: bool, flip: bool) -> str:
    if score >= SCORE_KEEP and signs_ok and not flip:
        return "Keep"
    if score >= SCORE_MAYBE and signs_ok:
        return "Maybe"
    if score >= SCORE_MAYBE and not signs_ok:
        return "Maybe"  # weak / wrong-sign — do not auto-combine
    return "Reject"


def _keep_emoji(decision: str) -> str:
    return {"Keep": "KEEP", "Maybe": "MAYBE", "Reject": "REJECT"}.get(decision, decision)


def _naive_oos_rmse(y: list[float], indices: list[int], min_train: int = MIN_TRAIN) -> float | None:
    """RMSE of predicting each OOS point with expanding train mean of y."""
    if not indices:
        return None
    errs: list[float] = []
    for i in indices:
        if i < min_train:
            continue
        mu = sum(y[:i]) / i
        errs.append((y[i] - mu) ** 2)
    if not errs:
        return None
    return math.sqrt(sum(errs) / len(errs))


def _walk_forward_fair_logs(
    y: list[float], x_col: list[float]
) -> tuple[list[float | None], dict[str, Any]]:
    """Point-in-time fair log-price series (None until min_train)."""
    n = len(y)
    fair: list[float | None] = [None] * n
    wf = _walk_forward_predictions(y, [x_col], feature_names=["x"])
    # Fill from expanding fits at each step boundary — approximate with last beta path.
    # Rebuild properly with step windows for extremes study.
    t = MIN_TRAIN
    while t < n:
        beta, r2 = _multivariate_ols(y[:t], [x_col[:t]])
        if not beta or r2 is None:
            t += STEP
            continue
        end = min(t + STEP, n)
        for i in range(t, end):
            fair[i] = _predict_log_price(beta, [x_col[i]])
        t += STEP
    return fair, wf


def _load_raw_levels(dates: list[str]) -> dict[str, list[float | None]]:
    """As-of weekly levels (pre-transform) for each candidate driver."""
    dgs10 = load_fred_daily_map("DGS10")
    dfii = load_fred_daily_map(REAL_YIELD_SERIES)
    breakeven = load_fred_daily_map("T10YIE")
    fed = load_fred_daily_map("DFF")
    broad = load_fred_daily_map(DXY_SERIES)
    if len(broad) < 52:
        broad = _load_dxy_series()
    dx = _load_dx_daily()
    return {
        "us_10y_real_yield": _asof_series(dfii, dates),
        "us_10y_yield": _asof_series(dgs10, dates),
        "breakeven_10y": _asof_series(breakeven, dates),
        "fed_funds": _asof_series(fed, dates),
        "broad_usd": _asof_series(broad, dates),
        "dxy_ice": _asof_series(dx, dates) if dx else [None] * len(dates),
    }


def _finite_level_col(series: list[float | None]) -> list[float] | None:
    if any(v is None or not math.isfinite(float(v)) for v in series):
        # Allow sparse None — transforms need a dense level where possible.
        # Forward-fill within panel for transform engineering only (as-of already causal).
        out: list[float] = []
        last: float | None = None
        for v in series:
            if v is not None and math.isfinite(float(v)):
                last = float(v)
            if last is None:
                return None
            out.append(last)
        return out
    return [float(v) for v in series]  # type: ignore[arg-type]


def run_gold_standalone_driver_ranking(*, as_of_week: str | None = None) -> dict[str, Any]:
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
            "research_only": True,
        }

    dates_all = [o.date for o in panel]
    y_all = [math.log(o.price) for o in panel]
    prices_all = [o.price for o in panel]
    raw_levels = _load_raw_levels(dates_all)
    price_by_date = {o.date: o.price for o in panel}

    ranking_rows: list[dict[str, Any]] = []
    dataset_quality: list[dict[str, Any]] = []
    # Best feature series for combination stage: variable_id -> transformed weekly series
    best_feature_map: dict[str, list[float | None]] = {}

    for spec in CANDIDATES:
        fid = spec["id"]
        series = raw_levels.get(fid) or []
        missing = sum(1 for v in series if v is None or not math.isfinite(float(v)))
        tip_val = None
        tip_date = None
        for d, v in zip(reversed(dates_all), reversed(series)):
            if v is not None and math.isfinite(float(v)):
                tip_val = float(v)
                tip_date = d
                break
        dataset_quality.append(
            {
                "id": fid,
                "label": spec["label"],
                "symbol": spec["symbol"],
                "provider": spec["provider"],
                "tier": spec["tier"],
                "n_panel": len(dates_all),
                "missing_on_panel": missing,
                "tip_date": tip_date,
                "tip_value": tip_val,
                "coverage_ok": missing < len(dates_all) * 0.05,
            }
        )

        level_col = _finite_level_col(series)
        if level_col is None:
            ranking_rows.append(
                {
                    "variable": fid,
                    "label": spec["label"],
                    "standalone_score": 0.0,
                    "keep": "Reject",
                    "keep_mark": "X",
                    "reason": "no usable level series",
                    "ok": False,
                }
            )
            continue

        transforms = _transforms_from_level(level_col, kind=str(spec["kind"]))
        transform_trials: list[dict[str, Any]] = []

        for t_name, t_series in transforms.items():
            feature_key = f"{fid}__{t_name}"
            d_al, y_al, x_al = _align(
                dates_all, y_all, {feature_key: t_series}, [feature_key]
            )
            if len(y_al) < MIN_TRAIN + 40:
                continue
            prices_al = [price_by_date[d] for d in d_al]
            eval_row = _eval_model(
                name=f"standalone_{feature_key}",
                dates=d_al,
                y=y_al,
                feature_names=[feature_key],
                cols=[x_al[feature_key]],
                expected_signs={feature_key: spec["expected_sign"]},
            )
            if not eval_row.get("ok"):
                continue
            stab = (eval_row.get("coefficient_stability") or {}).get(feature_key) or {}
            flip = bool(stab.get("sign_flip")) or _coef_sign_flip(
                list(stab.get("path_head") or []) + list(stab.get("path_tail") or [])
            )
            # Prefer stability flag from walk-forward helper when present
            if "sign_flip" in stab:
                flip = bool(stab["sign_flip"])
            eval_row["coef_sign_flip"] = flip
            eval_row["naive_oos_rmse"] = (
                round(_naive_oos_rmse(y_al, eval_row.get("_indices") or []) or 0.0, 6)
                if eval_row.get("_indices")
                else None
            )
            fair_logs, _ = _walk_forward_fair_logs(y_al, x_al[feature_key])
            extremes = _extreme_mean_reversion(d_al, prices_al, fair_logs)
            scored = _standalone_score(eval_row, extremes)
            transform_trials.append(
                {
                    "transform": t_name,
                    "feature_key": feature_key,
                    "n_aligned": len(y_al),
                    "sample_start": d_al[0],
                    "sample_end": d_al[-1],
                    "series": t_series,
                    "in_sample_r2": eval_row.get("r_squared"),
                    "oos_r2": eval_row.get("oos_r2"),
                    "oos_rmse": eval_row.get("oos_rmse"),
                    "oos_mae": eval_row.get("oos_mae"),
                    "naive_oos_rmse": eval_row.get("naive_oos_rmse"),
                    "coefficient": (eval_row.get("coefficients") or {}).get(feature_key),
                    "signs_ok": eval_row.get("signs_ok"),
                    "coef_sign_flip": flip,
                    "extremes": extremes,
                    **scored,
                }
            )

        if not transform_trials:
            ranking_rows.append(
                {
                    "variable": fid,
                    "label": spec["label"],
                    "standalone_score": 0.0,
                    "keep": "Reject",
                    "keep_mark": "X",
                    "reason": "all transforms failed",
                    "ok": False,
                }
            )
            continue

        # Best transform: highest standalone score, break ties with sign_ok then lower RMSE
        transform_trials.sort(
            key=lambda t: (
                -float(t["standalone_score"]),
                0 if t.get("signs_ok") else 1,
                float(t["oos_rmse"]) if t.get("oos_rmse") is not None else 9e9,
            )
        )
        best_t = transform_trials[0]
        decision = _keep_decision(
            float(best_t["standalone_score"]),
            bool(best_t.get("signs_ok")),
            bool(best_t.get("coef_sign_flip")),
        )
        best_feature_map[fid] = best_t["series"]
        ranking_rows.append(
            {
                "variable": fid,
                "label": spec["label"],
                "symbol": spec["symbol"],
                "tier": spec["tier"],
                "expected_sign": spec["expected_sign"],
                "rationale": spec["rationale"],
                "best_transform": best_t["transform"],
                "transforms_tested": [
                    {
                        "transform": t["transform"],
                        "standalone_score": t["standalone_score"],
                        "oos_r2": t["oos_r2"],
                        "oos_rmse": t["oos_rmse"],
                        "signs_ok": t["signs_ok"],
                        "coef_sign_flip": t["coef_sign_flip"],
                    }
                    for t in transform_trials
                ],
                "ok": True,
                "n_aligned": best_t["n_aligned"],
                "sample_start": best_t["sample_start"],
                "sample_end": best_t["sample_end"],
                "in_sample_r2": best_t["in_sample_r2"],
                "oos_r2": best_t["oos_r2"],
                "oos_rmse": best_t["oos_rmse"],
                "oos_mae": best_t["oos_mae"],
                "naive_oos_rmse": best_t["naive_oos_rmse"],
                "coefficient": best_t["coefficient"],
                "signs_ok": best_t["signs_ok"],
                "coef_sign_flip": best_t["coef_sign_flip"],
                "extremes": best_t["extremes"],
                "standalone_score": best_t["standalone_score"],
                "score_parts": best_t["score_parts"],
                "keep": decision,
                "keep_mark": _keep_emoji(decision),
            }
        )

    ranking_rows.sort(key=lambda r: (-float(r.get("standalone_score") or 0), r["variable"]))
    for i, row in enumerate(ranking_rows, start=1):
        row["rank"] = i

    winners = [r for r in ranking_rows if r.get("keep") == "Keep"]
    maybe = [r for r in ranking_rows if r.get("keep") == "Maybe"]
    rejected = [r for r in ranking_rows if r.get("keep") == "Reject"]

    # Dollar twin rule: if both dollar factors Keep/Maybe, keep the higher score only.
    dollar_ids = {"dxy_ice", "broad_usd"}
    dollar_alive = [
        r for r in ranking_rows if r["variable"] in dollar_ids and r["keep"] in ("Keep", "Maybe")
    ]
    dollar_note = None
    if len(dollar_alive) >= 2:
        dollar_alive.sort(key=lambda r: -float(r["standalone_score"]))
        keep_d = dollar_alive[0]
        drop_d = dollar_alive[1]
        if drop_d["keep"] != "Reject":
            drop_d["keep"] = "Reject"
            drop_d["keep_mark"] = "X"
            drop_d["reject_reason_override"] = (
                f"Dollar twin of {keep_d['variable']} - keep higher standalone score only"
            )
            dollar_note = drop_d["reject_reason_override"]
            winners = [r for r in ranking_rows if r.get("keep") == "Keep"]
            maybe = [r for r in ranking_rows if r.get("keep") == "Maybe"]
            rejected = [r for r in ranking_rows if r.get("keep") == "Reject"]

    # Incremental combinations — only Keep (+ optional single Maybe add-on).
    combo_pool = [r["variable"] for r in winners if r["variable"] in best_feature_map]
    # Allow at most one Maybe into the pool for exploratory nested tests.
    if maybe:
        maybe_sorted = sorted(maybe, key=lambda r: -float(r["standalone_score"]))
        if (
            maybe_sorted[0]["variable"] not in combo_pool
            and maybe_sorted[0]["variable"] in best_feature_map
        ):
            combo_pool.append(maybe_sorted[0]["variable"])

    combo_results: list[dict[str, Any]] = []
    if combo_pool:
        # Align all combo features on common sample using each variable's best transform
        d_c, y_c, x_c = _align(dates_all, y_all, best_feature_map, combo_pool)
        if len(y_c) >= MIN_TRAIN + 40:
            expected = {
                s["id"]: s["expected_sign"] for s in CANDIDATES if s["id"] in combo_pool
            }
            # Nested: start from best standalone, add next, …
            ordered = sorted(
                combo_pool,
                key=lambda fid: -next(
                    float(r["standalone_score"])
                    for r in ranking_rows
                    if r["variable"] == fid
                ),
            )
            growing: list[str] = []
            prev_rmse = None
            for fid in ordered:
                growing.append(fid)
                cols = [x_c[f] for f in growing]
                ev = _eval_model(
                    name="+".join(growing),
                    dates=d_c,
                    y=y_c,
                    feature_names=list(growing),
                    cols=cols,
                    expected_signs=expected,
                )
                if not ev.get("ok"):
                    continue
                rmse = ev.get("oos_rmse")
                delta = None
                if prev_rmse is not None and rmse is not None and prev_rmse > 1e-12:
                    delta = round(100.0 * (prev_rmse - float(rmse)) / prev_rmse, 2)
                combo_results.append(
                    {
                        "features": list(growing),
                        "signs_ok": ev.get("signs_ok"),
                        "oos_rmse": rmse,
                        "oos_r2": ev.get("oos_r2"),
                        "oos_mae": ev.get("oos_mae"),
                        "in_sample_r2": ev.get("r_squared"),
                        "oos_rmse_improvement_vs_prev_pct": delta,
                        "coefficients": ev.get("coefficients"),
                        "meaningful_improvement": bool(delta is not None and delta >= 2.0)
                        if delta is not None
                        else True,
                    }
                )
                prev_rmse = float(rmse) if rmse is not None else prev_rmse

            # Also evaluate all 2-feature Keep pairs (small)
            keep_ids = [r["variable"] for r in winners]
            for a, b in combinations(keep_ids, 2):
                feats = [a, b]
                if any(
                    set(c["features"]) == set(feats) for c in combo_results if c.get("features")
                ):
                    continue
                cols = [x_c[f] for f in feats]
                ev = _eval_model(
                    name="+".join(feats),
                    dates=d_c,
                    y=y_c,
                    feature_names=feats,
                    cols=cols,
                    expected_signs=expected,
                )
                if ev.get("ok"):
                    combo_results.append(
                        {
                            "features": feats,
                            "signs_ok": ev.get("signs_ok"),
                            "oos_rmse": ev.get("oos_rmse"),
                            "oos_r2": ev.get("oos_r2"),
                            "oos_mae": ev.get("oos_mae"),
                            "in_sample_r2": ev.get("r_squared"),
                            "oos_rmse_improvement_vs_prev_pct": None,
                            "coefficients": ev.get("coefficients"),
                            "pair_test": True,
                        }
                    )

    # Best combo: prefer signs_ok, then lowest OOS RMSE
    sign_ok_combos = [c for c in combo_results if c.get("signs_ok")]
    pool = sign_ok_combos or combo_results
    best_combo = None
    if pool:
        best_combo = min(
            pool,
            key=lambda c: (
                0 if c.get("signs_ok") else 1,
                float(c["oos_rmse"]) if c.get("oos_rmse") is not None else 9e9,
            ),
        )

    elapsed = round((datetime.now(timezone.utc) - t0).total_seconds(), 2)
    return {
        "generated_at": generated_at,
        "ok": True,
        "research_only": True,
        "phase": "gold_standalone_driver_ranking",
        "published_model_untouched": True,
        "published_model_id": PUBLISHED_MODEL_ID,
        "philosophy": {
            "rank_before_combine": True,
            "prefer_simple_economic_models": True,
            "avoid_overfitting": True,
            "score_keep_threshold": SCORE_KEEP,
            "score_maybe_threshold": SCORE_MAYBE,
        },
        "panel": {
            "n_weeks": len(panel),
            "start": dates_all[0],
            "end": dates_all[-1],
            "spot_latest": prices_all[-1] if prices_all else None,
        },
        "walk_forward": {"min_train": MIN_TRAIN, "step": STEP},
        "dataset_quality": dataset_quality,
        "ranking_table": ranking_rows,
        "winners": [r["variable"] for r in winners],
        "maybe": [r["variable"] for r in maybe],
        "rejected": [r["variable"] for r in rejected],
        "dollar_twin_note": dollar_note,
        "combination_pool": combo_pool,
        "incremental_combinations": combo_results,
        "best_combination": best_combo,
        "recommendation": _recommendation(winners, maybe, rejected, best_combo, dollar_note),
        "not_modified": [
            "published Gold valuation",
            "Natural Gas valuation",
            "weekly COT",
            "Stage 4",
            "Scanner",
            "Inspector",
            "Seasonality",
        ],
        "runtime_sec": elapsed,
    }


def _recommendation(
    winners: list[dict[str, Any]],
    maybe: list[dict[str, Any]],
    rejected: list[dict[str, Any]],
    best_combo: dict[str, Any] | None,
    dollar_note: str | None,
) -> dict[str, Any]:
    keep_ids = [r["variable"] for r in winners]
    maybe_ids = [r["variable"] for r in maybe]
    lines = [
        "Ranked every Tier-1 candidate standalone before combining.",
        f"Keep: {keep_ids or '[]'}; Maybe: {maybe_ids or '[]'}; "
        f"Reject: {[r['variable'] for r in rejected]}.",
    ]
    if dollar_note:
        lines.append(dollar_note)
    if best_combo and best_combo.get("signs_ok"):
        lines.append(
            f"Best sign-ok combination so far: {best_combo.get('features')} "
            f"(OOS R²={best_combo.get('oos_r2')}, RMSE={best_combo.get('oos_rmse')})."
        )
        lines.append(
            "Next: only add Tier-2 drivers (CB purchases, M2, VIX) if they beat this "
            "walk-forward baseline by ≥2% OOS RMSE with stable economic signs."
        )
        status = "PROCEED_TO_TIER2_ON_WINNERS_ONLY"
    elif keep_ids:
        lines.append(
            "Standalone Keep winners exist but no sign-stable combination cleared gates yet. "
            "Do not kitchen-sink. Re-check transforms (levels vs changes) on Keep set only."
        )
        status = "KEEP_WINNERS_NO_STABLE_COMBO"
    else:
        lines.append(
            "No Tier-1 variable cleared Keep. Do not build combinations. "
            "Investigate transforms / sample regimes before Tier-2."
        )
        status = "NO_TIER1_KEEPERS"
    lines.append("Published metals_real_yield_v1 left untouched. Research only.")
    return {
        "status": status,
        "keep_for_combinations": keep_ids,
        "narrative": " ".join(lines),
    }


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Gold Standalone Driver Ranking",
        "",
        f"Generated: `{payload.get('generated_at')}`",
        "",
        "**Research only. Published Gold valuation was not modified.**",
        "",
        "Philosophy: rank every variable standalone before combining. "
        "Only Keep / selected Maybe winners enter combination tests.",
        "",
        f"Panel: n={payload.get('panel', {}).get('n_weeks')} "
        f"({payload.get('panel', {}).get('start')} -> {payload.get('panel', {}).get('end')})",
        "",
        "## Ranking table",
        "",
        "| Rank | Variable | Best transform | Standalone Score | Keep? | OOS R2 | OOS RMSE | Sign OK | Coef flip | Extremes spread |",
        "|---:|---|---|---:|:---:|---:|---:|:---:|:---:|---:|",
    ]
    for row in payload.get("ranking_table") or []:
        ext = row.get("extremes") or {}
        lines.append(
            f"| {row.get('rank')} | {row.get('label')} (`{row.get('variable')}`) | "
            f"{row.get('best_transform')} | "
            f"{row.get('standalone_score')} | {row.get('keep')} | "
            f"{row.get('oos_r2')} | {row.get('oos_rmse')} | {row.get('signs_ok')} | "
            f"{row.get('coef_sign_flip')} | {ext.get('spread_under_minus_over_pp')} |"
        )
    lines.extend(
        [
            "",
            "### Score components (max 100)",
            "",
            "- Economic sign (full-sample): 25",
            "- Walk-forward OOS R²: 30",
            "- OOS RMSE vs expanding-mean naive: 20",
            "- Coefficient stability (no walk-forward sign flip): 15",
            "- Extreme mean-reversion quality: 10",
            "",
            f"- **Keep** if score ≥ {SCORE_KEEP}, sign OK, no coef flip",
            f"- **Maybe** if score ≥ {SCORE_MAYBE} and sign OK (or borderline)",
            "- **Reject** otherwise",
            "",
            "## Dataset quality",
            "",
            "| Driver | Symbol | Missing | Tip date | Tip value |",
            "|---|---|---:|---|---:|",
        ]
    )
    for q in payload.get("dataset_quality") or []:
        lines.append(
            f"| {q.get('label')} | `{q.get('symbol')}` | {q.get('missing_on_panel')} | "
            f"{q.get('tip_date')} | {q.get('tip_value')} |"
        )

    lines.extend(["", "## Incremental combinations (winners only)", ""])
    combos = payload.get("incremental_combinations") or []
    if not combos:
        lines.append("_No combinations run — insufficient Keep winners._")
    else:
        lines.append(
            "| Features | Signs OK | OOS R² | OOS RMSE | ΔRMSE vs prev % | In-sample R² |"
        )
        lines.append("|---|:---:|---:|---:|---:|---:|")
        for c in combos:
            lines.append(
                f"| {', '.join(c.get('features') or [])} | {c.get('signs_ok')} | "
                f"{c.get('oos_r2')} | {c.get('oos_rmse')} | "
                f"{c.get('oos_rmse_improvement_vs_prev_pct')} | {c.get('in_sample_r2')} |"
            )

    best = payload.get("best_combination") or {}
    rec = payload.get("recommendation") or {}
    lines.extend(
        [
            "",
            "## Best combination",
            "",
            f"- Features: `{best.get('features')}`",
            f"- Signs OK: `{best.get('signs_ok')}`",
            f"- OOS R² / RMSE: `{best.get('oos_r2')}` / `{best.get('oos_rmse')}`",
            "",
            "## Recommendation",
            "",
            f"- Status: **{rec.get('status')}**",
            f"- {rec.get('narrative')}",
            "",
            f"Runtime: {payload.get('runtime_sec')}s",
            "",
            "## Safety",
            "",
            "- No published valuation changes",
            "- Natural Gas / weekly COT / Stage 4 / Scanner / Inspector / Seasonality untouched",
            "",
        ]
    )
    return "\n".join(lines)


def write_outputs(payload: dict[str, Any]) -> dict[str, Path]:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    JSON_OUT.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    MD_OUT.write_text(render_markdown(payload), encoding="utf-8")
    return {"json": JSON_OUT, "md": MD_OUT}
