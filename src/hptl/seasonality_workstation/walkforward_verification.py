"""Strict walk-forward verification for the production Seasonal Roadmap.

This module answers two separate questions:

1. Historical shape fit: before an anchor date, did the production seasonal
   curve actually resemble the realised year-to-date price path after both are
   expressed relative to the same anchor price?
2. Forecast skill: at many historical anchor dates, using only information that
   existed at that date, did the 4/8/12-week seasonal forecast beat a flat
   (zero-return) baseline and get direction right out of sample?

The verifier deliberately calls ``build_production_roadmap`` so it audits the
same volatility-normalised daily texture method shown in Institutional Edge.
Future bars are never supplied to the roadmap builder.
"""
from __future__ import annotations

import math
from datetime import date
from typing import Any, Iterable

from hptl.seasonality_workstation.production_roadmap import (
    METHOD_VERSION as PRODUCTION_METHOD_VERSION,
    build_production_roadmap,
)

DEFAULT_HORIZONS = (4, 8, 12)
HORIZON_TRADING_DAYS = {4: 20, 8: 40, 12: 60}
DIRECTION_EPS = 0.0005  # 5 bp: avoid treating microscopic forecasts as conviction


def _clean_daily(daily: Iterable[tuple[str, float]]) -> list[tuple[str, float]]:
    rows: list[tuple[str, float]] = []
    for d, c in daily:
        try:
            px = float(c)
        except (TypeError, ValueError):
            continue
        ds = str(d)[:10]
        if not ds or not math.isfinite(px) or px <= 0:
            continue
        rows.append((ds, px))
    rows.sort(key=lambda x: x[0])
    # Last observation wins for duplicate dates.
    dedup: dict[str, float] = {d: c for d, c in rows}
    return sorted(dedup.items())


def _mean(xs: list[float]) -> float | None:
    return sum(xs) / len(xs) if xs else None


def _rmse(xs: list[float]) -> float | None:
    return math.sqrt(sum(x * x for x in xs) / len(xs)) if xs else None


def _corr(xs: list[float], ys: list[float]) -> float | None:
    n = min(len(xs), len(ys))
    if n < 3:
        return None
    x = xs[:n]
    y = ys[:n]
    mx = sum(x) / n
    my = sum(y) / n
    dx = [v - mx for v in x]
    dy = [v - my for v in y]
    sx = sum(v * v for v in dx)
    sy = sum(v * v for v in dy)
    if sx <= 0 or sy <= 0:
        return None
    return sum(a * b for a, b in zip(dx, dy)) / math.sqrt(sx * sy)


def _direction(v: float | None, eps: float = DIRECTION_EPS) -> int:
    if v is None:
        return 0
    if v > eps:
        return 1
    if v < -eps:
        return -1
    return 0


def _pct(v: float | None) -> float | None:
    return None if v is None else round(float(v) * 100.0, 3)


def _shape_fit(actual_prices: list[float], seasonal_prices: list[float]) -> dict[str, Any]:
    """Compare shape, not absolute level, with both paths pinned to their last point."""
    n = min(len(actual_prices), len(seasonal_prices))
    if n < 5:
        return {"available": False, "n": n, "reason": "insufficient_overlap"}
    actual = [float(v) for v in actual_prices[-n:]]
    seasonal = [float(v) for v in seasonal_prices[-n:]]
    a_anchor = actual[-1]
    s_anchor = seasonal[-1]
    if a_anchor <= 0 or s_anchor <= 0:
        return {"available": False, "n": n, "reason": "invalid_anchor"}

    a_rel = [v / a_anchor - 1.0 for v in actual]
    s_rel = [v / s_anchor - 1.0 for v in seasonal]
    level_errors = [s - a for s, a in zip(s_rel, a_rel)]

    a_ret = [actual[i] / actual[i - 1] - 1.0 for i in range(1, n)]
    s_ret = [seasonal[i] / seasonal[i - 1] - 1.0 for i in range(1, n)]
    nonzero_pairs = [
        (a, s)
        for a, s in zip(a_ret, s_ret)
        if abs(a) > 1e-12 or abs(s) > 1e-12
    ]
    dir_hits = sum(1 for a, s in nonzero_pairs if _direction(a, 0.0) == _direction(s, 0.0))

    return {
        "available": True,
        "n": n,
        "anchor_normalised": True,
        "level_path_correlation": None if (c := _corr(a_rel, s_rel)) is None else round(c, 4),
        "daily_return_correlation": None if (c := _corr(a_ret, s_ret)) is None else round(c, 4),
        "daily_direction_agreement": None if not nonzero_pairs else round(dir_hits / len(nonzero_pairs), 4),
        "path_mae_pct": round((sum(abs(e) for e in level_errors) / n) * 100.0, 3),
        "path_rmse_pct": round((_rmse(level_errors) or 0.0) * 100.0, 3),
        "note": "Absolute price levels are intentionally ignored; both paths are compared relative to the anchor.",
    }


def _minimal_research(
    *,
    instrument_id: str,
    training: list[tuple[str, float]],
    anchor_date: str,
    anchor_price: float,
    lookback_years: int,
) -> dict[str, Any]:
    label = f"{int(lookback_years)}Y"
    return {
        "status": "ok",
        "instrument_id": instrument_id,
        "price_instrument_id": instrument_id,
        "selected_lookback": label,
        "lookbacks": {
            label: {
                "sample_size": lookback_years,
                "sample_years": [],
                "forward_horizons": {},
            }
        },
        "anchor": {
            "date": anchor_date,
            "price": float(anchor_price),
            "iso_year": int(anchor_date[:4]),
        },
        "integrity": {"status": "PASS"},
        "lookback_agreement": {"score": None},
        "walk_forward": {"hit_rate": None, "n": 0},
        "_daily_closes": training,
    }


def build_roadmap_asof(
    daily: list[tuple[str, float]],
    *,
    instrument_id: str,
    anchor_index: int,
    lookback_years: int = 15,
) -> dict[str, Any]:
    """Build the exact production roadmap with the dataset cut off at the anchor."""
    rows = _clean_daily(daily)
    if anchor_index < 0 or anchor_index >= len(rows):
        return {"available": False, "reason": "anchor_out_of_range"}
    training = rows[: anchor_index + 1]
    anchor_date, anchor_price = training[-1]
    research = _minimal_research(
        instrument_id=instrument_id,
        training=training,
        anchor_date=anchor_date,
        anchor_price=anchor_price,
        lookback_years=lookback_years,
    )
    roadmap = build_production_roadmap(research)
    if roadmap.get("available"):
        roadmap["verification_training_end"] = training[-1][0]
        roadmap["verification_training_rows"] = len(training)
    return roadmap


def _current_year_actual(rows: list[tuple[str, float]], anchor_index: int) -> list[float]:
    anchor_year = int(rows[anchor_index][0][:4])
    return [c for d, c in rows[: anchor_index + 1] if int(d[:4]) == anchor_year]


def _historical_fit(rows: list[tuple[str, float]], anchor_index: int, roadmap: dict[str, Any]) -> dict[str, Any]:
    seasonal = [
        float(p["price"])
        for p in ((roadmap.get("unsmoothed") or {}).get("historical") or [])
        if p.get("price") is not None
    ]
    actual = _current_year_actual(rows, anchor_index)
    return _shape_fit(actual, seasonal)


def _future_excursions(rows: list[tuple[str, float]], anchor_index: int, bars: int) -> tuple[float | None, float | None]:
    if anchor_index + bars >= len(rows):
        return None, None
    p0 = rows[anchor_index][1]
    future = [c for _, c in rows[anchor_index + 1 : anchor_index + bars + 1]]
    if not future or p0 <= 0:
        return None, None
    returns = [p / p0 - 1.0 for p in future]
    return max(returns), min(returns)


def evaluate_anchor(
    daily: list[tuple[str, float]],
    *,
    instrument_id: str,
    anchor_index: int,
    lookback_years: int = 15,
    horizons: tuple[int, ...] = DEFAULT_HORIZONS,
) -> dict[str, Any]:
    """Score one historical anchor strictly out of sample."""
    rows = _clean_daily(daily)
    if anchor_index < 0 or anchor_index >= len(rows):
        return {"available": False, "reason": "anchor_out_of_range"}
    roadmap = build_roadmap_asof(
        rows,
        instrument_id=instrument_id,
        anchor_index=anchor_index,
        lookback_years=lookback_years,
    )
    if not roadmap.get("available"):
        return {
            "available": False,
            "anchor_date": rows[anchor_index][0],
            "reason": roadmap.get("reason") or "roadmap_unavailable",
        }

    anchor_date, anchor_price = rows[anchor_index]
    sample_years = [int(y) for y in roadmap.get("sample_years") or []]
    no_lookahead = (
        roadmap.get("verification_training_end") == anchor_date
        and all(y < int(anchor_date[:4]) for y in sample_years)
    )
    result: dict[str, Any] = {
        "available": True,
        "anchor_date": anchor_date,
        "anchor_price": anchor_price,
        "production_method": (roadmap.get("method") or {}).get("version"),
        "sample_years": sample_years,
        "sample_size": int(roadmap.get("sample_size") or 0),
        "target_daily_scale": roadmap.get("target_daily_scale"),
        "no_lookahead": bool(no_lookahead),
        "historical_shape_fit": _historical_fit(rows, anchor_index, roadmap),
        "horizons": {},
    }

    stats = roadmap.get("forecast_stats") or {}
    for weeks in horizons:
        bars = HORIZON_TRADING_DAYS[int(weeks)]
        key = f"{int(weeks)}w"
        st = stats.get(key) or {}
        predicted_mean = st.get("mean")
        predicted_median = st.get("median")
        actual = None
        if anchor_index + bars < len(rows):
            actual = rows[anchor_index + bars][1] / anchor_price - 1.0
        mfe, mae = _future_excursions(rows, anchor_index, bars)
        pred = None if predicted_mean is None else float(predicted_mean)
        act = None if actual is None else float(actual)
        error = None if pred is None or act is None else pred - act
        result["horizons"][key] = {
            "weeks": weeks,
            "trading_days": bars,
            "forecast_sample_n": int(st.get("n") or 0),
            "predicted_mean_return": pred,
            "predicted_median_return": None if predicted_median is None else float(predicted_median),
            "historical_bullish_frequency": st.get("bullish_frequency"),
            "historical_bearish_frequency": st.get("bearish_frequency"),
            "actual_return": act,
            "error": error,
            "absolute_error": None if error is None else abs(error),
            "direction_hit": None if pred is None or act is None else _direction(pred) == _direction(act),
            "close_based_mfe": mfe,
            "close_based_mae": mae,
        }
    return result


def _aggregate_horizon(anchors: list[dict[str, Any]], key: str) -> dict[str, Any]:
    rows = [a.get("horizons", {}).get(key) or {} for a in anchors if a.get("available")]
    rows = [r for r in rows if r.get("predicted_mean_return") is not None and r.get("actual_return") is not None]
    if not rows:
        return {"n": 0}
    preds = [float(r["predicted_mean_return"]) for r in rows]
    actuals = [float(r["actual_return"]) for r in rows]
    errors = [p - a for p, a in zip(preds, actuals)]
    abs_errors = [abs(e) for e in errors]
    baseline_abs = [abs(a) for a in actuals]
    hits = [bool(r["direction_hit"]) for r in rows]
    model_mae = _mean(abs_errors) or 0.0
    baseline_mae = _mean(baseline_abs) or 0.0
    skill_ratio = None if baseline_mae <= 0 else 1.0 - model_mae / baseline_mae
    mfes = [float(r["close_based_mfe"]) for r in rows if r.get("close_based_mfe") is not None]
    maes = [float(r["close_based_mae"]) for r in rows if r.get("close_based_mae") is not None]
    return {
        "n": len(rows),
        "direction_hit_rate": round(sum(hits) / len(hits), 4),
        "forecast_actual_correlation": None if (c := _corr(preds, actuals)) is None else round(c, 4),
        "mean_predicted_return_pct": _pct(_mean(preds)),
        "mean_actual_return_pct": _pct(_mean(actuals)),
        "bias_pct_points": _pct(_mean(errors)),
        "mae_pct_points": _pct(model_mae),
        "rmse_pct_points": _pct(_rmse(errors)),
        "flat_baseline_mae_pct_points": _pct(baseline_mae),
        "skill_vs_flat_ratio": None if skill_ratio is None else round(skill_ratio, 4),
        "beats_flat_baseline": None if skill_ratio is None else skill_ratio > 0,
        "mean_close_based_mfe_pct": _pct(_mean(mfes)),
        "mean_close_based_mae_pct": _pct(_mean(maes)),
    }


def _aggregate_shape_fit(anchors: list[dict[str, Any]]) -> dict[str, Any]:
    fits = [a.get("historical_shape_fit") or {} for a in anchors if a.get("available")]
    fits = [f for f in fits if f.get("available")]
    if not fits:
        return {"n": 0}
    def vals(name: str) -> list[float]:
        return [float(f[name]) for f in fits if f.get(name) is not None]
    return {
        "n": len(fits),
        "mean_level_path_correlation": None if not vals("level_path_correlation") else round(_mean(vals("level_path_correlation")) or 0.0, 4),
        "mean_daily_return_correlation": None if not vals("daily_return_correlation") else round(_mean(vals("daily_return_correlation")) or 0.0, 4),
        "mean_daily_direction_agreement": None if not vals("daily_direction_agreement") else round(_mean(vals("daily_direction_agreement")) or 0.0, 4),
        "mean_path_rmse_pct": None if not vals("path_rmse_pct") else round(_mean(vals("path_rmse_pct")) or 0.0, 3),
        "interpretation": "This tests whether the pre-anchor grey seasonal path resembled realised price shape; absolute price-level equality is not expected.",
    }


def _verdict(aggregate: dict[str, Any]) -> dict[str, Any]:
    h8 = aggregate.get("8w") or {}
    h12 = aggregate.get("12w") or {}
    n8 = int(h8.get("n") or 0)
    hit8 = h8.get("direction_hit_rate")
    skill8 = h8.get("skill_vs_flat_ratio")
    hit12 = h12.get("direction_hit_rate")
    skill12 = h12.get("skill_vs_flat_ratio")
    reasons: list[str] = []
    if n8 < 12:
        return {
            "status": "INCONCLUSIVE",
            "trust_for_decisions": False,
            "reasons": [f"fewer_than_12_independent_8w_anchors:{n8}"],
        }
    if hit8 is not None and hit8 >= 0.55 and skill8 is not None and skill8 > 0:
        if hit12 is None or hit12 >= 0.52:
            if skill12 is None or skill12 >= 0:
                return {
                    "status": "SUPPORTED",
                    "trust_for_decisions": True,
                    "reasons": ["8w_direction_above_55pct", "8w_error_beats_flat_baseline", "12w_not_contradictory"],
                }
    if hit8 is not None and hit8 < 0.50 and skill8 is not None and skill8 <= 0:
        reasons.extend(["8w_direction_below_50pct", "8w_does_not_beat_flat_baseline"])
        return {"status": "NOT_SUPPORTED", "trust_for_decisions": False, "reasons": reasons}
    return {
        "status": "MIXED",
        "trust_for_decisions": False,
        "reasons": ["out_of_sample_evidence_not_consistently_positive"],
    }


def verify_seasonality_walkforward(
    daily: list[tuple[str, float]],
    *,
    instrument_id: str,
    lookback_years: int = 15,
    step_bars: int = 60,
    horizons: tuple[int, ...] = DEFAULT_HORIZONS,
) -> dict[str, Any]:
    """Run non-overlapping-ish historical anchor tests plus a latest-fit snapshot."""
    rows = _clean_daily(daily)
    max_h = max(HORIZON_TRADING_DAYS[h] for h in horizons)
    anchors: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    # Start after at least five full calendar years can exist. The builder itself
    # is authoritative and will skip anything that still lacks enough history.
    first_year = int(rows[0][0][:4]) if rows else 0
    start_idx = next(
        (i for i, (d, _) in enumerate(rows) if int(d[:4]) >= first_year + 5),
        len(rows),
    )
    stop = max(start_idx, len(rows) - max_h)
    for idx in range(start_idx, stop, max(1, int(step_bars))):
        row = evaluate_anchor(
            rows,
            instrument_id=instrument_id,
            anchor_index=idx,
            lookback_years=lookback_years,
            horizons=horizons,
        )
        if row.get("available"):
            anchors.append(row)
        else:
            skipped.append({"anchor_date": row.get("anchor_date"), "reason": row.get("reason")})

    aggregate = {f"{h}w": _aggregate_horizon(anchors, f"{h}w") for h in horizons}
    shape = _aggregate_shape_fit(anchors)
    latest_fit = None
    latest_roadmap = None
    if rows:
        latest_roadmap = build_roadmap_asof(
            rows,
            instrument_id=instrument_id,
            anchor_index=len(rows) - 1,
            lookback_years=lookback_years,
        )
        if latest_roadmap.get("available"):
            latest_fit = _historical_fit(rows, len(rows) - 1, latest_roadmap)

    no_lookahead_ok = bool(anchors) and all(bool(a.get("no_lookahead")) for a in anchors)
    verdict = _verdict(aggregate)
    if not no_lookahead_ok:
        verdict = {
            "status": "FAIL_NO_LOOKAHEAD",
            "trust_for_decisions": False,
            "reasons": ["one_or_more_anchors_failed_no_lookahead_assertion"],
        }

    latest_forecast = {}
    if latest_roadmap and latest_roadmap.get("available"):
        for h in horizons:
            st = (latest_roadmap.get("forecast_stats") or {}).get(f"{h}w") or {}
            latest_forecast[f"{h}w"] = {
                "n": st.get("n"),
                "mean_return_pct": _pct(st.get("mean")),
                "median_return_pct": _pct(st.get("median")),
                "bullish_frequency": st.get("bullish_frequency"),
                "bearish_frequency": st.get("bearish_frequency"),
            }

    return {
        "instrument": instrument_id,
        "production_method": PRODUCTION_METHOD_VERSION,
        "verification_method": "strict_asof_walkforward_v1",
        "lookback_years": int(lookback_years),
        "anchor_step_bars": int(step_bars),
        "max_forward_horizon_trading_days": max_h,
        "bar_count": len(rows),
        "first_date": rows[0][0] if rows else None,
        "last_date": rows[-1][0] if rows else None,
        "anchors_evaluated": len(anchors),
        "anchors_skipped": len(skipped),
        "no_lookahead_pass": no_lookahead_ok,
        "current_snapshot": {
            "historical_shape_fit": latest_fit,
            "forecast": latest_forecast,
            "note": "Current snapshot fit answers whether the grey pre-today seasonal shape resembled this year's actual price path. It does not prove future forecast skill.",
        },
        "historical_shape_fit": shape,
        "out_of_sample": aggregate,
        "verdict": verdict,
        "anchors": anchors,
        "skipped": skipped,
        "metric_notes": {
            "direction_hit_rate": "Share of historical anchors where forecast mean-return direction matched realised forward direction.",
            "skill_vs_flat_ratio": "1 - model_MAE/zero_return_baseline_MAE. Positive is better than simply forecasting no change.",
            "mfe_mae": "Close-based forward excursions because the canonical seasonality loader supplies daily closes.",
            "historical_shape_fit": "Anchor-normalised; absolute historical price levels are intentionally not compared.",
        },
    }
