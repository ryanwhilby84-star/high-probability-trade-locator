"""Natural Gas Valuation Workstation — historical PIT reconstruction.

Research / diagnostic only. Does not modify published ng_storage_production_v2
coefficients, live valuation export, COT, Stage 4, Scanner, Inspector, or Seasonality.

Series:
  A) Frozen v2 diagnostic — fixed tip coefficients applied to PIT inputs
  B) Walk-forward point-in-time — authoritative expanding-window refit each week
"""

from __future__ import annotations

import json
import math
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.valuation.energy_ng_drivers import (
    _asof_series_with_obs_date,
    _load_cache_series,
    _load_config,
    _monthly_production_yoy,
    _storage_surplus_vs_5y,
    build_ng_driver_bundle,
)
from hptl.valuation.energy_natural_gas_valuation_v1 import _multivariate_ols
from hptl.valuation.metals_valuation_v1 import _predict_log_price
from hptl.valuation.ng_storage_production_v2 import (
    MAX_PRODUCTION_STALENESS_DAYS,
    MODEL_V1,
    MODEL_V2,
)

AUDIT_DIR = PROJECT_ROOT / "data" / "audits" / "ng_valuation_workstation"
SERIES_JSON = AUDIT_DIR / "ng_valuation_historical_series.json"
WF_META_JSON = AUDIT_DIR / "ng_valuation_walkforward_metadata.json"
BUCKET_JSON = AUDIT_DIR / "ng_valuation_bucket_outcomes.json"
EVENT_JSON = AUDIT_DIR / "ng_valuation_event_study.json"
AUDIT_MD = AUDIT_DIR / "ng_valuation_workstation_audit.md"
PUBLIC_JSON = (
    PROJECT_ROOT
    / "web-dashboard"
    / "public"
    / "data"
    / "ng_valuation_workstation_latest.json"
)
DIST_JSON = (
    PROJECT_ROOT / "web-dashboard" / "dist" / "data" / "ng_valuation_workstation_latest.json"
)
DATA_JSON = PROJECT_ROOT / "data" / "ng_valuation_workstation_latest.json"

# Current published tip fit (diagnostic freeze — not a live historical model).
FROZEN_V2_INTERCEPT = 1.231183
FROZEN_V2_BETA_STORAGE = -0.000799
FROZEN_V2_BETA_YOY = -0.021977
FROZEN_LABEL = "Frozen v2 diagnostic"
WF_LABEL = "Walk-forward point-in-time"

MIN_TRAIN = 156
STORAGE_MAX_AGE_DAYS = 14  # weekly EIA cadence + short publish lag
EVENT_COOLDOWN_WEEKS = 4
FORWARD_HORIZONS = (1, 2, 4, 8, 12)

BUCKETS = (
    ("materially_undervalued", None, -15.0),
    ("undervalued", -15.0, -5.0),
    ("near_fair", -5.0, 5.0),
    ("overvalued", 5.0, 15.0),
    ("materially_overvalued", 15.0, None),
)


def _parse(d: str | None) -> datetime | None:
    if not d:
        return None
    try:
        return datetime.strptime(str(d)[:10], "%Y-%m-%d")
    except ValueError:
        return None


def _age_days(obs: str | None, week: str | None) -> int | None:
    a, b = _parse(obs), _parse(week)
    if a is None or b is None:
        return None
    return (b - a).days


def _bucket(dev: float | None) -> str | None:
    if dev is None or not math.isfinite(dev):
        return None
    if dev <= -15:
        return "materially_undervalued"
    if dev < -5:
        return "undervalued"
    if dev <= 5:
        return "near_fair"
    if dev < 15:
        return "overvalued"
    return "materially_overvalued"


def _fair(intercept: float, b_s: float, b_y: float, s: float, y: float) -> float:
    return math.exp(intercept + b_s * s + b_y * y)


def _dev_pct(price: float, fair: float) -> float:
    return 100.0 * (price - fair) / fair if fair > 0 else float("nan")


def _fwd_return(prices: list[float], i: int, h: int) -> float | None:
    if i + h >= len(prices):
        return None
    p0, p1 = prices[i], prices[i + h]
    if p0 <= 0 or not math.isfinite(p0) or not math.isfinite(p1):
        return None
    return 100.0 * (p1 - p0) / p0


def _mfe_mae(prices: list[float], i: int, h: int) -> tuple[float | None, float | None]:
    """Max favourable / adverse excursion over next h weeks (long-oriented)."""
    if i + h >= len(prices) or prices[i] <= 0:
        return None, None
    p0 = prices[i]
    path = prices[i : i + h + 1]
    rets = [100.0 * (p - p0) / p0 for p in path if math.isfinite(p)]
    if not rets:
        return None, None
    return max(rets), min(rets)


def _mean(xs: list[float]) -> float | None:
    return sum(xs) / len(xs) if xs else None


def _median(xs: list[float]) -> float | None:
    if not xs:
        return None
    s = sorted(xs)
    m = len(s) // 2
    return s[m] if len(s) % 2 else 0.5 * (s[m - 1] + s[m])


def _std(xs: list[float]) -> float | None:
    if len(xs) < 2:
        return None
    mu = sum(xs) / len(xs)
    return math.sqrt(sum((x - mu) ** 2 for x in xs) / (len(xs) - 1))


def _pearson(a: list[float], b: list[float]) -> float | None:
    n = min(len(a), len(b))
    if n < 8:
        return None
    ax, bx = a[:n], b[:n]
    mx, my = sum(ax) / n, sum(bx) / n
    num = sum((ax[i] - mx) * (bx[i] - my) for i in range(n))
    denx = math.sqrt(sum((x - mx) ** 2 for x in ax))
    deny = math.sqrt(sum((y - my) ** 2 for y in bx))
    if denx < 1e-12 or deny < 1e-12:
        return None
    return num / (denx * deny)


def _spearman(a: list[float], b: list[float]) -> float | None:
    n = min(len(a), len(b))
    if n < 8:
        return None

    def ranks(xs: list[float]) -> list[float]:
        order = sorted(range(n), key=lambda i: xs[i])
        r = [0.0] * n
        for rank, i in enumerate(order):
            r[i] = float(rank + 1)
        return r

    return _pearson(ranks(a[:n]), ranks(b[:n]))


def _bootstrap_ci(
    xs: list[float], *, n_boot: int = 400, alpha: float = 0.05
) -> dict[str, float | None]:
    if len(xs) < 8:
        return {"low": None, "high": None, "mean": _mean(xs)}
    rng = random.Random(42)
    means: list[float] = []
    for _ in range(n_boot):
        sample = [xs[rng.randrange(len(xs))] for _ in range(len(xs))]
        means.append(sum(sample) / len(sample))
    means.sort()
    lo = means[int(alpha / 2 * len(means))]
    hi = means[int((1 - alpha / 2) * len(means))]
    return {"low": round(lo, 4), "high": round(hi, 4), "mean": round(sum(xs) / len(xs), 4)}


def build_pit_rows() -> list[dict[str, Any]]:
    """Point-in-time weekly table — no future information."""
    bundle = build_ng_driver_bundle()
    cfg = _load_config()
    cache_map = cfg.get("cache_paths") or {}
    storage_raw = _load_cache_series(cache_map.get("working_gas_storage", ""))
    surplus_raw = _storage_surplus_vs_5y(storage_raw) if storage_raw else {}
    surplus_vals, surplus_obs = _asof_series_with_obs_date(surplus_raw, bundle.dates)

    prod_path = cache_map.get("dry_gas_production", "")
    prod_raw = _load_cache_series(prod_path)
    using_proxy = not bool(prod_raw)
    if using_proxy:
        # Do not invent YoY from proxy for historical workstation — mark unavailable.
        prod_yoy_m: dict[str, float] = {}
    else:
        prod_yoy_m = _monthly_production_yoy(prod_raw)
    yoy_vals, yoy_obs = _asof_series_with_obs_date(prod_yoy_m, bundle.dates)

    # Prefer bundle features when aligned (same as-of logic)
    bund_surplus = bundle.features.get("storage_surplus_bcf") or []
    bund_yoy = bundle.features.get("production_yoy_pct") or []
    bund_yoy_obs = bundle.features.get("production_yoy_observation_date") or []

    rows: list[dict[str, Any]] = []
    for i, week in enumerate(bundle.dates):
        price = float(bundle.price[i])
        s = surplus_vals.get(week)
        if s is None and i < len(bund_surplus) and bund_surplus[i] is not None:
            s = float(bund_surplus[i])
        s_obs = surplus_obs.get(week)
        y = yoy_vals.get(week)
        if y is None and i < len(bund_yoy) and bund_yoy[i] is not None and not using_proxy:
            y = float(bund_yoy[i])
        y_obs = yoy_obs.get(week)
        if y_obs is None and i < len(bund_yoy_obs) and bund_yoy_obs[i]:
            y_obs = str(bund_yoy_obs[i])[:10]

        s_age = _age_days(s_obs, week)
        y_age = _age_days(y_obs, week)
        storage_ok = (
            s is not None
            and math.isfinite(float(s))
            and s_obs is not None
            and s_age is not None
            and 0 <= s_age <= STORAGE_MAX_AGE_DAYS
        )
        prod_ok = (
            not using_proxy
            and y is not None
            and math.isfinite(float(y))
            and y_obs is not None
            and y_age is not None
            and 0 <= y_age <= MAX_PRODUCTION_STALENESS_DAYS
        )
        inputs_available = bool(storage_ok and prod_ok)
        quality = "OK" if inputs_available else (
            "FALLBACK_V1_ELIGIBLE" if storage_ok and not prod_ok else "UNAVAILABLE"
        )
        rows.append(
            {
                "model_week": week,
                "market_price": round(price, 6),
                "storage_surplus_bcf": round(float(s), 6) if storage_ok else None,
                "storage_observation_date": s_obs if storage_ok else None,
                "storage_as_of_date": s_obs if storage_ok else None,
                "storage_age_days": s_age,
                "production_yoy_pct": round(float(y), 6) if prod_ok else None,
                "production_observation_date": y_obs if prod_ok else None,
                "production_as_of_date": y_obs if prod_ok else None,
                "production_age_days": y_age,
                "inputs_available_as_of_week": inputs_available,
                "storage_ok": storage_ok,
                "production_ok": prod_ok,
                "quality_status": quality,
                "active_model_version": MODEL_V2 if inputs_available else None,
                "using_production_proxy": using_proxy,
            }
        )
    return rows


def apply_frozen_v2(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        rec = dict(r)
        if not r.get("inputs_available_as_of_week"):
            rec.update(
                {
                    "series": FROZEN_LABEL,
                    "fair_value": None,
                    "deviation_pct": None,
                    "valuation_bucket": None,
                    "coefficients": None,
                    "quality_status": r.get("quality_status") or "UNAVAILABLE",
                }
            )
            out.append(rec)
            continue
        fair = _fair(
            FROZEN_V2_INTERCEPT,
            FROZEN_V2_BETA_STORAGE,
            FROZEN_V2_BETA_YOY,
            float(r["storage_surplus_bcf"]),
            float(r["production_yoy_pct"]),
        )
        dev = _dev_pct(float(r["market_price"]), fair)
        rec.update(
            {
                "series": FROZEN_LABEL,
                "fair_value": round(fair, 6),
                "deviation_pct": round(dev, 4),
                "valuation_bucket": _bucket(dev),
                "coefficients": {
                    "intercept": FROZEN_V2_INTERCEPT,
                    "storage_surplus_bcf": FROZEN_V2_BETA_STORAGE,
                    "production_yoy_pct": FROZEN_V2_BETA_YOY,
                },
                "active_model_version": MODEL_V2,
                "model_type": FROZEN_LABEL,
                "training_window": None,
                "note": "Diagnostic only — not a true historical live model",
            }
        )
        out.append(rec)
    return out


def apply_walk_forward(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Expanding-window refit; never uses future observations."""
    eligible_idx = [
        i
        for i, r in enumerate(rows)
        if r.get("inputs_available_as_of_week")
        and r.get("storage_surplus_bcf") is not None
        and r.get("production_yoy_pct") is not None
    ]
    out: list[dict[str, Any]] = []
    meta: list[dict[str, Any]] = []
    # Map week -> row for output completeness
    for i, r in enumerate(rows):
        rec = dict(r)
        if i not in eligible_idx:
            rec.update(
                {
                    "series": WF_LABEL,
                    "fair_value": None,
                    "deviation_pct": None,
                    "valuation_bucket": None,
                    "coefficients": None,
                    "model_type": WF_LABEL,
                    "training_window": None,
                    "quality_status": r.get("quality_status") or "UNAVAILABLE",
                }
            )
            out.append(rec)
            continue

        # Training set: eligible weeks strictly before i
        train_ids = [j for j in eligible_idx if j < i]
        if len(train_ids) < MIN_TRAIN:
            rec.update(
                {
                    "series": WF_LABEL,
                    "fair_value": None,
                    "deviation_pct": None,
                    "valuation_bucket": None,
                    "coefficients": None,
                    "model_type": WF_LABEL,
                    "training_window": {
                        "n": len(train_ids),
                        "min_required": MIN_TRAIN,
                        "status": "insufficient_history",
                    },
                    "quality_status": "INSUFFICIENT_TRAIN",
                }
            )
            out.append(rec)
            continue

        y = [math.log(float(rows[j]["market_price"])) for j in train_ids]
        xs = [float(rows[j]["storage_surplus_bcf"]) for j in train_ids]
        xy = [float(rows[j]["production_yoy_pct"]) for j in train_ids]
        beta, r2 = _multivariate_ols(y, [xs, xy])
        if not beta or len(beta) < 3 or r2 is None:
            rec.update(
                {
                    "series": WF_LABEL,
                    "fair_value": None,
                    "deviation_pct": None,
                    "valuation_bucket": None,
                    "coefficients": None,
                    "model_type": WF_LABEL,
                    "quality_status": "FIT_FAILED",
                }
            )
            out.append(rec)
            continue

        intercept, b_s, b_y = float(beta[0]), float(beta[1]), float(beta[2])
        fair = _fair(
            intercept,
            b_s,
            b_y,
            float(r["storage_surplus_bcf"]),
            float(r["production_yoy_pct"]),
        )
        # Leakage guard: training end must be before model week
        train_end = rows[train_ids[-1]]["model_week"]
        train_start = rows[train_ids[0]]["model_week"]
        assert train_end < r["model_week"], "walk-forward leakage: train_end >= model_week"
        dev = _dev_pct(float(r["market_price"]), fair)
        coefs = {
            "intercept": round(intercept, 6),
            "storage_surplus_bcf": round(b_s, 6),
            "production_yoy_pct": round(b_y, 6),
        }
        tw = {
            "start": train_start,
            "end": train_end,
            "n": len(train_ids),
            "in_sample_r2": round(r2, 4),
        }
        rec.update(
            {
                "series": WF_LABEL,
                "fair_value": round(fair, 6),
                "deviation_pct": round(dev, 4),
                "valuation_bucket": _bucket(dev),
                "coefficients": coefs,
                "model_type": WF_LABEL,
                "training_window": tw,
                "active_model_version": MODEL_V2,
                "quality_status": "OK",
            }
        )
        out.append(rec)
        meta.append(
            {
                "model_week": r["model_week"],
                "coefficients": coefs,
                "training_window": tw,
                "fair_value": round(fair, 6),
                "deviation_pct": round(dev, 4),
            }
        )
    return out, meta


def _outcome_table(
    rows: list[dict[str, Any]], *, label: str
) -> dict[str, Any]:
    prices = [float(r["market_price"]) for r in rows]
    by_bucket: dict[str, dict[str, Any]] = {}
    for bname, _, _ in BUCKETS:
        by_bucket[bname] = {str(h): [] for h in FORWARD_HORIZONS}
        by_bucket[bname]["_mfe"] = {str(h): [] for h in FORWARD_HORIZONS}
        by_bucket[bname]["_mae"] = {str(h): [] for h in FORWARD_HORIZONS}

    for i, r in enumerate(rows):
        b = r.get("valuation_bucket")
        if not b or r.get("fair_value") is None:
            continue
        for h in FORWARD_HORIZONS:
            ret = _fwd_return(prices, i, h)
            if ret is None:
                continue
            by_bucket[b][str(h)].append(ret)
            mfe, mae = _mfe_mae(prices, i, h)
            if mfe is not None:
                by_bucket[b]["_mfe"][str(h)].append(mfe)
            if mae is not None:
                by_bucket[b]["_mae"][str(h)].append(mae)

    stats: dict[str, Any] = {}
    for bname, _, _ in BUCKETS:
        stats[bname] = {}
        for h in FORWARD_HORIZONS:
            xs = by_bucket[bname][str(h)]
            mfe_xs = by_bucket[bname]["_mfe"][str(h)]
            mae_xs = by_bucket[bname]["_mae"][str(h)]
            ci = _bootstrap_ci(xs) if xs else {"low": None, "high": None, "mean": None}
            stats[bname][str(h)] = {
                "n": len(xs),
                "mean_forward_return_pct": round(_mean(xs), 4) if xs else None,
                "median_forward_return_pct": round(_median(xs), 4) if xs else None,
                "positive_return_frequency": (
                    round(sum(1 for x in xs if x > 0) / len(xs), 4) if xs else None
                ),
                "max_favourable_excursion_mean": round(_mean(mfe_xs), 4) if mfe_xs else None,
                "max_adverse_excursion_mean": round(_mean(mae_xs), 4) if mae_xs else None,
                "std_dev": round(_std(xs), 4) if xs and len(xs) >= 2 else None,
                "bootstrap_ci_95_mean": ci,
            }

    # Correlations deviation vs forward returns
    corrs: dict[str, Any] = {}
    for h in FORWARD_HORIZONS:
        devs: list[float] = []
        rets: list[float] = []
        for i, r in enumerate(rows):
            if r.get("deviation_pct") is None or r.get("fair_value") is None:
                continue
            ret = _fwd_return(prices, i, h)
            if ret is None:
                continue
            devs.append(float(r["deviation_pct"]))
            rets.append(ret)
        pear = _pearson(devs, rets)
        spear = _spearman(devs, rets)
        corrs[str(h)] = {
            "n": len(devs),
            "pearson": round(pear, 4) if pear is not None else None,
            "spearman": round(spear, 4) if spear is not None else None,
            "expected_sign": "negative (higher overvaluation → weaker forward returns)",
            "sign_matches_expectation": bool(pear is not None and pear < 0),
        }

    # Material undervalued vs overvalued spread
    spreads: dict[str, Any] = {}
    for h in FORWARD_HORIZONS:
        u = by_bucket["materially_undervalued"][str(h)]
        o = by_bucket["materially_overvalued"][str(h)]
        spreads[str(h)] = {
            "undervalued_mean": round(_mean(u), 4) if u else None,
            "overvalued_mean": round(_mean(o), 4) if o else None,
            "spread_undervalued_minus_overvalued": (
                round(_mean(u) - _mean(o), 4) if u and o else None
            ),
            "n_undervalued": len(u),
            "n_overvalued": len(o),
        }

    # Monotonicity of mean returns across ordered buckets
    order = [b[0] for b in BUCKETS]
    mono: dict[str, Any] = {}
    for h in FORWARD_HORIZONS:
        means = [_mean(by_bucket[b][str(h)]) for b in order]
        finite = [(i, m) for i, m in enumerate(means) if m is not None]
        decreasing = all(
            finite[i][1] >= finite[i + 1][1] for i in range(len(finite) - 1)
        ) if len(finite) >= 3 else False
        mono[str(h)] = {
            "bucket_means": {order[i]: (round(m, 4) if m is not None else None) for i, m in enumerate(means)},
            "approximately_monotone_decreasing": decreasing,
        }

    # Regime split
    n = len(rows)
    mid = n // 2
    regimes = {}
    for name, sl in (("first_half", rows[:mid]), ("second_half", rows[mid:])):
        d4: list[float] = []
        r4: list[float] = []
        px = [float(r["market_price"]) for r in sl]
        for i, r in enumerate(sl):
            if r.get("deviation_pct") is None:
                continue
            ret = _fwd_return(px, i, 4)
            if ret is None:
                continue
            d4.append(float(r["deviation_pct"]))
            r4.append(ret)
        pear = _pearson(d4, r4)
        regimes[name] = {
            "n": len(d4),
            "date_start": sl[0]["model_week"] if sl else None,
            "date_end": sl[-1]["model_week"] if sl else None,
            "pearson_dev_vs_4w": round(pear, 4) if pear is not None else None,
            "sign_ok": bool(pear is not None and pear < 0),
        }

    return {
        "series": label,
        "overlap_note": (
            "Adjacent weekly observations share overlapping forward windows at "
            "multiweek horizons; treat n as overlapping sample size, not independent trials."
        ),
        "bucket_forward_stats": stats,
        "deviation_forward_correlations": corrs,
        "material_spread": spreads,
        "monotonicity": mono,
        "regime_stability": regimes,
    }


def build_event_study(
    rows: list[dict[str, Any]], *, cooldown: int = EVENT_COOLDOWN_WEEKS
) -> dict[str, Any]:
    """Non-overlapping events: first entry into a bucket after cooldown outside."""
    prices = [float(r["market_price"]) for r in rows]
    events: list[dict[str, Any]] = []
    streak_outside = {b[0]: cooldown for b in BUCKETS}
    prev: str | None = None
    for i, r in enumerate(rows):
        b = r.get("valuation_bucket")
        if b is None or r.get("fair_value") is None:
            for k in streak_outside:
                streak_outside[k] += 1
            prev = None
            continue
        entered = b != prev
        if entered and streak_outside[b] >= cooldown:
            events.append({"index": i, "bucket": b, "week": r["model_week"]})
        for k in streak_outside:
            streak_outside[k] = 0 if k == b else streak_outside[k] + 1
        prev = b

    by_bucket: dict[str, dict[str, Any]] = {
        b[0]: {str(h): [] for h in FORWARD_HORIZONS} for b in BUCKETS
    }
    for ev in events:
        i = ev["index"]
        b = ev["bucket"]
        for h in FORWARD_HORIZONS:
            ret = _fwd_return(prices, i, h)
            if ret is not None:
                by_bucket[b][str(h)].append(ret)

    stats = {}
    for bname, _, _ in BUCKETS:
        stats[bname] = {}
        for h in FORWARD_HORIZONS:
            xs = by_bucket[bname][str(h)]
            stats[bname][str(h)] = {
                "n_events": len(xs),
                "mean_forward_return_pct": round(_mean(xs), 4) if xs else None,
                "median_forward_return_pct": round(_median(xs), 4) if xs else None,
                "positive_return_frequency": (
                    round(sum(1 for x in xs if x > 0) / len(xs), 4) if xs else None
                ),
                "std_dev": round(_std(xs), 4) if xs and len(xs) >= 2 else None,
                "bootstrap_ci_95_mean": _bootstrap_ci(xs) if xs else None,
            }

    return {
        "cooldown_weeks": cooldown,
        "n_events": len(events),
        "events_sample": events[:50],
        "bucket_forward_stats": stats,
        "note": (
            f"Event = first week price enters a valuation bucket after ≥{cooldown} "
            "weeks outside that bucket. Reduces repeated counting of prolonged episodes."
        ),
    }


def _verdict(wf_outcomes: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    corrs = wf_outcomes.get("deviation_forward_correlations") or {}
    spreads = wf_outcomes.get("material_spread") or {}
    regimes = wf_outcomes.get("regime_stability") or {}
    mono = wf_outcomes.get("monotonicity") or {}

    sign_ok_horizons = sum(
        1 for h in FORWARD_HORIZONS if (corrs.get(str(h)) or {}).get("sign_matches_expectation")
    )
    spread_ok = sum(
        1
        for h in FORWARD_HORIZONS
        if ((spreads.get(str(h)) or {}).get("spread_undervalued_minus_overvalued") or -999) > 0
    )
    regime_ok = sum(1 for v in regimes.values() if v.get("sign_ok"))
    n4 = ((wf_outcomes.get("bucket_forward_stats") or {}).get("materially_undervalued") or {}).get(
        "4", {}
    ).get("n") or 0
    n4o = ((wf_outcomes.get("bucket_forward_stats") or {}).get("materially_overvalued") or {}).get(
        "4", {}
    ).get("n") or 0

    if n4 < 20 or n4o < 20:
        verdict = "Insufficient sample"
        reason = f"Material buckets have small n at 4w (under={n4}, over={n4o})."
    elif sign_ok_horizons >= 4 and spread_ok >= 3 and regime_ok >= 2:
        verdict = "Useful confluence"
        reason = (
            "Deviation↔forward-return correlations are mostly negative, "
            "undervalued outperforms overvalued, and both halves agree on sign."
        )
    elif sign_ok_horizons >= 2 and spread_ok >= 2:
        verdict = "Weak / conditional confluence"
        reason = (
            "Some horizons show the expected relationship, but strength / "
            "monotonicity / regime stability are incomplete."
        )
    else:
        verdict = "No demonstrated usefulness"
        reason = (
            "Walk-forward valuation deviation does not show a consistent "
            "historically useful link to forward returns under these tests."
        )

    return {
        "verdict": verdict,
        "reason": reason,
        "sign_ok_horizons": sign_ok_horizons,
        "spread_ok_horizons": spread_ok,
        "regime_halves_ok": regime_ok,
        "monotonicity_4w": (mono.get("4") or {}).get("approximately_monotone_decreasing"),
        "event_n": event.get("n_events"),
        "thresholds_are_research_buckets_only": True,
    }


def run_ng_valuation_workstation_build(*, write: bool = True) -> dict[str, Any]:
    t0 = datetime.now(timezone.utc)
    pit = build_pit_rows()
    frozen = apply_frozen_v2(pit)
    wf, wf_meta = apply_walk_forward(pit)

    frozen_out = _outcome_table(frozen, label=FROZEN_LABEL)
    wf_out = _outcome_table(wf, label=WF_LABEL)
    events = build_event_study(wf)
    verdict = _verdict(wf_out, events)

    eligible_wf = [r for r in wf if r.get("fair_value") is not None]
    eligible_frozen = [r for r in frozen if r.get("fair_value") is not None]

    # Paired frozen vs WF fair value diffs
    diffs = []
    for a, b in zip(frozen, wf):
        if a.get("fair_value") is not None and b.get("fair_value") is not None:
            diffs.append(abs(float(a["fair_value"]) - float(b["fair_value"])))

    payload = {
        "generated_at": t0.replace(microsecond=0).isoformat(),
        "research_only": True,
        "published_model_untouched": True,
        "published_model_id": MODEL_V2,
        "frozen_equation": (
            f"log(P) = {FROZEN_V2_INTERCEPT} + ({FROZEN_V2_BETA_STORAGE}) * storage_surplus_bcf "
            f"+ ({FROZEN_V2_BETA_YOY}) * production_yoy_pct; fair = exp(log(P))"
        ),
        "coverage": {
            "first_week": pit[0]["model_week"] if pit else None,
            "last_week": pit[-1]["model_week"] if pit else None,
            "n_weeks": len(pit),
            "n_inputs_available": sum(1 for r in pit if r.get("inputs_available_as_of_week")),
            "n_walkforward_fair_values": len(eligible_wf),
            "n_frozen_fair_values": len(eligible_frozen),
            "min_train": MIN_TRAIN,
            "storage_max_age_days": STORAGE_MAX_AGE_DAYS,
            "production_max_age_days": MAX_PRODUCTION_STALENESS_DAYS,
        },
        "frozen_vs_walkforward": {
            "n_paired": len(diffs),
            "mean_abs_fair_diff": round(_mean(diffs), 6) if diffs else None,
            "median_abs_fair_diff": round(_median(diffs), 6) if diffs else None,
            "max_abs_fair_diff": round(max(diffs), 6) if diffs else None,
            "note": (
                f"'{FROZEN_LABEL}' applies fixed tip coefficients; "
                f"'{WF_LABEL}' is the authoritative validation series."
            ),
        },
        "weeks": [
            {
                "model_week": r["model_week"],
                "market_price": r["market_price"],
                "storage_surplus_bcf": r.get("storage_surplus_bcf"),
                "storage_observation_date": r.get("storage_observation_date"),
                "production_yoy_pct": r.get("production_yoy_pct"),
                "production_observation_date": r.get("production_observation_date"),
                "inputs_available_as_of_week": r.get("inputs_available_as_of_week"),
                "quality_status": wf[i].get("quality_status"),
                "walk_forward": {
                    "fair_value": wf[i].get("fair_value"),
                    "deviation_pct": wf[i].get("deviation_pct"),
                    "valuation_bucket": wf[i].get("valuation_bucket"),
                    "coefficients": wf[i].get("coefficients"),
                    "training_window": wf[i].get("training_window"),
                    "model_type": WF_LABEL,
                },
                "frozen_v2": {
                    "fair_value": frozen[i].get("fair_value"),
                    "deviation_pct": frozen[i].get("deviation_pct"),
                    "valuation_bucket": frozen[i].get("valuation_bucket"),
                    "coefficients": frozen[i].get("coefficients"),
                    "model_type": FROZEN_LABEL,
                },
            }
            for i, r in enumerate(pit)
        ],
        "walkforward_metadata": wf_meta,
        "bucket_outcomes_walkforward": wf_out,
        "bucket_outcomes_frozen": frozen_out,
        "event_study_walkforward": events,
        "verdict": verdict,
        "bucket_definitions": [
            {"id": b, "lo": lo, "hi": hi} for b, lo, hi in BUCKETS
        ],
        "runtime_seconds": round((datetime.now(timezone.utc) - t0).total_seconds(), 2),
    }

    if write:
        _write_outputs(payload)
    return payload


def _write_outputs(payload: dict[str, Any]) -> dict[str, Path]:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    DATA_JSON.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_JSON.parent.mkdir(parents=True, exist_ok=True)

    SERIES_JSON.write_text(
        json.dumps(
            {
                "generated_at": payload["generated_at"],
                "coverage": payload["coverage"],
                "weeks": payload["weeks"],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    WF_META_JSON.write_text(
        json.dumps(payload["walkforward_metadata"], indent=2), encoding="utf-8"
    )
    BUCKET_JSON.write_text(
        json.dumps(
            {
                "walkforward": payload["bucket_outcomes_walkforward"],
                "frozen": payload["bucket_outcomes_frozen"],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    EVENT_JSON.write_text(
        json.dumps(payload["event_study_walkforward"], indent=2), encoding="utf-8"
    )
    AUDIT_MD.write_text(_render_md(payload), encoding="utf-8")

    slim = {
        "generated_at": payload["generated_at"],
        "published_model_id": payload["published_model_id"],
        "frozen_equation": payload["frozen_equation"],
        "coverage": payload["coverage"],
        "frozen_vs_walkforward": payload["frozen_vs_walkforward"],
        "verdict": payload["verdict"],
        "bucket_definitions": payload["bucket_definitions"],
        "weeks": payload["weeks"],
        "bucket_outcomes_walkforward": payload["bucket_outcomes_walkforward"],
        "bucket_outcomes_frozen": payload["bucket_outcomes_frozen"],
        "event_study_walkforward": {
            k: v
            for k, v in (payload["event_study_walkforward"] or {}).items()
            if k != "events_sample"
        },
        "research_only": True,
    }
    text = json.dumps(slim, indent=2)
    DATA_JSON.write_text(text, encoding="utf-8")
    PUBLIC_JSON.write_text(text, encoding="utf-8")
    try:
        DIST_JSON.parent.mkdir(parents=True, exist_ok=True)
        DIST_JSON.write_text(text, encoding="utf-8")
    except Exception:
        pass
    return {
        "series": SERIES_JSON,
        "wf_meta": WF_META_JSON,
        "buckets": BUCKET_JSON,
        "events": EVENT_JSON,
        "audit": AUDIT_MD,
        "public": PUBLIC_JSON,
        "data": DATA_JSON,
    }


def _render_md(payload: dict[str, Any]) -> str:
    cov = payload.get("coverage") or {}
    verd = payload.get("verdict") or {}
    diff = payload.get("frozen_vs_walkforward") or {}
    wf = payload.get("bucket_outcomes_walkforward") or {}
    lines = [
        "# Natural Gas Valuation Workstation — Historical Audit",
        "",
        f"Generated: `{payload.get('generated_at')}`",
        "",
        f"**Verdict: {verd.get('verdict')}** — {verd.get('reason')}",
        "",
        "## Coverage",
        "",
        f"- Weeks: {cov.get('first_week')} → {cov.get('last_week')} (n={cov.get('n_weeks')})",
        f"- Inputs available: {cov.get('n_inputs_available')}",
        f"- Walk-forward fair values: {cov.get('n_walkforward_fair_values')}",
        f"- Frozen fair values: {cov.get('n_frozen_fair_values')}",
        "",
        "## Frozen vs walk-forward",
        "",
        f"- Paired n: {diff.get('n_paired')}",
        f"- Mean |Δ fair|: {diff.get('mean_abs_fair_diff')}",
        f"- Max |Δ fair|: {diff.get('max_abs_fair_diff')}",
        "",
        "## Correlations (walk-forward deviation → forward return)",
        "",
    ]
    for h, c in (wf.get("deviation_forward_correlations") or {}).items():
        lines.append(
            f"- {h}w: pearson={c.get('pearson')} spearman={c.get('spearman')} "
            f"sign_ok={c.get('sign_matches_expectation')} n={c.get('n')}"
        )
    lines += [
        "",
        "Published v2 and weekly COT untouched.",
        "",
    ]
    return "\n".join(lines) + "\n"


__all__ = [
    "run_ng_valuation_workstation_build",
    "build_pit_rows",
    "apply_frozen_v2",
    "apply_walk_forward",
    "FROZEN_V2_INTERCEPT",
    "FROZEN_V2_BETA_STORAGE",
    "FROZEN_V2_BETA_YOY",
    "_bucket",
    "_fair",
    "_dev_pct",
    "_fwd_return",
    "_mfe_mae",
    "build_event_study",
    "EVENT_COOLDOWN_WEEKS",
]
