"""Return-based seasonality engine for Seasonality Workstation V1."""

from __future__ import annotations

import math
import sys
from datetime import datetime, timedelta
from typing import Any

from hptl.seasonality_workstation.integrity import audit_daily_series
from hptl.seasonality_workstation.models import (
    DEFAULT_LOOKBACK,
    ENGINE_VERSION,
    FORWARD_WEEKS,
    LOOKBACKS,
)
from hptl.seasonality_workstation.indexed_seasonality import (
    DEFAULT_LOOKBACK_YEARS,
    DEFAULT_SMOOTH,
    build_normalised_seasonal_curve,
    load_daily_closes_for_seasonality,
    walk_forward_hit_rate,
)
from hptl.seasonality_workstation.seasonal_price_path import (
    build_seasonal_price_path_curve,
)
from hptl.seasonality_workstation.seasonal_roadmap import (
    build_seasonal_roadmap_curve,
)
from hptl.seasonality_workstation.weekly_roadmap import build_weekly_roadmap
from hptl.seasonality_workstation.returns import (
    iso_week,
    weekly_closes_from_daily,
    weekly_return_rows,
)
from hptl.seasonality_workstation.stats import bucket_stats
from hptl.seasonality_workstation.turns import detect_turning_windows


def _lookback_years(label: str) -> int | None:
    for name, yrs in LOOKBACKS:
        if name == label:
            return yrs
    return 10


def _eligible_years(
    rows: list[dict[str, Any]],
    *,
    lookback: str,
    asof_year: int,
    usable_years: list[int],
) -> list[int]:
    yrs = _lookback_years(lookback)
    hist = [y for y in usable_years if y < asof_year]
    if yrs is None:
        return hist
    cutoff = asof_year - yrs
    return [y for y in hist if y >= cutoff]


def _week_buckets(
    rows: list[dict[str, Any]], years: list[int]
) -> dict[int, list[float]]:
    year_set = set(years)
    buckets: dict[int, list[float]] = {w: [] for w in range(1, 53)}
    for r in rows:
        if r["iso_year"] not in year_set:
            continue
        ret = r.get("return")
        w = int(r["iso_week"])
        if ret is None or w < 1 or w > 52:
            continue
        if not math.isfinite(ret):
            continue
        buckets[w].append(float(ret))
    return buckets


def _compound_path(
    week_stats: dict[int, dict[str, Any]],
    *,
    field: str = "trimmed_mean",
    fallback: str = "median",
) -> dict[int, float]:
    """Cumulative index path starting at 100 using seasonal week returns."""
    idx = 100.0
    out: dict[int, float] = {}
    for w in range(1, 53):
        st = week_stats.get(w) or {}
        r = st.get(field)
        if r is None:
            r = st.get(fallback)
        if r is None:
            r = st.get("mean")
        if r is None:
            r = 0.0
        idx *= 1.0 + float(r)
        out[w] = idx
    return out


def _current_year_path(
    weekly: list[tuple[str, float]], year: int
) -> dict[int, float]:
    closes = {iso_week(d)[1]: c for d, c in weekly if iso_week(d)[0] == year}
    if not closes:
        return {}
    base_w = min(closes.keys())
    base = closes[base_w]
    if not base:
        return {}
    return {w: (c / base) * 100.0 for w, c in closes.items()}


def _align_seasonal_to_price(
    seasonal_index: dict[int, float],
    *,
    anchor_week: int,
    anchor_price: float,
) -> dict[int, float | None]:
    """Map seasonal index onto price space so path passes through anchor_price at anchor_week."""
    base_idx = seasonal_index.get(anchor_week)
    if base_idx is None or base_idx == 0 or anchor_price is None:
        return {w: None for w in range(1, 53)}
    scale = anchor_price / base_idx
    return {w: (seasonal_index[w] * scale if w in seasonal_index else None) for w in range(1, 53)}


def _forward_dates(last_date: str, weeks: int) -> list[str]:
    dt = datetime.strptime(last_date[:10], "%Y-%m-%d")
    out: list[str] = []
    for i in range(1, weeks + 1):
        out.append((dt + timedelta(weeks=i)).strftime("%Y-%m-%d"))
    return out


def _project_price_path(
    week_stats: dict[int, dict[str, Any]],
    *,
    field: str,
    anchor_week: int,
    anchor_price: float,
    anchor_date: str,
    weeks: int = FORWARD_WEEKS,
    fallbacks: tuple[str, ...] = ("median", "mean", "trimmed_mean"),
) -> list[dict[str, Any]]:
    """Apply existing seasonal week returns to latest close — price units, no jump."""
    fwd_dates = _forward_dates(anchor_date, weeks)
    path: list[dict[str, Any]] = [
        {
            "week_offset": 0,
            "iso_week": anchor_week,
            "date": anchor_date,
            "price": float(anchor_price),
            "cumulative_return": 0.0,
            "expected_week_return": None,
            "segment": "anchor",
        }
    ]
    px = float(anchor_price)
    for i, d in enumerate(fwd_dates, start=1):
        w = ((anchor_week - 1 + i) % 52) + 1
        st = week_stats.get(w) or {}
        r = st.get(field)
        if r is None:
            for fb in fallbacks:
                if fb == field:
                    continue
                if st.get(fb) is not None:
                    r = st.get(fb)
                    break
        if r is None:
            r = 0.0
        px = px * (1.0 + float(r))
        path.append(
            {
                "week_offset": i,
                "iso_week": w,
                "date": d,
                "price": float(px),
                "cumulative_return": float(px / anchor_price - 1.0),
                "expected_week_return": float(r),
                "segment": "forward",
            }
        )
    return path


def _confidence_pack(
    *,
    week_stats: dict[int, dict[str, Any]],
    anchor_week: int,
    lookback_agreement: dict[str, Any],
    usable_n: int,
) -> dict[str, Any]:
    near = [
        week_stats[w]
        for w in range(max(1, anchor_week - 1), min(52, anchor_week + 2) + 1)
        if week_stats.get(w)
    ]
    ns = [s.get("n") or 0 for s in near]
    dispersions = [s.get("dispersion") for s in near if s.get("dispersion") is not None]
    pos = [s.get("positive_frequency") for s in near if s.get("positive_frequency") is not None]
    sample = sum(ns) / len(ns) if ns else 0
    disp = sum(dispersions) / len(dispersions) if dispersions else None
    pos_f = sum(pos) / len(pos) if pos else None
    agree = lookback_agreement.get("score")

    # Transparent multi-factor confidence (not a black box)
    sample_score = min(1.0, sample / 15.0)
    disp_score = 1.0 - min(1.0, (disp or 0.05) / 0.08) if disp is not None else 0.4
    agree_score = agree if agree is not None else 0.5
    freq_score = abs((pos_f or 0.5) - 0.5) * 2.0  # stronger when away from coin-flip
    history_score = min(1.0, usable_n / 15.0)
    composite = (
        0.30 * sample_score
        + 0.20 * disp_score
        + 0.25 * agree_score
        + 0.15 * freq_score
        + 0.10 * history_score
    )
    label = "HIGH" if composite >= 0.7 else "MEDIUM" if composite >= 0.45 else "LOW"
    return {
        "label": label,
        "composite": round(composite, 3),
        "sample_size": round(sample, 2),
        "dispersion": round(disp, 4) if disp is not None else None,
        "positive_frequency": round(pos_f, 3) if pos_f is not None else None,
        "lookback_agreement": agree,
        "usable_history_years": usable_n,
        "factors": {
            "sample_score": round(sample_score, 3),
            "dispersion_score": round(disp_score, 3),
            "agreement_score": round(agree_score, 3),
            "frequency_score": round(freq_score, 3),
            "history_score": round(history_score, 3),
        },
    }


def _lookback_agreement(
    paths: dict[str, dict[int, float]], anchor_week: int
) -> dict[str, Any]:
    """Agreement of forward 8-week cumulative returns across lookbacks."""
    labels = [k for k in ("5Y", "10Y", "15Y", "20Y", "FULL") if k in paths]
    fwd_rets: dict[str, float] = {}
    for lab in labels:
        p = paths[lab]
        a = p.get(anchor_week)
        b_w = min(52, anchor_week + 8)
        b = p.get(b_w)
        if a and b and a > 0:
            fwd_rets[lab] = b / a - 1.0
    if len(fwd_rets) < 2:
        return {"score": None, "forward_8w_by_lookback": fwd_rets, "sign_agreement": None}
    signs = [1 if v > 0.002 else -1 if v < -0.002 else 0 for v in fwd_rets.values()]
    nonzero = [s for s in signs if s != 0]
    if not nonzero:
        score = 0.5
        sign_agree = None
    else:
        maj = 1 if sum(1 for s in nonzero if s > 0) >= sum(1 for s in nonzero if s < 0) else -1
        sign_agree = sum(1 for s in nonzero if s == maj) / len(nonzero)
        score = sign_agree
    return {
        "score": round(score, 3) if score is not None else None,
        "forward_8w_by_lookback": {k: round(v, 4) for k, v in fwd_rets.items()},
        "sign_agreement": round(sign_agree, 3) if sign_agree is not None else None,
    }


def _forward_horizon_stats(
    rows: list[dict[str, Any]],
    years: list[int],
    anchor_week: int,
    horizons: tuple[int, ...] = (4, 8, 12),
) -> dict[str, Any]:
    """Average forward N-week cumulative return from anchor_week across years."""
    by_year: dict[int, dict[int, float]] = {}
    for r in rows:
        y = r["iso_year"]
        if y not in years or r.get("return") is None:
            continue
        by_year.setdefault(y, {})[int(r["iso_week"])] = float(r["return"])

    out: dict[str, Any] = {}
    for h in horizons:
        cumuls: list[float] = []
        for y, week_rets in by_year.items():
            c = 1.0
            ok = True
            for w in range(anchor_week + 1, min(52, anchor_week + h) + 1):
                if w not in week_rets:
                    ok = False
                    break
                c *= 1.0 + week_rets[w]
            if ok and h > 0:
                cumuls.append(c - 1.0)
        st = bucket_stats(cumuls)
        out[f"{h}w"] = {
            "mean_return": st["mean"],
            "median_return": st["median"],
            "positive_frequency": st["positive_frequency"],
            "n": st["n"],
            "dispersion": st["dispersion"],
        }
    return out


def _right_panel_stats(
    *,
    horizon: dict[str, Any],
    confidence: dict[str, Any],
    week_stats: dict[int, dict[str, Any]],
    anchor_week: int,
    turns: list[dict[str, Any]],
    years: list[int],
) -> dict[str, Any]:
    h4 = horizon.get("4w") or {}
    h8 = horizon.get("8w") or {}
    h12 = horizon.get("12w") or {}
    mean8 = h8.get("mean_return")
    bias = "NEUTRAL"
    if mean8 is not None:
        if mean8 >= 0.01:
            bias = "BULLISH"
        elif mean8 <= -0.01:
            bias = "BEARISH"

    # Drawdown / rally from seasonal path over next 12 weeks using mean week returns
    path = 1.0
    peak = 1.0
    trough = 1.0
    for w in range(anchor_week + 1, min(52, anchor_week + 12) + 1):
        r = (week_stats.get(w) or {}).get("trimmed_mean")
        if r is None:
            r = (week_stats.get(w) or {}).get("mean") or 0.0
        path *= 1.0 + float(r)
        peak = max(peak, path)
        trough = min(trough, path)
    avg_rally = peak - 1.0
    avg_drawdown = trough - 1.0

    # Largest historical absolute 8w move from this calendar week
    largest = None
    largest_abs = -1.0
    for lab, pack in horizon.items():
        # computed below in caller via year paths — keep simple from h8 dispersion
        pass
    st_anchor = week_stats.get(anchor_week) or {}

    active_turn = None
    for t in turns:
        win = t.get("window") or {}
        if win.get("start_week") <= anchor_week <= win.get("end_week"):
            active_turn = t
            break

    return {
        "current_seasonal_bias": bias,
        "average_4w_return_pct": None if h4.get("mean_return") is None else round(h4["mean_return"] * 100, 2),
        "average_8w_return_pct": None if h8.get("mean_return") is None else round(h8["mean_return"] * 100, 2),
        "average_12w_return_pct": None if h12.get("mean_return") is None else round(h12["mean_return"] * 100, 2),
        "positive_years_pct": None
        if h8.get("positive_frequency") is None
        else round(h8["positive_frequency"] * 100, 1),
        "negative_years_pct": None
        if h8.get("positive_frequency") is None
        else round((1.0 - h8["positive_frequency"]) * 100, 1),
        "average_drawdown_pct": round(avg_drawdown * 100, 2),
        "average_rally_pct": round(avg_rally * 100, 2),
        "confidence": confidence,
        "largest_historical_move_note": (
            f"Anchor week dispersion (weekly return σ) "
            f"{(st_anchor.get('dispersion') or 0) * 100:.2f}%"
        ),
        "current_seasonal_window": active_turn,
        "sample_years": len(years),
        "anchor_week_stats": {
            k: (round(v, 6) if isinstance(v, float) else v)
            for k, v in st_anchor.items()
        },
    }


def compute_lookback_block(
    rows: list[dict[str, Any]],
    weekly: list[tuple[str, float]],
    *,
    lookback: str,
    asof_year: int,
    usable_years: list[int],
    anchor_week: int,
    anchor_price: float,
    anchor_date: str,
) -> dict[str, Any]:
    years = _eligible_years(rows, lookback=lookback, asof_year=asof_year, usable_years=usable_years)
    buckets = _week_buckets(rows, years)
    week_stats = {w: bucket_stats(buckets[w]) for w in range(1, 53)}
    mean_path = _compound_path(week_stats, field="mean")
    median_path = _compound_path(week_stats, field="median")
    trimmed_path = _compound_path(week_stats, field="trimmed_mean")
    q25_path = _compound_path(week_stats, field="q25")
    q75_path = _compound_path(week_stats, field="q75")

    seasonal_price = _align_seasonal_to_price(
        trimmed_path, anchor_week=anchor_week, anchor_price=anchor_price
    )
    upper_price = _align_seasonal_to_price(
        q75_path, anchor_week=anchor_week, anchor_price=anchor_price
    )
    lower_price = _align_seasonal_to_price(
        q25_path, anchor_week=anchor_week, anchor_price=anchor_price
    )
    median_price = _align_seasonal_to_price(
        median_path, anchor_week=anchor_week, anchor_price=anchor_price
    )

    # Historical years as price-aligned paths (optional overlay)
    year_paths: dict[str, dict[int, float | None]] = {}
    for y in years[-12:]:  # cap overlays
        y_closes = {iso_week(d)[1]: c for d, c in weekly if iso_week(d)[0] == y}
        if not y_closes or anchor_week not in y_closes or not y_closes[anchor_week]:
            # align via week-1 style rebase then scale to anchor price using seasonal method
            if not y_closes:
                continue
            base_w = min(y_closes)
            base = y_closes[base_w]
            idx = {w: (c / base) * 100.0 for w, c in y_closes.items()}
        else:
            base = y_closes[anchor_week]
            idx = {w: (c / base) * 100.0 for w, c in y_closes.items()}
        # scale so anchor week = anchor_price when present else skip
        if anchor_week in idx and idx[anchor_week]:
            scale = anchor_price / idx[anchor_week]
            year_paths[str(y)] = {w: idx.get(w, None) and idx[w] * scale for w in range(1, 53)}

    # Forward projection in actual price units (no jump at latest close).
    # Default product model = median; mean also emitted for UI model switch.
    def _round_path(path: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out = []
        for p in path:
            row = {
                **p,
                "price": round(p["price"], 6),
                "cumulative_return": round(p["cumulative_return"], 6),
            }
            if p.get("expected_week_return") is not None:
                row["expected_week_return"] = round(p["expected_week_return"], 6)
                row["expected_return"] = row["expected_week_return"]
            out.append(row)
        return out

    projection_median = _round_path(
        _project_price_path(
            week_stats,
            field="median",
            anchor_week=anchor_week,
            anchor_price=anchor_price,
            anchor_date=anchor_date,
        )
    )
    projection_mean = _round_path(
        _project_price_path(
            week_stats,
            field="mean",
            anchor_week=anchor_week,
            anchor_price=anchor_price,
            anchor_date=anchor_date,
        )
    )
    projection_upper = _round_path(
        _project_price_path(
            week_stats,
            field="q75",
            anchor_week=anchor_week,
            anchor_price=anchor_price,
            anchor_date=anchor_date,
            fallbacks=("median", "mean"),
        )
    )
    projection_lower = _round_path(
        _project_price_path(
            week_stats,
            field="q25",
            anchor_week=anchor_week,
            anchor_price=anchor_price,
            anchor_date=anchor_date,
            fallbacks=("median", "mean"),
        )
    )
    # Legacy key retained for compatibility — product default is median.
    projection = projection_median

    horizon = _forward_horizon_stats(rows, years, anchor_week)
    turns = detect_turning_windows(week_stats, rows, years)

    return {
        "lookback": lookback,
        "sample_years": years,
        "sample_size": len(years),
        "week_stats": {
            str(w): {
                k: (round(v, 6) if isinstance(v, float) else v)
                for k, v in st.items()
            }
            for w, st in week_stats.items()
        },
        "index_paths": {
            "mean": {str(k): round(v, 4) for k, v in mean_path.items()},
            "median": {str(k): round(v, 4) for k, v in median_path.items()},
            "trimmed_mean": {str(k): round(v, 4) for k, v in trimmed_path.items()},
            "q25": {str(k): round(v, 4) for k, v in q25_path.items()},
            "q75": {str(k): round(v, 4) for k, v in q75_path.items()},
        },
        "price_aligned": {
            "trimmed_mean": {str(k): (None if v is None else round(v, 6)) for k, v in seasonal_price.items()},
            "median": {str(k): (None if v is None else round(v, 6)) for k, v in median_price.items()},
            "upper_band": {str(k): (None if v is None else round(v, 6)) for k, v in upper_price.items()},
            "lower_band": {str(k): (None if v is None else round(v, 6)) for k, v in lower_price.items()},
        },
        "historical_year_paths": {
            y: {str(k): (None if v is None else round(v, 6)) for k, v in path.items()}
            for y, path in year_paths.items()
        },
        "projection": projection,
        "forecast": {
            "start_date": anchor_date,
            "start_price": round(float(anchor_price), 6),
            "horizon_weeks": FORWARD_WEEKS,
            "models": {
                "median": projection_median,
                "mean": projection_mean,
            },
            "bands": {
                "upper": projection_upper,
                "lower": projection_lower,
            },
        },
        "forward_horizons": {
            k: {
                kk: (round(vv, 6) if isinstance(vv, float) else vv)
                for kk, vv in pack.items()
            }
            for k, pack in horizon.items()
        },
        "turning_windows": turns,
        "_trimmed_path_raw": trimmed_path,
        "_week_stats_raw": week_stats,
        "_horizon_raw": horizon,
        "_years": years,
    }


def _indexed_stats_panel(
    normalised: dict[str, Any],
    *,
    walk_forward: dict[str, Any],
    confidence: dict[str, Any],
) -> dict[str, Any]:
    """Stats for the default normalised (indexed) seasonal curve."""
    horizons = normalised.get("horizons") or {}
    h4 = horizons.get("4w") or {}
    h8 = horizons.get("8w") or {}
    h12 = horizons.get("12w") or {}

    def _dir_move(h: dict[str, Any]) -> dict[str, Any]:
        return {
            "direction": h.get("direction"),
            "median_move_pct": h.get("median_move_pct"),
        }

    move8 = h8.get("median_move_pct")
    bias = "NEUTRAL"
    if move8 is not None:
        if move8 >= 0.2:
            bias = "BULLISH"
        elif move8 <= -0.2:
            bias = "BEARISH"

    pos = normalised.get("positive_frequency_8w")
    neg = normalised.get("negative_frequency_8w")
    return {
        "current_seasonal_bias": bias,
        "horizon_4w": _dir_move(h4),
        "horizon_8w": _dir_move(h8),
        "horizon_12w": _dir_move(h12),
        "average_4w_return_pct": h4.get("median_move_pct"),
        "average_8w_return_pct": h8.get("median_move_pct"),
        "average_12w_return_pct": h12.get("median_move_pct"),
        "direction_4w": h4.get("direction"),
        "direction_8w": h8.get("direction"),
        "direction_12w": h12.get("direction"),
        "positive_years_pct": None if pos is None else round(pos * 100, 1),
        "negative_years_pct": None if neg is None else round(neg * 100, 1),
        "sample_years": normalised.get("sample_size"),
        "sample_year_list": normalised.get("sample_years"),
        "walk_forward_hit_rate": walk_forward.get("hit_rate"),
        "walk_forward_n": walk_forward.get("n"),
        "method": normalised.get("method"),
        "confidence": confidence,
        "current_seasonal_window": None,
    }


def build_seasonality_research(
    instrument_id: str,
    *,
    lookback: str = DEFAULT_LOOKBACK,
    fail_on_integrity: bool = True,
) -> dict[str, Any]:
    """Full research payload for one instrument. Fails loudly on integrity FAIL."""
    daily, load_meta = load_daily_closes_for_seasonality(instrument_id)
    source = load_meta.get("source")
    load_err = load_meta.get("error")
    if load_err or not daily:
        weekly_unavailable = build_weekly_roadmap(
            [],
            lookback_years=_lookback_years(lookback) or DEFAULT_LOOKBACK_YEARS,
            integrity={"status": "FAIL", "issues": [load_err or "missing_source_data"]},
        )
        return {
            "status": "FAIL",
            "instrument_id": instrument_id,
            "engine": ENGINE_VERSION,
            "error": load_err or "no_daily_data",
            "price_identity": load_meta,
            "integrity": {
                "status": "FAIL",
                "issues": [load_err or "no_daily_data"],
            },
            "weekly_roadmap": weekly_unavailable,
            "monthly_roadmap": None,
        }

    price_instrument_id = load_meta.get("price_instrument_id") or instrument_id
    integrity = audit_daily_series(price_instrument_id, daily, source=source)
    if integrity["status"] != "PASS" and fail_on_integrity:
        # Still emit Weekly Roadmap unavailable payload with exact gate reasons
        # (does not weaken the gate or invent a seasonal curve).
        lookback_years = _lookback_years(lookback) or DEFAULT_LOOKBACK_YEARS
        weekly_unavailable = build_weekly_roadmap(
            daily,
            lookback_years=lookback_years,
            integrity=integrity,
        )
        return {
            "status": "FAIL",
            "instrument_id": instrument_id,
            "engine": ENGINE_VERSION,
            "error": "integrity_failed",
            "price_identity": load_meta,
            "integrity": integrity,
            "message": (
                "Seasonality Workstation refused to compute — price integrity FAIL: "
                + ", ".join(integrity.get("issues") or [])
            ),
            "weekly_roadmap": weekly_unavailable,
            "monthly_roadmap": None,
        }

    weekly = weekly_closes_from_daily(daily)
    rows = weekly_return_rows(weekly)
    if not weekly:
        return {
            "status": "FAIL",
            "instrument_id": instrument_id,
            "engine": ENGINE_VERSION,
            "error": "no_weekly_bars",
            "price_identity": load_meta,
            "integrity": integrity,
        }

    anchor_date, anchor_price = weekly[-1]
    asof_year, anchor_week = iso_week(anchor_date)
    usable = list(integrity.get("usable_history_years") or [])

    lookback_years = _lookback_years(lookback) or DEFAULT_LOOKBACK_YEARS
    normalised = build_normalised_seasonal_curve(
        daily,
        asof=anchor_date,
        lookback_years=lookback_years,
        smooth=DEFAULT_SMOOTH,
        aggregation="mean",
    )
    seasonal_price_path = build_seasonal_price_path_curve(
        daily,
        asof=anchor_date,
        lookback_years=lookback_years,
    )
    seasonal_roadmap = build_seasonal_roadmap_curve(
        daily,
        asof=anchor_date,
        lookback_years=lookback_years,
        smooth=DEFAULT_SMOOTH,
    )
    # Independent Weekly Roadmap — reuses the already-loaded ``daily`` series once.
    weekly_roadmap = build_weekly_roadmap(
        daily,
        asof=anchor_date,
        lookback_years=lookback_years,
        integrity=integrity,
        seasonal_roadmap=seasonal_roadmap,
    )
    if weekly_roadmap.get("calculation_ms") is not None:
        # Dev log must not touch stdout — route CLI emits JSON on stdout only.
        print(
            f"[seasonality] weekly_roadmap {instrument_id}: "
            f"{weekly_roadmap.get('calculation_ms')}ms "
            f"status={weekly_roadmap.get('quality_status')} "
            f"years={weekly_roadmap.get('valid_year_count')}",
            file=sys.stderr,
        )
    walk_forward = walk_forward_hit_rate(
        daily,
        lookback_years=lookback_years,
        smooth=DEFAULT_SMOOTH,
        horizon_days=56,
    )

    # Advanced-only: legacy ISO-week price-unit forecast (not mixed into normalised chart).
    all_blocks: dict[str, dict[str, Any]] = {}
    for label, _ in LOOKBACKS:
        all_blocks[label] = compute_lookback_block(
            rows,
            weekly,
            lookback=label,
            asof_year=asof_year,
            usable_years=usable,
            anchor_week=anchor_week,
            anchor_price=anchor_price,
            anchor_date=anchor_date,
        )

    agreement = _lookback_agreement(
        {k: v["_trimmed_path_raw"] for k, v in all_blocks.items()},
        anchor_week,
    )

    selected = lookback if lookback in all_blocks else DEFAULT_LOOKBACK
    block = all_blocks[selected]
    confidence = _confidence_pack(
        week_stats=block["_week_stats_raw"],
        anchor_week=anchor_week,
        lookback_agreement=agreement,
        usable_n=len(usable),
    )
    panel = _indexed_stats_panel(
        normalised,
        walk_forward=walk_forward,
        confidence=confidence,
    )
    # Keep legacy week-based panel under advanced for audits
    legacy_panel = _right_panel_stats(
        horizon=block["_horizon_raw"],
        confidence=confidence,
        week_stats=block["_week_stats_raw"],
        anchor_week=anchor_week,
        turns=block["turning_windows"],
        years=block["_years"],
    )

    current_year_index = _current_year_path(weekly, asof_year)
    if anchor_week in current_year_index and current_year_index[anchor_week]:
        scale = anchor_price / current_year_index[anchor_week]
        current_year_price = {
            w: current_year_index[w] * scale for w in current_year_index if w <= anchor_week
        }
    else:
        current_year_price = {iso_week(d)[1]: c for d, c in weekly if iso_week(d)[0] == asof_year}

    seasonal_hist = []
    price_aligned = block["price_aligned"]["trimmed_mean"]
    for w in range(1, anchor_week + 1):
        seasonal_hist.append(
            {
                "iso_week": w,
                "price": price_aligned.get(str(w)),
                "segment": "historical",
            }
        )

    cutoff = (datetime.strptime(anchor_date, "%Y-%m-%d") - timedelta(days=550)).strftime("%Y-%m-%d")
    price_series = [{"date": d, "close": c} for d, c in daily if d >= cutoff]

    def _clean(b: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in b.items() if not k.startswith("_")}

    exchange = None
    try:
        from hptl.markets.instrument_registry import get_instrument

        meta = get_instrument(instrument_id) or {}
        exchange = meta.get("exchange") or meta.get("venue")
    except Exception:
        exchange = None

    sample_size = normalised.get("sample_size") if normalised.get("available") else block["sample_size"]

    return {
        "status": "ok",
        "engine": ENGINE_VERSION,
        "instrument_id": instrument_id,
        "price_instrument_id": price_instrument_id,
        "price_identity": load_meta,
        "exchange": exchange,
        "report_date": anchor_date,
        "selected_lookback": selected,
        "available_lookbacks": [name for name, _ in LOOKBACKS],
        "sample_size": sample_size,
        "confidence": confidence,
        "data_quality": integrity.get("data_quality"),
        "integrity": integrity,
        "anchor": {
            "date": anchor_date,
            "price": anchor_price,
            "iso_year": asof_year,
            "iso_week": anchor_week,
            "doy": normalised.get("asof_doy"),
        },
        "price_series": price_series,
        "normalised_seasonality": normalised,
        "seasonal_price_path": seasonal_price_path,
        "seasonal_roadmap": seasonal_roadmap,
        "weekly_roadmap": weekly_roadmap,
        "monthly_roadmap": seasonal_roadmap,  # alias for UI naming; same object, unchanged maths
        "walk_forward": walk_forward,
        "seasonality": {
            "primary": "indexed_year_path",
            "normalised": normalised,
            "seasonal_price_path": seasonal_price_path,
            "seasonal_roadmap": seasonal_roadmap,
            "weekly_roadmap": weekly_roadmap,
            "monthly_roadmap": seasonal_roadmap,
            "week_stats": block["week_stats"],
            "index_paths": block["index_paths"],
            "price_aligned": block["price_aligned"],
            "historical_year_paths": block["historical_year_paths"],
            "current_year_price": {
                str(k): round(v, 6) for k, v in current_year_price.items()
            },
            "historical_curve": seasonal_hist,
            "projection": block["projection"],
            "forecast": block.get("forecast"),
            "forward_weeks": FORWARD_WEEKS,
        },
        "lookbacks": {k: _clean(v) for k, v in all_blocks.items()},
        "lookback_agreement": agreement,
        "turning_windows": block["turning_windows"],
        "stats_panel": panel,
        "advanced": {
            "price_unit_forecast": block.get("forecast"),
            "legacy_week_stats_panel": legacy_panel,
            "note": (
                "Price-unit forecast is Advanced-only. "
                "Do not mix with the normalised seasonal curve."
            ),
        },
        "display_defaults": {
            "primary_chart": "seasonal_roadmap",
            "seasonal_view": "roadmap",
            "available_seasonal_views": ["roadmap", "price_path", "freeze_index"],
            "methodology_label": "Seasonal Roadmap",
            "price_unit_forecast": "advanced_only",
            "show_average": False,
            "show_median": True,
            "show_current_year": False,
            "show_individual_years": False,
            "show_upper_band": False,
            "show_lower_band": False,
        },
    }
