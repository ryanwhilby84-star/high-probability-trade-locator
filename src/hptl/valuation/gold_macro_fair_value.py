"""Gold Macro Fair Value V2 (research only).

One combined expanding walk-forward model:

  log(Gold) = α_t + β1·DXY + β2·Real10Y + β3·US2Y + β4·US30Y
                    + β5·Inflation + β6·CB_Purchases

α_t is a past-only trailing level anchor (mean log Gold). Drivers are
past-only z-scored so units coexist. Sign-constrained slopes.
Fair value = exp(predicted log). Driver contributions in $/oz.

Does NOT modify NG valuation, metals_real_yield_v1, COT, Scanner,
Seasonality, or production endpoints.
"""

from __future__ import annotations

import csv
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from scipy.optimize import lsq_linear

from hptl.config import PROJECT_ROOT
from hptl.valuation.gold_focused_macro_valuation import (
    CB_PUBLICATION_LAG_DAYS,
    LEVEL_ANCHOR_WEEKS,
    MIN_TRAIN,
    STEP,
    build_focused_panel,
    _forward_bucket_stats,
    _pooled_spread,
    _select_transforms,
    _sign_bounds,
)
from hptl.valuation.gold_structural_valuation_research import _classify_deviation
from hptl.valuation.metals_valuation_v1 import MODEL_ID as PUBLISHED_GOLD_MODEL_ID

AUDIT_DIR = PROJECT_ROOT / "data" / "audits" / "gold_macro_fair_value"
CHART_DIR = AUDIT_DIR / "charts"
REPORT_MD = AUDIT_DIR / "gold_macro_report.md"
CONTRIB_CSV = AUDIT_DIR / "gold_driver_contributions.csv"
FWD_CSV = AUDIT_DIR / "gold_forward_returns.csv"
COEF_CSV = AUDIT_DIR / "gold_coefficients.csv"
HISTORY_CSV = AUDIT_DIR / "gold_fair_value_history.csv"
JSON_OUT = AUDIT_DIR / "gold_macro_fair_value.json"

MODEL_ID = "gold_macro_fair_value_v2"
FEATURE_ORDER = ["dxy", "real10y", "us2y", "us30y", "inflation", "cb_demand"]
FEATURE_LABELS = {
    "dxy": "DXY",
    "real10y": "Real Yield",
    "us2y": "2Y",
    "us30y": "30Y",
    "inflation": "Inflation",
    "cb_demand": "Central Banks",
}


def _constrained_slopes(
    y_demean: list[float], cols: list[list[float]], names: list[str]
) -> tuple[list[float], float | None]:
    n = len(y_demean)
    if n < len(names) + 8:
        return [], None
    X = np.column_stack([np.asarray(c, float) for c in cols])
    yy = np.asarray(y_demean, float)
    lo_full, hi_full = _sign_bounds(names)
    lo, hi = lo_full[1:], hi_full[1:]
    try:
        res = lsq_linear(X, yy, bounds=(lo, hi), method="bvls", max_iter=300)
        beta = [float(b) for b in res.x]
    except Exception:
        beta_arr, _, _, _ = np.linalg.lstsq(X, yy, rcond=None)
        beta = [float(b) for b in beta_arr]
    yhat = X @ np.asarray(beta)
    ss_res = float(np.sum((yy - yhat) ** 2))
    ss_tot = float(np.sum((yy - yy.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else None
    return beta, r2


def _dollar_contributions(
    *,
    alpha: float,
    slopes: list[float],
    names: list[str],
    feats: list[float],
    spot: float,
) -> dict[str, Any]:
    """NG-style decomposition with $/oz driver impacts.

    base = exp(α)
    fair = exp(α + Σ βᵢxᵢ)
    Allocate (fair − base) across drivers by log-contribution share.
    """
    log_cs = [slopes[i] * feats[i] for i in range(len(names))]
    base = math.exp(alpha)
    fair = math.exp(alpha + sum(log_cs))
    gap = fair - base
    sum_log = sum(log_cs)
    rows = []
    for i, name in enumerate(names):
        log_c = log_cs[i]
        if abs(sum_log) < 1e-15:
            dollar = 0.0
        else:
            dollar = gap * (log_c / sum_log)
        rows.append(
            {
                "feature": name,
                "label": FEATURE_LABELS.get(name, name),
                "coefficient": round(slopes[i], 6),
                "transformed_input": round(feats[i], 6),
                "log_contribution": round(log_c, 6),
                "price_impact_pct": round(100.0 * (math.exp(log_c) - 1.0), 4),
                "dollar_contribution": round(dollar, 2),
                "direction": (
                    "raises fair value"
                    if log_c > 0
                    else "lowers fair value"
                    if log_c < 0
                    else "neutral"
                ),
            }
        )
    net = fair - base
    row_sum = sum(r["dollar_contribution"] for r in rows)
    if abs(row_sum) > 1e-9 and abs(net - row_sum) > 0.05:
        scale = net / row_sum
        for r in rows:
            r["dollar_contribution"] = round(r["dollar_contribution"] * scale, 2)
    dev = 100.0 * (spot - fair) / fair if fair > 0 else None
    premium = "Premium" if dev is not None and dev > 0 else (
        "Discount" if dev is not None and dev < 0 else "Fair"
    )
    return {
        "identity": "log(fair)=α+Σ(βᵢ·xᵢ); fair=exp(log(fair)); $/oz share of (fair−exp(α))",
        "alpha": round(alpha, 6),
        "base_fair_value": round(base, 3),
        "drivers": rows,
        "net_macro_effect_usd": round(net, 2),
        "fair_value": round(fair, 3),
        "market_price": round(spot, 3),
        "deviation_pct": round(dev, 3) if dev is not None else None,
        "premium_discount": premium,
        "bucket": _classify_deviation(dev) if dev is not None else None,
    }


def _walk_forward_engine(
    dates: list[str],
    prices: list[float],
    y: list[float],
    cols: list[list[float]],
    names: list[str],
    *,
    min_train: int,
    step: int = STEP,
) -> dict[str, Any]:
    n = len(y)
    fair_logs: list[float | None] = [None] * n
    history_rows: list[dict[str, Any]] = []
    coef_rows: list[dict[str, Any]] = []
    contrib_rows: list[dict[str, Any]] = []
    preds: list[float] = []
    actuals: list[float] = []
    slope_paths: dict[str, list[float]] = {f: [] for f in names}

    t = min_train
    while t < n:
        y_demean: list[float] = []
        cols_fit: list[list[float]] = [[] for _ in cols]
        for i in range(t):
            a0 = max(0, i - LEVEL_ANCHOR_WEEKS)
            if i - a0 < 52:
                continue
            mu_i = sum(y[a0:i]) / (i - a0)
            y_demean.append(y[i] - mu_i)
            for j, c in enumerate(cols):
                cols_fit[j].append(c[i])
        if len(y_demean) < max(40, len(names) + 10):
            t += step
            continue
        slopes, r2 = _constrained_slopes(y_demean, cols_fit, names)
        if not slopes or r2 is None:
            t += step
            continue
        for f, s in zip(names, slopes):
            slope_paths[f].append(s)
        coef_rows.append(
            {
                "train_end": dates[t - 1],
                "n_train": t,
                "r2_demean": round(r2, 4),
                **{f: round(s, 6) for f, s in zip(names, slopes)},
            }
        )
        end = min(t + step, n)
        for i in range(t, end):
            a0 = max(0, i - LEVEL_ANCHOR_WEEKS)
            alpha = sum(y[a0:i]) / max(1, i - a0)
            feats = [c[i] for c in cols]
            log_fair = alpha + sum(s * f for s, f in zip(slopes, feats))
            fair_logs[i] = log_fair
            fair = math.exp(log_fair)
            spot = prices[i]
            contrib = _dollar_contributions(
                alpha=alpha, slopes=slopes, names=names, feats=feats, spot=spot
            )
            preds.append(log_fair)
            actuals.append(y[i])
            history_rows.append(
                {
                    "date": dates[i],
                    "gold_price": round(spot, 3),
                    "fair_value": contrib["fair_value"],
                    "deviation_pct": contrib["deviation_pct"],
                    "premium_discount": contrib["premium_discount"],
                    "bucket": contrib["bucket"],
                    "base_fair_value": contrib["base_fair_value"],
                    "net_macro_effect_usd": contrib["net_macro_effect_usd"],
                    "alpha": contrib["alpha"],
                    **{f"coef_{f}": round(s, 6) for f, s in zip(names, slopes)},
                    **{
                        f"usd_{f}": next(
                            d["dollar_contribution"]
                            for d in contrib["drivers"]
                            if d["feature"] == f
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
                        "dollar_contribution": d["dollar_contribution"],
                        "log_contribution": d["log_contribution"],
                        "price_impact_pct": d["price_impact_pct"],
                        "coefficient": d["coefficient"],
                        "transformed_input": d["transformed_input"],
                        "fair_value": contrib["fair_value"],
                        "market_price": contrib["market_price"],
                        "deviation_pct": contrib["deviation_pct"],
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

    # Stability: share of windows with expected sign
    stability = {}
    for f, path in slope_paths.items():
        if not path:
            continue
        if f in {"dxy", "us2y", "real10y"}:
            ok_share = sum(1 for v in path if v <= 0) / len(path)
        elif f in {"inflation", "cb_demand"}:
            ok_share = sum(1 for v in path if v >= 0) / len(path)
        else:
            ok_share = 1.0
        flips = any(a * b < 0 for a, b in zip(path, path[1:]))
        stability[f] = {
            "n_windows": len(path),
            "expected_sign_share": round(ok_share, 3),
            "sign_flip": flips,
            "mean": round(sum(path) / len(path), 6),
            "tip": round(path[-1], 6),
        }

    return {
        "fair_logs": fair_logs,
        "history": history_rows,
        "coefficients": coef_rows,
        "contributions": contrib_rows,
        "oos": oos,
        "stability": stability,
        "tip": history_rows[-1] if history_rows else None,
        "tip_slopes": {
            f: (stability[f]["tip"] if f in stability else None) for f in names
        },
    }


def _write_charts(
    *,
    dates: list[str],
    prices: list[float],
    fair_logs: list[float | None],
    history: list[dict[str, Any]],
) -> list[str]:
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    paths: list[str] = []

    # Price / FV / deviation
    w, h = 1200, 720
    pad_l, pad_r, pad_t = 55, 20, 36
    y0, y1 = pad_t, 340
    plot_w = w - pad_l - pad_r
    pairs = []
    for d, px, fl in zip(dates, prices, fair_logs):
        if fl is None:
            continue
        pairs.append((d, px, math.exp(fl), 100.0 * (px / math.exp(fl) - 1.0)))
    if len(pairs) < 10:
        return paths

    def x_of(i: int) -> float:
        return pad_l + (i / max(1, len(pairs) - 1)) * plot_w

    pxs = [p[1] for p in pairs]
    fvs = [p[2] for p in pairs]
    dvs = [p[3] for p in pairs]
    ymin, ymax = min(min(pxs), min(fvs)), max(max(pxs), max(fvs))
    if ymax <= ymin:
        ymax = ymin + 1
    dmin, dmax = min(dvs), max(dvs)
    if abs(dmax - dmin) < 1e-9:
        dmax = dmin + 1

    def yp(v: float) -> float:
        return y0 + (1 - (v - ymin) / (ymax - ymin)) * (y1 - y0 - 10)

    def yd(v: float) -> float:
        return y1 + 30 + (1 - (v - dmin) / (dmax - dmin)) * (h - y1 - 70)

    def poly(vals: list[float], yfun: Any, color: str, width: float = 1.6) -> str:
        pts = " ".join(f"{x_of(i):.1f},{yfun(v):.1f}" for i, v in enumerate(vals))
        return f'<polyline fill="none" stroke="{color}" stroke-width="{width}" points="{pts}"/>'

    svg1 = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" style="background:#0b1220;font-family:Segoe UI,Arial,sans-serif">',
        f'<text x="{pad_l}" y="22" fill="#e2e8f0" font-size="16">Gold Macro Fair Value V2</text>',
        poly(pxs, yp, "#38bdf8", 1.8),
        poly(fvs, yp, "#f472b6", 1.8),
        poly(dvs, yd, "#a3e635", 1.4),
        f'<line x1="{pad_l}" y1="{yd(0):.1f}" x2="{w-pad_r}" y2="{yd(0):.1f}" stroke="#64748b" stroke-dasharray="4 3"/>',
        f'<text x="{pad_l}" y="{y0+12}" fill="#38bdf8" font-size="11">Gold</text>',
        f'<text x="{pad_l+50}" y="{y0+12}" fill="#f472b6" font-size="11">Fair value</text>',
        f'<text x="{pad_l}" y="{y1+24}" fill="#a3e635" font-size="11">Deviation %</text>',
        f'<text x="{pad_l}" y="{h-12}" fill="#94a3b8" font-size="10">{pairs[0][0]} → {pairs[-1][0]} · n={len(pairs)}</text>',
        "</svg>",
    ]
    p1 = CHART_DIR / "gold_price_fair_deviation.svg"
    p1.write_text("\n".join(svg1), encoding="utf-8")
    paths.append(str(p1.relative_to(PROJECT_ROOT)).replace("\\", "/"))

    # Stacked contribution bars at tip (last 80 weeks as lines)
    if history:
        recent = history[-min(120, len(history)) :]
        colors = {
            "dxy": "#38bdf8",
            "real10y": "#f472b6",
            "us2y": "#fbbf24",
            "us30y": "#a78bfa",
            "inflation": "#34d399",
            "cb_demand": "#fb7185",
        }
        w2, h2 = 1200, 420
        pad = 50
        plot_w2 = w2 - pad - 20
        plot_h2 = h2 - 80
        # cumulative stacked? show each as polyline of usd contrib
        series = {f: [float(r.get(f"usd_{f}") or 0.0) for r in recent] for f in FEATURE_ORDER}
        allv = [v for s in series.values() for v in s]
        vmin, vmax = min(allv + [0]), max(allv + [0])
        if abs(vmax - vmin) < 1e-9:
            vmax = vmin + 1

        def x2(i: int) -> float:
            return pad + (i / max(1, len(recent) - 1)) * plot_w2

        def y2(v: float) -> float:
            return 40 + (1 - (v - vmin) / (vmax - vmin)) * plot_h2

        parts = [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{w2}" height="{h2}" style="background:#0b1220;font-family:Segoe UI,Arial,sans-serif">',
            f'<text x="{pad}" y="24" fill="#e2e8f0" font-size="16">Driver contributions ($/oz)</text>',
            f'<line x1="{pad}" y1="{y2(0):.1f}" x2="{w2-20}" y2="{y2(0):.1f}" stroke="#475569" stroke-dasharray="4 3"/>',
        ]
        lx = pad
        for f in FEATURE_ORDER:
            pts = " ".join(f"{x2(i):.1f},{y2(v):.1f}" for i, v in enumerate(series[f]))
            parts.append(
                f'<polyline fill="none" stroke="{colors[f]}" stroke-width="1.5" points="{pts}"/>'
            )
            parts.append(
                f'<text x="{lx}" y="{h2-14}" fill="{colors[f]}" font-size="11">{FEATURE_LABELS[f]}</text>'
            )
            lx += 110
        parts.append("</svg>")
        p2 = CHART_DIR / "gold_driver_contributions.svg"
        p2.write_text("\n".join(parts), encoding="utf-8")
        paths.append(str(p2.relative_to(PROJECT_ROOT)).replace("\\", "/"))

    return paths


def _verdict(engine: dict[str, Any], spread: dict[str, Any], tip: dict[str, Any] | None) -> dict[str, Any]:
    stab = engine.get("stability") or {}
    constrained = ["dxy", "real10y", "us2y", "inflation", "cb_demand"]
    shares = [float((stab.get(f) or {}).get("expected_sign_share") or 0) for f in constrained]
    signs_ok = all(s >= 0.70 for s in shares) if shares else False
    flip_bad = any(bool((stab.get(f) or {}).get("sign_flip")) for f in ["dxy", "real10y", "us2y"])
    sp = spread.get("spread_pp")
    under_n = int(spread.get("under_n") or 0)
    n_hist = len(engine.get("history") or [])
    tip_dev = tip.get("deviation_pct") if tip else None
    tip_extreme = tip_dev is not None and abs(float(tip_dev)) > 40

    promote = (
        signs_ok
        and not flip_bad
        and sp is not None
        and float(sp) > 2.0
        and under_n >= 20
        and n_hist >= 520
        and not tip_extreme
    )
    if promote:
        return {
            "verdict": "PROMOTE",
            "narrative": (
                f"Combined macro fair value is usable: signs stable, "
                f"under-over 13w spread={sp}pp, tip_dev={tip_dev}%."
            ),
        }
    caveats = []
    if not signs_ok:
        caveats.append("constrained signs not stably met")
    if flip_bad:
        caveats.append("material sign flips on core drivers")
    if sp is None or float(sp) <= 0:
        caveats.append(f"valuation spread13={sp}")
    if under_n < 20:
        caveats.append(f"few undervalued obs (n={under_n})")
    if n_hist < 520:
        caveats.append(f"short walk-forward history (n={n_hist})")
    if tip_extreme:
        caveats.append("extreme tip deviation")
    cb_share = (stab.get("cb_demand") or {}).get("expected_sign_share")
    cb_tip = (stab.get("cb_demand") or {}).get("tip")
    if cb_tip is not None and abs(float(cb_tip)) < 1e-9:
        caveats.append("CB coefficient inert at 0 (short CB series)")
    return {
        "verdict": "USEFUL_BUT_RESEARCH",
        "narrative": (
            f"Research-only combined macro FV ready for inspection "
            f"(tip_fv={tip.get('fair_value') if tip else None}, "
            f"tip_dev={tip_dev}%, spread13={sp}). "
            f"Not promotion-ready: {'; '.join(caveats) if caveats else 'see report'}."
        ),
        "cb_sign_share": cb_share,
    }


def run_gold_macro_fair_value(*, start: str = "2003-01-01") -> dict[str, Any]:
    t0 = datetime.now(timezone.utc)
    try:
        from hptl.data_sources.cb_gold_purchases_ingest import ingest_cb_gold_purchases

        ingest_cb_gold_purchases(write_status=False)
    except Exception:
        pass

    panel = build_focused_panel(start=start)
    transforms, transform_choice = _select_transforms(panel["raw"])

    # Align to weeks with all features including CB
    dates_all = panel["dates"]
    y_all = panel["log_gold"]
    prices_all = panel["prices"]
    names = list(FEATURE_ORDER)
    d_al: list[str] = []
    y_al: list[float] = []
    p_al: list[float] = []
    x_al: dict[str, list[float]] = {f: [] for f in names}
    for i, d in enumerate(dates_all):
        vals = []
        ok = True
        for f in names:
            v = transforms[f][i]
            if v is None or not math.isfinite(float(v)):
                ok = False
                break
            vals.append(float(v))
        if not ok:
            continue
        d_al.append(d)
        y_al.append(y_all[i])
        p_al.append(prices_all[i])
        for f, v in zip(names, vals):
            x_al[f].append(v)

    min_train = MIN_TRAIN if len(y_al) >= MIN_TRAIN + 40 else max(52, len(y_al) // 3)
    if len(y_al) < min_train + 20:
        return {
            "ok": False,
            "error": f"Insufficient aligned weeks n={len(y_al)}",
            "research_only": True,
        }

    cols = [x_al[f] for f in names]
    engine = _walk_forward_engine(
        d_al, p_al, y_al, cols, names, min_train=min_train, step=STEP
    )
    deviations = []
    for fl, px in zip(engine["fair_logs"], p_al):
        if fl is None:
            deviations.append(None)
        else:
            fair = math.exp(fl)
            deviations.append(100.0 * (px / fair - 1.0) if fair > 0 else None)

    fwd = _forward_bucket_stats(d_al, p_al, deviations)
    spread13 = _pooled_spread(fwd, horizon=13)
    spread52 = _pooled_spread(fwd, horizon=52)
    tip = engine.get("tip")
    verdict = _verdict(engine, spread13, tip)
    charts = _write_charts(
        dates=d_al,
        prices=p_al,
        fair_logs=engine["fair_logs"],
        history=engine["history"],
    )

    # Tip contribution card (dashboard-ready)
    tip_card = None
    if tip and engine["history"]:
        last = engine["history"][-1]
        tip_card = {
            "date": last["date"],
            "drivers_usd": {FEATURE_LABELS[f]: last.get(f"usd_{f}") for f in names},
            "net_macro_effect_usd": last.get("net_macro_effect_usd"),
            "fair_value": last.get("fair_value"),
            "market_price": last.get("gold_price"),
            "deviation_pct": last.get("deviation_pct"),
            "premium_discount": last.get("premium_discount"),
            "bucket": last.get("bucket"),
            "coefficients": {FEATURE_LABELS[f]: last.get(f"coef_{f}") for f in names},
        }

    payload = {
        "generated_at": t0.replace(microsecond=0).isoformat(),
        "ok": True,
        "research_only": True,
        "model_id": MODEL_ID,
        "published_models_untouched": {
            "gold_model_id": PUBLISHED_GOLD_MODEL_ID,
            "prices_latest_not_modified": True,
            "ng_untouched": True,
        },
        "panel": {
            **panel["meta"],
            "aligned_n": len(d_al),
            "aligned_start": d_al[0],
            "aligned_end": d_al[-1],
            "min_train": min_train,
            "level_anchor_weeks": LEVEL_ANCHOR_WEEKS,
            "cb_publication_lag_days": CB_PUBLICATION_LAG_DAYS,
            "transform_choice": transform_choice,
        },
        "equation": (
            "log(Gold)=α_t + β1·z(DXY) + β2·z(Real10Y) + β3·z(US2Y) + β4·z(US30Y) "
            "+ β5·z(Inflation) + β6·z(CB); α_t=trailing mean log Gold; fair=exp(·)"
        ),
        "oos": engine["oos"],
        "stability": engine["stability"],
        "spread_13w": spread13,
        "spread_52w": spread52,
        "forward_returns": fwd,
        "tip": tip_card,
        "verdict": verdict,
        "charts": charts,
        "runtime_sec": round((datetime.now(timezone.utc) - t0).total_seconds(), 2),
        "_history": engine["history"],
        "_coefficients": engine["coefficients"],
        "_contributions": engine["contributions"],
    }
    return payload


def render_markdown(payload: dict[str, Any]) -> str:
    v = payload.get("verdict") or {}
    tip = payload.get("tip") or {}
    panel = payload.get("panel") or {}
    lines = [
        "# Gold Macro Fair Value V2",
        "",
        f"Generated: `{payload.get('generated_at')}`",
        "",
        f"**Model:** `{payload.get('model_id')}`",
        "",
        "**Research only — not deployed.**",
        "",
        f"**Verdict: {v.get('verdict')}**",
        "",
        v.get("narrative") or "",
        "",
        "## Equation",
        "",
        f"`{payload.get('equation')}`",
        "",
        "## Panel",
        "",
        f"- Aligned weeks: **{panel.get('aligned_n')}** "
        f"({panel.get('aligned_start')} → {panel.get('aligned_end')})",
        f"- Level anchor: trailing **{panel.get('level_anchor_weeks')}** weeks",
        f"- CB lag: **{panel.get('cb_publication_lag_days')}** days (carry-forward, no interpolation)",
        f"- CB meta: `{(panel.get('cb') or {})}`",
        "",
        "## Live tip card",
        "",
    ]
    if tip:
        lines.append("```")
        for label, usd in (tip.get("drivers_usd") or {}).items():
            sign = "+" if (usd or 0) >= 0 else ""
            lines.append(f"{label:<18}{sign}{usd}")
        lines.append("-------------------------")
        net = tip.get("net_macro_effect_usd")
        sign = "+" if (net or 0) >= 0 else ""
        lines.append(f"{'Net Effect':<18}{sign}{net}")
        lines.append(f"{'Fair Value':<18}{tip.get('fair_value')}")
        lines.append(f"{'Current Price':<18}{tip.get('market_price')}")
        lines.append(
            f"{tip.get('premium_discount'):<18}{tip.get('deviation_pct')}%"
        )
        lines.append("```")
        lines.append("")
        lines.append(f"Bucket: **{tip.get('bucket')}**")
        lines.append(f"Coefficients: `{tip.get('coefficients')}`")
    lines.extend(
        [
            "",
            "## Walk-forward",
            "",
            f"- OOS: `{(payload.get('oos') or {})}`",
            f"- Stability: `{(payload.get('stability') or {})}`",
            f"- Spread 13w: `{(payload.get('spread_13w') or {})}`",
            f"- Spread 52w: `{(payload.get('spread_52w') or {})}`",
            "",
            "## 13-week forward returns",
            "",
            "| Bucket | n | Mean % | Median % | Hit |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for fr in payload.get("forward_returns") or []:
        if fr.get("horizon_weeks") != 13:
            continue
        lines.append(
            f"| {fr.get('bucket')} | {fr.get('n')} | {fr.get('mean_return_pct')} | "
            f"{fr.get('median_return_pct')} | {fr.get('positive_return_rate')} |"
        )
    lines.extend(
        [
            "",
            "## Charts",
            "",
        ]
    )
    for c in payload.get("charts") or []:
        lines.append(f"- `{c}`")
    lines.extend(
        [
            "",
            "## Safety",
            "",
            f"- Published Gold model untouched: `{PUBLISHED_GOLD_MODEL_ID}`",
            "- NG / COT / Scanner / Seasonality / production endpoints untouched",
            "- Outputs under `data/audits/gold_macro_fair_value/`",
            "",
            f"Runtime: {payload.get('runtime_sec')}s",
            "",
        ]
    )
    return "\n".join(lines)


def write_outputs(payload: dict[str, Any]) -> dict[str, str]:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    public = {k: v for k, v in payload.items() if not str(k).startswith("_")}
    JSON_OUT.write_text(json.dumps(public, indent=2, ensure_ascii=False), encoding="utf-8")
    REPORT_MD.write_text(render_markdown(public), encoding="utf-8")

    history = list(payload.get("_history") or [])
    coefs = list(payload.get("_coefficients") or [])
    contribs = list(payload.get("_contributions") or [])
    fwd = list(payload.get("forward_returns") or [])

    if history:
        fields = list(history[0].keys())
        with HISTORY_CSV.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            w.writeheader()
            w.writerows(history)

    if coefs:
        fields = list(coefs[0].keys())
        with COEF_CSV.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            w.writeheader()
            w.writerows(coefs)

    if contribs:
        fields = list(contribs[0].keys())
        with CONTRIB_CSV.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
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

    return {
        "report": str(REPORT_MD.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "contributions_csv": str(CONTRIB_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "forward_csv": str(FWD_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "coefficients_csv": str(COEF_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "history_csv": str(HISTORY_CSV.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "ranking_json": str(JSON_OUT.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "charts_dir": str(CHART_DIR.relative_to(PROJECT_ROOT)).replace("\\", "/"),
    }
