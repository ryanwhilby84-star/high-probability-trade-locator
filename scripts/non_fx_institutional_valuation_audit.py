"""Phase 3 — Institutional valuation audit for all non-FX scanner markets.

Read-only evidence gatherer. Does not rebuild or publish models.
"""
from __future__ import annotations

import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.config import PROJECT_ROOT
from hptl.valuation.agri_fundamental_valuation import (
    MIN_OBS_PERCENTILE,
    MIN_OBS_REGRESSION,
    MIN_R2,
    MODEL_ID_PERCENTILE,
    MODEL_ID_REGRESSION,
    _align_price_stu,
    _fair_from_stu_percentile,
    _ols_slope_intercept,
    _price_on_date,
    compute_agri_valuation,
    discover_instrument_data,
    load_balance_sheet,
)
from hptl.valuation.metals_valuation_v1 import (
    METALS_MARKETS,
    MIN_WEEKS,
    _build_weekly_panel,
    _compute_fair_value,
    _predict_log_price,
    compute_metals_valuation,
)

AUDIT_MARKETS: list[tuple[str, str]] = [
    ("Wheat", "grains"),
    ("Corn", "grains"),
    ("Soybeans", "grains"),
    ("Sugar", "softs"),
    ("Cotton", "softs"),
    ("Coffee", "softs"),
    ("Cocoa", "softs"),
    ("Gold", "metals"),
    ("Silver", "metals"),
    ("Copper / HG", "metals"),
    ("Platinum", "metals"),
    ("Palladium", "metals"),
    ("Crude Oil / CL", "energy"),
    ("Natural Gas / NG", "energy"),
    ("S&P 500 / ES", "indices"),
    ("NASDAQ / NQ", "indices"),
    ("Dow / YM", "indices"),
]

REVERSION_HORIZONS = (30, 60, 90)


def _load_valuation_latest() -> dict[str, Any]:
    path = PROJECT_ROOT / "data" / "valuation_latest.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _reversion_rates(series: list[dict[str, Any]], horizons: tuple[int, ...]) -> dict[str, Any]:
    """When |dev|>=5%, does |dev| shrink by horizon (daily bars)?"""
    out: dict[str, Any] = {}
    for h in horizons:
        hits = trials = 0
        for i, row in enumerate(series):
            dev = row.get("deviation_pct")
            if dev is None or abs(dev) < 5.0:
                continue
            trials += 1
            future = series[i + 1 : i + 1 + h]
            if not future:
                continue
            if abs(future[-1]["deviation_pct"]) < abs(dev):
                hits += 1
        out[str(h)] = {
            "trials": trials,
            "reversions": hits,
            "rate_pct": round(100.0 * hits / trials, 1) if trials else None,
        }
    return out


def _fit_metrics(actual: list[float], predicted: list[float]) -> dict[str, float | None]:
    if not actual or len(actual) != len(predicted):
        return {"r_squared": None, "mae": None, "rmse": None, "avg_deviation_pct": None, "max_deviation_pct": None}
    n = len(actual)
    mean_a = sum(actual) / n
    ss_res = sum((a - p) ** 2 for a, p in zip(actual, predicted))
    ss_tot = sum((a - mean_a) ** 2 for a in actual)
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else None
    mae = sum(abs(a - p) for a, p in zip(actual, predicted)) / n
    rmse = math.sqrt(sum((a - p) ** 2 for a, p in zip(actual, predicted)) / n)
    devs = [100.0 * (a - p) / p if p else 0.0 for a, p in zip(actual, predicted)]
    return {
        "r_squared": round(r2, 4) if r2 is not None else None,
        "mae": round(mae, 4),
        "rmse": round(rmse, 4),
        "avg_deviation_pct": round(sum(devs) / n, 2),
        "max_deviation_pct": round(max(abs(d) for d in devs), 2),
    }


def _audit_agri(market: str, live: dict[str, Any]) -> dict[str, Any]:
    val = compute_agri_valuation(market=market)
    inv = discover_instrument_data(market)
    bs, _ = load_balance_sheet(market)
    pairs = _align_price_stu(market, bs) if bs else []

    model_id = val.get("model_id") or live.get("model_id")
    wired = bool(val.get("wired"))

    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]
    slope, intercept, reg_r2 = _ols_slope_intercept(xs, ys)

    # Build historical series for validation + reversion (expanding window percentile)
    series: list[dict[str, Any]] = []
    actuals: list[float] = []
    preds: list[float] = []
    for i in range(MIN_OBS_PERCENTILE, len(bs)):
        sub_bs = bs[: i + 1]
        sub_pairs = _align_price_stu(market, sub_bs)
        if len(sub_pairs) < MIN_OBS_PERCENTILE:
            continue
        cur = sub_bs[-1]
        px = _price_on_date(market, cur.date)
        if px is None or px <= 0:
            continue
        sub_xs = [p[0] for p in sub_pairs]
        sub_ys = [p[1] for p in sub_pairs]
        s, ic, r2 = _ols_slope_intercept(sub_xs, sub_ys)
        fair: float | None = None
        if len(sub_pairs) >= MIN_OBS_REGRESSION and r2 is not None and r2 >= MIN_R2:
            fair = ic + s * cur.stocks_to_use
        else:
            fair = _fair_from_stu_percentile(sub_pairs, cur.stocks_to_use)
        if fair is None or fair <= 0:
            continue
        dev = round(100.0 * (px - fair) / fair, 2)
        series.append({"date": cur.date, "deviation_pct": dev, "price": px, "fair_value": fair})
        actuals.append(px)
        preds.append(fair)

    metrics = _fit_metrics(actuals, preds)
    reversion = _reversion_rates(series, REVERSION_HORIZONS)

    equation = (
        "fair_value = percentile_map(current_stocks_to_use → historical_price_distribution)"
        if model_id == MODEL_ID_PERCENTILE
        else f"price = {intercept:.4f} + {slope:.4f} × stocks_to_use"
    )

    # Classification
    if not wired:
        status = "REBUILD_REQUIRED"
        grade = "F"
        publish = False
        priority = 1
    elif model_id == MODEL_ID_PERCENTILE:
        status = "NEEDS_IMPROVEMENT"
        grade = "D" if (reg_r2 or 0) < MIN_R2 else "C"
        publish = False
        priority = 3
        if market == "Corn" and abs(val.get("deviation_pct") or 0) > 20:
            priority = 2
    else:
        status = "NEEDS_IMPROVEMENT" if (metrics.get("r_squared") or 0) < 0.25 else "PRODUCTION_READY"
        grade = "B" if status == "PRODUCTION_READY" else "C"
        publish = status == "PRODUCTION_READY"
        priority = 4 if publish else 3

    rev60 = (reversion.get("60") or {}).get("rate_pct")
    if rev60 is not None and rev60 < 45 and wired:
        if status == "PRODUCTION_READY":
            status = "NEEDS_IMPROVEMENT"
        publish = False
        priority = min(priority, 2)

    rationale = (
        "Tight stocks-to-use ratios historically support higher cash prices; loose S/U supports lower prices. "
        "USDA WASDE/PSD balance sheet is the native fundamental anchor for grains/softs."
    )

    return {
        "market": market,
        "category": "grains" if market in ("Wheat", "Corn", "Soybeans") else "softs",
        "live_model": model_id,
        "live_wired": wired,
        "inputs": ["USDA WASDE/PSD stocks-to-use", "canonical spot price"],
        "sample_size": len(pairs),
        "regression_r2_on_stu_price": round(reg_r2, 4) if reg_r2 is not None else None,
        "equation": equation,
        "fair_value": val.get("fair_value"),
        "valuation_pct": val.get("deviation_pct"),
        "validation": metrics,
        "usefulness_reversion": reversion,
        "economic_rationale": rationale,
        "status": status,
        "validation_grade": grade,
        "publish": publish,
        "priority": priority,
        "blocker": val.get("unavailable_reason") or val.get("valuation_reason"),
    }


def _audit_metals(market: str, live: dict[str, Any]) -> dict[str, Any]:
    panel = _build_weekly_panel(market)
    val = compute_metals_valuation(market=market)
    wired = bool(val.get("wired"))
    result = _compute_fair_value(panel, market) if len(panel) >= MIN_WEEKS else {"ok": False}

    beta = [result.get("intercept", 0), *result.get("beta", {}).values()] if result.get("ok") else []
    feature_names = list((result.get("beta") or {}).keys())

    actuals: list[float] = []
    preds: list[float] = []
    series: list[dict[str, Any]] = []
    use_china = result.get("use_china_pmi", False)

    for obs in panel:
        feats = [obs.real_yield, math.log(obs.dxy)]
        if use_china and obs.china_pmi is not None:
            feats.append(obs.china_pmi)
        if not beta or len(beta) != len(feats) + 1:
            continue
        lp = _predict_log_price(beta, feats)
        if lp is None:
            continue
        fair = math.exp(lp)
        if fair <= 0:
            continue
        actuals.append(obs.price)
        preds.append(fair)
        dev = round(100.0 * (obs.price - fair) / fair, 2)
        series.append({"date": obs.date, "deviation_pct": dev})

    metrics = _fit_metrics(actuals, preds)
    if result.get("ok"):
        metrics["r_squared"] = result.get("r_squared")
    reversion = _reversion_rates(series, REVERSION_HORIZONS)

    intercept = result.get("intercept")
    betas = result.get("beta") or {}
    eq_parts = [f"log(price) = {intercept:.4f}"] if intercept is not None else ["log(price) = β₀"]
    for name, b in betas.items():
        eq_parts.append(f"+ {b:.4f}×{name}")
    equation = " ".join(eq_parts) if result.get("ok") else "log(price) ~ real_yield + log(DXY)"

    r2 = metrics.get("r_squared") or 0
    rev30 = (reversion.get("30") or {}).get("rate_pct")
    rev60 = (reversion.get("60") or {}).get("rate_pct")
    mad = metrics.get("avg_deviation_pct")

    if not wired:
        status, grade, publish, priority = "REBUILD_REQUIRED", "F", False, 1
    elif r2 >= 0.15 and (rev60 or 0) >= 50:
        status, grade, publish, priority = "PRODUCTION_READY", "A" if r2 >= 0.25 else "B", True, 5
    elif r2 >= 0.08:
        status, grade, publish, priority = "NEEDS_IMPROVEMENT", "C", False, 3
        if abs(val.get("deviation_pct") or 0) > 35:
            priority = 2
    else:
        status, grade, publish, priority = "REBUILD_REQUIRED", "F", False, 1

    # Gold/Silver show extreme deviations — macro model weak at current regime
    if market in ("Gold", "Silver") and abs(val.get("deviation_pct") or 0) > 40:
        status = "NEEDS_IMPROVEMENT"
        publish = False
        priority = 2

    rationale = (
        "Precious/industrial metals respond inversely to real yields and USD strength: "
        "higher real rates raise opportunity cost of non-yielding assets; stronger USD weighs on "
        "USD-denominated commodities. Copper would add China PMI when wired."
    )

    return {
        "market": market,
        "category": "metals",
        "live_model": val.get("model_id") or live.get("model_id"),
        "live_wired": wired,
        "inputs": ["DFII10 (10Y TIPS real yield)", "DTWEXBGS (broad USD)", "canonical metal price"],
        "sample_size": result.get("n_obs") or len(panel),
        "equation": equation,
        "fair_value": val.get("fair_value"),
        "valuation_pct": val.get("deviation_pct"),
        "validation": metrics,
        "usefulness_reversion": reversion,
        "forward_4w_corr": None,
        "economic_rationale": rationale,
        "status": status,
        "validation_grade": grade,
        "publish": publish,
        "priority": priority,
        "blocker": val.get("valuation_reason") if not wired else None,
        "notes": f"4W forward-return correlation unavailable in this audit run; avg |dev| from in-sample panel.",
    }


def _audit_missing(
    market: str,
    category: str,
    model_id: str,
    *,
    rationale: str,
    blocker: str,
) -> dict[str, Any]:
    return {
        "market": market,
        "category": category,
        "live_model": model_id,
        "live_wired": False,
        "inputs": [],
        "sample_size": 0,
        "equation": "—",
        "fair_value": None,
        "valuation_pct": None,
        "validation": {
            "r_squared": None,
            "mae": None,
            "rmse": None,
            "avg_deviation_pct": None,
            "max_deviation_pct": None,
        },
        "usefulness_reversion": {str(h): {"trials": 0, "reversions": 0, "rate_pct": None} for h in REVERSION_HORIZONS},
        "economic_rationale": rationale,
        "status": "REBUILD_REQUIRED",
        "validation_grade": "F",
        "publish": False,
        "priority": 1,
        "blocker": blocker,
        "scanner_fallback": "52-week location percentile (ValuationCell) — NOT fundamental valuation",
    }


def run_audit() -> dict[str, Any]:
    latest = _load_valuation_latest()
    instruments = latest.get("instruments") or {}
    rows: list[dict[str, Any]] = []

    for market, category in AUDIT_MARKETS:
        live = instruments.get(market) or {}
        if category in ("grains", "softs"):
            rows.append(_audit_agri(market, live))
        elif category == "metals":
            rows.append(_audit_metals(market, live))
        elif category == "energy":
            rows.append(
                _audit_missing(
                    market,
                    "energy",
                    live.get("model_id") or "energy_inventory_dxy_v3",
                    rationale=(
                        "Energy fair value requires inventory vs 5Y norm (EIA weekly stocks), "
                        "USD regime, and seasonal demand context. Inventories anchor physical surplus/deficit."
                    ),
                    blocker="No approved engine — planned energy_inventory_dxy_v3 (V3.3). EIA feed not wired.",
                )
            )
        else:
            rows.append(
                _audit_missing(
                    market,
                    "indices",
                    live.get("model_id") or "indices_erp_cape_v3",
                    rationale=(
                        "Equity index fair value from CAPE/Shiller PE, earnings yield minus 10Y (ERP), "
                        "and dividend yield vs rates. Mean-reversion of valuation multiples drives long-horizon returns."
                    ),
                    blocker=(
                        "No live model — indices_erp_cape_v3 audit-only. "
                        "index_valuation_v2_audit: production_ready_after_audit=false; FRED CAPE unavailable."
                    ),
                )
            )

    rows.sort(key=lambda r: (r["priority"], r["market"]))
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "phase": "3 — Institutional Valuation Audit (Non-FX)",
        "markets_audited": len(rows),
        "summary": {
            "production_ready": sum(1 for r in rows if r["status"] == "PRODUCTION_READY"),
            "needs_improvement": sum(1 for r in rows if r["status"] == "NEEDS_IMPROVEMENT"),
            "rebuild_required": sum(1 for r in rows if r["status"] == "REBUILD_REQUIRED"),
            "publishable": sum(1 for r in rows if r["publish"]),
        },
        "master_table": [
            {
                "market": r["market"],
                "model": r["live_model"],
                "status": r["status"],
                "validation_grade": r["validation_grade"],
                "publish": r["publish"],
                "priority": r["priority"],
            }
            for r in rows
        ],
        "markets": {r["market"]: r for r in rows},
    }


def write_reports(doc: dict[str, Any]) -> None:
    out_json = PROJECT_ROOT / "data" / "audits" / "non_fx_institutional_valuation_audit.json"
    out_md = PROJECT_ROOT / "data" / "audits" / "non_fx_institutional_valuation_audit.md"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(doc, indent=2), encoding="utf-8")

    lines = [
        "# Phase 3 — Institutional Valuation Audit (Non-FX Markets)",
        "",
        f"- Generated: `{doc['generated_at']}`",
        f"- Markets audited: **{doc['markets_audited']}**",
        f"- Production ready: **{doc['summary']['production_ready']}** · "
        f"Needs improvement: **{doc['summary']['needs_improvement']}** · "
        f"Rebuild required: **{doc['summary']['rebuild_required']}**",
        "",
        "## Master Table (sorted by rebuild priority)",
        "",
        "| Market | Model | Status | Validation Grade | Publish? | Priority |",
        "| --- | --- | --- | --- | --- | ---: |",
    ]
    for row in doc["master_table"]:
        pub = "Yes" if row["publish"] else "**No**"
        lines.append(
            f"| {row['market']} | {row['model']} | {row['status']} | {row['validation_grade']} | {pub} | {row['priority']} |"
        )

    lines.extend(["", "## Per-Market Evidence", ""])
    for market, r in doc["markets"].items():
        v = r["validation"]
        rev = r["usefulness_reversion"]
        lines.extend(
            [
                f"### {market}",
                "",
                f"- **Live model:** `{r['live_model']}` · wired={r['live_wired']}",
                f"- **Inputs:** {', '.join(r['inputs']) if r['inputs'] else '—'}",
                f"- **Sample size:** {r['sample_size']}",
                f"- **Equation:** {r['equation']}",
                f"- **Fair value / valuation %:** {r['fair_value']} / {r['valuation_pct']}%",
                f"- **Validation:** R²={v.get('r_squared')} · MAE={v.get('mae')} · RMSE={v.get('rmse')} · "
                f"avg dev={v.get('avg_deviation_pct')}% · max dev={v.get('max_deviation_pct')}%",
                f"- **Reversion (|dev|≥5%):** 30d={rev.get('30', {}).get('rate_pct')}% · "
                f"60d={rev.get('60', {}).get('rate_pct')}% · 90d={rev.get('90', {}).get('rate_pct')}%",
                f"- **Economic rationale:** {r['economic_rationale']}",
                f"- **Classification:** {r['status']} · Grade {r['validation_grade']} · Publish={r['publish']}",
            ]
        )
        if r.get("blocker"):
            lines.append(f"- **Blocker:** {r['blocker']}")
        if r.get("scanner_fallback"):
            lines.append(f"- **Scanner fallback:** {r['scanner_fallback']}")
        lines.append("")

    out_md.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {out_json}")
    print(f"Wrote {out_md}")


if __name__ == "__main__":
    doc = run_audit()
    write_reports(doc)
    print(json.dumps(doc["summary"], indent=2))
    print("\nMaster table:")
    for row in doc["master_table"]:
        print(f"  P{row['priority']} | {row['market']:20} | {row['status']:20} | publish={row['publish']}")
