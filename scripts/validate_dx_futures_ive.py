"""Phase 1D — prove DX futures IVE model (no new architecture)."""
from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from hptl.config import DATA_DIR, PROJECT_ROOT
from hptl.fx.fx_macro_history import currency_histories
from hptl.valuation.currency_futures_ive_v1 import (
    DEPENDENT_SERIES,
    FUTURES_REGISTRY,
    _build_dx_panel,
    _current_futures_price,
    _load_futures_daily,
    _ols_log_futures,
    _predict_log_fv,
    _reconciles,
    valuation_label_from_pct,
)

AUDIT_JSON = DATA_DIR / "audits/dx_futures_ive_validation.json"
AUDIT_MD = DATA_DIR / "audits/dx_futures_ive_validation.md"


def _build_historical_series(
    panel: list[dict[str, Any]],
    reg: dict[str, Any],
    features: tuple[str, ...],
) -> list[dict[str, Any]]:
    coef = reg.get("coefficients") or {}
    series: list[dict[str, Any]] = []
    for row in panel:
        drivers = {f: row.get(f) for f in features}
        log_fv = _predict_log_fv(reg, drivers)
        if log_fv is None:
            continue
        fv = math.exp(log_fv)
        close = float(row["close"])
        dev = (close - fv) / fv * 100.0
        series.append(
            {
                "date": row["date"],
                "close": close,
                "fair_value": round(fv, 4),
                "deviation_pct": round(dev, 4),
                "log_close": math.log(close),
                "log_fair_value": log_fv,
            }
        )
    return series


def _in_sample_metrics(series: list[dict[str, Any]], reg: dict[str, Any]) -> dict[str, Any]:
    y = np.array([s["log_close"] for s in series])
    pred = np.array([s["log_fair_value"] for s in series])
    ss_res = float(((y - pred) ** 2).sum())
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    price_actual = np.array([s["close"] for s in series])
    price_pred = np.array([s["fair_value"] for s in series])
    err = price_actual - price_pred
    mae = float(np.mean(np.abs(err)))
    rmse = float(np.sqrt(np.mean(err**2)))
    devs = [abs(s["deviation_pct"]) for s in series]
    return {
        "r_squared": round(r2, 4),
        "mae_price": round(mae, 4),
        "rmse_price": round(rmse, 4),
        "avg_abs_deviation_pct": round(float(np.mean(devs)), 4),
        "max_abs_deviation_pct": round(float(np.max(devs)), 4),
        "observation_count": len(series),
    }


def _reversion_test(series: list[dict[str, Any]]) -> dict[str, Any]:
    thresholds = (5.0, 10.0, 15.0, 20.0)
    horizons = (30, 60, 90)
    out: dict[str, Any] = {}

    for thr in thresholds:
        key = f"threshold_{int(thr)}pct"
        by_h: dict[str, Any] = {}
        episodes: list[dict[str, Any]] = []

        for i, row in enumerate(series):
            dev = row["deviation_pct"]
            if abs(dev) < thr:
                continue
            ep: dict[str, Any] = {"date": row["date"], "deviation_pct": dev, "horizons": {}}
            for h in horizons:
                future = series[i + 1 : i + 1 + h]
                if len(future) < h:
                    continue
                end = future[-1]
                end_dev = end["deviation_pct"]
                # Reversion: deviation magnitude shrinks OR crosses toward zero
                reverted = abs(end_dev) < abs(dev) or (dev < 0 < end_dev) or (dev > 0 > end_dev)
                price_to_fv = abs(end["close"] - end["fair_value"]) < abs(row["close"] - row["fair_value"])
                ep["horizons"][str(h)] = {
                    "end_date": end["date"],
                    "end_deviation_pct": round(end_dev, 4),
                    "deviation_change": round(end_dev - dev, 4),
                    "reverted": reverted,
                    "moved_toward_fv": price_to_fv,
                    "spot_change_pct": round((end["close"] / row["close"] - 1) * 100, 4),
                }
            if ep["horizons"]:
                episodes.append(ep)

        by_h = {}
        for h in horizons:
            trials = rev = toward_fv = 0
            for ep in episodes:
                hdata = ep["horizons"].get(str(h))
                if not hdata:
                    continue
                trials += 1
                if hdata["reverted"]:
                    rev += 1
                if hdata["moved_toward_fv"]:
                    toward_fv += 1
            by_h[str(h)] = {
                "trials": trials,
                "reversion_count": rev,
                "reversion_rate_pct": round(rev / trials * 100, 1) if trials else None,
                "toward_fv_rate_pct": round(toward_fv / trials * 100, 1) if trials else None,
            }

        out[key] = {
            "threshold_pct": thr,
            "episode_count": len(episodes),
            "horizons": by_h,
            "sample_episodes": episodes[:3],
        }
    return out


def _walkthrough(
    spec,
    panel: list[dict[str, Any]],
    reg: dict[str, Any],
    current: float | None,
    price_as_of: str | None,
) -> dict[str, Any]:
    last = panel[-1] if panel else {}
    drivers = {f: last.get(f) for f in spec.feature_names}
    coef = reg.get("coefficients") or {}
    intercept = float(coef.get("intercept") or 0)

    contributions: list[dict[str, Any]] = []
    log_sum = intercept
    for name in spec.feature_names:
        val = drivers.get(name)
        beta = coef.get(name)
        if val is None or beta is None:
            continue
        term = float(beta) * float(val)
        log_sum += term
        contributions.append(
            {
                "input": name,
                "value": val,
                "coefficient": beta,
                "log_contribution": round(term, 8),
            }
        )

    fair_value = round(math.exp(log_sum), 4)
    reconciled = _reconciles(reg, drivers, fair_value)
    valuation_pct = None
    label = "—"
    if current is not None and fair_value:
        valuation_pct = round((current - fair_value) / fair_value * 100.0, 4)
        label = valuation_label_from_pct(valuation_pct)

    return {
        "as_of_date": price_as_of or last.get("date"),
        "current_dx_price": current,
        "inputs": drivers,
        "intercept": intercept,
        "contributions": contributions,
        "log_fair_value": round(log_sum, 8),
        "calculated_fair_value": fair_value,
        "valuation_pct": valuation_pct,
        "valuation_label": label,
        "reconciles": reconciled,
    }


def _economic_sanity(coef: dict[str, float]) -> list[dict[str, Any]]:
    return [
        {
            "input": "avg_g10_2y_vs_usd",
            "coefficient": coef.get("avg_g10_2y_vs_usd"),
            "economic_rationale": (
                "Average G10 2Y yield minus USD 2Y. When foreign short rates rise relative to the US, "
                "capital tends to leave USD assets for higher yields abroad, weakening the broad dollar — "
                "DX should fall. A negative coefficient is economically consistent; positive would imply "
                "USD strengthens when the rest of G10 offers more yield, which contradicts carry/rate-diff logic."
            ),
            "sign_consistent": (coef.get("avg_g10_2y_vs_usd") or 0) < 0,
        },
        {
            "input": "fed_funds",
            "coefficient": coef.get("fed_funds"),
            "economic_rationale": (
                "US effective federal funds rate. Higher US policy rates increase the return on USD cash "
                "and short-dated USD assets, attracting capital and supporting dollar strength — DX should rise. "
                "A positive coefficient is economically consistent."
            ),
            "sign_consistent": (coef.get("fed_funds") or 0) > 0,
        },
        {
            "input": "real_yield_10y",
            "coefficient": coef.get("real_yield_10y"),
            "economic_rationale": (
                "US 10-year TIPS yield (DFII10) as inflation-adjusted long-rate anchor. Higher US real yields "
                "raise the opportunity cost of holding non-USD assets and support USD on a capital-flow basis — "
                "DX should rise. A positive coefficient is economically consistent."
            ),
            "sign_consistent": (coef.get("real_yield_10y") or 0) > 0,
        },
    ]


def _verdict(metrics: dict[str, Any], reversion: dict[str, Any], sanity: list[dict[str, Any]], walk: dict[str, Any]) -> dict[str, Any]:
    r2 = metrics.get("r_squared") or 0
    signs_ok = all(s.get("sign_consistent") for s in sanity)
    t10 = reversion.get("threshold_10pct", {}).get("horizons", {})
    r60_10 = (t10.get("60") or {}).get("reversion_rate_pct")
    r60_5 = (reversion.get("threshold_5pct", {}).get("horizons", {}).get("60") or {}).get("reversion_rate_pct")
    max_dev = metrics.get("max_abs_deviation_pct") or 0

    reasons: list[str] = []
    if r2 >= 0.30:
        reasons.append(f"In-sample R²={r2} exceeds 0.30 on {metrics['observation_count']} daily obs.")
    elif r2 >= 0.08:
        reasons.append(f"In-sample R²={r2} passes minimum gate (0.08) but is modest for production.")
    else:
        reasons.append(f"In-sample R²={r2} below minimum gate.")

    if signs_ok:
        reasons.append("All three coefficient signs match standard USD macro intuition.")
    else:
        reasons.append("One or more coefficient signs conflict with economic intuition.")

    if r60_10 is not None:
        reasons.append(f"At |deviation|≥10%, 60-day reversion rate = {r60_10}% ({t10.get('60', {}).get('trials')} trials).")
    if r60_5 is not None:
        reasons.append(f"At |deviation|≥5%, 60-day reversion rate = {r60_5}%.")

    reasons.append(f"Average |deviation| = {metrics.get('avg_abs_deviation_pct')}%; max = {max_dev}%.")

    if not walk.get("reconciles"):
        reasons.append("Current walkthrough does NOT reconcile to engine equation.")

    # Classification
    if (
        r2 >= 0.30
        and signs_ok
        and walk.get("reconciles")
        and r60_10 is not None
        and r60_10 >= 55
        and (r60_5 is None or r60_5 >= 50)
    ):
        classification = "PRODUCTION_READY"
    elif r2 >= 0.08 and signs_ok and walk.get("reconciles"):
        classification = "NEEDS_IMPROVEMENT"
    else:
        classification = "REBUILD_REQUIRED"

    if max_dev > 25 and r60_10 is not None and r60_10 < 50:
        classification = "REBUILD_REQUIRED"
        reasons.append("Large historical deviations do not reliably mean-revert within 60 days.")

    return {"classification": classification, "reasons": reasons}


def build_dx_validation_report() -> dict[str, Any]:
    spec = FUTURES_REGISTRY["DX"]
    histories = currency_histories()
    futures_daily, price_meta = _load_futures_daily(spec.instrument_id)
    panel = _build_dx_panel(futures_daily, histories)
    reg = _ols_log_futures(panel, spec.feature_names)
    current, price_as_of = _current_futures_price(spec.instrument_id)
    coef = reg.get("coefficients") or {}

    series = _build_historical_series(panel, reg, spec.feature_names)
    metrics = _in_sample_metrics(series, reg)
    reversion = _reversion_test(series)
    walk = _walkthrough(spec, panel, reg, current, price_as_of)
    sanity = _economic_sanity(coef)
    verdict = _verdict(metrics, reversion, sanity, walk)

    equation = (
        "log(DX_close) = "
        f"{coef.get('intercept'):.8f}"
        f" + ({coef.get('avg_g10_2y_vs_usd'):.8f}) × avg_g10_2y_vs_usd"
        f" + ({coef.get('fed_funds'):.8f}) × fed_funds"
        f" + ({coef.get('real_yield_10y'):.8f}) × real_yield_10y"
        "\nDX_fair_value = exp(log(DX_close_model))"
        "\nvaluation_pct = ((current_price - fair_value) / fair_value) × 100"
    )

    return {
        "phase": "1D DX Futures IVE Validation",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model_specification": {
            "model_name": spec.model_name,
            "instrument_id": spec.instrument_id,
            "futures_symbol": "DX",
            "dependent_variable": DEPENDENT_SERIES,
            "sample_start": panel[0]["date"] if panel else None,
            "sample_end": panel[-1]["date"] if panel else None,
            "observation_count": reg.get("n"),
            "inputs": list(spec.feature_names),
            "coefficients": coef,
            "intercept": coef.get("intercept"),
            "price_history_meta": price_meta,
        },
        "full_equation": equation,
        "current_walkthrough": walk,
        "historical_validation": metrics,
        "trading_usefulness": reversion,
        "economic_sanity": sanity,
        "final_verdict": verdict,
    }


def _render_md(doc: dict[str, Any]) -> str:
    spec = doc["model_specification"]
    coef = spec["coefficients"]
    walk = doc["current_walkthrough"]
    metrics = doc["historical_validation"]
    lines = [
        "# Phase 1D — DX Futures IVE Validation",
        "",
        f"Generated: {doc['generated_at']}",
        "",
        "## 1. Full Model Specification",
        "",
        f"- **Model name:** {spec['model_name']}",
        f"- **Instrument:** {spec['instrument_id']} ({spec['futures_symbol']})",
        f"- **Dependent variable:** {spec['dependent_variable']} (continuous DX futures daily close)",
        f"- **Sample period:** {spec['sample_start']} → {spec['sample_end']}",
        f"- **Observation count:** {spec['observation_count']}",
        f"- **Inputs:** {', '.join(spec['inputs'])}",
        "",
        "### Coefficients",
        "",
        f"| Term | Coefficient |",
        f"|------|-------------|",
        f"| intercept | {coef.get('intercept')} |",
    ]
    for name in spec["inputs"]:
        lines.append(f"| {name} | {coef.get(name)} |")

    lines.extend(
        [
            "",
            "## 2. Full Equation",
            "",
            "```",
            doc["full_equation"],
            "```",
            "",
            "## 3. Current Fair Value Walkthrough",
            "",
            f"- **As-of date:** {walk.get('as_of_date')}",
            f"- **Current DX price:** {walk.get('current_dx_price')}",
            "",
            "### Inputs and log contributions",
            "",
            f"| Input | Value | β | β × input (log) |",
            f"|-------|-------|---|-----------------|",
            f"| intercept | — | — | {walk.get('intercept')} |",
        ]
    )
    for c in walk.get("contributions") or []:
        lines.append(f"| {c['input']} | {c['value']} | {c['coefficient']} | {c['log_contribution']} |")

    lines.extend(
        [
            "",
            f"- **Sum log fair value:** {walk.get('log_fair_value')}",
            f"- **Fair value = exp(log):** {walk.get('calculated_fair_value')}",
            f"- **Valuation %:** {walk.get('valuation_pct')}% ({walk.get('valuation_label')})",
            f"- **Reconciles:** {walk.get('reconciles')}",
            "",
            "## 4. Historical Validation",
            "",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| R² (in-sample, log space) | {metrics.get('r_squared')} |",
            f"| MAE (price) | {metrics.get('mae_price')} |",
            f"| RMSE (price) | {metrics.get('rmse_price')} |",
            f"| Avg |deviation| % | {metrics.get('avg_abs_deviation_pct')} |",
            f"| Max |deviation| % | {metrics.get('max_abs_deviation_pct')} |",
            "",
            "## 5. Trading Usefulness — Reversion After Extreme Valuation",
            "",
            "Reversion = deviation magnitude shrinks or crosses zero within N **trading days**.",
            "",
        ]
    )

    for thr_key in ("threshold_5pct", "threshold_10pct", "threshold_15pct", "threshold_20pct"):
        blk = doc["trading_usefulness"].get(thr_key) or {}
        thr = blk.get("threshold_pct")
        lines.append(f"### |deviation| ≥ {thr}%")
        lines.append("")
        lines.append("| Horizon | Trials | Reversion rate | Toward FV rate |")
        lines.append("|---------|--------|----------------|----------------|")
        for h in ("30", "60", "90"):
            row = (blk.get("horizons") or {}).get(h) or {}
            lines.append(
                f"| {h}d | {row.get('trials')} | {row.get('reversion_rate_pct')}% | {row.get('toward_fv_rate_pct')}% |"
            )
        lines.append("")

    lines.append("## 6. Economic Sanity Check")
    lines.append("")
    for s in doc["economic_sanity"]:
        ok = "✓" if s.get("sign_consistent") else "✗"
        lines.extend(
            [
                f"### {s['input']} (β = {s['coefficient']}) — sign {ok}",
                "",
                s["economic_rationale"],
                "",
            ]
        )

    v = doc["final_verdict"]
    lines.extend(
        [
            "## 7. Final Verdict",
            "",
            f"### **{v['classification']}**",
            "",
        ]
    )
    for r in v["reasons"]:
        lines.append(f"- {r}")

    return "\n".join(lines) + "\n"


def main() -> int:
    doc = build_dx_validation_report()
    AUDIT_JSON.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_JSON.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    AUDIT_MD.write_text(_render_md(doc), encoding="utf-8")
    print(f"Wrote {AUDIT_JSON}")
    print(f"Wrote {AUDIT_MD}")
    print(f"VERDICT: {doc['final_verdict']['classification']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
