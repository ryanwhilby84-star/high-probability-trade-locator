"""Phase 4 — Prove or kill the metals valuation framework (read-only evidence)."""
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
from hptl.valuation.metals_valuation_select import (
    ROLLING_WEEKS,
    _panel_for,
    _run_variant,
    _variants_for_market,
    select_metals_model,
)

MARKETS = ("Gold", "Silver", "Copper / HG", "Platinum", "Palladium")
THRESHOLDS = (10, 20, 30, 40)
HORIZONS = (30, 60, 90)

INSTITUTIONAL_DRIVERS: dict[str, dict[str, list[str]]] = {
    "Gold": {
        "current": ["DFII10 (10Y TIPS real yield)", "DTWEXBGS (broad USD index)"],
        "missing": [
            "Central bank net purchases (WGC / IMF)",
            "Gold ETF holdings (GLD, IAU flows)",
            "Real rates beyond 10Y (5Y TIPS, forward real)",
            "Reserve / de-dollarization demand proxy",
            "Inflation uncertainty / geopolitical risk premium",
            "Mine supply / AISC cost floor",
        ],
    },
    "Silver": {
        "current": ["DFII10", "DTWEXBGS", "Gold/silver ratio (Silver variant only, not selected)"],
        "missing": [
            "Industrial demand (solar PV, electronics)",
            "ETF holdings (SLV flows)",
            "Mine supply / by-product ratio to base metals",
            "Gold/silver ratio (when not in selected model)",
            "LBMA/COMEX inventory",
        ],
    },
    "Copper / HG": {
        "current": ["DFII10", "DTWEXBGS", "China PMI (reserved, not wired in regression)"],
        "missing": [
            "China PMI / industrial production (not in live regression)",
            "LME warehouse inventories",
            "SHFE inventories",
            "Treatment & refining charges (TC/RC)",
            "Mine supply deficit / concentrate tightness",
            "USD strength beyond broad index (CNY)",
        ],
    },
    "Platinum": {
        "current": ["DFII10", "DTWEXBGS"],
        "missing": [
            "Auto catalyst demand (ICE + hybrid, diesel share)",
            "Jewelry demand (China, India)",
            "South Africa / Russia mine supply disruptions",
            "Palladium substitution elasticity",
            "Hydrogen economy / PEM catalyst demand",
            "Above-ground stock drawdown",
        ],
    },
    "Palladium": {
        "current": ["DFII10", "DTWEXBGS"],
        "missing": [
            "Auto catalyst demand (gasoline, hybrid)",
            "Platinum substitution in autocats",
            "Russia supply risk / export flows",
            "Recycling scrap supply",
            "EV adoption curve (demand destruction)",
            "Above-ground stock / deficit tracking",
        ],
    },
}


def _attribution(selected: dict[str, Any]) -> dict[str, Any]:
    """Incremental log-linear build: show actual math."""
    beta_map = selected["beta"]
    intercept = selected["intercept"]
    ry = selected["real_yield"]
    dxy = selected["dxy"]
    log_dxy = math.log(dxy)
    spot = selected["spot_price"]
    fair = selected["fair_value"]

    names = list(beta_map.keys())
    betas = [beta_map[n] for n in names]

    # Build feature vector in model order
    feats: list[float] = []
    feat_labels: list[str] = []
    for n in names:
        if n == "real_yield":
            feats.append(ry)
            feat_labels.append(f"real_yield={ry}")
        elif n == "log_dxy":
            feats.append(log_dxy)
            feat_labels.append(f"log(DXY)=log({dxy})={round(log_dxy, 6)}")
        elif n == "log_gold_silver_ratio":
            # only silver variant
            gp = selected.get("gold_spot")
            if gp:
                gsr = math.log(gp / spot)
                feats.append(gsr)
                feat_labels.append(f"log(G/S)={round(gsr, 6)}")
            else:
                feats.append(0.0)
                feat_labels.append("log(G/S)=n/a")

    log_terms = [("intercept", intercept, intercept)]
    cumulative_log = intercept
    steps: list[dict[str, Any]] = []
    price_prev = math.exp(intercept)
    steps.append(
        {
            "step": "intercept_only",
            "formula": f"exp({intercept:.6f})",
            "price_level": round(price_prev, 4),
            "pct_change_from_prior": 0.0,
        }
    )
    for i, (name, b, x) in enumerate(zip(names, betas, feats)):
        term_log = b * x
        cumulative_log += term_log
        price_now = math.exp(cumulative_log)
        pct = round(100.0 * (price_now / price_prev - 1.0), 2) if price_prev else 0.0
        steps.append(
            {
                "step": name,
                "formula": f"+ β_{name}×x = + ({b:.6f})×({x:.6f}) = + {term_log:.6f} log pts",
                "cumulative_log": round(cumulative_log, 6),
                "price_level": round(price_now, 4),
                "pct_change_from_prior": pct,
                "log_term": round(term_log, 6),
            }
        )
        log_terms.append((name, term_log, b * x))
        price_prev = price_now

    log_fair = cumulative_log
    log_spot = math.log(spot)
    gap_log = log_spot - log_fair

    # % contribution of each log term to |log(fair)| (excluding intercept for driver share)
    driver_logs = [abs(t[1]) for t in log_terms[1:]]
    total_driver = sum(driver_logs) or 1.0
    driver_pct = {
        log_terms[i + 1][0]: round(100.0 * abs(log_terms[i + 1][1]) / total_driver, 1)
        for i in range(len(driver_logs))
    }

    equation = f"log(fair) = {intercept:.6f}" + "".join(
        f" + ({beta_map[n]:.6f})×({feats[i]:.6f})" for i, n in enumerate(names)
    )

    return {
        "equation": equation,
        "log_fair": round(log_fair, 6),
        "fair_value": fair,
        "log_spot": round(log_spot, 6),
        "spot_price": spot,
        "deviation_pct": selected["deviation_pct"],
        "gap_log_spot_minus_fair": round(gap_log, 6),
        "incremental_price_build": steps,
        "driver_log_share_pct": driver_pct,
        "feature_labels": feat_labels,
        "betas": beta_map,
        "intercept": intercept,
    }


def _historical_reality(panel_dates_prices: list[tuple[str, float, float]], thresholds: tuple[int, ...]) -> dict[str, Any]:
    """Reversion + directional hit rate when |dev| exceeds threshold."""
    # panel_dates_prices: (date, spot, fair) weekly series
    n = len(panel_dates_prices)
    out: dict[str, Any] = {}
    for thr in thresholds:
        bucket: dict[str, Any] = {"threshold_pct": thr, "horizons": {}}
        for h in HORIZONS:
            trials = reverts = directional_hits = 0
            for i in range(n - h):
                _, spot_i, fair_i = panel_dates_prices[i]
                if fair_i <= 0:
                    continue
                dev = 100.0 * (spot_i - fair_i) / fair_i
                if abs(dev) < thr:
                    continue
                trials += 1
                _, spot_h, fair_h = panel_dates_prices[i + h]
                dev_h = 100.0 * (spot_h - fair_h) / fair_h if fair_h > 0 else dev
                if abs(dev_h) < abs(dev):
                    reverts += 1
                # Directional: overvalued -> price should fall; undervalued -> rise
                price_chg = spot_h - spot_i
                if dev > 0 and price_chg < 0:
                    directional_hits += 1
                elif dev < 0 and price_chg > 0:
                    directional_hits += 1
            bucket["horizons"][str(h)] = {
                "trials": trials,
                "reversion_rate_pct": round(100.0 * reverts / trials, 1) if trials else None,
                "directional_hit_rate_pct": round(100.0 * directional_hits / trials, 1) if trials else None,
            }
        out[str(thr)] = bucket
    return out


def _build_weekly_fair_series(selected: dict[str, Any], market: str) -> list[tuple[str, float, float]]:
    """Rebuild fair value series using selected model coefficients."""
    panel = _panel_for(market)
    window = selected.get("window") or len(panel)
    use = panel[-window:] if window <= len(panel) else panel
    intercept = selected["intercept"]
    beta = selected["beta"]
    names = list(beta.keys())
    gold_map = {o.date: o.price for o in _panel_for("Gold")} if "log_gold_silver_ratio" in names else {}

    series: list[tuple[str, float, float]] = []
    for obs in use:
        feats: list[float] = []
        for n in names:
            if n == "real_yield":
                feats.append(obs.real_yield)
            elif n == "log_dxy":
                feats.append(math.log(obs.dxy))
            elif n == "log_gold_silver_ratio":
                gp = gold_map.get(obs.date)
                if gp is None or gp <= 0:
                    continue
                feats.append(math.log(gp / obs.price))
        if len(feats) != len(names):
            continue
        lp = intercept + sum(beta[n] * feats[i] for i, n in enumerate(names))
        fair = math.exp(lp)
        series.append((obs.date, obs.price, fair))
    return series


def _trust_classify(
    market: str,
    selected: dict[str, Any],
    reality: dict[str, Any],
    drivers: dict[str, list[str]],
) -> tuple[str, str, list[str]]:
    """TRUSTED | PARTIALLY_TRUSTED | UNPROVEN | REJECTED + KEEP/MODIFY/FULL REBUILD"""
    dev = abs(selected.get("deviation_pct") or 0)
    n = selected.get("n_obs") or 0
    r2 = selected.get("r_squared") or 0
    missing_count = len(drivers.get("missing") or [])

    # 30% threshold 60d reversion + directional
    r60_20 = (reality.get("20") or {}).get("horizons", {}).get("60") or {}
    rev60 = r60_20.get("reversion_rate_pct")
    dir60 = r60_20.get("directional_hit_rate_pct")
    trials20 = (reality.get("20") or {}).get("horizons", {}).get("60", {}).get("trials") or 0

    r60_30 = (reality.get("30") or {}).get("horizons", {}).get("60") or {}
    rev60_30 = r60_30.get("reversion_rate_pct")

    notes: list[str] = []

    # Economic logic: macro-only precious/industrial without sector drivers
    if market in ("Gold", "Silver") and missing_count >= 4:
        notes.append("Macro-only model; omits institutional flow/stock variables")
    if market == "Copper / HG":
        notes.append("China PMI not in regression; inventory/supply absent")
    if market in ("Platinum", "Palladium"):
        notes.append("Autocat demand entirely absent from model")

    # Rejection criteria
    if rev60 is not None and trials20 >= 30 and rev60 < 45 and dir60 is not None and dir60 < 48:
        trust = "REJECTED"
        decision = "FULL REBUILD"
    elif rev60 is not None and trials20 >= 20 and rev60 >= 55 and dir60 is not None and dir60 >= 52:
        if missing_count <= 3 and dev <= 35:
            trust = "TRUSTED"
            decision = "KEEP"
        else:
            trust = "PARTIALLY_TRUSTED"
            decision = "MODIFY"
    elif n >= 150 and r2 >= 0.15 and rev60 is not None and rev60 >= 50:
        trust = "PARTIALLY_TRUSTED"
        decision = "MODIFY"
    else:
        trust = "UNPROVEN"
        decision = "FULL REBUILD" if missing_count >= 5 or (rev60_30 is not None and rev60_30 < 45) else "MODIFY"

    # Palladium special: published but macro-only for autocat metal
    if market == "Palladium" and "Autocat" in str(drivers.get("missing")):
        if trust == "TRUSTED":
            trust = "PARTIALLY_TRUSTED"
            decision = "MODIFY"

    # Gold/Silver large persistent gaps
    if market in ("Gold", "Silver") and dev > 50:
        notes.append(f"Current gap {selected.get('deviation_pct')}% — model cannot explain regime")
        if trust != "REJECTED":
            trust = "UNPROVEN"
            decision = "FULL REBUILD"

    return trust, decision, notes


def _rebuild_drivers(market: str) -> list[str]:
    m = INSTITUTIONAL_DRIVERS.get(market, {})
    return list(m.get("missing") or [])[:6]


def run_audit() -> dict[str, Any]:
    results: dict[str, Any] = {}
    for market in MARKETS:
        sel = select_metals_model(market)
        if not sel.get("ok"):
            results[market] = {"error": sel.get("reason")}
            continue
        best = sel["selected"]
        if market == "Silver" and "Gold" in str(best.get("beta", {})):
            gp = _panel_for("Gold")
            best = {**best, "gold_spot": gp[-1].price if gp else None}

        attr = _attribution(best)
        fair_series = _build_weekly_fair_series(best, market)
        reality = _historical_reality(fair_series, THRESHOLDS)
        trust, decision, notes = _trust_classify(
            market, best, reality, INSTITUTIONAL_DRIVERS.get(market, {})
        )
        results[market] = {
            "model_id": best["model_variant"],
            "window_weeks": best["window"],
            "n_obs": best["n_obs"],
            "r_squared": best["r_squared"],
            "drivers": INSTITUTIONAL_DRIVERS.get(market),
            "attribution": attr,
            "historical_reality": reality,
            "trust_score": trust,
            "decision": decision,
            "decision_notes": notes,
            "rebuild_driver_list": _rebuild_drivers(market) if decision == "FULL REBUILD" else [],
            "publish": sel["gated"].get("publish"),
        }
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "phase": "4 — Prove or Kill Metals Valuation Framework",
        "framework_verdict": _framework_verdict(results),
        "markets": results,
    }


def _framework_verdict(results: dict[str, Any]) -> str:
    scores = [r.get("trust_score") for r in results.values() if isinstance(r, dict)]
    if any(s == "REJECTED" for s in scores):
        return "FRAMEWORK_NOT_TRUSTWORTHY — macro-only regression fails historical reality for key metals"
    if sum(1 for s in scores if s in ("TRUSTED", "PARTIALLY_TRUSTED")) >= 3:
        return "FRAMEWORK_PARTIALLY_VALID — sector-specific rebuild required per metal"
    return "FRAMEWORK_UNPROVEN — replace generic macro regression with driver-specific models"


def write_reports(doc: dict[str, Any]) -> None:
    out_json = PROJECT_ROOT / "data" / "audits" / "metals_framework_trust_audit.json"
    out_md = PROJECT_ROOT / "data" / "audits" / "metals_framework_trust_audit.md"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(doc, indent=2), encoding="utf-8")

    lines = [
        "# Phase 4 — Metals Framework Trust Audit",
        "",
        f"- Generated: `{doc['generated_at']}`",
        f"- **Framework verdict:** {doc['framework_verdict']}",
        "",
    ]
    for market, r in doc["markets"].items():
        if r.get("error"):
            lines.append(f"## {market}\n\nError: {r['error']}\n")
            continue
        d = r["drivers"]
        a = r["attribution"]
        lines.extend(
            [
                f"## {market}",
                "",
                "### Task 1 — Drivers",
                f"- **Current inputs:** {', '.join(d['current'])}",
                f"- **Missing institutional:** {', '.join(d['missing'])}",
                "",
                "### Task 2 — Attribution (actual math)",
                f"- **Equation:** `{a['equation']}`",
                f"- **Spot:** {a['spot_price']} · **Fair:** {a['fair_value']} · **Dev:** {a['deviation_pct']}%",
                f"- **log(spot) − log(fair):** {a['gap_log_spot_minus_fair']}",
                "",
                "| Step | Formula | Price | Δ% from prior |",
                "| --- | --- | ---: | ---: |",
            ]
        )
        for s in a["incremental_price_build"]:
            lines.append(
                f"| {s['step']} | {s.get('formula', '—')} | {s['price_level']} | {s.get('pct_change_from_prior', 0)} |"
            )
        lines.append(f"- **Driver log-share %:** {a['driver_log_share_pct']}")
        lines.append("")
        lines.append("### Task 3 — Historical reality")
        lines.append("| Threshold | Horizon | Trials | Reversion % | Directional hit % |")
        lines.append("| ---: | ---: | ---: | ---: | ---: |")
        for thr, block in r["historical_reality"].items():
            for h, stats in block["horizons"].items():
                lines.append(
                    f"| {thr}% | {h}d | {stats['trials']} | {stats['reversion_rate_pct']} | {stats['directional_hit_rate_pct']} |"
                )
        lines.extend(
            [
                "",
                f"### Task 4–5 — Trust: **{r['trust_score']}** · Decision: **{r['decision']}**",
                "",
            ]
        )
        if r.get("rebuild_driver_list"):
            lines.append(f"- **Rebuild drivers:** {', '.join(r['rebuild_driver_list'])}")
        lines.append("")
    out_md.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {out_json}")
    print(f"Wrote {out_md}")


if __name__ == "__main__":
    doc = run_audit()
    write_reports(doc)
    print("Framework:", doc["framework_verdict"])
    for m, r in doc["markets"].items():
        if r.get("error"):
            print(m, "ERR", r["error"])
        else:
            print(m, r["trust_score"], r["decision"], "dev=", r["attribution"]["deviation_pct"])
