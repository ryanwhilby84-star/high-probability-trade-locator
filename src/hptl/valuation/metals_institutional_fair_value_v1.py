"""Phase 4B — metal-specific institutional fair value models.

Replaces generic metals_real_yield_v1 / variant selection. Each metal has its own
regression spec, required drivers, and validation gates. No publish without gate pass.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.valuation.engine import BIAS_UNAVAILABLE
from hptl.valuation.institutional_publish_gate import apply_metals_institutional_publish_gate
from hptl.valuation.metals_institutional_drivers import DriverBundle, build_driver_bundle
from hptl.valuation.metals_valuation_v1 import (
    METALS_MARKETS,
    MIN_WEEKS,
    _bias_from_deviation,
    _multivariate_ols,
    _predict_log_price,
    is_metals_valuation_market,
)

VALUATION_PHASE = "V4B Metals Institutional"
METALS_PILLAR = "metals_institutional_fair_value_v1"
CONFIG_PATH = PROJECT_ROOT / "data" / "config" / "metals_institutional_sources.json"


@dataclass(frozen=True)
class MetalModelSpec:
    market: str
    model_name: str
    feature_names: tuple[str, ...]
    sign_expectations: dict[str, str]  # negative | positive | any


def _load_model_specs() -> dict[str, MetalModelSpec]:
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8")) if CONFIG_PATH.exists() else {}
    models = cfg.get("models") or {}

    specs: dict[str, MetalModelSpec] = {}

    gold = models.get("Gold") or {}
    specs["Gold"] = MetalModelSpec(
        market="Gold",
        model_name=str(gold.get("model_name") or "gold_institutional_fair_value_v1"),
        feature_names=("real_yield", "log_dxy", "cb_net_purchases", "etf_holdings"),
        sign_expectations={
            "real_yield": "negative",
            "log_dxy": "negative",
            "cb_net_purchases": "positive",
            "etf_holdings": "positive",
        },
    )

    silver = models.get("Silver") or {}
    specs["Silver"] = MetalModelSpec(
        market="Silver",
        model_name=str(silver.get("model_name") or "silver_institutional_fair_value_v1"),
        feature_names=(
            "log_gold_silver_ratio",
            "real_yield",
            "log_dxy",
            "etf_holdings",
            "industrial_demand_proxy",
        ),
        sign_expectations={
            "log_gold_silver_ratio": "negative",
            "real_yield": "negative",
            "log_dxy": "negative",
            "etf_holdings": "positive",
            "industrial_demand_proxy": "positive",
        },
    )

    copper = models.get("Copper / HG") or {}
    specs["Copper / HG"] = MetalModelSpec(
        market="Copper / HG",
        model_name=str(copper.get("model_name") or "copper_institutional_fair_value_v1"),
        feature_names=("china_pmi", "lme_inventory", "log_dxy", "real_yield"),
        sign_expectations={
            "china_pmi": "positive",
            "lme_inventory": "negative",
            "log_dxy": "negative",
            "real_yield": "negative",
        },
    )

    plat = models.get("Platinum") or {}
    specs["Platinum"] = MetalModelSpec(
        market="Platinum",
        model_name=str(plat.get("model_name") or "platinum_institutional_fair_value_v1"),
        feature_names=("autocat_demand_proxy", "log_dxy", "real_yield", "log_pt_pd_ratio"),
        sign_expectations={
            "autocat_demand_proxy": "positive",
            "log_dxy": "negative",
            "real_yield": "negative",
            "log_pt_pd_ratio": "positive",
        },
    )

    pall = models.get("Palladium") or {}
    specs["Palladium"] = MetalModelSpec(
        market="Palladium",
        model_name=str(pall.get("model_name") or "palladium_institutional_fair_value_v1"),
        feature_names=("autocat_demand_proxy", "log_pt_pd_ratio", "real_yield", "log_dxy"),
        sign_expectations={
            "autocat_demand_proxy": "positive",
            "log_pt_pd_ratio": "negative",
            "real_yield": "negative",
            "log_dxy": "negative",
        },
    )
    return specs


MODEL_SPECS = _load_model_specs()


def _resolve_features(bundle: DriverBundle, spec: MetalModelSpec) -> tuple[list[str], list[list[float]]] | None:
    """Pick available feature columns; prefer etf_holdings over etf_flows for Gold/Silver."""
    names: list[str] = []
    cols: list[list[float]] = []
    n = bundle.n
    for fname in spec.feature_names:
        col = bundle.features.get(fname)
        if col is None and fname == "etf_holdings":
            col = bundle.features.get("etf_flows")
        if col is None or len(col) != n:
            return None
        names.append(fname)
        cols.append(col)
    return names, cols


def _fit_model(
    bundle: DriverBundle,
    spec: MetalModelSpec,
) -> dict[str, Any] | None:
    resolved = _resolve_features(bundle, spec)
    if not resolved:
        return None
    feature_names, x_cols = resolved
    n = bundle.n
    if n < MIN_WEEKS:
        return None

    y = [math.log(p) for p in bundle.price]
    beta, r2 = _multivariate_ols(y, x_cols)
    if not beta or r2 is None:
        return None

    latest_feats = [col[-1] for col in x_cols]
    log_fair = _predict_log_price(beta, latest_feats)
    if log_fair is None:
        return None
    fair = math.exp(log_fair)
    spot = bundle.price[-1]
    dev_pct = round(100.0 * (spot - fair) / fair, 2) if fair > 0 else None

    series: list[dict[str, Any]] = []
    for i in range(n):
        feats_i = [col[i] for col in x_cols]
        lp = _predict_log_price(beta, feats_i)
        if lp is None:
            continue
        f = math.exp(lp)
        if f <= 0:
            continue
        series.append(
            {"date": bundle.dates[i], "deviation_pct": round(100.0 * (bundle.price[i] - f) / f, 2)}
        )

    intercept = beta[0]
    feat_contrib = sum(abs(b * f) for b, f in zip(beta[1:], latest_feats))
    intercept_dominance = abs(intercept) / max(feat_contrib, 1e-9)

    breakdown_steps: list[dict[str, Any]] = [
        {"step": 1, "description": "log(spot)", "value": round(math.log(spot), 6)},
        {"step": 2, "description": "Intercept β₀", "value": round(intercept, 6)},
    ]
    log_fair_check = intercept
    for i, (fname, b, x) in enumerate(zip(feature_names, beta[1:], latest_feats), start=3):
        contrib = b * x
        log_fair_check += contrib
        breakdown_steps.append(
            {"step": i, "description": f"β·{fname} ({round(b, 6)} × {round(x, 4)})", "value": round(contrib, 6)}
        )
    breakdown_steps.append({"step": len(breakdown_steps) + 1, "description": "log(fair) sum", "value": round(log_fair_check, 6)})
    breakdown_steps.append({"step": len(breakdown_steps) + 1, "description": "fair = exp(log fair)", "value": round(fair, 4)})
    breakdown_steps.append(
        {
            "step": len(breakdown_steps) + 1,
            "description": "deviation % = (spot − fair) / fair × 100",
            "value": dev_pct,
        }
    )
    reconcile_ok = abs(log_fair_check - log_fair) < 1e-4 and abs(math.exp(log_fair_check) - fair) < 0.01

    return {
        "model_name": spec.model_name,
        "feature_names": feature_names,
        "n_obs": n,
        "r_squared": round(r2, 4),
        "intercept": round(intercept, 6),
        "beta": {name: round(beta[i + 1], 6) for i, name in enumerate(feature_names)},
        "sign_expectations": spec.sign_expectations,
        "fair_value": round(fair, 4),
        "spot_price": round(spot, 4),
        "deviation_pct": dev_pct,
        "as_of_date": bundle.as_of,
        "reversion_series": series,
        "intercept_dominance_ratio": round(intercept_dominance, 2),
        "calculation_breakdown": breakdown_steps,
        "breakdown_reconciles": reconcile_ok,
        "drivers_snapshot": {fname: round(latest_feats[i], 4) for i, fname in enumerate(feature_names)},
    }


def compute_metals_institutional_valuation(
    *,
    market: str,
    as_of_week: str | None = None,
) -> dict[str, Any]:
    """Metal-specific fair value with Phase 4B gates — never falls back to metals_real_yield_v1."""
    base: dict[str, Any] = {
        "market": market,
        "as_of_week": as_of_week,
        "asset_class": "metals",
        "wired": False,
        "publish": False,
        "publish_gate": False,
        "valuation_state": BIAS_UNAVAILABLE,
        "valuation_bias": BIAS_UNAVAILABLE,
        "valuation_score": None,
        "fair_value": None,
        "deviation_pct": None,
        "spot_price": None,
        "model_id": None,
        "valuation_phase": VALUATION_PHASE,
        "valuation_pillar": METALS_PILLAR,
        "pass": False,
        "blocker_reason": None,
        "missing_inputs": [],
    }

    if not is_metals_valuation_market(market):
        base["blocker_reason"] = f"{market} is not a metals valuation market."
        base["valuation_reason"] = f"WITHHELD — {base['blocker_reason']}"
        return base

    spec = MODEL_SPECS.get(market)
    if not spec:
        base["blocker_reason"] = "No metal-specific model spec defined."
        base["valuation_reason"] = f"WITHHELD — {base['blocker_reason']}"
        return base

    base["model_id"] = spec.model_name
    bundle = build_driver_bundle(market, as_of_week=as_of_week)

    if bundle.missing_required:
        reason = f"Required drivers unavailable: {', '.join(sorted(set(bundle.missing_required)))}"
        base["missing_inputs"] = sorted(set(bundle.missing_required))
        base["blocker_reason"] = reason
        base["valuation_reason"] = f"WITHHELD — {reason}"
        base["model_status"] = "DATA_MISSING"
        base["data_depth"] = bundle.n
        return base

    if bundle.stale:
        reason = f"Stale driver inputs: {', '.join(bundle.stale)}"
        base["stale_inputs"] = bundle.stale
        base["blocker_reason"] = reason
        base["valuation_reason"] = f"WITHHELD — {reason}"
        base["model_status"] = "DATA_STALE"
        base["data_depth"] = bundle.n
        return base

    fit = _fit_model(bundle, spec)
    if not fit:
        reason = "Metal-specific regression failed — insufficient aligned features or singular design."
        base["blocker_reason"] = reason
        base["valuation_reason"] = f"WITHHELD — {reason}"
        base["model_status"] = "MODEL_INCOMPLETE"
        base["data_depth"] = bundle.n
        return base

    gated = apply_metals_institutional_publish_gate(
        {
            "fair_value": fit["fair_value"],
            "deviation_pct": fit["deviation_pct"],
            "spot_price": fit["spot_price"],
            "model_id": spec.model_name,
            "model_name": spec.model_name,
            "regression": {
                "n": fit["n_obs"],
                "r_squared": fit["r_squared"],
                "intercept": fit["intercept"],
                "features": fit["beta"],
            },
            "sign_expectations": fit["sign_expectations"],
            "intercept_dominance_ratio": fit["intercept_dominance_ratio"],
            "breakdown_reconciles": fit["breakdown_reconciles"],
            "calculation_breakdown": fit["calculation_breakdown"],
        },
        reversion_series=fit["reversion_series"],
        market=market,
    )

    dev = fit["deviation_pct"]
    bias = _bias_from_deviation(dev)
    model_note = (
        f"{spec.model_name}: log(price) ~ {', '.join(fit['feature_names'])} "
        f"(R²={fit['r_squared']}, n={fit['n_obs']})"
    )

    base.update(
        {
            "fair_value": fit["fair_value"],
            "deviation_pct": dev,
            "spot_price": fit["spot_price"],
            "model_note": model_note,
            "regression": gated.get("regression") or {
                "n": fit["n_obs"],
                "r_squared": fit["r_squared"],
                "intercept": fit["intercept"],
                "features": fit["beta"],
            },
            "drivers": fit["drivers_snapshot"],
            "input_freshness": {"price_as_of": fit["as_of_date"], "inputs_fresh": not bundle.stale},
            "source_lineage": [v for v in bundle.lineage.values()],
            "calculation_breakdown": fit["calculation_breakdown"],
            "institutional_audit": gated.get("institutional_audit"),
            "wired": gated.get("wired", False),
            "publish": gated.get("publish", False),
            "publish_gate": gated.get("publish", False),
            "blocker_reason": gated.get("blocker_reason") or gated.get("withheld_reason"),
            "withheld_reason": gated.get("withheld_reason"),
            "model_status": gated.get("model_status", "WITHHELD"),
            "pass": gated.get("pass", False),
            "valuation_reason": gated.get("valuation_reason") or model_note,
            "valuation_state": bias if gated.get("publish") else BIAS_UNAVAILABLE,
            "valuation_bias": bias if gated.get("publish") else BIAS_UNAVAILABLE,
            "data_depth": fit["n_obs"],
        }
    )
    base["driver_summary"] = model_note
    return base


def build_all_metals_institutional_valuations(*, as_of_week: str | None = None) -> dict[str, Any]:
    instruments: dict[str, Any] = {}
    wired = published = 0
    for market in METALS_MARKETS:
        val = compute_metals_institutional_valuation(market=market, as_of_week=as_of_week)
        instruments[market] = val
        if val.get("wired"):
            wired += 1
        if val.get("publish"):
            published += 1
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "engine": METALS_PILLAR,
        "valuation_phase": VALUATION_PHASE,
        "summary": {
            "total_instruments": len(METALS_MARKETS),
            "wired_count": wired,
            "published_count": published,
            "unavailable_count": len(METALS_MARKETS) - wired,
        },
        "instruments": instruments,
    }


# Back-compat alias for engine router
compute_metals_valuation_v2 = compute_metals_institutional_valuation
