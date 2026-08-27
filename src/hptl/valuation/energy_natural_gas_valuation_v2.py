"""Natural Gas Institutional Valuation V2 — provisional multi-driver fair value.

Fits a standardised Ridge regression on all six populated institutional drivers.
Seasonality remains informational only and is excluded from fair value.
V1 storage-only baseline is preserved for comparison.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from hptl.valuation.energy_ng_drivers import MARKET, NgDriverBundle, build_ng_driver_bundle
from hptl.valuation.energy_natural_gas_valuation_v1 import (
    compute_natural_gas_valuation as compute_natural_gas_valuation_v1,
)
from hptl.valuation.engine import BIAS_UNAVAILABLE
from hptl.valuation.metals_valuation_v1 import MIN_WEEKS, _bias_from_deviation

MODEL_ID = "energy_natural_gas_v2"
VALUATION_PHASE = "Energy NG V2 — Provisional Multi-Driver"
RIDGE_ALPHA = 10.0  # fixed L2 penalty after standardisation

V2_FEATURES = [
    "storage_surplus_pct",
    "dry_gas_production_level",
    "lng_exports_level",
    "hdd_anomaly",
    "cdd_anomaly",
    "log_dxy",
]

FEATURE_LABELS = {
    "storage_surplus_pct": "Storage surplus/deficit %",
    "dry_gas_production_level": "Dry gas production",
    "lng_exports_level": "LNG exports",
    "hdd_anomaly": "HDD anomaly",
    "cdd_anomaly": "CDD anomaly",
    "log_dxy": "log DXY",
}

CARD_TO_FEATURE = {
    "storage": "storage_surplus_pct",
    "production": "dry_gas_production_level",
    "lng_exports": "lng_exports_level",
    "hdd": "hdd_anomaly",
    "cdd": "cdd_anomaly",
    "dxy": "log_dxy",
}


def _standardize_cols(cols: list[list[float]]) -> tuple[list[list[float]], list[float], list[float]]:
    means: list[float] = []
    stds: list[float] = []
    out: list[list[float]] = []
    for col in cols:
        mu = sum(col) / len(col)
        var = sum((v - mu) ** 2 for v in col) / len(col)
        sd = math.sqrt(var) if var > 1e-18 else 1.0
        means.append(mu)
        stds.append(sd)
        out.append([(v - mu) / sd for v in col])
    return out, means, stds


def _ridge_fit(y: list[float], x_cols: list[list[float]], *, alpha: float) -> tuple[list[float], float]:
    n = len(y)
    k = len(x_cols)
    import numpy as np

    X = np.column_stack([np.ones(n)] + [np.array(c, dtype=float) for c in x_cols])
    yv = np.array(y, dtype=float)
    pen = np.eye(k + 1) * float(alpha)
    pen[0, 0] = 0.0
    beta = np.linalg.solve(X.T @ X + pen, X.T @ yv)
    pred = X @ beta
    ss_res = float(np.sum((yv - pred) ** 2))
    ss_tot = float(np.sum((yv - yv.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return [float(b) for b in beta], float(r2)


def _predict(beta: list[float], feats: list[float]) -> float:
    return float(beta[0] + sum(beta[i + 1] * feats[i] for i in range(len(feats))))


def _scale_position(dev_pct: float | None) -> dict[str, Any]:
    if dev_pct is None or not math.isfinite(dev_pct):
        return {"pct": 50.0, "band": "Fair Value"}
    clamped = max(-30.0, min(30.0, float(dev_pct)))
    pct = 50.0 + (clamped / 30.0) * 50.0
    if clamped <= -15:
        band = "Strongly Undervalued"
    elif clamped <= -5:
        band = "Moderately Undervalued"
    elif clamped < 5:
        band = "Fair Value"
    elif clamped < 15:
        band = "Moderately Overvalued"
    else:
        band = "Strongly Overvalued"
    return {"pct": round(pct, 1), "band": band, "deviation_pct": round(clamped, 2)}


def _institutional_bias(dev_pct: float | None, confidence: str) -> str:
    if dev_pct is None:
        return "Unavailable"
    if confidence in {"Low", "None", "Provisional"}:
        if abs(dev_pct) < 5:
            return "Neutral (provisional)"
        return "Tentative — provisional multi-driver model"
    if abs(dev_pct) < 5:
        return "Neutral"
    return "Bullish" if dev_pct < 0 else "Bearish"


def _annotate_v2_cards(cards: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for card in cards.values():
        c = dict(card)
        cid = c.get("id")
        if cid == "seasonality":
            c["valuation_role"] = "INFORMATIONAL ONLY — NOT INCLUDED IN FAIR VALUE"
            c["valuation_badge"] = "INFORMATIONAL ONLY — NOT INCLUDED IN FAIR VALUE"
            c["in_fair_value"] = False
            c["valuation_note"] = "NOT INCLUDED IN FAIR VALUE"
        elif cid in CARD_TO_FEATURE:
            c["valuation_role"] = "INCLUDED IN PROVISIONAL V2 FAIR VALUE"
            c["valuation_badge"] = "INCLUDED IN PROVISIONAL V2 FAIR VALUE"
            c["in_fair_value"] = True
            c["valuation_note"] = "Included in provisional V2 fair value"
        out.append(c)
    return out


def compute_natural_gas_valuation_v2(*, as_of_week: str | None = None) -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    bundle = build_ng_driver_bundle(as_of_week=as_of_week)
    v1 = compute_natural_gas_valuation_v1(as_of_week=as_of_week)
    cards = bundle.driver_cards

    base: dict[str, Any] = {
        "market": MARKET,
        "model_id": MODEL_ID,
        "valuation_phase": VALUATION_PHASE,
        "valuation_pillar": "energy_natural_gas",
        "model_label": "PROVISIONAL MULTI-DRIVER V2",
        "provisional": True,
        "generated_at": generated_at,
        "wired": False,
        "publish": True,
        "fair_value": None,
        "spot_price": bundle.price[-1] if bundle.price else None,
        "deviation_pct": None,
        "valuation_bias": BIAS_UNAVAILABLE,
        "institutional_bias": "Unavailable",
        "confidence": "Provisional",
        "as_of_week": bundle.as_of or as_of_week,
        "driver_cards": _annotate_v2_cards(cards),
        "history": [],
        "awaiting_drivers": [],
        "v1_baseline": {
            "model_id": v1.get("model_id"),
            "fair_value": v1.get("fair_value"),
            "deviation_pct": v1.get("deviation_pct"),
            "confidence": v1.get("confidence"),
            "active_features": v1.get("active_features") or [],
            "note": "V1 storage-only validated baseline retained for comparison",
        },
    }

    missing = [
        f for f in V2_FEATURES if f not in bundle.features or len(bundle.features[f]) != bundle.n
    ]
    if missing or bundle.n < MIN_WEEKS:
        base["summary_text"] = f"V2 panel incomplete; missing={missing}"
        return base

    keep: list[int] = []
    for i in range(bundle.n):
        ok = True
        for f in V2_FEATURES:
            v = bundle.features[f][i]
            if v is None or not math.isfinite(float(v)):
                ok = False
                break
        if ok and bundle.price[i] > 0:
            keep.append(i)
    if len(keep) < MIN_WEEKS:
        base["summary_text"] = "Insufficient complete multi-driver observations for V2."
        return base

    dates = [bundle.dates[i] for i in keep]
    prices = [bundle.price[i] for i in keep]
    raw_cols = [[float(bundle.features[f][i]) for i in keep] for f in V2_FEATURES]
    y = [math.log(p) for p in prices]
    std_cols, means, stds = _standardize_cols(raw_cols)
    beta, r2 = _ridge_fit(y, std_cols, alpha=RIDGE_ALPHA)
    if len(beta) != len(V2_FEATURES) + 1:
        base["summary_text"] = "V2 Ridge fit failed."
        return base

    n = len(y)
    k = len(V2_FEATURES)
    adj = 1.0 - (1.0 - r2) * (n - 1) / (n - k - 1) if n > k + 1 else None

    history = []
    for i in range(n):
        feats = [col[i] for col in std_cols]
        lp = _predict(beta, feats)
        fair_i = math.exp(lp)
        spot_i = prices[i]
        history.append(
            {
                "date": dates[i],
                "spot_price": round(spot_i, 4),
                "fair_value": round(fair_i, 4),
                "deviation_pct": round(100.0 * (spot_i - fair_i) / fair_i, 2),
            }
        )

    latest_raw = [col[-1] for col in raw_cols]
    latest_std = [col[-1] for col in std_cols]
    log_fair = _predict(beta, latest_std)
    fair = math.exp(log_fair)
    spot = prices[-1]

    raw_obs = {
        "storage_surplus_pct": (cards.get("storage") or {}).get("surplus_deficit_pct", latest_raw[0]),
        "dry_gas_production_level": (cards.get("production") or {}).get("current", latest_raw[1]),
        "lng_exports_level": (cards.get("lng_exports") or {}).get("current", latest_raw[2]),
        "hdd_anomaly": (cards.get("hdd") or {}).get("anomaly", latest_raw[3]),
        "cdd_anomaly": (cards.get("cdd") or {}).get("anomaly", latest_raw[4]),
        "log_dxy": (cards.get("dxy") or {}).get("current"),
    }

    rows = []
    log_sum = beta[0]
    for i, name in enumerate(V2_FEATURES):
        log_c = beta[i + 1] * latest_std[i]
        log_sum += log_c
        rows.append(
            {
                "feature": name,
                "label": FEATURE_LABELS.get(name, name),
                "raw_observation": raw_obs.get(name),
                "transformed_input": round(latest_std[i], 6),
                "standardization": {
                    "mean": round(means[i], 6),
                    "std": round(stds[i], 6),
                    "raw_panel_value": round(latest_raw[i], 6),
                },
                "coefficient": round(beta[i + 1], 6),
                "log_contribution": round(log_c, 6),
                "direction": (
                    "raises fair value"
                    if log_c > 0
                    else "lowers fair value"
                    if log_c < 0
                    else "neutral"
                ),
            }
        )
    recon_fair = math.exp(log_sum)
    contrib = {
        "space": "log_price",
        "identity": "log(fair) = intercept + Sigma(beta_i * z_i); fair = exp(log(fair)); z = standardised x",
        "intercept_log_contribution": round(beta[0], 6),
        "drivers": rows,
        "sum_log_contributions": round(log_sum, 6),
        "reconstructed_log_fair": round(log_sum, 6),
        "reconstructed_fair_value": round(recon_fair, 4),
        "reconciliation_ok": abs(recon_fair - fair) < 1e-6,
        "market_price": round(spot, 4),
        "deviation_pct": round(100.0 * (spot - recon_fair) / recon_fair, 2),
        "note": (
            f"PROVISIONAL V2 Ridge (alpha={RIDGE_ALPHA}) on standardised drivers. "
            "Seasonality excluded. Contributions are exact in log-price space."
        ),
    }
    fair = contrib["reconstructed_fair_value"]
    dev_pct = contrib["deviation_pct"]

    conf = "Provisional"
    bias = _bias_from_deviation(dev_pct)
    inst = _institutional_bias(dev_pct, conf)
    direction = (
        "undervalued" if (dev_pct or 0) < 0 else "overvalued" if (dev_pct or 0) > 0 else "near fair value"
    )
    summary = (
        f"PROVISIONAL MULTI-DRIVER V2 implies Natural Gas is approximately "
        f"{abs(dev_pct or 0):.1f}% {direction}. "
        f"All six populated institutional datasets are included via standardised Ridge "
        f"(alpha={RIDGE_ALPHA}). Seasonality is informational only. "
        f"V1 storage-only baseline fair value={v1.get('fair_value')} "
        f"(dev={v1.get('deviation_pct')}%)."
    )

    base.update(
        {
            "wired": True,
            "publish": True,
            "fair_value": fair,
            "spot_price": round(spot, 4),
            "deviation_pct": dev_pct,
            "valuation_bias": bias,
            "valuation_state": bias,
            "institutional_bias": inst,
            "confidence": conf,
            "regression": {
                "method": "ridge_standardised",
                "ridge_alpha": RIDGE_ALPHA,
                "n": n,
                "sample_start": dates[0],
                "sample_end": dates[-1],
                "r_squared": round(r2, 4),
                "adj_r_squared": round(adj, 4) if adj is not None else None,
                "intercept": round(beta[0], 6),
                "features": {V2_FEATURES[i]: round(beta[i + 1], 6) for i in range(k)},
                "feature_means": {V2_FEATURES[i]: round(means[i], 6) for i in range(k)},
                "feature_stds": {V2_FEATURES[i]: round(stds[i], 6) for i in range(k)},
            },
            "active_features": list(V2_FEATURES),
            "validated_features": list(V2_FEATURES),
            "rejected_features": [],
            "experimental_features": [],
            "informational_features": ["seasonality_factor"],
            "history": history,
            "scale": _scale_position(dev_pct),
            "contribution_breakdown": contrib,
            "driver_cards": _annotate_v2_cards(cards),
            "model_note": (
                f"{MODEL_ID}: PROVISIONAL MULTI-DRIVER V2 Ridge alpha={RIDGE_ALPHA} "
                f"n={n} R2={round(r2, 4)} features={V2_FEATURES}"
            ),
            "valuation_reason": (
                f"Provisional V2 FV: spot {round(spot, 4)} vs fair {fair} ({dev_pct:+.2f}%)"
            ),
            "summary_text": summary,
            "seasonality_decision": "INFORMATIONAL ONLY — NOT INCLUDED IN FAIR VALUE",
            "pass": True,
            "source_lineage": list(bundle.lineage.values()),
        }
    )
    return base


def build_natural_gas_valuation_document(*, as_of_week: str | None = None) -> dict[str, Any]:
    block = compute_natural_gas_valuation_v2(as_of_week=as_of_week)
    return {
        "version": 4,
        "generated_at": block.get("generated_at"),
        "engine": MODEL_ID,
        "valuation_phase": VALUATION_PHASE,
        "model_label": "PROVISIONAL MULTI-DRIVER V2",
        "market": MARKET,
        "summary": {
            "wired": bool(block.get("wired")),
            "publish": bool(block.get("publish")),
            "provisional": True,
            "model_label": "PROVISIONAL MULTI-DRIVER V2",
            "validated_features": block.get("active_features") or [],
            "rejected_features": [],
            "experimental_features": [],
            "informational_features": block.get("informational_features") or [],
            "v1_fair_value": (block.get("v1_baseline") or {}).get("fair_value"),
            "v2_fair_value": block.get("fair_value"),
            "awaiting_drivers": block.get("awaiting_drivers") or [],
        },
        "instrument": block,
    }
