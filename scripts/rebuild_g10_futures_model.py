"""Rebuild G10 currency futures IVE models — Phase 2 candidate selection (DX-style process)."""
from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np

from hptl.config import DATA_DIR
from hptl.fx.fx_macro_history import currency_histories
from hptl.valuation.currency_futures_ive_v1 import (
    FUTURES_REGISTRY,
    MIN_R_SQUARED,
    _load_futures_daily,
    _ols_log_futures,
    _rate_diff,
)
from hptl.valuation.series_asof import value_as_of

AUDIT_JSON = DATA_DIR / "audits/g10_futures_model_rebuild.json"
AUDIT_MD = DATA_DIR / "audits/g10_futures_model_rebuild.md"

G10_CANDIDATES: dict[str, tuple[str, ...]] = {
    "A_y2_diff": ("y2_diff",),
    "B_policy_diff": ("policy_diff",),
    "C_y2_plus_policy": ("y2_diff", "policy_diff"),
}

EXPECTED_SIGN: dict[str, str] = {
    "y2_diff": "positive",
    "policy_diff": "positive",
}


def _build_panel(
    symbol: str,
    features: tuple[str, ...],
    futures_daily: list[dict[str, Any]],
    histories: dict[str, Any],
) -> list[dict[str, Any]]:
    spec = FUTURES_REGISTRY[symbol]
    leg = spec.currency
    leg_y2 = dict((histories.get(leg) or {}).get("y2") or {})
    leg_pol = dict((histories.get(leg) or {}).get("policy") or {})
    usd_y2 = dict((histories.get("USD") or {}).get("y2") or {})
    usd_pol = dict((histories.get("USD") or {}).get("policy") or {})

    panel: list[dict[str, Any]] = []
    for row in futures_daily:
        d = row["date"]
        rec: dict[str, Any] = {"date": d, "close": row["close"]}
        if "y2_diff" in features:
            ly2 = value_as_of(leg_y2, d)
            uy2 = value_as_of(usd_y2, d)
            if ly2 is None or uy2 is None:
                continue
            y2d = _rate_diff(ly2, uy2, usd_quoted=spec.usd_quoted)
            if y2d is None:
                continue
            rec["y2_diff"] = y2d
        if "policy_diff" in features:
            lp = value_as_of(leg_pol, d)
            up = value_as_of(usd_pol, d)
            if lp is None or up is None:
                continue
            pd = _rate_diff(lp, up, usd_quoted=spec.usd_quoted)
            if pd is None:
                continue
            rec["policy_diff"] = pd
        panel.append(rec)
    return panel


def _sign_ok(features: tuple[str, ...], coef: dict[str, float]) -> bool:
    for f in features:
        exp = EXPECTED_SIGN.get(f)
        val = coef.get(f)
        if val is None or exp is None:
            continue
        if exp == "positive" and val <= 0:
            return False
        if exp == "negative" and val >= 0:
            return False
    return True


def _metrics(panel: list[dict[str, Any]], reg: dict[str, Any], features: tuple[str, ...]) -> dict[str, float]:
    coef = reg.get("coefficients") or {}
    y_log: list[float] = []
    pred_log: list[float] = []
    dx: list[float] = []
    for row in panel:
        c = row.get("close")
        if c is None or c <= 0:
            continue
        log_fv = float(coef.get("intercept", 0))
        ok = True
        for f in features:
            v = row.get(f)
            b = coef.get(f)
            if v is None or b is None:
                ok = False
                break
            log_fv += float(b) * float(v)
        if not ok:
            continue
        y_log.append(math.log(float(c)))
        pred_log.append(log_fv)
        dx.append(float(c))
    y = np.array(y_log)
    p = np.array(pred_log)
    d = np.array(dx)
    ss_res = float(((y - p) ** 2).sum())
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    err = d - np.exp(p)
    return {
        "r_squared": round(r2, 4),
        "mae_price": round(float(np.mean(np.abs(err))), 6),
        "rmse_price": round(float(np.sqrt(np.mean(err**2))), 6),
    }


def _evaluate(symbol: str, name: str, features: tuple[str, ...], panel: list[dict[str, Any]]) -> dict[str, Any]:
    reg = _ols_log_futures(panel, features)
    coef = reg.get("coefficients") or {}
    m = _metrics(panel, reg, features)
    rejected: list[str] = []
    if not reg.get("ok"):
        rejected.append("insufficient_panel")
    if m["r_squared"] < MIN_R_SQUARED:
        rejected.append(f"r_squared_below_gate ({m['r_squared']})")
    if not _sign_ok(features, coef):
        rejected.append("coefficient_sign_violates_economic_logic")
    verdict = "PRODUCTION_READY" if not rejected else "REJECTED"
    return {
        "symbol": symbol,
        "candidate": name,
        "features": list(features),
        "coefficients": coef,
        "n": reg.get("n"),
        **m,
        "rejected": rejected,
        "verdict": verdict,
    }


def rebuild_symbol(symbol: str) -> dict[str, Any]:
    spec = FUTURES_REGISTRY[symbol]
    histories = currency_histories()
    futures_daily, _ = _load_futures_daily(spec.instrument_id)
    results = []
    for name, features in G10_CANDIDATES.items():
        panel = _build_panel(symbol, features, futures_daily, histories)
        results.append(_evaluate(symbol, name, features, panel))
    passing = [r for r in results if r["verdict"] == "PRODUCTION_READY"]
    selected = max(passing, key=lambda r: r["r_squared"]) if passing else None
    return {
        "symbol": symbol,
        "instrument": spec.instrument_id,
        "candidates": results,
        "selected": selected,
    }


def main() -> int:
    symbols = ("6E", "6B")
    report = {
        "phase": "Phase 2 G10 futures model rebuild",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "symbols": {sym: rebuild_symbol(sym) for sym in symbols},
    }
    AUDIT_JSON.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")

    lines = ["# G10 Futures Model Rebuild\n"]
    for sym in symbols:
        block = report["symbols"][sym]
        sel = block.get("selected")
        lines.append(f"## {sym}\n")
        if sel:
            lines.append(f"**Selected:** {sel['candidate']} — R²={sel['r_squared']} features={sel['features']}\n")
        else:
            lines.append("**Selected:** none (all candidates rejected)\n")
        for c in block["candidates"]:
            lines.append(f"- {c['candidate']}: R²={c['r_squared']} verdict={c['verdict']} rejected={c['rejected']}")
        lines.append("")
    AUDIT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"\nWrote {AUDIT_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
