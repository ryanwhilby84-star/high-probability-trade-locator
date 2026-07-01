"""Phase 1F — DX model rebuild: compare candidates A–D and record selection."""
from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd

from hptl.config import DATA_DIR
from hptl.fx.fx_macro_history import currency_histories
from hptl.valuation.currency_futures_ive_v1 import (
    FUTURES_REGISTRY,
    _build_dx_panel,
    _load_futures_daily,
    _ols_log_futures,
)

AUDIT_JSON = DATA_DIR / "audits/dx_model_rebuild_selection.json"
AUDIT_MD = DATA_DIR / "audits/dx_model_rebuild_selection.md"

CANDIDATES: dict[str, tuple[str, ...]] = {
    "A_fed_funds_only": ("fed_funds",),
    "B_real_yield_10y_only": ("real_yield_10y",),
    "C_fed_funds_plus_real_yield": ("fed_funds", "real_yield_10y"),
    "D_orthogonal_g10_plus_fed_funds": ("g10_orthogonal", "fed_funds"),
}

EXPECTED_SIGN: dict[str, str] = {
    "fed_funds": "positive",
    "real_yield_10y": "positive",
    "g10_orthogonal": "negative",
    "avg_g10_2y_vs_usd": "negative",
}


def _panel_with_orthogonal(panel: list[dict[str, Any]]) -> list[dict[str, Any]]:
    df = pd.DataFrame(panel)
    if df.empty:
        return panel
    X = df[["fed_funds"]].assign(intercept=1.0).values
    y = df["avg_g10_2y_vs_usd"].values
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ coef
    df["g10_orthogonal"] = resid
    return df.to_dict("records")


def _metrics(panel: list[dict[str, Any]], reg: dict[str, Any], features: tuple[str, ...]) -> dict[str, float]:
    coef = reg.get("coefficients") or {}
    y_log: list[float] = []
    pred_log: list[float] = []
    dx: list[float] = []
    for row in panel:
        c = row.get("close")
        if c is None or c <= 0:
            continue
        drivers = {f: row.get(f) for f in features}
        log_fv = float(coef.get("intercept", 0))
        ok = True
        for f in features:
            v = drivers.get(f)
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
    pred_price = np.exp(p)
    err = d - pred_price
    return {
        "r_squared": round(r2, 4),
        "mae_price": round(float(np.mean(np.abs(err))), 4),
        "rmse_price": round(float(np.sqrt(np.mean(err**2))), 4),
    }


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


def _window_stability(
    panel: list[dict[str, Any]], features: tuple[str, ...], end_date: str
) -> dict[str, Any]:
    end = datetime.strptime(end_date[:10], "%Y-%m-%d").date()
    windows = {
        "3_year": end - timedelta(days=int(3 * 365.25)),
        "5_year": end - timedelta(days=int(5 * 365.25)),
        "full_sample": date.min,
    }
    out: dict[str, Any] = {}
    for label, start in windows.items():
        sub = [r for r in panel if datetime.strptime(r["date"][:10], "%Y-%m-%d").date() >= start]
        reg = _ols_log_futures(sub, features)
        coef = reg.get("coefficients") or {}
        out[label] = {
            "n": reg.get("n"),
            "coefficients": coef,
            "r_squared": reg.get("r_squared"),
            "signs_ok": _sign_ok(features, coef),
            **_metrics(sub, reg, features),
        }
    signs_full = out["full_sample"]["signs_ok"]
    signs_3y = out["3_year"]["signs_ok"]
    signs_5y = out["5_year"]["signs_ok"]
    flip = False
    for f in features:
        vals = [
            (out[w]["coefficients"] or {}).get(f)
            for w in ("full_sample", "3_year", "5_year")
            if (out[w]["coefficients"] or {}).get(f) is not None
        ]
        if len(vals) >= 2 and any(v > 0 for v in vals) and any(v < 0 for v in vals):
            flip = True
    out["sign_flip_across_windows"] = flip
    out["all_windows_signs_ok"] = signs_full and signs_3y and signs_5y
    return out


def _evaluate_candidate(name: str, features: tuple[str, ...], panel: list[dict[str, Any]]) -> dict[str, Any]:
    reg = _ols_log_futures(panel, features)
    coef = reg.get("coefficients") or {}
    m = _metrics(panel, reg, features)
    end = panel[-1]["date"] if panel else ""
    stability = _window_stability(panel, features, end)
    rejected: list[str] = []
    if not _sign_ok(features, coef):
        rejected.append("coefficient_sign_violates_economic_logic")
    if stability.get("sign_flip_across_windows"):
        rejected.append("coefficient_sign_flips_across_windows")
    return {
        "candidate": name,
        "features": list(features),
        "coefficients": coef,
        "n": reg.get("n"),
        **m,
        "stability": stability,
        "rejected": rejected,
        "accepted": len(rejected) == 0,
    }


def _select(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    viable = [c for c in candidates if c["accepted"]]
    if not viable:
        viable = sorted(candidates, key=lambda c: len(c["rejected"]))

    best_mae = min(c["mae_price"] for c in viable)
    # Drop materially worse (>5% MAE vs best among viable)
    viable = [c for c in viable if c["mae_price"] <= best_mae * 1.05]

    def rank_key(c: dict[str, Any]) -> tuple:
        flip = 1 if c["stability"].get("sign_flip_across_windows") else 0
        n_feat = len(c["features"])
        return (flip, n_feat, c["mae_price"], -c["r_squared"])

    chosen = sorted(viable, key=rank_key)[0]
    return {
        "selected": chosen["candidate"],
        "selected_features": chosen["features"],
        "selection_rationale": (
            "Ranked: economic sign consistency → no window flips → fewest features → MAE → R². "
            f"Selected {chosen['candidate']} as simplest economically valid model."
        ),
        "all_candidates": candidates,
    }


def build_selection_report() -> dict[str, Any]:
    histories = currency_histories()
    futures_daily, _ = _load_futures_daily(FUTURES_REGISTRY["DX"].instrument_id)
    panel = _build_dx_panel(futures_daily, histories)
    panel = _panel_with_orthogonal(panel)

    candidates = [_evaluate_candidate(name, feats, panel) for name, feats in CANDIDATES.items()]
    selection = _select(candidates)

    return {
        "phase": "1F DX Model Rebuild Selection",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sample_n": len(panel),
        **selection,
    }


def _render_md(doc: dict[str, Any]) -> str:
    lines = [
        "# Phase 1F — DX Model Rebuild Selection",
        "",
        f"Generated: {doc['generated_at']}",
        f"**Selected: {doc['selected']}**",
        "",
        doc["selection_rationale"],
        "",
        "## Candidate Comparison",
        "",
        "| Model | Features | R² | MAE | RMSE | Signs OK | Flip | Rejected |",
        "|-------|----------|-----|-----|------|----------|------|----------|",
    ]
    for c in doc["all_candidates"]:
        st = c["stability"]
        lines.append(
            f"| {c['candidate']} | {', '.join(c['features'])} | {c['r_squared']} | {c['mae_price']} | "
            f"{c['rmse_price']} | {st['all_windows_signs_ok']} | {st['sign_flip_across_windows']} | "
            f"{', '.join(c['rejected']) or '—'} |"
        )
    sel = next(c for c in doc["all_candidates"] if c["candidate"] == doc["selected"])
    lines.extend(["", "## Selected Model Coefficients", ""])
    for k, v in (sel.get("coefficients") or {}).items():
        lines.append(f"- **{k}:** {v}")
    return "\n".join(lines) + "\n"


def main() -> int:
    doc = build_selection_report()
    AUDIT_JSON.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_JSON.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    AUDIT_MD.write_text(_render_md(doc), encoding="utf-8")
    print(f"Selected: {doc['selected']}")
    print(f"Wrote {AUDIT_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
