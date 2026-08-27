"""Quick candidate eval for Phase 1J — not part of production."""
from __future__ import annotations

import json
from dataclasses import replace

from hptl.fx.fx_macro_history import (
    FRED_CHF_Y2_FALLBACK_ID,
    FRED_NZD_Y2_FALLBACK_ID,
    currency_histories,
    ensure_ecb_yield_history_caches,
    load_fred_daily_map,
)
from hptl.valuation.currency_futures_ive_v1 import (
    FUTURES_REGISTRY,
    MIN_R_SQUARED,
    _load_futures_daily,
    _ols_log_futures,
    _rate_diff,
)
from hptl.valuation.series_asof import value_as_of

CANDIDATES = {
    "y2_diff": ("y2_diff",),
    "y10_diff": ("y10_diff",),
    "policy_diff": ("policy_diff",),
    "y2+y10": ("y2_diff", "y10_diff"),
    "y2+policy": ("y2_diff", "policy_diff"),
}


def build_panel_ext(spec, features, fd, hist):
    leg = spec.currency
    leg_y2 = dict((hist.get(leg) or {}).get("y2") or {})
    leg_y10 = dict((hist.get(leg) or {}).get("y10") or {})
    leg_pol = dict((hist.get(leg) or {}).get("policy") or {})
    usd_y2 = dict((hist.get("USD") or {}).get("y2") or {})
    usd_y10 = dict((hist.get("USD") or {}).get("y10") or {})
    usd_pol = dict((hist.get("USD") or {}).get("policy") or {})
    panel = []
    for row in fd:
        d = row["date"]
        rec: dict = {"date": d, "close": row["close"]}
        ok = True
        if "y2_diff" in features:
            ly2, uy2 = value_as_of(leg_y2, d), value_as_of(usd_y2, d)
            if ly2 is None or uy2 is None:
                ok = False
            else:
                rec["y2_diff"] = _rate_diff(ly2, uy2, usd_quoted=spec.usd_quoted)
        if "y10_diff" in features:
            ly10, uy10 = value_as_of(leg_y10, d), value_as_of(usd_y10, d)
            if ly10 is None or uy10 is None:
                ok = False
            else:
                rec["y10_diff"] = _rate_diff(ly10, uy10, usd_quoted=spec.usd_quoted)
        if "policy_diff" in features:
            lp, up = value_as_of(leg_pol, d), value_as_of(usd_pol, d)
            if lp is None or up is None:
                ok = False
            else:
                rec["policy_diff"] = _rate_diff(lp, up, usd_quoted=spec.usd_quoted)
        if ok:
            panel.append(rec)
    return panel


def sign_ok(features, coef):
    for f in features:
        v = coef.get(f)
        if v is None:
            continue
        if v <= 0:
            return False
    return True


def main() -> int:
    ensure_ecb_yield_history_caches()
    hist = currency_histories()

    for sid in (
        FRED_CHF_Y2_FALLBACK_ID,
        FRED_NZD_Y2_FALLBACK_ID,
        "IRLTLT01CHM156N",
        "IRLTLT01NZM156N",
    ):
        m = load_fred_daily_map(sid)
        print(sid, "n=", len(m), "max=", max(m) if m else None)

    report = {}
    for sym in ("6E", "6B", "6S", "6N"):
        spec = FUTURES_REGISTRY[sym]
        fd, _ = _load_futures_daily(spec.instrument_id)
        rows = []
        best = None
        for name, feats in CANDIDATES.items():
            panel = build_panel_ext(spec, feats, fd, hist)
            reg = _ols_log_futures(panel, feats)
            r2 = reg.get("r_squared")
            coef = reg.get("coefficients") or {}
            passed = (
                reg.get("ok")
                and r2 is not None
                and r2 >= MIN_R_SQUARED
                and sign_ok(feats, coef)
            )
            row = {
                "candidate": name,
                "features": list(feats),
                "panel_n": len(panel),
                "reg_n": reg.get("n"),
                "r_squared": r2,
                "signs_ok": sign_ok(feats, coef),
                "passed": passed,
                "coefficients": coef,
            }
            rows.append(row)
            if passed and (best is None or (r2 or 0) > (best["r_squared"] or 0)):
                best = row
        report[sym] = {"candidates": rows, "best": best}
        print(f"=== {sym} === best={best['candidate'] if best else None}")

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
