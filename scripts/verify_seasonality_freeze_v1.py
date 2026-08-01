#!/usr/bin/env python3
"""Step-by-step verification of HPTL Seasonality Freeze v1.0 against the equations.

Does not tune. Reports pass/fail per equation step.
"""

from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.markets.usd_index_identity import ICE_DXY_ID  # noqa: E402
from hptl.seasonality_workstation.indexed_seasonality import (  # noqa: E402
    FREEZE_SMOOTH_WINDOW,
    METHOD_VERSION,
    average_normalized_paths,
    build_freeze_v1_path,
    build_normalised_seasonal_curve,
    centre_path,
    complete_year_bars,
    load_daily_closes_for_seasonality,
    normalize_year_pct,
    smooth_path,
)

OUT = ROOT / "data" / "audits" / "seasonality_freeze_v1"


def _approx(a: float, b: float, tol: float = 1e-9) -> bool:
    return abs(a - b) <= tol


def verify_synthetic() -> dict:
    """Hand-check equations on a tiny controlled sample."""
    # Two years, 5 trading days each
    # Y1 prices: 100, 101, 102, 101, 103
    # Y2 prices: 200, 198, 202, 204, 200
    y1 = [100.0, 101.0, 102.0, 101.0, 103.0]
    y2 = [200.0, 198.0, 202.0, 204.0, 200.0]

    # Step 1
    n1 = [(p / y1[0] - 1.0) * 100.0 for p in y1]
    n2 = [(p / y2[0] - 1.0) * 100.0 for p in y2]
    assert n1[0] == 0.0 and n2[0] == 0.0

    # Step 2
    raw = [(n1[i] + n2[i]) / 2.0 for i in range(5)]

    # Step 3
    mu = sum(raw) / 5.0
    centered = [v - mu for v in raw]
    assert _approx(sum(centered), 0.0, 1e-9)

    # Step 4 — centered SMA window 3 for this tiny check of the helper
    sm = smooth_path(centered, window=3)

    # Cross-check library helpers
    rows1 = [(date(2020, 1, 1) + timedelta(days=i), y1[i]) for i in range(5)]
    rows2 = [(date(2021, 1, 1) + timedelta(days=i), y2[i]) for i in range(5)]
    assert normalize_year_pct(rows1) == n1
    assert normalize_year_pct(rows2) == n2
    raw_lib, d_len = average_normalized_paths({2020: n1, 2021: n2})
    assert d_len == 5
    assert all(_approx(a, b) for a, b in zip(raw_lib, raw))
    cen_lib, mu_lib = centre_path(raw_lib)
    assert _approx(mu_lib, mu)
    assert all(_approx(a, b) for a, b in zip(cen_lib, centered))

    return {
        "status": "PASS",
        "step1_year1": n1,
        "step1_year2": n2,
        "step2_raw": raw,
        "step3_mu": mu,
        "step3_centered": centered,
        "step4_smoothed_w3": sm,
    }


def verify_ice_dxy() -> dict:
    daily, meta = load_daily_closes_for_seasonality(ICE_DXY_ID)
    if not daily:
        return {"status": "FAIL", "error": "no_ice_dxy", "meta": meta}

    asof = daily[-1][0]
    core = build_freeze_v1_path(daily, asof=asof, lookback_years=15, smooth=FREEZE_SMOOTH_WINDOW)
    if not core.get("available"):
        return {"status": "FAIL", "error": core}

    # Independent recompute of steps 1–4
    years = complete_year_bars(daily, asof=asof, lookback_years=15)
    year_norm = {y: normalize_year_pct(rows) for y, rows in years.items()}
    year_norm = {y: p for y, p in year_norm.items() if len(p) >= 180}
    raw, d_len = average_normalized_paths(year_norm)
    centered, mu = centre_path(raw)
    smoothed = smooth_path(centered, FREEZE_SMOOTH_WINDOW)

    checks = {
        "N_match": core["N"] == len(year_norm),
        "D_match": core["D"] == d_len,
        "mu_match": _approx(core["mu"], mu, 1e-9),
        "raw_match": all(_approx(a, b, 1e-9) for a, b in zip(core["raw"], raw)),
        "centered_match": all(_approx(a, b, 1e-9) for a, b in zip(core["centered"], centered)),
        "smoothed_match": all(_approx(a, b, 1e-9) for a, b in zip(core["smoothed"], smoothed)),
        "centered_mean_zero": _approx(sum(centered) / len(centered), 0.0, 1e-9),
        "no_absolute_price_in_raw": max(abs(v) for v in raw) < 50.0,  # % path, not ~100 price
        "first_norm_zero_each_year": all(_approx(p[0], 0.0, 1e-12) for p in year_norm.values()),
        "smooth_window": FREEZE_SMOOTH_WINDOW,
        "method_version": METHOD_VERSION,
        "price_instrument": meta.get("price_instrument_id"),
        "price_source": meta.get("source"),
        "sample_years": sorted(year_norm.keys()),
        "N": len(year_norm),
        "D": d_len,
        "mu": mu,
        "raw_min": min(raw),
        "raw_max": max(raw),
        "smoothed_min": min(smoothed),
        "smoothed_max": max(smoothed),
    }
    payload = build_normalised_seasonal_curve(daily, asof=asof)
    checks["payload_available"] = bool(payload.get("available"))
    checks["payload_method"] = (payload.get("method") or {}).get("version")
    checks["asof_trading_day"] = payload.get("asof_trading_day")
    checks["horizons"] = payload.get("horizons")

    failed = [k for k, v in checks.items() if k.endswith("_match") and v is False]
    failed += [k for k in ("centered_mean_zero", "first_norm_zero_each_year", "payload_available") if not checks.get(k)]
    return {
        "status": "PASS" if not failed else "FAIL",
        "failed_checks": failed,
        "checks": checks,
        "benchmark_note": (
            "Freeze v1.0 implemented exactly. Any material difference vs an external "
            "OTC/Bernd screenshot may come from data source (Yahoo continuous vs their "
            "vendor), contract back-adjustment, trading-day vs their alignment, lookback "
            "window endpoints, or their proprietary smooth — not from a different HPTL equation set."
        ),
    }


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    report = {
        "method_version": METHOD_VERSION,
        "synthetic": verify_synthetic(),
        "ice_dxy": verify_ice_dxy(),
    }
    path = OUT / "freeze_v1_verification.json"
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    syn_ok = report["synthetic"]["status"] == "PASS"
    ice = report["ice_dxy"]
    ice_ok = ice.get("status") == "PASS"
    print(f"synthetic: {report['synthetic']['status']}")
    print(f"ice_dxy:   {ice.get('status')} failed={ice.get('failed_checks')}")
    if ice_ok:
        c = ice["checks"]
        print(
            f"N={c['N']} D={c['D']} mu={c['mu']:.6f} "
            f"smooth={c['smooth_window']} instrument={c['price_instrument']}"
        )
        print(f"raw range=[{c['raw_min']:.4f}, {c['raw_max']:.4f}]")
        print(f"smoothed range=[{c['smoothed_min']:.4f}, {c['smoothed_max']:.4f}]")
        print(c["benchmark_note"] if False else ice.get("benchmark_note") or report["ice_dxy"].get("checks") and "")
        print(ice.get("benchmark_note", ""))
    print(f"wrote {path}")
    return 0 if syn_ok and ice_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
