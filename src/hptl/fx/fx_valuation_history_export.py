"""FX Valuation History chart export — research / validation only (not scoring).

Usage:
    python -m hptl.fx.fx_valuation_history_export

Writes:
    data/processed/fx_valuation_history_latest.json
    web-dashboard/public/data/fx_valuation_history_latest.json
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.fx.fx_rate_history_loaders import (
    MIN_PANEL_POINTS,
    build_differential_series,
    currency_histories,
)
from hptl.fx.fx_valuation import resolve_pair_currencies, value_fx_pair
from hptl.fx.fx_valuation_attach import _spot_and_percentile
from hptl.fx.fx_valuation_export import DEFAULT_PAIRS
from hptl.prices.fx_daily_backfill import STAGING_DIR, staging_path
from hptl.prices.fx_oanda_backfill_feasibility_audit import TEST_PAIRS
from hptl.seasonality.seasonality_v2 import normalize_daily_bars

CANONICAL_PATH = PROCESSED_DIR / "fx_valuation_history_latest.json"
PUBLIC_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "fx_valuation_history_latest.json"

_DISPLAY_TO_PAIR: dict[str, str] = {
    "EURUSD": "EUR/USD",
    "GBPUSD": "GBP/USD",
    "AUDUSD": "AUD/USD",
    "NZDUSD": "NZD/USD",
    "USDJPY": "USD/JPY",
    "USDCAD": "USD/CAD",
    "USDCHF": "USD/CHF",
    "EURJPY": "EUR/JPY",
}
_PAIR_STORE: dict[str, str] = {
    pair: store for display, _oanda, store in TEST_PAIRS for pair in [_DISPLAY_TO_PAIR[display]]
}

CHART_TITLE = "FX Valuation History — Yield/Rate Differential V1"
CHART_NOTE = (
    "This shows historical macro support versus spot price. "
    "Fair-value regression is not yet modelled."
)

_ECB_BASE = "https://data-api.ecb.europa.eu/service/data"
_AUDIT_ECB_SERIES = {
    "eur_dfr": f"{_ECB_BASE}/FM/B.U2.EUR.4F.KR.DFR.LEV?format=csvdata&lastNObservations=260",
    "eur_2y": f"{_ECB_BASE}/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y?format=csvdata&lastNObservations=260",
    "eur_10y": f"{_ECB_BASE}/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y?format=csvdata&lastNObservations=260",
}
_BOC_RECENT_URL = (
    "https://www.bankofcanada.ca/valet/observations/"
    "BD.CDN.2YR.DQ.YLD,BD.CDN.10YR.DQ.YLD,V39079/json?recent=90"
)
_JGB_URL = "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/jgbcme.csv"
_TREASURY_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
    "daily-treasury-rates.csv/{year}/all"
    "?type=daily_treasury_yield_curve&field_tdr_date_value={year}&page&_format=csv"
)


def _audit_extend_rate_caches() -> None:
    """Deepen adapter caches for research export only (skipped when offline)."""
    from hptl.fx.rate_adapter_base import fetch_text, offline_mode

    if offline_mode():
        return
    for key, url in _AUDIT_ECB_SERIES.items():
        try:
            fetch_text(url, cache_key=key)
        except Exception:
            pass
    try:
        fetch_text(_BOC_RECENT_URL, cache_key="cad_valet")
    except Exception:
        pass
    try:
        fetch_text(_JGB_URL, cache_key="jpy_jgb")
    except Exception:
        pass
    try:
        yr = datetime.now(timezone.utc).year
        fetch_text(_TREASURY_URL.format(year=yr), cache_key="usd_treasury")
    except Exception:
        pass


def _spot_weekly_series(store_key: str) -> list[dict[str, Any]]:
    path = staging_path(store_key)
    if not path.exists():
        return []
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    daily = normalize_daily_bars(doc.get("daily") or [])
    if not daily:
        return []
    # Weekly (Friday or last bar of ISO week) for readable chart density.
    by_week: dict[tuple[int, int], dict[str, Any]] = {}
    for bar in daily:
        d = str(bar.get("date") or "")[:10]
        if not d:
            continue
        try:
            dt = datetime.strptime(d, "%Y-%m-%d").date()
        except ValueError:
            continue
        cal = dt.isocalendar()
        key = (int(cal.year), int(cal.week))
        by_week[key] = {"date": d, "close": float(bar["close"])}
    ordered = sorted(by_week.values(), key=lambda r: r["date"])
    return [{"date": r["date"], "spot": round(r["close"], 6)} for r in ordered]


def _panel_status(series: list[dict[str, Any]], *, label: str) -> dict[str, Any]:
    n = len(series)
    return {
        "label": label,
        "available": n >= MIN_PANEL_POINTS,
        "points": n,
        "series": series,
    }


def build_pair_history(pair_id: str, histories: dict[str, dict[str, Any]]) -> dict[str, Any]:
    resolved = resolve_pair_currencies(pair_id)
    if not resolved:
        return {"pair": pair_id, "available": False, "reason": "Unsupported pair"}
    base, quote, canonical = resolved
    store_key = _PAIR_STORE.get(canonical) or _PAIR_STORE.get(pair_id)
    spot = _spot_weekly_series(store_key) if store_key else []

    y2 = build_differential_series(base, quote, "y2", histories)
    policy = build_differential_series(base, quote, "policy", histories)
    y10 = build_differential_series(base, quote, "y10", histories)

    spot_panel = _panel_status(spot, label="FX spot (staging OANDA weekly)")
    y2_panel = _panel_status(y2, label="2Y yield differential (base − quote)")
    pol_panel = _panel_status(policy, label="Policy rate differential (base − quote)")
    y10_panel = _panel_status(y10, label="10Y yield differential (base − quote)")

    rate_panels_ok = any(p["available"] for p in (y2_panel, pol_panel, y10_panel))
    spot_ok = len(spot) >= MIN_PANEL_POINTS

    spot_val, pctl = _spot_and_percentile(canonical)
    current = value_fx_pair(base, quote, spot=spot_val, price_percentile_52w=pctl).as_block()

    if not spot_ok and not rate_panels_ok:
        return {
            "pair": canonical,
            "available": False,
            "reason": "Historical valuation chart unavailable — insufficient rate/yield history.",
            "spot_points": len(spot),
            "rate_points": max(y2_panel["points"], pol_panel["points"], y10_panel["points"]),
        }

    return {
        "pair": canonical,
        "available": True,
        "audit_only": True,
        "title": CHART_TITLE,
        "note": CHART_NOTE,
        "base": base,
        "quote": quote,
        "staging_store_key": store_key,
        "current": {
            "spot": current.get("spot"),
            "policy_rate_diff": current.get("policy_rate_diff"),
            "yield_2y_diff": current.get("yield_2y_diff"),
            "yield_10y_diff": current.get("yield_10y_diff"),
            "confidence": current.get("confidence"),
            "valuation_model_type": current.get("valuation_model_type"),
        },
        "panels": {
            "spot": spot_panel,
            "yield_2y_diff": y2_panel,
            "policy_rate_diff": pol_panel,
            "yield_10y_diff": y10_panel,
        },
        "rate_history_sufficient": rate_panels_ok,
        "spot_history_sufficient": spot_ok,
    }


def build_payload(pairs: tuple[str, ...] = DEFAULT_PAIRS, *, extend_caches: bool = True) -> dict[str, Any]:
    if extend_caches:
        _audit_extend_rate_caches()
    histories = currency_histories()
    pair_blocks = [build_pair_history(pid, histories) for pid in pairs]
    by_pair = {b["pair"]: b for b in pair_blocks if b.get("pair")}
    sufficient = [p for p in pair_blocks if p.get("available") and p.get("rate_history_sufficient")]
    spot_only = [p for p in pair_blocks if p.get("available") and not p.get("rate_history_sufficient")]
    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parser": "hptl.fx.fx_valuation_history_export",
        "audit_only": True,
        "live_wired": False,
        "title": CHART_TITLE,
        "note": CHART_NOTE,
        "staging_dir": str(STAGING_DIR),
        "rate_cache_dir": str(PROJECT_ROOT / "data" / "cache" / "fx_rates"),
        "summary": {
            "pairs_requested": len(pairs),
            "charts_available": sum(1 for p in pair_blocks if p.get("available")),
            "rate_history_sufficient": len(sufficient),
            "spot_only": len(spot_only),
            "pairs_with_rate_history": [p["pair"] for p in sufficient],
            "pairs_spot_only": [p["pair"] for p in spot_only],
            "pairs_unavailable": [p.get("pair") or "?" for p in pair_blocks if not p.get("available")],
        },
        "pairs": pair_blocks,
        "by_pair": by_pair,
    }


def write_exports(payload: dict[str, Any] | None = None) -> Path:
    payload = payload or build_payload()
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    CANONICAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    CANONICAL_PATH.write_text(text, encoding="utf-8")
    PUBLIC_PATH.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_PATH.write_text(text, encoding="utf-8")
    dist = PROJECT_ROOT / "web-dashboard" / "dist" / "data" / "fx_valuation_history_latest.json"
    if dist.parent.exists():
        dist.write_text(text, encoding="utf-8")
    return CANONICAL_PATH


def run() -> Path:
    path = write_exports()
    s = (json.loads(path.read_text(encoding="utf-8"))).get("summary") or {}
    print(f"Wrote {path}")
    print(
        f"Charts: {s.get('charts_available')}/{s.get('pairs_requested')} | "
        f"rate history OK: {s.get('pairs_with_rate_history')}"
    )
    return path


def main() -> int:
    run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
