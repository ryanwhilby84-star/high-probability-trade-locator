"""Seasonality coverage audit across Opportunity Engine instruments."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.markets.instrument_registry import all_instrument_ids, get_instrument
from hptl.prices.coverage import load_price_coverage, select_price_source
from hptl.seasonality.seasonality_price_bars import record_has_price_bars, resolve_price_record
from hptl.seasonality.seasonality_price_export import PRICES_PATH, block_for_market

CANONICAL_PATH = PROCESSED_DIR / "seasonality_coverage_audit_latest.json"
PUBLIC_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "seasonality_coverage_audit_latest.json"


def _asset_group(market: str) -> str:
    spec = get_instrument(market)
    if not spec:
        return "Other"
    ac = spec.asset_class
    sub = spec.subgroup or ""
    if ac == "fx":
        return "FX currencies"
    if ac == "indices" or "index" in sub:
        return "Indices"
    if ac == "metals":
        return "Metals"
    if ac == "commodities":
        if sub == "energy" or any(x in market.lower() for x in ("oil", "gas", "wti", "brent")):
            return "Energy"
        if sub == "ag" or market in {"Corn", "Wheat", "Soybeans"}:
            return "Grains"
        if sub == "soft" or market in {"Coffee", "Cocoa", "Sugar"}:
            return "Softs"
        return "Energy"
    return "Other"


def _status(
    *,
    available: bool,
    years: int | None,
    reason_code: str | None,
    forward: bool,
) -> str:
    if available and years is not None and years >= 3 and forward:
        return "PASS"
    if available:
        return "WARN"
    if reason_code in {"missing_price_history", "price_fetch_error", "mapping_failure"}:
        return "FAIL"
    return "WARN"


def build_audit(*, markets: list[str] | None = None) -> dict[str, Any]:
    cov = load_price_coverage()
    instruments: dict[str, Any] = {}
    if PRICES_PATH.exists():
        instruments = json.loads(PRICES_PATH.read_text(encoding="utf-8")).get("instruments") or {}

    target = markets or all_instrument_ids()
    rows: dict[str, Any] = {}
    by_group: dict[str, list[str]] = {}

    for market in target:
        group = _asset_group(market)
        by_group.setdefault(group, []).append(market)

        rec, price_key, resolve_fail = resolve_price_record(market, instruments)
        block = block_for_market(market, instruments)
        price_source = select_price_source(market, cov)

        weekly_n = len((rec or {}).get("weekly") or [])
        daily_n = len((rec or {}).get("daily") or [])

        available = bool(block.get("available"))
        years = block.get("years_of_history")
        reason_code = block.get("reason_code") or resolve_fail
        forward = bool(block.get("forward_projection_available"))

        if available and years is None:
            years = block.get("years_of_history", 0)

        missing_why = ""
        if not available:
            if reason_code == "missing_price_history":
                missing_why = "missing price history"
            elif reason_code == "price_fetch_error":
                missing_why = "price fetch error"
            elif reason_code == "mapping_failure":
                missing_why = "mapping failure"
            elif reason_code == "unsupported_instrument":
                missing_why = "unsupported instrument"
            elif reason_code == "insufficient_history":
                missing_why = "insufficient history"
            else:
                missing_why = block.get("reason") or "export unavailable"

        rows[market] = {
            "instrument": market,
            "asset_group": group,
            "seasonality_available": available,
            "years_available": years,
            "price_source": price_source,
            "price_store_key": price_key or block.get("price_store_key"),
            "weekly_bars": weekly_n,
            "daily_bars": daily_n,
            "bar_source": block.get("bar_source"),
            "forward_projection_available": forward,
            "windows_available": block.get("windows_available") or [],
            "status": _status(
                available=available,
                years=years if isinstance(years, int) else None,
                reason_code=reason_code,
                forward=forward,
            ),
            "reason_code": reason_code,
            "missing_reason": missing_why if not available else None,
            "reason": block.get("reason"),
        }

    summary = {
        "instruments_audited": len(rows),
        "seasonality_available": sum(1 for r in rows.values() if r["seasonality_available"]),
        "missing": sum(1 for r in rows.values() if not r["seasonality_available"]),
        "pass": sum(1 for r in rows.values() if r["status"] == "PASS"),
        "warn": sum(1 for r in rows.values() if r["status"] == "WARN"),
        "fail": sum(1 for r in rows.values() if r["status"] == "FAIL"),
        "by_group": {
            g: {
                "total": len(ids),
                "available": sum(1 for i in ids if rows[i]["seasonality_available"]),
                "missing": sum(1 for i in ids if not rows[i]["seasonality_available"]),
            }
            for g, ids in sorted(by_group.items())
        },
    }

    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_prices_file": str(PRICES_PATH),
        "summary": summary,
        "markets": rows,
    }


def write_exports(payload: dict[str, Any] | None = None) -> Path:
    payload = payload or build_audit()
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    CANONICAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    CANONICAL_PATH.write_text(text, encoding="utf-8")
    PUBLIC_PATH.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_PATH.write_text(text, encoding="utf-8")
    return CANONICAL_PATH


def run() -> Path:
    payload = build_audit()
    path = write_exports(payload)
    s = payload["summary"]
    print(
        f"Wrote {path} — available={s['seasonality_available']}/{s['instruments_audited']} "
        f"PASS={s['pass']} WARN={s['warn']} FAIL={s['fail']}"
    )
    return path


if __name__ == "__main__":
    run()
