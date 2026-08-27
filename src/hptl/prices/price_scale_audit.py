"""Price scale audit for chart pipelines (3Y COT vs Price, Seasonality).

Detects unit/contract mapping errors — e.g. Alpha Vantage COPPER returns USD/metric
tonne but COMEX HG charts display USD/lb × 1000 (~6000 area).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.confluence.build_decision_table import TARGET_MARKETS
from hptl.markets.instrument_registry import get_instrument
from hptl.prices.coverage import load_price_coverage, select_price_source

PRICES_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "prices_latest.json"
CANONICAL_PATH = PROCESSED_DIR / "price_scale_audit_latest.json"
PUBLIC_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "price_scale_audit_latest.json"

from hptl.prices.copper_hg_scale import (
    COPPER_HG_INSTRUMENT_ID,
    LB_PER_METRIC_TONNE,
    VALUE_KIND,
    metric_tonne_to_hg_chart,
)

QUARANTINE_MSG = "Price data failed audit — chart disabled until source mapping is fixed."


def _source_symbol(market: str, source: str | None, cov: dict[str, Any]) -> str:
    row = next((r for r in cov.get("instruments") or [] if r.get("htpl_instrument_id") == market), None)
    if not row:
        return ""
    for s in row.get("sources") or []:
        if source == "oanda" and s.get("source") == "oanda" and s.get("coverage_status") == "supported":
            return str(s.get("symbol") or "")
        if source == "alpha_vantage" and s.get("source") == "alpha_vantage" and s.get("coverage_status") == "supported":
            return str(s.get("function") or s.get("symbol") or "")
    return ""


def _audit_instrument(
    market: str,
    rec: dict[str, Any] | None,
    *,
    source: str | None,
    symbol: str,
) -> dict[str, Any]:
    bars = (rec or {}).get("daily") or (rec or {}).get("weekly") or []
    latest_date = str(bars[-1].get("date") or "")[:10] if bars else None
    bar_close = float(bars[-1]["close"]) if bars and bars[-1].get("close") is not None else None
    bar_raw = float(bars[-1]["raw_close"]) if bars and bars[-1].get("raw_close") is not None else None
    price_scale = (rec or {}).get("price_scale") or {}

    raw_close = price_scale.get("raw_close")
    if raw_close is None:
        raw_close = bar_raw if bar_raw is not None else bar_close
    else:
        raw_close = float(raw_close)

    transformed_close = price_scale.get("transformed_close")
    if transformed_close is None:
        transformed_close = bar_close
    elif transformed_close is not None:
        transformed_close = float(transformed_close)

    dashboard_close = transformed_close if transformed_close is not None else bar_close

    row: dict[str, Any] = {
        "market": market,
        "source_file": str(PRICES_PATH),
        "price_source": source,
        "mapped_symbol": symbol,
        "latest_price_date": latest_date,
        "raw_close": raw_close,
        "transformed_close": transformed_close,
        "dashboard_displayed_close": dashboard_close,
        "value_kind": price_scale.get("value_kind") or "raw_close",
        "raw_unit": price_scale.get("raw_unit"),
        "transformed_unit": price_scale.get("transformed_unit"),
        "conversion_factor": price_scale.get("conversion_factor"),
        "multiplier_applied": None,
        "contract_mapping": market,
        "expected_live_approx": None,
        "status": "PASS",
        "chart_quarantined": False,
        "reason": "",
    }

    if dashboard_close is None and raw_close is None:
        row["status"] = "WARN"
        row["reason"] = "No price bars in prices_latest.json."
        return row

    spec = get_instrument(market)

    # --- Copper / HG: transformed AV COPPER -> HG chart scale ---
    if market == COPPER_HG_INSTRUMENT_ID and price_scale.get("value_kind") == VALUE_KIND:
        row["value_kind"] = VALUE_KIND
        row["dashboard_displayed_close"] = dashboard_close
        row["raw_close"] = float(price_scale.get("raw_close") or raw_close or 0)
        row["transformed_close"] = float(price_scale.get("transformed_close") or dashboard_close or 0)
        row["expected_live_approx"] = round(row["transformed_close"], 0)
        if 2000 <= row["transformed_close"] <= 15000:
            row["status"] = "PASS"
            row["chart_quarantined"] = False
            row["reason"] = (
                f"Alpha Vantage COPPER raw {row['raw_close']:.2f} {price_scale.get('raw_unit')} "
                f"transformed to HG chart scale {row['transformed_close']:.2f} "
                f"({price_scale.get('transformed_unit')})."
            )
        else:
            row["status"] = "WARN"
            row["reason"] = f"Transformed close {row['transformed_close']} outside expected HG range."
        return row

    if market == COPPER_HG_INSTRUMENT_ID and source == "alpha_vantage" and symbol == "COPPER":
        hg_equiv = metric_tonne_to_hg_chart(float(raw_close)) if raw_close is not None else None
        usd_per_lb = float(raw_close) / LB_PER_METRIC_TONNE if raw_close is not None else None
        row["value_kind"] = "alpha_vantage_global_usd_per_metric_tonne"
        row["expected_live_approx"] = round(hg_equiv, 0) if hg_equiv is not None else None
        row["dashboard_displayed_close"] = raw_close
        row["transformed_close"] = round(hg_equiv, 2) if hg_equiv is not None else None
        row["usd_per_lb"] = round(usd_per_lb, 4) if usd_per_lb is not None else None
        row["status"] = "FAIL"
        row["chart_quarantined"] = True
        row["reason"] = (
            f"Alpha Vantage COPPER raw USD/metric tonne ({raw_close:.2f}) not transformed. "
            f"Expected HG chart scale ~ {hg_equiv:.0f}."
        )
        return row

    # Standalone Copper (not COT board id) — same AV feed, not transformed by design
    if market == "Copper" and source == "alpha_vantage" and symbol == "COPPER":
        hg_equiv = metric_tonne_to_hg_chart(float(raw_close)) if raw_close is not None else None
        row["value_kind"] = "alpha_vantage_global_usd_per_metric_tonne"
        row["expected_live_approx"] = round(hg_equiv, 0) if hg_equiv is not None else None
        row["transformed_close"] = round(hg_equiv, 2) if hg_equiv is not None else None
        row["status"] = "WARN"
        row["reason"] = "Standalone Copper uses AV USD/metric tonne (not HG-transformed)."
        return row

    # OANDA copper CFD — expected HG-scale area
    if market == COPPER_HG_INSTRUMENT_ID and source == "oanda" and symbol == "XCUUSD":
        row["value_kind"] = "oanda_xcuusd_cfd"
        row["expected_live_approx"] = round(raw_close, 0)
        if raw_close > 15000 or raw_close < 2000:
            row["status"] = "WARN"
            row["reason"] = f"OANDA XCUUSD close {raw_close} outside expected HG chart range (~4000-9000)."
        else:
            row["reason"] = "OANDA XCUUSD — COMEX copper CFD scale."
        return row

    # FX majors — sanity band
    if spec and spec.asset_class == "fx" and "/" in market:
        row["expected_live_approx"] = dashboard_close if dashboard_close is not None else raw_close
        if raw_close <= 0 or raw_close > 500:
            row["status"] = "WARN"
            row["reason"] = f"FX close {raw_close} outside plausible range."
        return row

    # Gold spot
    if market == "Gold" and source == "oanda":
        row["value_kind"] = "oanda_xau_usd_spot"
        row["expected_live_approx"] = round(raw_close, 0)
        row["reason"] = "OANDA XAU/USD spot (USD/oz)."
        return row

    # Natural gas
    if "Natural Gas" in market and raw_close is not None:
        row["expected_live_approx"] = round(raw_close, 2)
        if raw_close > 50:
            row["status"] = "WARN"
            row["reason"] = f"Nat gas close {raw_close} unusually high for $/MMBtu."
        return row

    # Alpha commodity indices — flag if likely wrong scale for COT contract
    if source == "alpha_vantage" and symbol in {"COFFEE", "CORN", "WHEAT", "SUGAR"}:
        row["value_kind"] = f"alpha_vantage_{symbol.lower()}_index"
        row["expected_live_approx"] = dashboard_close if dashboard_close is not None else raw_close
        row["status"] = "WARN"
        row["reason"] = (
            f"Alpha Vantage {symbol} global index — verify units vs CME/COT contract before chart use."
        )
        # Don't quarantine ags unless user confirms — only copper is confirmed FAIL
        row["chart_quarantined"] = False
        return row

    row["expected_live_approx"] = dashboard_close if dashboard_close is not None else raw_close
    row["reason"] = "Within expected pipeline; no scale mismatch detected."
    return row


def build_audit(*, markets: list[str] | None = None) -> dict[str, Any]:
    cov = load_price_coverage()
    px_doc = json.loads(PRICES_PATH.read_text(encoding="utf-8")) if PRICES_PATH.exists() else {}
    instruments = px_doc.get("instruments") or {}

    target = markets or list(TARGET_MARKETS)
    out: dict[str, Any] = {}
    for market in target:
        rec = instruments.get(market)
        src = select_price_source(market, cov)
        sym = _source_symbol(market, src, cov)
        out[market] = _audit_instrument(market, rec, source=src, symbol=sym)

    fails = sum(1 for v in out.values() if v.get("status") == "FAIL")
    warns = sum(1 for v in out.values() if v.get("status") == "WARN")
    quarantined = sum(1 for v in out.values() if v.get("chart_quarantined"))

    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_prices_file": str(PRICES_PATH),
        "quarantine_message": QUARANTINE_MSG,
        "summary": {
            "instruments_audited": len(out),
            "fail": fails,
            "warn": warns,
            "chart_quarantined": quarantined,
        },
        "markets": out,
    }


def write_exports(payload: dict[str, Any] | None = None) -> Path:
    payload = payload or build_audit()
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    CANONICAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    CANONICAL_PATH.write_text(text, encoding="utf-8")
    PUBLIC_PATH.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_PATH.write_text(text, encoding="utf-8")
    return CANONICAL_PATH


def is_chart_quarantined(audit: dict[str, Any] | None, market: str) -> tuple[bool, str | None]:
    if not audit or not market:
        return False, None
    row = (audit.get("markets") or {}).get(market)
    if not row:
        return False, None
    if row.get("chart_quarantined"):
        return True, row.get("reason") or QUARANTINE_MSG
    return False, None


def run() -> Path:
    payload = build_audit()
    path = write_exports(payload)
    s = payload["summary"]
    print(f"Wrote {path} — FAIL={s['fail']} WARN={s['warn']} quarantined={s['chart_quarantined']}")
    copper = payload["markets"].get("Copper / HG")
    if copper:
        print("\n=== Copper / HG ===")
        for k in (
            "source_file", "mapped_symbol", "raw_close", "transformed_close",
            "dashboard_displayed_close", "value_kind", "latest_price_date", "status", "reason",
        ):
            if k in copper:
                print(f"  {k}: {copper[k]}")
    return path


if __name__ == "__main__":
    run()
