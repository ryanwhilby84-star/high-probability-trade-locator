"""Seasonality vs current price chart export (visual audit layer only).

Generic engine: any instrument with weekly (or resampled weekly) price history
gets the same forward-looking seasonality framework.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.markets.instrument_registry import all_instrument_ids
from hptl.prices.coverage import load_price_coverage, select_price_source
from hptl.prices.price_store import load_price_store
from hptl.seasonality.seasonality_engine import compute_seasonality_price_block
from hptl.seasonality.seasonality_outlier_filter import (
    OUTLIER_FILTER_MARKETS,
    filter_weekly_bars_for_seasonality,
)
from hptl.seasonality.seasonality_price_bars import weekly_closes_for_instrument
from hptl.seasonality.seasonality_trust import attach_trust_metadata

CANONICAL_PATH = PROCESSED_DIR / "seasonality_price_latest.json"
PUBLIC_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "seasonality_price_latest.json"

_REASON_LABELS: dict[str, str] = {
    "missing_price_history": "No price bars in prices_latest.json.",
    "price_fetch_error": "Price fetch failed for this instrument.",
    "mapping_failure": "No price store mapping for this instrument.",
    "unsupported_instrument": "Instrument not in registry.",
    "insufficient_history": "Price bars present but insufficient history for seasonality path.",
}

_OUTLIER_FILTER_AUDITS: list[dict[str, Any]] = []


def _log_outlier_filter_audit(audit: dict[str, Any]) -> None:
    _OUTLIER_FILTER_AUDITS.append(audit)
    print(
        f"[OUTLIER FILTER] {audit.get('market')}: "
        f"median={audit.get('median_close')} "
        f"dropped={audit.get('bars_dropped')}/{audit.get('bars_before')} "
        f"indexed {audit.get('max_indexed_before')} -> {audit.get('max_indexed_after')} "
        f"proj {audit.get('max_projection_before')} -> {audit.get('max_projection_after')}",
        flush=True,
    )


def _load_price_instruments() -> tuple[dict[str, dict[str, Any]], str]:
    doc = load_price_store()
    return doc.get("instruments") or {}, str(doc.get("generated_at") or "prices_latest.json")


def _unavailable_block(market: str, *, reason_code: str, detail: str | None = None) -> dict[str, Any]:
    return {
        "market": market,
        "available": False,
        "reason": detail or _REASON_LABELS.get(reason_code, reason_code),
        "reason_code": reason_code,
        "forward_projection_available": False,
    }


def block_for_market(
    market: str,
    instruments: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    bars, bar_source, tl = weekly_closes_for_instrument(market)
    if not tl or not bars:
        rec = instruments.get(market)
        if rec and rec.get("error"):
            return _unavailable_block(
                market,
                reason_code="price_fetch_error",
                detail=f"Price fetch failed: {str(rec.get('error') or '')[:120]}",
            )
        return _unavailable_block(market, reason_code="missing_price_history")

    filter_audit: dict[str, Any] = {}
    seasonality_bars = bars
    if market in OUTLIER_FILTER_MARKETS:
        seasonality_bars, filter_audit = filter_weekly_bars_for_seasonality(market, bars)
        if filter_audit.get("applied"):
            _log_outlier_filter_audit(filter_audit)

    block = compute_seasonality_price_block(
        market,
        seasonality_bars,
        price_store_key=tl.resolved_store_key or market,
        bar_source=bar_source,
        canonical_source=tl.canonical_source,
        canonical_symbol=tl.canonical_symbol,
        price_derivation=bar_source,
        proxy=tl.proxy,
        proxy_explanation=tl.proxy_explanation,
    )
    block = attach_trust_metadata(block, seasonality_bars, filter_audit=filter_audit or None)
    if not block.get("available"):
        block.setdefault("reason_code", "insufficient_history")
        block.setdefault("forward_projection_available", False)
    return block


def build_payload(markets: list[str] | None = None) -> dict[str, Any]:
    global _OUTLIER_FILTER_AUDITS
    _OUTLIER_FILTER_AUDITS = []
    instruments, price_source = _load_price_instruments()
    target = markets or all_instrument_ids()

    out: dict[str, Any] = {}
    for market in target:
        out[market] = block_for_market(market, instruments)

    ok = sum(1 for m in out.values() if m.get("available"))
    payload = {
        "schema_version": 6,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "hptl.prices.canonical_timeline (one daily series per instrument; weekly derived ISO from canonical daily)",
        "notes": (
            "All seasonality price paths derive from canonical_price_timeline. "
            "Weekly bars = derived_iso_week_end_from_canonical_daily. Not a trade signal."
        ),
        "summary": {
            "instruments_total": len(out),
            "available": ok,
            "unavailable": len(out) - ok,
        },
        "markets": out,
    }
    if _OUTLIER_FILTER_AUDITS:
        payload["outlier_filter_audits"] = list(_OUTLIER_FILTER_AUDITS)
        _write_outlier_filter_report(_OUTLIER_FILTER_AUDITS, payload["generated_at"])
    return payload


def _write_outlier_filter_report(audits: list[dict[str, Any]], generated_at: str) -> None:
    path = PROCESSED_DIR / "audits" / "seasonality_outlier_filter_report.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"generated_at": generated_at, "audits": audits}, indent=2),
        encoding="utf-8",
    )
    pub = PROJECT_ROOT / "data" / "audits" / "seasonality_outlier_filter_report.json"
    pub.parent.mkdir(parents=True, exist_ok=True)
    pub.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")


def write_exports(payload: dict[str, Any] | None = None) -> Path:
    payload = payload or build_payload()
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    CANONICAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    CANONICAL_PATH.write_text(text, encoding="utf-8")
    PUBLIC_PATH.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_PATH.write_text(text, encoding="utf-8")
    dist = PROJECT_ROOT / "web-dashboard" / "dist" / "data" / "seasonality_price_latest.json"
    if dist.parent.exists():
        dist.write_text(text, encoding="utf-8")
    return CANONICAL_PATH


def run(audit: str | None = None) -> Path:
    payload = build_payload()
    path = write_exports(payload)
    s = payload["summary"]
    print(f"Wrote {path} ({s['available']}/{s['instruments_total']} markets with seasonality chart).")
    if audit:
        blk = payload["markets"].get(audit)
        if blk:
            slim = {k: v for k, v in blk.items() if k not in ("current_path", "forward_projection")}
            print(json.dumps(slim, indent=2))
    return path


if __name__ == "__main__":
    import sys

    run(audit=sys.argv[1] if len(sys.argv) > 1 else None)
