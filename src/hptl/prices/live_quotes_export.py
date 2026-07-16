"""Live market quotes export — display/valuation UI only.

Separates live/current quotes from COT-aligned weekly OHLC used on workstation charts.
Does not modify canonical timeline, valuation fair-value math, COT, or seasonality.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT, get_oanda_api_key
from hptl.oanda.oanda_client import OandaApiError
from hptl.oanda.oanda_prices import fetch_pricing
from hptl.prices.coverage import oanda_symbol_for, select_price_source
from hptl.prices.current_price_service import load_instrument_mappings
from hptl.prices.price_store import load_price_store
from hptl.prices.workstation_index_ohlc_history import WORKSTATION_INDEX_SOURCES
from hptl.markets.instrument_registry import get_instrument

logger = logging.getLogger(__name__)

OUT_PATH = PROCESSED_DIR / "live_quotes_latest.json"
PUBLIC_OUT = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "live_quotes_latest.json"

# Explicit live-quote routing (OANDA preferred for metals + index CFD proxies).
LIVE_QUOTE_OANDA: dict[str, dict[str, str]] = {
    "Gold": {"oanda_symbol": "XAU_USD", "historical_ohlc_source": "oanda:XAU_USD"},
    "Silver": {"oanda_symbol": "XAG_USD", "historical_ohlc_source": "oanda:XAG_USD"},
    "Crude Oil / CL": {"oanda_symbol": "WTICO_USD", "historical_ohlc_source": "oanda:WTICO_USD"},
    "Copper / HG": {"oanda_symbol": "XCU_USD", "historical_ohlc_source": "oanda:XCU_USD"},
    "Sugar": {"oanda_symbol": "SUGAR_USD", "historical_ohlc_source": "oanda:SUGAR_USD"},
    "Soybeans": {"oanda_symbol": "SOYBN_USD", "historical_ohlc_source": "oanda:SOYBN_USD"},
}
for _iid, _spec in WORKSTATION_INDEX_SOURCES.items():
    sym = _spec.get("oanda_symbol") or ""
    if sym:
        LIVE_QUOTE_OANDA[_iid] = {
            "oanda_symbol": sym,
            "historical_ohlc_source": f"oanda:{sym}",
        }


def _num(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _canonical_symbol(instrument_id: str) -> str | None:
    """OANDA symbol from the canonical Current Price Service mapping (single source)."""
    try:
        mapping = load_instrument_mappings().get(instrument_id)
    except Exception:  # noqa: BLE001 - mapping is best-effort; legacy fallback below
        return None
    if mapping and mapping.provider == "oanda" and mapping.provider_symbol:
        return mapping.provider_symbol
    return None


def _canonical_precision(instrument_id: str) -> int | None:
    try:
        mapping = load_instrument_mappings().get(instrument_id)
    except Exception:  # noqa: BLE001
        return None
    return mapping.price_precision if mapping else None


def _resolve_oanda_symbol(instrument_id: str) -> str | None:
    # Canonical Current Price Service mapping (discovery-driven) takes priority so
    # the dashboard header and valuation share one authoritative OANDA symbol.
    canonical = _canonical_symbol(instrument_id)
    if canonical:
        return canonical
    if instrument_id in LIVE_QUOTE_OANDA:
        return LIVE_QUOTE_OANDA[instrument_id]["oanda_symbol"]
    spec = get_instrument(instrument_id)
    if not spec:
        return None
    if select_price_source(instrument_id) != "oanda":
        return None
    from hptl.prices.coverage import load_price_coverage

    return oanda_symbol_for(spec, load_price_coverage())


def _historical_from_workstation_block(block: dict[str, Any] | None) -> dict[str, Any]:
    if not block:
        return {}
    weekly = block.get("weekly_ohlc") or []
    last = weekly[-1] if weekly else {}
    matched = (block.get("tail_alignment_audit") or {}).get("final_12_matched") or []
    last_match = next((m for m in reversed(matched) if m.get("matched")), None)
    return {
        "historical_ohlc_source": block.get("price_source") or block.get("canonical_source"),
        "canonical_symbol": block.get("canonical_symbol"),
        "latest_completed_ohlc_date": last_match.get("ohlc_date") if last_match else block.get("ohlc_last_date"),
        "latest_completed_ohlc_close": last_match.get("close") if last_match else last.get("close"),
        "latest_cot_week": last_match.get("cot_date") if last_match else block.get("cot_last_date"),
    }


def _fallback_from_price_store(instrument_id: str, instruments: dict[str, Any]) -> dict[str, Any]:
    rec = instruments.get(instrument_id) or {}
    price = rec.get("price") or {}
    daily = rec.get("daily") or []
    last_daily = daily[-1] if daily else {}
    src = select_price_source(instrument_id) or "price_store"
    sym = _resolve_oanda_symbol(instrument_id) or instrument_id
    return {
        "live_price": _num(price.get("mid")) or _num(price.get("bid")) or _num(price.get("ask")),
        "live_bid": _num(price.get("bid")),
        "live_ask": _num(price.get("ask")),
        "live_price_as_of": price.get("as_of"),
        "live_price_source": f"{src}:{sym}",
        "live_fetch_ok": bool(price.get("mid") or price.get("bid")),
        "live_fetch_error": None if price.get("mid") else "no_price_snapshot_in_store",
        "latest_completed_ohlc_date": last_daily.get("date"),
        "latest_completed_ohlc_close": _num(last_daily.get("close")),
    }


def build_live_quotes_latest(*, fetch_live: bool = True) -> dict[str, Any]:
    """Build live quote snapshot merged with workstation historical OHLC metadata."""
    generated_at = datetime.now(timezone.utc).isoformat()
    ws_path = PROCESSED_DIR / "workstation_ohlc_latest.json"
    ws_doc: dict[str, Any] = {}
    if ws_path.is_file():
        try:
            ws_doc = json.loads(ws_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            ws_doc = {}

    ws_instruments = ws_doc.get("instruments") or {}
    price_store = load_price_store()
    px_instruments = price_store.get("instruments") or {}

    # Union instrument ids we can quote.
    instrument_ids = sorted(
        set(LIVE_QUOTE_OANDA.keys())
        | set(ws_instruments.keys())
        | {k for k, v in px_instruments.items() if (v.get("price") or v.get("daily"))}
    )

    oanda_map: dict[str, str] = {}
    for iid in instrument_ids:
        sym = _resolve_oanda_symbol(iid)
        if sym:
            oanda_map[iid] = sym

    live_by_symbol: dict[str, dict[str, Any]] = {}
    fetch_error: str | None = None
    if fetch_live and oanda_map and get_oanda_api_key():
        symbols = sorted(set(oanda_map.values()))
        try:
            pricing = fetch_pricing(symbols)
            for sym, snap in pricing.items():
                live_by_symbol[sym] = dict(snap)
        except OandaApiError as exc:
            fetch_error = str(exc)[:300]
            logger.warning("OANDA live quote fetch failed: %s", fetch_error)
    elif fetch_live and oanda_map:
        fetch_error = "OANDA_API_KEY not configured"

    instruments_out: dict[str, Any] = {}
    for iid in instrument_ids:
        ws_block = ws_instruments.get(iid)
        hist = _historical_from_workstation_block(ws_block)
        spec = LIVE_QUOTE_OANDA.get(iid) or {}
        if not hist.get("historical_ohlc_source") and spec.get("historical_ohlc_source"):
            hist["historical_ohlc_source"] = spec["historical_ohlc_source"]

        row: dict[str, Any] = {
            "instrument_id": iid,
            **hist,
            "ohlc_price_quality": (ws_block or {}).get("price_quality"),
            "live_price": None,
            "live_bid": None,
            "live_ask": None,
            "live_price_source": None,
            "live_price_as_of": None,
            "live_price_precision": _canonical_precision(iid),
            "live_fetch_ok": False,
            "live_fetch_error": fetch_error,
        }

        sym = oanda_map.get(iid)
        snap = live_by_symbol.get(sym or "") if sym else None
        if snap and snap.get("mid") is not None:
            row.update(
                {
                    "live_price": snap.get("mid"),
                    "live_bid": snap.get("bid"),
                    "live_ask": snap.get("ask"),
                    "live_price_as_of": snap.get("as_of"),
                    "live_price_source": f"oanda:{sym}",
                    "live_fetch_ok": True,
                    "live_fetch_error": None,
                }
            )
        else:
            fb = _fallback_from_price_store(iid, px_instruments)
            if fb.get("live_price") is not None:
                row.update(fb)
            if not row.get("historical_ohlc_source"):
                row["historical_ohlc_source"] = fb.get("live_price_source")

        instruments_out[iid] = row

    return {
        "version": 1,
        "generated_at": generated_at,
        "parser": "hptl.prices.live_quotes_export",
        "note": (
            "Live/current market quotes for valuation display. "
            "Workstation charts use COT-aligned completed weekly OHLC from workstation_ohlc_latest.json."
        ),
        "instruments": instruments_out,
    }


def write_live_quotes_exports(payload: dict[str, Any] | None = None) -> Path:
    doc = payload or build_live_quotes_latest()
    text = json.dumps(doc, indent=2)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(text, encoding="utf-8")
    PUBLIC_OUT.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_OUT.write_text(text, encoding="utf-8")
    return OUT_PATH


def load_live_quotes_doc() -> dict[str, Any]:
    path = OUT_PATH if OUT_PATH.is_file() else PUBLIC_OUT
    if not path.is_file():
        return {"instruments": {}}
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    path = write_live_quotes_exports()
    n = len((json.loads(path.read_text(encoding="utf-8")).get("instruments") or {}))
    print(f"Wrote {path} ({n} instruments).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
