#!/usr/bin/env python3
"""One-shot diagnostic trace for index price fetch (NASDAQ / S&P / DOW)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.alpha_vantage.client import AlphaVantageApiError, _get
from hptl.alpha_vantage.mappings import _INDEX_SYMBOL
from hptl.markets.instrument_registry import get_instrument
from hptl.prices.coverage import load_price_coverage, select_price_source

INDEX_IDS = ["NASDAQ / NQ", "S&P 500 / ES", "Dow / YM"]


def _parse_av_daily_series(doc: dict) -> list:
    series_key = next((k for k in doc if "Time Series" in k), None)
    if not series_key or not isinstance(doc.get(series_key), dict):
        return []
    return list(doc[series_key].keys())


def _probe(instrument_id: str) -> dict:
    spec = get_instrument(instrument_id)
    sym = _INDEX_SYMBOL.get(instrument_id, "?")
    cov = load_price_coverage()
    source = select_price_source(instrument_id, cov)

    rows: list[dict] = []
    for fn, outputsize in (
        ("TIME_SERIES_DAILY", "full"),
        ("TIME_SERIES_DAILY", "compact"),
        ("TIME_SERIES_WEEKLY", "compact"),
    ):
        entry: dict = {
            "function": fn,
            "symbol": sym,
            "outputsize": outputsize,
            "endpoint": f"https://www.alphavantage.co/query?function={fn}&symbol={sym}&outputsize={outputsize}",
        }
        try:
            params = {"symbol": sym, "outputsize": outputsize}
            doc = _get(fn, **params)
            keys = list(doc.keys())
            entry["response_keys"] = keys
            entry["information"] = str(doc.get("Information") or doc.get("Note") or "")[:200]
            bars = _parse_av_daily_series(doc)
            entry["parser_bars"] = len(bars)
            entry["stored"] = len(bars) > 0
            entry["failure_reason"] = None if bars else "parser returned 0 bars (no Time Series key or empty series)"
        except AlphaVantageApiError as exc:
            entry["response_keys"] = []
            entry["information"] = (exc.note or str(exc))[:200]
            entry["parser_bars"] = 0
            entry["stored"] = False
            entry["failure_reason"] = str(exc)
        rows.append(entry)

    store_path = ROOT / "data" / "processed" / "prices" / (
        instrument_id.replace("/", "_").replace(" ", "_").replace("&", "_").replace("__", "_")
    )
    # use safe filename logic
    from hptl.prices.price_store import _safe_filename

    store_path = ROOT / "data" / "processed" / "prices" / f"{_safe_filename(instrument_id)}.json"
    stored = {}
    if store_path.exists():
        stored = json.loads(store_path.read_text(encoding="utf-8"))

    return {
        "instrument": instrument_id,
        "selected_source": source,
        "etf_symbol": sym,
        "probes": rows,
        "current_store": {
            "daily_bars": len(stored.get("daily") or []),
            "weekly_bars": len(stored.get("weekly") or []),
            "error": stored.get("error"),
            "_fetched_via": stored.get("_fetched_via"),
        },
    }


def main() -> int:
    out = [_probe(iid) for iid in INDEX_IDS]
    path = ROOT / "data" / "index_feed_diagnostic.json"
    path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    print(f"\nWrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
