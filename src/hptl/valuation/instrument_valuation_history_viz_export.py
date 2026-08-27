"""Point-in-time institutional valuation history — visualization export only.

Calls existing valuation engines with ``as_of_week`` truncation (no look-ahead).
NOT wired to weekly_refresh or production pipelines.

Usage:
    python -m hptl.valuation.instrument_valuation_history_viz_export
    python -m hptl.valuation.instrument_valuation_history_viz_export --market Gold
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.prices.price_store import load_instrument_record_internal
from hptl.valuation.engine import compute_valuation
from hptl.valuation.metals_valuation_v1 import METALS_MARKETS, is_metals_valuation_market

PUBLIC_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "instrument_valuation_history_latest.json"
PROCESSED_PATH = PROCESSED_DIR / "instrument_valuation_history_latest.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None  # NaN check


def _weekly_bars(market: str) -> list[dict[str, Any]]:
    rec = load_instrument_record_internal(market)
    if not rec:
        return []
    weekly = rec.get("weekly") or []
    return [b for b in weekly if b.get("date")]


def _point_in_time_row(market: str, bar: dict[str, Any]) -> dict[str, Any]:
    as_of = str(bar["date"])[:10]
    close = _num(bar.get("close"))
    val = compute_valuation(market=market, as_of_week=as_of)
    fair = _num(val.get("fair_value"))
    if fair is None:
        fair = _num(val.get("engine_fair_value"))
    dev = _num(val.get("deviation_pct"))
    if dev is None:
        dev = _num(val.get("engine_deviation_pct"))
    return {
        "date": as_of,
        "spot_price": close,
        "fair_value": fair,
        "deviation_pct": dev,
        "publish": bool(val.get("publish")),
        "model_id": val.get("model_id"),
        "model_status": val.get("model_status"),
        "valuation_state": val.get("valuation_state"),
        "wired": bool(val.get("wired")),
    }


def build_market_history(market: str, *, max_weeks: int | None = None) -> dict[str, Any]:
    bars = _weekly_bars(market)
    if max_weeks and len(bars) > max_weeks:
        bars = bars[-max_weeks:]
    series: list[dict[str, Any]] = []
    errors = 0
    for bar in bars:
        try:
            series.append(_point_in_time_row(market, bar))
        except Exception:
            errors += 1
    computed = sum(1 for r in series if r.get("fair_value") is not None)
    return {
        "market": market,
        "n_weeks": len(series),
        "n_with_fair_value": computed,
        "errors": errors,
        "sample_start": series[0]["date"] if series else None,
        "sample_end": series[-1]["date"] if series else None,
        "series": series,
    }


def _default_viz_markets() -> list[str]:
    """Markets with valuation engines — visualization export only."""
    from hptl.markets.instrument_registry import all_instrument_ids
    from hptl.valuation.engine import compute_valuation

    out: list[str] = []
    for mid in all_instrument_ids(tradeable_only=False):
        try:
            probe = compute_valuation(market=mid, as_of_week=None)
            model_id = probe.get("model_id")
            if model_id and model_id not in {"unavailable", None}:
                out.append(mid)
        except Exception:
            continue
    if not out:
        out = [m for m in METALS_MARKETS if is_metals_valuation_market(m)]
    return sorted(set(out))


def export_instrument_valuation_history(
    *,
    markets: list[str] | None = None,
    max_weeks: int | None = None,
) -> dict[str, Any]:
    if markets is None:
        markets = _default_viz_markets()
    instruments: dict[str, Any] = {}
    for market in markets:
        instruments[market] = build_market_history(market, max_weeks=max_weeks)

    doc = {
        "generated_at": _now_iso(),
        "export_type": "instrument_valuation_history_viz",
        "note": (
            "Point-in-time institutional fair value per weekly bar. "
            "Each row uses compute_valuation(market, as_of_week=date) — no look-ahead. "
            "Visualization-only; not a production pipeline artifact."
        ),
        "instruments": instruments,
    }
    return doc


def write_export(doc: dict[str, Any]) -> tuple[Path, Path]:
    PROCESSED_PATH.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(doc, indent=2, ensure_ascii=False)
    PROCESSED_PATH.write_text(payload, encoding="utf-8")
    PUBLIC_PATH.write_text(payload, encoding="utf-8")
    return PROCESSED_PATH, PUBLIC_PATH


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export point-in-time valuation history for UI overlay")
    parser.add_argument("--market", action="append", dest="markets", help="Instrument id (repeatable)")
    parser.add_argument("--max-weeks", type=int, default=None, help="Limit trailing weeks per market")
    args = parser.parse_args(argv)
    doc = export_instrument_valuation_history(markets=args.markets, max_weeks=args.max_weeks)
    proc, pub = write_export(doc)
    n = len(doc.get("instruments") or {})
    print(f"Wrote {n} market(s) -> {pub}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
