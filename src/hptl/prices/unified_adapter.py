"""Unified price adapter — OANDA + Alpha Vantage behind one interface."""

from __future__ import annotations

from typing import Any

from hptl.alpha_vantage.alpha_prices import fetch_instrument_prices as av_fetch
from hptl.alpha_vantage.client import AlphaVantageApiError
from hptl.markets.instrument_registry import InstrumentSpec, get_instrument
from hptl.oanda.oanda_client import OandaApiError
from hptl.oanda.oanda_prices import fetch_instrument_prices as oanda_fetch
from hptl.prices.coverage import load_price_coverage, oanda_symbol_for, select_price_source
from hptl.prices.models import (
    InstrumentPriceRecord,
    OhlcBar,
    PriceSnapshot,
    build_history_meta,
    compute_range_52w,
)


class UnifiedPriceAdapter:
    """Fetch canonical OHLC for HTPL instruments; source selection is internal."""

    def __init__(self, coverage: dict[str, Any] | None = None) -> None:
        self._coverage = coverage or load_price_coverage()

    def source_for(self, instrument_id: str) -> str | None:
        return select_price_source(instrument_id, self._coverage)

    def fetch(
        self,
        instrument_id: str,
        *,
        spec: InstrumentSpec | None = None,
    ) -> InstrumentPriceRecord:
        spec = spec or get_instrument(instrument_id)
        if spec is None:
            return {
                "instrument_id": instrument_id,
                "price": None,
                "daily": [],
                "weekly": [],
                "range_52w": None,
                "history": None,
                "error": "unknown_instrument",
            }

        source = self.source_for(instrument_id)
        if not source:
            return {
                "instrument_id": instrument_id,
                "price": None,
                "daily": [],
                "weekly": [],
                "range_52w": None,
                "history": None,
                "error": "unsupported_instrument",
            }

        price: PriceSnapshot | None = None
        daily: list[OhlcBar] = []
        weekly: list[OhlcBar] = []
        err: str | None = None

        try:
            if source == "oanda":
                sym = oanda_symbol_for(spec, self._coverage)
                if not sym:
                    raise OandaApiError(f"No OANDA symbol for {instrument_id}")
                price, daily, weekly = oanda_fetch(sym)
            else:
                price, daily, weekly = av_fetch(spec)
        except OandaApiError:
            if source == "oanda" and instrument_id in set(self._coverage.get("alpha_supported") or []):
                try:
                    price, daily, weekly = av_fetch(spec)
                    source = "alpha_vantage"
                except AlphaVantageApiError as exc:
                    err = f"{type(exc).__name__}: {exc}"[:200]
            else:
                err = f"OandaApiError: fetch failed for {instrument_id}"[:200]
        except AlphaVantageApiError as exc:
            err = f"{type(exc).__name__}: {exc}"[:200]

        range_52w = compute_range_52w(daily)
        history = build_history_meta(daily, weekly, range_52w) if daily or weekly else None

        return {
            "instrument_id": instrument_id,
            "price": price,
            "daily": daily,
            "weekly": weekly,
            "range_52w": range_52w,
            "history": history,
            "error": err,
        }

    def fetch_many(
        self,
        instrument_ids: list[str],
        *,
        on_progress: Any = None,
    ) -> dict[str, InstrumentPriceRecord]:
        out: dict[str, InstrumentPriceRecord] = {}
        total = len(instrument_ids)
        for i, iid in enumerate(instrument_ids):
            out[iid] = self.fetch(iid)
            if on_progress:
                on_progress(i + 1, total, iid, out[iid])
        return out
