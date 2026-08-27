"""Unified price adapter — OANDA + Alpha Vantage behind one interface."""
from __future__ import annotations
from typing import Any
from hptl.alpha_vantage.alpha_prices import fetch_instrument_prices as av_fetch
from hptl.alpha_vantage.client import AlphaVantageApiError
from hptl.config import get_oanda_api_key
from hptl.markets.instrument_registry import InstrumentSpec, get_instrument
from hptl.oanda.oanda_client import OandaApiError
from hptl.oanda.oanda_prices import fetch_instrument_prices as oanda_fetch
from hptl.prices.cot_fail_backfill import FRED_COT_FAIL_SERIES, OANDA_COT_FAIL_PAIRS, fred_series_to_daily_bars
from hptl.prices.coverage import OANDA_SYMBOL_OVERRIDES, load_price_coverage, oanda_symbol_for, select_price_source
from hptl.prices.models import InstrumentPriceRecord, OhlcBar, PriceSnapshot, build_history_meta, compute_range_52w

OANDA_STORE_SYMBOL: dict[str, str] = {store_key: symbol for _, symbol, store_key in OANDA_COT_FAIL_PAIRS}


def _legacy_price_only_spec(instrument_id: str) -> InstrumentSpec | None:
    """Bridge legacy coverage IDs that pre-date the canonical registry."""
    if instrument_id == "Ether/Ether":
        return InstrumentSpec(
            id="Ether/Ether",
            display_name="Ether/Ether",
            oanda_symbol="ETH_USD",
            asset_class="crypto",
            subgroup="major",
            tradeable=True,
            macro_driver_profile="crypto",
            positioning_status="price_only",
            usd_sensitivity=0.8,
            rates_sensitivity=0.75,
            risk_sensitivity=0.9,
            risk_on_score=0.95,
        )
    return None


class UnifiedPriceAdapter:
    def __init__(self, coverage: dict[str, Any] | None = None) -> None:
        self._coverage = coverage or load_price_coverage()

    def source_for(self, instrument_id: str) -> str | None:
        return select_price_source(instrument_id, self._coverage)

    def fetch(self, instrument_id: str, *, spec: InstrumentSpec | None = None) -> InstrumentPriceRecord:
        spec = spec or get_instrument(instrument_id) or _legacy_price_only_spec(instrument_id)
        if spec is None:
            return {"instrument_id": instrument_id,"price":None,"daily":[],"weekly":[],"range_52w":None,"history":None,"error":"unknown_instrument"}
        source = self.source_for(instrument_id)
        if not source and instrument_id not in FRED_COT_FAIL_SERIES and instrument_id not in OANDA_STORE_SYMBOL:
            return {"instrument_id": instrument_id,"price":None,"daily":[],"weekly":[],"range_52w":None,"history":None,"error":"unsupported_instrument"}
        if not source and instrument_id in FRED_COT_FAIL_SERIES:
            source="fred"
        price: PriceSnapshot | None=None; daily:list[OhlcBar]=[]; weekly:list[OhlcBar]=[]; err:str|None=None; price_scale=None
        forming_daily=None; forming_weekly=None
        if source=="yahoo_futures":
            from hptl.markets.usd_index_identity import is_ice_dx_price_id
            from hptl.prices.price_store import load_instrument_record
            try:
                if is_ice_dx_price_id(instrument_id):
                    from hptl.prices.ice_dx_futures_backfill import promote_ice_dx_futures
                    promote_ice_dx_futures((instrument_id,))
                else:
                    from hptl.prices.softs_futures_backfill import promote_soft_futures
                    promote_soft_futures(instrument_id)
                existing=load_instrument_record(instrument_id) or {}; daily=existing.get("daily") or []; weekly=existing.get("weekly") or []; price=existing.get("price"); price_scale=existing.get("price_scale")
            except Exception as exc:
                err=f"yahoo_futures:{type(exc).__name__}: {exc}"[:200]
                if is_ice_dx_price_id(instrument_id): source=None
                else: source="fred"
        oanda_sym=OANDA_STORE_SYMBOL.get(instrument_id)
        if not oanda_sym and source=="oanda":
            if instrument_id in OANDA_SYMBOL_OVERRIDES:
                oanda_sym=OANDA_SYMBOL_OVERRIDES[instrument_id]
            else:
                oanda_sym=oanda_symbol_for(spec,self._coverage)
        if source=="oanda":
            if not get_oanda_api_key():
                err="oanda_config:missing_api_key"
            elif not oanda_sym:
                err=f"oanda_mapping:no_symbol:{instrument_id}"
            else:
                try:
                    price,daily,weekly,forming_daily,forming_weekly=oanda_fetch(oanda_sym)
                    if not daily: err=f"oanda_empty_daily:{oanda_sym}"
                except OandaApiError as exc:
                    err=f"OandaApiError:{oanda_sym}: {exc}"[:200]
                    price=None; daily=[]; weekly=[]; forming_daily=None; forming_weekly=None
        try:
            if not daily and source!="oanda":
                if instrument_id in FRED_COT_FAIL_SERIES and instrument_id not in OANDA_STORE_SYMBOL:
                    fred_daily=fred_series_to_daily_bars(FRED_COT_FAIL_SERIES[instrument_id])
                    if fred_daily:
                        daily=fred_daily; source="fred"; price={"mid":daily[-1]["close"],"as_of":daily[-1]["date"]}; err=None
                if not daily and source not in ("yahoo_futures","fred",None):
                    price,daily,weekly=av_fetch(spec)
        except AlphaVantageApiError as exc:
            err=f"{type(exc).__name__}: {exc}"[:200]
        range_52w=compute_range_52w(daily); history=build_history_meta(daily,weekly,range_52w) if daily or weekly else None
        if price_scale is None and source=="oanda" and daily and oanda_sym: price_scale={"source":"oanda","symbol":oanda_sym}
        elif price_scale is None and source=="fred" and instrument_id in FRED_COT_FAIL_SERIES: price_scale={"source":"fred","series_id":FRED_COT_FAIL_SERIES[instrument_id]}
        return {"instrument_id":instrument_id,"price":price,"daily":daily,"weekly":weekly,"forming_daily":forming_daily,"forming_weekly":forming_weekly,"range_52w":range_52w,"history":history,"error":err,"price_scale":price_scale,"_fetched_via":source}

    def fetch_many(self,instrument_ids:list[str],*,on_progress:Any=None)->dict[str,InstrumentPriceRecord]:
        out={}; total=len(instrument_ids)
        for i,iid in enumerate(instrument_ids):
            out[iid]=self.fetch(iid)
            if on_progress: on_progress(i+1,total,iid,out[iid])
        return out
