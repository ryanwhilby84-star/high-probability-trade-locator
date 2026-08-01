"""USD index identity — FRED broad and ICE DX must never silently substitute."""

from __future__ import annotations

from hptl.markets.usd_index_identity import (
    BROAD_USD_ID,
    DX_COT_ID,
    FRED_BROAD_SERIES,
    ICE_DXY_ID,
    is_broad_usd_id,
    is_ice_dx_price_id,
    seasonality_preferred_id,
)
from hptl.prices.cot_fail_backfill import FRED_COT_FAIL_SERIES
from hptl.prices.fred_prices import fred_series_for


def test_fred_bound_only_to_broad_usd():
    assert fred_series_for(BROAD_USD_ID) == FRED_BROAD_SERIES
    assert fred_series_for(DX_COT_ID) is None
    assert fred_series_for(ICE_DXY_ID) is None
    assert DX_COT_ID not in FRED_COT_FAIL_SERIES
    assert ICE_DXY_ID not in FRED_COT_FAIL_SERIES
    assert FRED_COT_FAIL_SERIES.get(BROAD_USD_ID) == FRED_BROAD_SERIES


def test_ice_and_broad_identity_helpers():
    assert is_ice_dx_price_id(DX_COT_ID)
    assert is_ice_dx_price_id(ICE_DXY_ID)
    assert not is_ice_dx_price_id(BROAD_USD_ID)
    assert is_broad_usd_id(BROAD_USD_ID)
    assert not is_broad_usd_id(ICE_DXY_ID)
    assert seasonality_preferred_id(DX_COT_ID) == ICE_DXY_ID
    assert seasonality_preferred_id(ICE_DXY_ID) == ICE_DXY_ID
    assert seasonality_preferred_id(BROAD_USD_ID) == BROAD_USD_ID


def test_coverage_routes_ice_to_yahoo():
    from hptl.prices.coverage import select_price_source

    assert select_price_source(ICE_DXY_ID) == "yahoo_futures"
    assert select_price_source(DX_COT_ID) == "yahoo_futures"
    assert select_price_source(BROAD_USD_ID) == "fred"
