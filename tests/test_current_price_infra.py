"""Unit tests for stream quote ordering and status labelling invariants."""

from __future__ import annotations

from hptl.prices.current_price_service import (
    CURRENT_PRICE_STALE_SECONDS,
    STATUS_FALLBACK,
    STATUS_LIVE,
    STATUS_STALE,
    STATUS_UNAVAILABLE,
    InstrumentMapping,
    _build_current_price,
)
from hptl.prices.current_price_stream import OandaStreamCache


def test_update_quote_rejects_older_as_of():
    cache = OandaStreamCache(symbols=["XAU_USD"])
    cache.update_quote(
        "XAU_USD",
        {"mid": 2400.0, "bid": 2399.0, "ask": 2401.0, "as_of": "2026-07-21T12:00:00.000000000Z"},
        tradeable=True,
        status="tradeable",
    )
    cache.update_quote(
        "XAU_USD",
        {"mid": 2300.0, "bid": 2299.0, "ask": 2301.0, "as_of": "2026-07-21T11:59:00.000000000Z"},
        tradeable=True,
        status="tradeable",
    )
    snap = cache.get_snapshots(["XAU_USD"])["XAU_USD"]
    assert snap["mid"] == 2400.0


def test_stale_never_labelled_live():
    mapping = InstrumentMapping(
        internal_key="Gold",
        display_name="Gold",
        provider="oanda",
        provider_symbol="XAU_USD",
        asset_type="metal",
        currency="USD",
        price_precision=3,
        supports_streaming=True,
    )
    # Force an old timestamp
    old = "2020-01-01T00:00:00+00:00"
    cp = _build_current_price(
        mapping,
        {"mid": 1800.0, "bid": 1799.0, "ask": 1801.0, "as_of": old},
        allow_fallback=False,
    )
    assert cp.status == STATUS_STALE
    assert cp.status != STATUS_LIVE
    assert cp.age_seconds is not None
    assert cp.age_seconds > CURRENT_PRICE_STALE_SECONDS


def test_fallback_never_labelled_live():
    mapping = InstrumentMapping(
        internal_key="US Dollar Index / DX",
        display_name="DX",
        provider="fred",
        provider_symbol="DTWEXBGS",
        asset_type="fx",
        currency="USD",
        price_precision=4,
        supports_streaming=False,
    )
    cp = _build_current_price(mapping, None, allow_fallback=True)
    assert cp.status in (STATUS_FALLBACK, STATUS_UNAVAILABLE)
    assert cp.status != STATUS_LIVE
