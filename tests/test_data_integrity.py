"""Tests for price data integrity checks."""

from __future__ import annotations

from hptl.prices.data_integrity import (
    REMOVED_PRICE_PROXIES,
    check_instrument_integrity,
    expected_price_source,
    unavailable_pillar_fields,
)


def test_cocoa_has_no_expected_source():
    assert "Cocoa" in REMOVED_PRICE_PROXIES
    assert expected_price_source("Cocoa") is None


def test_cocoa_integrity_fail_without_native_data():
    row = check_instrument_integrity("Cocoa")
    assert row.status == "FAIL"
    assert row.valuation_available is False
    assert row.seasonality_available is False


def test_pillar_fields_gated_on_integrity_fail():
    from hptl.pillars.confluence_attach import pillar_fields_for_market_week

    fields = pillar_fields_for_market_week("Cocoa", "2026-05-26")
    assert fields["valuation_bias"] == "UNAVAILABLE"
    assert fields["seasonality_bias"] == "UNAVAILABLE"
    assert fields["valuation_score"] is None
    assert fields["seasonality_score"] is None
    assert fields["data_integrity"] == "FAIL"


def test_unavailable_pillar_fields_shape():
    fields = unavailable_pillar_fields()
    assert fields["valuation_wired"] is False
    assert fields["seasonality_wired"] is False
