"""Price coverage audit — offline fixtures."""

from __future__ import annotations

from hptl.prices.price_coverage_audit import build_price_coverage_audit


def _oanda_meta(names: list[str]) -> dict:
    by = {n: {"displayName": n, "type": "CURRENCY"} for n in names}
    return {
        "account_id": "test-001",
        "api_host": "https://api-fxpractice.oanda.com",
        "last_successful_response": "2026-06-01T12:00:00+00:00",
        "endpoint": "/v3/accounts/{accountId}/instruments",
        "instrument_count": len(names),
        "names_set": set(names),
        "by_name": by,
    }


def _av_meta(functions: list[str]) -> dict:
    ts = "2026-06-01T12:00:00+00:00"
    return {
        "verified_functions": functions,
        "category_timestamps": {"fx": ts, "commodity_wheat": ts, "index": ts},
        "per_function_timestamps": {f: ts for f in functions},
        "last_successful_response": ts,
        "supported_categories": ["fx"],
        "category_probes": [],
    }


def test_eur_supported_both():
    payload = build_price_coverage_audit(
        oanda_meta=_oanda_meta(["EUR_USD", "GBP_USD"]),
        av_meta=_av_meta(["CURRENCY_EXCHANGE_RATE"]),
    )
    assert "Euro FX / 6E" in payload["oanda_supported"]
    assert "Euro FX / 6E" in payload["alpha_supported"]
    assert "Euro FX / 6E" in payload["supported_by_both"]


def test_sugar_alpha_only_when_av_has_sugar():
    payload = build_price_coverage_audit(
        oanda_meta=_oanda_meta(["EUR_USD"]),
        av_meta=_av_meta(["SUGAR"]),
    )
    assert "Sugar" in payload["alpha_supported"]
    assert "Sugar" not in payload["oanda_supported"]
    row = next(r for r in payload["instruments"] if r["htpl_instrument_id"] == "Sugar")
    assert row["coverage_status"] == "alpha_only"


def test_sugar_unsupported_when_no_av_commodity():
    payload = build_price_coverage_audit(
        oanda_meta=_oanda_meta(["EUR_USD"]),
        av_meta=_av_meta(["CURRENCY_EXCHANGE_RATE"]),
    )
    assert "Sugar" in payload["unsupported"]


def test_instruments_have_evidence():
    payload = build_price_coverage_audit(
        oanda_meta=_oanda_meta(["EUR_USD"]),
        av_meta=_av_meta(["CURRENCY_EXCHANGE_RATE"]),
    )
    row = next(r for r in payload["instruments"] if r["htpl_instrument_id"] == "Euro FX / 6E")
    assert len(row["sources"]) == 2
    assert row["sources"][0]["source"] == "oanda"
    assert row["sources"][0]["coverage_status"] == "supported"
