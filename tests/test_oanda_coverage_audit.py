"""OANDA coverage audit — registry resolution without live API."""

from __future__ import annotations

from hptl.oanda.oanda_coverage_audit import build_oanda_coverage_audit, resolve_lookup_symbol
from hptl.markets.instrument_registry import get_instrument, load_registry


def _fake_oanda(names: list[str]) -> list[dict]:
    return [{"name": n, "displayName": n, "type": "CURRENCY"} for n in names]


def test_registry_symbol_supported():
    reg = load_registry()
    names = {"EUR_USD", "GBP_USD", "NAS100USD", "WTICOUSD", "XAU_EUR", "XCUUSD"}
    payload = build_oanda_coverage_audit(
        account_id="test-account",
        instruments=_fake_oanda(sorted(names)),
    )
    assert payload["summary"]["supported_count"] >= 5
    ids = {r["htpl_instrument_id"] for r in payload["supported"]}
    assert "Euro FX / 6E" in ids
    assert "British Pound / 6B" in ids


def test_sugar_unsupported_without_symbol():
    reg = load_registry()
    names = {"EUR_USD", "WTICOUSD"}
    payload = build_oanda_coverage_audit(
        account_id="test-account",
        instruments=_fake_oanda(sorted(names)),
    )
    unsupported_ids = {r["htpl_instrument_id"] for r in payload["unsupported"]}
    assert "Sugar" in unsupported_ids


def test_gold_resolves_via_preference():
    reg = load_registry()
    spec = get_instrument("Gold")
    assert spec is not None
    sym, source, _ = resolve_lookup_symbol(spec, reg, {"XAU_USD", "EUR_USD"})
    assert sym == "XAU_USD"
    assert source == "primary_preference"
