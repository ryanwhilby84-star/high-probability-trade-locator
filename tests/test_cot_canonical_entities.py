"""Canonical COT entity layer + COT coverage audit tests."""

from __future__ import annotations

from hptl.cot.canonical_entities import (
    CANONICAL_COT_ENTITIES,
    COT_STATUS_BROKEN,
    COT_STATUS_DIRECT,
    COT_STATUS_LEG,
    COT_STATUS_MACRO,
    COT_STATUS_NONE,
    COT_STATUS_PROXY,
    leg_entities_for_pair,
    resolve_cot_status,
)
from hptl.markets.cot_coverage_audit import build_cot_coverage_audit
from hptl.markets.instrument_registry import all_instrument_ids, get_instrument, load_registry

# All real CFTC markets with valid rows in the sample master (USD/HKD have no contract).
VALID = {
    "NASDAQ / NQ", "S&P 500 / ES", "Dow / YM", "Euro FX / 6E", "British Pound / 6B",
    "Japanese Yen / 6J", "Swiss Franc / 6S", "Australian Dollar / 6A", "Canadian Dollar / 6C",
    "NZ Dollar / 6N", "Gold", "Silver", "Copper / HG", "Crude Oil / CL", "Natural Gas / NG",
    "Coffee", "Cocoa", "Corn", "Wheat", "Soybeans",
}


def test_no_duplicate_canonical_entities():
    ids = list(CANONICAL_COT_ENTITIES.keys())
    assert len(ids) == len(set(ids))


def test_direct_entity_classified_direct():
    res = resolve_cot_status(get_instrument("Gold"), valid_entities=VALID)
    assert res.cot_status == COT_STATUS_DIRECT
    assert res.direct_cot_market == "Gold"


def test_gbp_nzd_uses_both_currency_legs():
    res = resolve_cot_status(get_instrument("GBP/NZD"), valid_entities=VALID)
    assert res.cot_status == COT_STATUS_LEG
    assert set(res.leg_cot_markets) == {"British Pound / 6B", "NZ Dollar / 6N"}
    assert "leg" in res.note.lower()


def test_fx_pair_has_no_direct_pair_cot():
    for pair in ["GBP/NZD", "NZD/JPY", "AUD/CAD"]:
        res = resolve_cot_status(get_instrument(pair), valid_entities=VALID)
        assert res.direct_cot_market is None
        assert res.cot_status != COT_STATUS_DIRECT


def test_usd_pair_is_leg_derived_via_single_leg():
    res = resolve_cot_status(get_instrument("NZD/USD"), valid_entities=VALID)
    assert res.cot_status == COT_STATUS_LEG
    assert res.leg_cot_markets == ["NZ Dollar / 6N"]


def test_one_sided_em_cross_is_macro_only_not_duplicate():
    # NZD/HKD: only NZD has a CFTC contract; HKD does not → macro-only, not a fake NZD COT.
    res = resolve_cot_status(get_instrument("NZD/HKD"), valid_entities=VALID)
    assert res.cot_status == COT_STATUS_MACRO
    assert res.direct_cot_market is None


def test_nzd_pairs_do_not_each_duplicate_full_nzd_cot():
    pairs = ["NZD/USD", "NZD/JPY", "NZD/HKD", "NZD/CHF"]
    for p in pairs:
        spec = get_instrument(p)
        if spec is None:
            continue
        res = resolve_cot_status(spec, valid_entities=VALID)
        # None of them claim a direct NZ Dollar COT table as their own.
        assert res.direct_cot_market is None


def test_proxy_classified_proxy():
    res = resolve_cot_status(get_instrument("Copper"), valid_entities=VALID)
    assert res.cot_status == COT_STATUS_PROXY
    assert res.proxy_cot_markets == ["Copper / HG"]


def test_broken_mapping_when_entity_absent():
    # Sugar/Platinum/Palladium are mapped but absent from the master → broken.
    res = resolve_cot_status(get_instrument("Sugar"), valid_entities=VALID)
    assert res.cot_status == COT_STATUS_BROKEN


# --- Coverage audit contract ---------------------------------------------------

def test_audit_contains_every_instrument():
    audit = build_cot_coverage_audit()
    ids = {x["instrument_id"] for x in audit["instruments"]}
    assert ids == set(all_instrument_ids())


def test_every_instrument_has_required_fields():
    audit = build_cot_coverage_audit()
    required = {
        "instrument_id", "display_name", "asset_class", "cot_status", "direct_cot_market",
        "leg_cot_markets", "proxy_cot_markets", "latest_valid_cot_week", "valid_rows_count",
        "invalid_rows_count", "duplicate_of", "data_quality_status", "exclusion_reason",
    }
    valid_status = {
        "direct_cot", "leg_derived_cot", "proxy_cot", "macro_only",
        "no_cot_available", "broken_mapping", "invalid_data",
    }
    valid_dq = {"clean", "incomplete", "duplicate", "invalid_rows_detected", "stale", "missing", "broken"}
    for x in audit["instruments"]:
        assert required <= set(x.keys()), x["instrument_id"]
        assert x["cot_status"] in valid_status
        assert x["data_quality_status"] in valid_dq


def test_invalid_rows_detected_in_audit():
    audit = build_cot_coverage_audit()
    # AUD has known placeholder rows in the sample master.
    aud = next(x for x in audit["instruments"] if x["instrument_id"] == "Australian Dollar / 6A")
    assert aud["invalid_rows_count"] > 0
    assert audit["summary"]["invalid_cot_rows_detected"] > 0


def test_proxy_instruments_flagged_as_duplicate():
    audit = build_cot_coverage_audit()
    copper = next(x for x in audit["instruments"] if x["instrument_id"] == "Copper")
    assert copper["duplicate_of"] == "Copper / HG"
    assert audit["summary"]["duplicate_mappings_flagged"] > 0


def test_broken_mapping_cannot_be_clean():
    audit = build_cot_coverage_audit()
    for iid in ("Sugar", "Platinum", "Palladium"):
        row = next(x for x in audit["instruments"] if x["instrument_id"] == iid)
        assert row["cot_status"] == "broken_mapping"
        assert row["data_quality_status"] != "clean"
