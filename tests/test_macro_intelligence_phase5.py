"""Phase 5 Macro Intelligence — architecture / placeholder tests."""

from __future__ import annotations

from hptl.macro_intelligence.contributors import default_contributors
from hptl.macro_intelligence.engine import (
    aggregate_overall_bias,
    analyse_macro_intelligence,
)
from hptl.macro_intelligence.models import BIAS_LABELS, MacroContributorResult
from hptl.macro_intelligence.service import build_macro_intelligence_payload


EXPECTED_CONTRIBUTORS = (
    "Interest Rates",
    "Inflation",
    "Economic Growth",
    "Commodity Exposure",
    "Risk Sentiment",
    "Central Banks",
    "Government Bonds",
    "Dollar Environment",
)


def test_default_contributor_registry_order():
    names = [c.name for c in default_contributors()]
    assert names == list(EXPECTED_CONTRIBUTORS)


def test_placeholder_gold_payload():
    payload = build_macro_intelligence_payload(instrument_id="Gold")
    assert payload["status"] == "ok"
    assert payload["phase"] == "5"
    assert payload["engine"] == "macro_intelligence_v5"
    assert payload["instrument_id"] == "Gold"
    assert payload["overall_macro_bias"] == "Neutral"
    assert payload["overall_macro_bias"] in BIAS_LABELS
    assert payload["no_trade_signals"] is True
    assert payload["architecture_only"] is True
    assert len(payload["contributors"]) == 8
    for c in payload["contributors"]:
        assert c["status"] == "Unavailable"
        assert "Phase 5 architecture placeholder" in c["summary"]
        assert c["last_updated"] is None
        assert c["weight"] == 0.0
        assert "name" in c and "contributor_id" in c


def test_fx_and_commodity_instruments_supported():
    for iid in ("AUD/NZD", "Corn", "NASDAQ / NQ", "Euro FX / 6E"):
        result = analyse_macro_intelligence(iid)
        assert result.status == "ok", iid
        assert result.overall_macro_bias == "Neutral"
        assert len(result.contributors) == 8


def test_missing_instrument_errors():
    result = analyse_macro_intelligence("")
    assert result.status == "error"
    assert "missing_instrument_id" in result.errors


def test_unknown_instrument_errors():
    result = analyse_macro_intelligence("Not A Real Market XYZ")
    assert result.status == "error"
    assert any("unknown_instrument_id" in e for e in result.errors)


def test_aggregate_all_unavailable_is_neutral():
    rows = [
        MacroContributorResult(
            name="X",
            status="Unavailable",
            strength=None,
            summary="n/a",
            last_updated=None,
        )
        for _ in range(3)
    ]
    assert aggregate_overall_bias(rows) == "Neutral"
    assert aggregate_overall_bias([]) == "Neutral"


def test_contributors_are_independent_instances():
    a = default_contributors()
    b = default_contributors()
    assert [c.contributor_id for c in a] == [c.contributor_id for c in b]
    # Mutating one registry list must not affect a fresh registry.
    a.pop()
    assert len(default_contributors()) == 8


def test_ui_payload_contract_keys():
    payload = build_macro_intelligence_payload(instrument_id="Silver")
    assert set(payload.keys()) >= {
        "status",
        "engine",
        "phase",
        "instrument_id",
        "overall_macro_bias",
        "contributors",
        "notes",
        "no_trade_signals",
        "architecture_only",
    }
    row = payload["contributors"][0]
    assert set(row.keys()) >= {
        "name",
        "status",
        "strength",
        "summary",
        "last_updated",
        "weight",
        "contributor_id",
    }
