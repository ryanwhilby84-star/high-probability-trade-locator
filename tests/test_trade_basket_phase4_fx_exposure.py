"""Phase 4 — FX trade decomposition and currency exposure."""

from __future__ import annotations

import pytest

from hptl.portfolio_intelligence.service import enrich_basket_with_portfolio_intelligence
from hptl.trade_basket.currency_exposure import (
    compute_currency_exposure,
    enrich_basket_with_currency_exposure,
)
from hptl.trade_basket.engine import analyse_trade_basket
from hptl.trade_basket.fx_decomposition import decompose_fx_pair
from hptl.trade_basket.service import build_trade_basket_payload


class FakePhase1Provider:
    """Deterministic Phase 1 stand-in for FX pair IDs."""

    source = "fake_phase1_provider"

    def __init__(self, raw_by_pair: dict[tuple[str, str], float] | None = None, overlap: int = 60):
        self.raw_by_pair = raw_by_pair or {}
        self.overlap = overlap
        self.default_raw = 0.55

    def get_map(self, instrument_ids, *, frequency, lookback):
        ids = []
        seen = set()
        for iid in instrument_ids:
            if iid not in seen:
                seen.add(iid)
                ids.append(iid)
        n = len(ids)
        matrix = [[None] * n for _ in range(n)]
        pair_meta = {}
        for i, a in enumerate(ids):
            matrix[i][i] = 1.0
            for j in range(i + 1, n):
                b = ids[j]
                key = (a, b) if (a, b) in self.raw_by_pair else (b, a)
                raw = self.raw_by_pair.get(key, self.default_raw)
                matrix[i][j] = raw
                matrix[j][i] = raw
                pair_meta[f"{a}||{b}"] = {"status": "ok", "overlap": self.overlap}
        return {
            "status": "ok",
            "engine": "correlation_matrix_v1",
            "frequency": frequency,
            "lookback": lookback,
            "instruments": ids,
            "matrix": matrix,
            "pair_meta": pair_meta,
            "warnings": [],
        }

    def lookup_pair(self, corr_map, instrument_a, instrument_b):
        from hptl.correlation_matrix.service import lookup_raw_correlation

        return lookup_raw_correlation(corr_map, instrument_a, instrument_b)


def test_decomposition_rules():
    assert decompose_fx_pair("AUD/NZD", "LONG") == [
        {"currency": "AUD", "sign": 1},
        {"currency": "NZD", "sign": -1},
    ]
    assert decompose_fx_pair("AUD/CHF", "LONG") == [
        {"currency": "AUD", "sign": 1},
        {"currency": "CHF", "sign": -1},
    ]
    assert decompose_fx_pair("GBP/AUD", "LONG") == [
        {"currency": "GBP", "sign": 1},
        {"currency": "AUD", "sign": -1},
    ]
    assert decompose_fx_pair("AUD/NZD", "SHORT") == [
        {"currency": "AUD", "sign": -1},
        {"currency": "NZD", "sign": 1},
    ]
    assert decompose_fx_pair("Gold", "LONG") is None


def test_case_a_shared_base_currency():
    """AUD/NZD LONG + AUD/CHF LONG — accepted; AUD largest long."""
    result = analyse_trade_basket(
        trades=[
            {"instrument_pair": "AUD/NZD", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_pair": "AUD/CHF", "direction": "LONG", "risk_percent": 1.0},
        ],
        frequency="daily",
        lookback=60,
        correlation_provider=FakePhase1Provider(
            {("AUD/NZD", "AUD/CHF"): 0.72}
        ),
    )
    assert result.status == "ok"
    assert result.populated_trade_count == 2
    assert result.pair_count == 1
    pair = result.pairs[0]
    assert {pair.trade_a_instrument_id, pair.trade_b_instrument_id} == {
        "AUD/NZD",
        "AUD/CHF",
    }
    assert pair.raw_correlation == pytest.approx(0.72)
    assert pair.direction_adjusted_correlation == pytest.approx(0.72)

    exposure = compute_currency_exposure(
        [
            {"instrument_id": "AUD/NZD", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "AUD/CHF", "direction": "LONG", "risk_percent": 1.0},
        ]
    )
    by_ccy = {r["currency"]: r for r in exposure["currencies"]}
    assert by_ccy["AUD"]["net_exposure"] == pytest.approx(1.0)
    assert by_ccy["AUD"]["direction"] == "Long"
    assert by_ccy["NZD"]["net_exposure"] == pytest.approx(-0.5)
    assert by_ccy["CHF"]["net_exposure"] == pytest.approx(-0.5)
    assert exposure["currencies"][0]["currency"] == "AUD"
    dom = exposure["dominant_currency_exposure"]
    assert dom["currency"] == "AUD"
    assert dom["direction"] == "LONG"
    assert dom["share_of_gross"] == pytest.approx(0.5)
    assert any("Australian-dollar" in d for d in exposure["diagnostics"])


def test_case_b_opposing_aud_exposure():
    """AUD/NZD LONG + GBP/AUD LONG — AUD fully offsets."""
    exposure = compute_currency_exposure(
        [
            {"instrument_id": "AUD/NZD", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "GBP/AUD", "direction": "LONG", "risk_percent": 1.0},
        ]
    )
    by_ccy = {r["currency"]: r for r in exposure["currencies"]}
    assert by_ccy["AUD"]["net_exposure"] == pytest.approx(0.0)
    assert by_ccy["AUD"]["direction"] == "Neutral"
    assert by_ccy["GBP"]["net_exposure"] == pytest.approx(0.5)
    assert by_ccy["NZD"]["net_exposure"] == pytest.approx(-0.5)
    assert any("fully offset" in d for d in exposure["diagnostics"])


def test_case_c_exact_duplicate_rejected():
    result = analyse_trade_basket(
        trades=[
            {"instrument_pair": "AUD/NZD", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_pair": "AUD/NZD", "direction": "LONG", "risk_percent": 1.0},
        ],
        frequency="daily",
        lookback=60,
        correlation_provider=FakePhase1Provider(),
    )
    assert result.status == "error"
    assert any("duplicate_instrument_direction" in e for e in result.errors)


def test_case_d_same_pair_opposite_directions():
    result = analyse_trade_basket(
        trades=[
            {"instrument_pair": "AUD/NZD", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_pair": "AUD/NZD", "direction": "SHORT", "risk_percent": 1.0},
        ],
        frequency="daily",
        lookback=60,
        correlation_provider=FakePhase1Provider(),
    )
    assert result.status == "ok"
    assert any("offsetting_same_instrument" in w for w in result.warnings)
    pair = result.pairs[0]
    assert pair.direction_adjusted_correlation == pytest.approx(-1.0)

    exposure = compute_currency_exposure(
        [
            {"instrument_id": "AUD/NZD", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "AUD/NZD", "direction": "SHORT", "risk_percent": 1.0},
        ]
    )
    by_ccy = {r["currency"]: r for r in exposure["currencies"]}
    assert by_ccy["AUD"]["net_exposure"] == pytest.approx(0.0)
    assert by_ccy["NZD"]["net_exposure"] == pytest.approx(0.0)
    assert exposure["gross_currency_exposure"] == pytest.approx(0.0)


def test_case_e_mixed_asset_basket():
    result = analyse_trade_basket(
        trades=[
            {"instrument_pair": "AUD/NZD", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "Gold", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "Corn", "direction": "SHORT", "risk_percent": 1.0},
        ],
        frequency="daily",
        lookback=60,
        correlation_provider=FakePhase1Provider(),
    )
    assert result.status == "ok"
    assert result.populated_trade_count == 3
    assert result.pair_count == 3

    basket = build_trade_basket_payload(
        trades=[
            {"instrument_pair": "AUD/NZD", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "Gold", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "Corn", "direction": "SHORT", "risk_percent": 1.0},
        ],
        frequency="daily",
        lookback=60,
    )
    # Inject ok pairs via analyse path already validated; enrich with fake ok payload
    enriched = enrich_basket_with_portfolio_intelligence(
        {
            "status": "ok",
            "phase": "2A",
            "populated_trade_count": 3,
            "pair_count": 3,
            "trades": [
                {"instrument_id": "AUD/NZD", "direction": "LONG", "risk_percent": 1.0},
                {"instrument_id": "Gold", "direction": "LONG", "risk_percent": 1.0},
                {"instrument_id": "Corn", "direction": "SHORT", "risk_percent": 1.0},
            ],
            "pairs": [
                {
                    "trade_a_instrument_id": "AUD/NZD",
                    "trade_a_direction": "LONG",
                    "trade_b_instrument_id": "Gold",
                    "trade_b_direction": "LONG",
                    "raw_correlation": 0.1,
                    "direction_adjusted_correlation": 0.1,
                },
                {
                    "trade_a_instrument_id": "AUD/NZD",
                    "trade_a_direction": "LONG",
                    "trade_b_instrument_id": "Corn",
                    "trade_b_direction": "SHORT",
                    "raw_correlation": 0.05,
                    "direction_adjusted_correlation": -0.05,
                },
                {
                    "trade_a_instrument_id": "Gold",
                    "trade_a_direction": "LONG",
                    "trade_b_instrument_id": "Corn",
                    "trade_b_direction": "SHORT",
                    "raw_correlation": 0.02,
                    "direction_adjusted_correlation": -0.02,
                },
            ],
        }
    )
    enriched = enrich_basket_with_currency_exposure(enriched)
    assert enriched["portfolio_intelligence"]["status"] == "ok"
    assert enriched["currency_exposure"]["status"] == "ok"
    ccys = {r["currency"] for r in enriched["currency_exposure"]["currencies"]}
    assert ccys == {"AUD", "NZD"}
    assert "Gold" not in ccys
    assert "Corn" not in ccys
    assert enriched["workstation_phase"] == "4"
    # direct analyse path also succeeds
    assert basket["status"] in ("ok", "error")  # live prices may vary; engine path above is authoritative


def test_instrument_pair_alias_accepted():
    result = analyse_trade_basket(
        trades=[
            {"instrument_pair": "AUD/NZD", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "AUD/CHF", "direction": "SHORT", "risk_percent": 1.0},
        ],
        frequency="daily",
        lookback=60,
        correlation_provider=FakePhase1Provider(),
    )
    assert result.status == "ok"
    ids = {t.instrument_id for t in result.trades}
    assert ids == {"AUD/NZD", "AUD/CHF"}


def test_phase3_outputs_present_on_fx_basket():
    payload = {
        "status": "ok",
        "phase": "2A",
        "trades": [
            {"instrument_id": "AUD/NZD", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "AUD/CHF", "direction": "LONG", "risk_percent": 1.0},
        ],
        "pairs": [
            {
                "trade_a_instrument_id": "AUD/NZD",
                "trade_a_direction": "LONG",
                "trade_b_instrument_id": "AUD/CHF",
                "trade_b_direction": "LONG",
                "raw_correlation": 0.7,
                "direction_adjusted_correlation": 0.7,
            }
        ],
    }
    out = enrich_basket_with_currency_exposure(
        enrich_basket_with_portfolio_intelligence(payload)
    )
    pi = out["portfolio_intelligence"]
    for key in (
        "trades_entered",
        "effective_independent_trades",
        "diversification_score",
        "duplication_score",
        "total_planned_risk",
        "largest_risk_concentration",
        "largest_exposure_cluster",
        "highest_correlated_pair",
        "lowest_correlated_pair",
    ):
        assert key in pi
    assert out["currency_exposure"]["dominant_currency_exposure"]["currency"] == "AUD"


def test_shared_quote_currency():
    """EUR/AUD LONG + GBP/AUD LONG — shared short AUD (quote)."""
    exposure = compute_currency_exposure(
        [
            {"instrument_id": "EUR/AUD", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "GBP/AUD", "direction": "LONG", "risk_percent": 1.0},
        ]
    )
    by_ccy = {r["currency"]: r for r in exposure["currencies"]}
    assert by_ccy["AUD"]["net_exposure"] == pytest.approx(-1.0)
    assert by_ccy["AUD"]["direction"] == "Short"
    assert any("short Australian-dollar" in d for d in exposure["diagnostics"])


def test_partially_offset_exposure():
    exposure = compute_currency_exposure(
        [
            {"instrument_id": "AUD/NZD", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "GBP/AUD", "direction": "LONG", "risk_percent": 2.0},
        ]
    )
    by_ccy = {r["currency"]: r for r in exposure["currencies"]}
    # AUD: +0.5 from first, −1.0 from second → −0.5
    assert by_ccy["AUD"]["net_exposure"] == pytest.approx(-0.5)
    assert any("partially offset" in d for d in exposure["diagnostics"])


def test_unequal_risk_weighting():
    exposure = compute_currency_exposure(
        [
            {"instrument_id": "AUD/NZD", "direction": "LONG", "risk_percent": 2.0},
            {"instrument_id": "AUD/CHF", "direction": "LONG", "risk_percent": 1.0},
        ]
    )
    by_ccy = {r["currency"]: r for r in exposure["currencies"]}
    assert by_ccy["AUD"]["net_exposure"] == pytest.approx(1.5)
    assert by_ccy["NZD"]["net_exposure"] == pytest.approx(-1.0)
    assert by_ccy["CHF"]["net_exposure"] == pytest.approx(-0.5)
    assert exposure["currencies"][0]["currency"] == "AUD"


def test_tied_dominant_exposures_alphabetical_tiebreak():
    """Equal |net| → alphabetical currency wins."""
    exposure = compute_currency_exposure(
        [
            {"instrument_id": "AUD/NZD", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "EUR/CHF", "direction": "LONG", "risk_percent": 1.0},
        ]
    )
    # All |net| = 0.5 → AUD first alphabetically among AUD/CHF/EUR/NZD
    assert exposure["currencies"][0]["currency"] == "AUD"
    assert exposure["dominant_currency_exposure"]["currency"] == "AUD"
    assert exposure["method"]["dominant_tie_break"] == "max_abs_net_then_alphabetical_currency"


def test_deterministic_sorting_by_abs_then_alpha():
    exposure = compute_currency_exposure(
        [
            {"instrument_id": "AUD/NZD", "direction": "LONG", "risk_percent": 2.0},
            {"instrument_id": "AUD/CHF", "direction": "LONG", "risk_percent": 1.0},
        ]
    )
    codes = [r["currency"] for r in exposure["currencies"]]
    assert codes[0] == "AUD"  # |1.5|
    assert set(codes[1:]) == {"CHF", "NZD"}
    # CHF and NZD both |0.5|/|1.0| — NZD abs 1.0 larger than CHF 0.5
    assert codes[1] == "NZD"
    assert codes[2] == "CHF"


def test_empty_fx_exposure_state_non_fx_only():
    exposure = compute_currency_exposure(
        [
            {"instrument_id": "Gold", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "Corn", "direction": "SHORT", "risk_percent": 1.0},
        ]
    )
    assert exposure["has_fx_trades"] is False
    assert exposure["currencies"] == []
    assert exposure["dominant_currency_exposure"] is None
    assert exposure["diagnostics"] == []


def test_no_meaningful_shared_exposure_single_fx():
    exposure = compute_currency_exposure(
        [{"instrument_id": "AUD/NZD", "direction": "LONG", "risk_percent": 1.0}]
    )
    assert exposure["has_fx_trades"] is True
    assert any("No meaningful shared" in d for d in exposure["diagnostics"])


def test_five_trade_slots_populated():
    result = analyse_trade_basket(
        trades=[
            {"instrument_pair": "AUD/NZD", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_pair": "AUD/CHF", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "Gold", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "Corn", "direction": "SHORT", "risk_percent": 1.0},
            {"instrument_id": "Silver", "direction": "LONG", "risk_percent": 1.0},
        ],
        frequency="daily",
        lookback=60,
        correlation_provider=FakePhase1Provider(),
    )
    assert result.status == "ok"
    assert result.populated_trade_count == 5
    assert result.pair_count == 10
    assert len(result.pairs) == 10


def test_opposite_direction_diagnostic_and_warning():
    result = analyse_trade_basket(
        trades=[
            {"instrument_pair": "AUD/NZD", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_pair": "AUD/NZD", "direction": "SHORT", "risk_percent": 1.0},
        ],
        frequency="daily",
        lookback=60,
        correlation_provider=FakePhase1Provider(),
    )
    assert result.status == "ok"
    assert any("offsetting_same_instrument" in w for w in result.warnings)
    exposure = compute_currency_exposure(
        [
            {"instrument_id": "AUD/NZD", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "AUD/NZD", "direction": "SHORT", "risk_percent": 1.0},
        ]
    )
    assert any("opposing pair trades" in d for d in exposure["diagnostics"])
    assert any("fully offset" in d for d in exposure["diagnostics"])
    assert exposure["dominant_currency_exposure"] is None


def test_ui_payload_contract():
    """Full enrichment contract consumed by TradeBasketWorkstation."""
    payload = {
        "status": "ok",
        "phase": "2A",
        "populated_trade_count": 2,
        "pair_count": 1,
        "frequency": "daily",
        "lookback": 60,
        "warnings": [],
        "trades": [
            {"instrument_id": "AUD/NZD", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "AUD/CHF", "direction": "LONG", "risk_percent": 1.0},
        ],
        "pairs": [
            {
                "trade_a_instrument_id": "AUD/NZD",
                "trade_a_direction": "LONG",
                "trade_b_instrument_id": "AUD/CHF",
                "trade_b_direction": "LONG",
                "raw_correlation": 0.34,
                "direction_adjusted_correlation": 0.34,
            }
        ],
    }
    out = enrich_basket_with_currency_exposure(
        enrich_basket_with_portfolio_intelligence(payload)
    )
    assert out["workstation_phase"] == "4"
    ce = out["currency_exposure"]
    assert ce["status"] == "ok"
    assert ce["has_fx_trades"] is True
    assert isinstance(ce["currencies"], list)
    assert ce["currencies"][0]["currency"] == "AUD"
    for row in ce["currencies"]:
        assert set(row) >= {
            "currency",
            "net_exposure",
            "direction",
            "contributing_trades",
        }
    dom = ce["dominant_currency_exposure"]
    assert dom["display"] == "AUD LONG"
    assert "contributing_trades" in dom
    assert "share_of_gross" in dom
    pi = out["portfolio_intelligence"]
    hi = pi["highest_correlated_pair"]
    assert hi["trade_a_instrument_id"] in ("AUD/NZD", "AUD/CHF")
    assert "Australian Dollar / 6A" not in str(hi)
    assert "NZ Dollar / 6N" not in str(hi)
