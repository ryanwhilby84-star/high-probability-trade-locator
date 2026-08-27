"""Phase 2A trade-basket mathematics — automated validation."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from hptl.trade_basket.calculator import (
    DirectionAdjustedCorrelationCalculator,
    Phase1ServiceCorrelationProvider,
    direction_adjusted_correlation,
)
from hptl.trade_basket.engine import analyse_trade_basket
from hptl.trade_basket.models import TradeEntry
from hptl.trade_basket.pairs import TradePairGenerator
from hptl.trade_basket.service import build_trade_basket_payload
from hptl.trade_basket.validator import TradeBasketValidator

# Validated independent Gold/Silver daily 60-period Pearson
GOLD_SILVER_RAW = 0.8578954035833313
GOLD_SILVER_RAW_PHASE1 = 0.857895  # Phase 1 engine rounds to 6 d.p.


class FakePhase1Provider:
    """Injected Phase 1 stand-in — proves basket does not compute Pearson."""

    source = "fake_phase1_provider"

    def __init__(self, raw: float = GOLD_SILVER_RAW, overlap: int = 60) -> None:
        self.raw = raw
        self.overlap = overlap
        self.get_map_calls = 0

    def get_map(self, instrument_ids, *, frequency, lookback):
        self.get_map_calls += 1
        ids = list(instrument_ids)
        n = len(ids)
        matrix = [[None] * n for _ in range(n)]
        pair_meta = {}
        for i, a in enumerate(ids):
            matrix[i][i] = 1.0
            for j in range(i + 1, n):
                b = ids[j]
                matrix[i][j] = self.raw
                matrix[j][i] = self.raw
                pair_meta[f"{a}||{b}"] = {
                    "status": "ok",
                    "overlap": self.overlap,
                }
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


def test_direction_adjustment_formula():
    assert direction_adjusted_correlation(0.5, "LONG", "LONG") == 0.5
    assert direction_adjusted_correlation(0.5, "LONG", "SHORT") == -0.5
    assert direction_adjusted_correlation(0.5, "SHORT", "SHORT") == 0.5
    assert direction_adjusted_correlation(-0.4, "LONG", "LONG") == -0.4
    assert direction_adjusted_correlation(-0.4, "LONG", "SHORT") == 0.4


def test_pair_counts():
    assert TradePairGenerator.expected_pair_count(0) == 0
    assert TradePairGenerator.expected_pair_count(1) == 0
    assert TradePairGenerator.expected_pair_count(2) == 1
    assert TradePairGenerator.expected_pair_count(3) == 3
    assert TradePairGenerator.expected_pair_count(4) == 6
    assert TradePairGenerator.expected_pair_count(5) == 10


def test_1_long_gold_long_silver():
    result = analyse_trade_basket(
        trades=[
            {"instrument_id": "Gold", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "Silver", "direction": "LONG", "risk_percent": 1.0},
        ],
        frequency="daily",
        lookback=60,
        correlation_provider=FakePhase1Provider(GOLD_SILVER_RAW),
    )
    assert result.status == "ok"
    assert result.populated_trade_count == 2
    assert result.pair_count == 1
    pair = result.pairs[0]
    assert pair.raw_correlation == pytest.approx(GOLD_SILVER_RAW)
    assert pair.direction_adjusted_correlation == pytest.approx(GOLD_SILVER_RAW)
    assert pair.direction_adjusted_correlation > 0


def test_2_long_gold_short_silver():
    result = analyse_trade_basket(
        trades=[
            {"instrument_id": "Gold", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "Silver", "direction": "SHORT", "risk_percent": 1.0},
        ],
        frequency="daily",
        lookback=60,
        correlation_provider=FakePhase1Provider(GOLD_SILVER_RAW),
    )
    assert result.status == "ok"
    pair = result.pairs[0]
    assert pair.raw_correlation == pytest.approx(GOLD_SILVER_RAW)
    assert pair.direction_adjusted_correlation == pytest.approx(-GOLD_SILVER_RAW)
    assert pair.direction_adjusted_correlation < 0


def test_3_short_gold_short_silver():
    result = analyse_trade_basket(
        trades=[
            {"instrument_id": "Gold", "direction": "SHORT", "risk_percent": 1.0},
            {"instrument_id": "Silver", "direction": "SHORT", "risk_percent": 1.0},
        ],
        frequency="daily",
        lookback=60,
        correlation_provider=FakePhase1Provider(GOLD_SILVER_RAW),
    )
    assert result.status == "ok"
    pair = result.pairs[0]
    assert pair.raw_correlation > 0
    assert pair.direction_adjusted_correlation == pytest.approx(GOLD_SILVER_RAW)
    assert pair.direction_adjusted_correlation > 0


def test_4_five_trades_ten_pairs():
    trades = [
        {"instrument_id": "Gold", "direction": "LONG", "risk_percent": 1.0},
        {"instrument_id": "Silver", "direction": "SHORT", "risk_percent": 1.0},
        {"instrument_id": "Crude Oil / CL", "direction": "LONG", "risk_percent": 1.0},
        {"instrument_id": "Copper / HG", "direction": "SHORT", "risk_percent": 1.0},
        {"instrument_id": "Euro FX / 6E", "direction": "LONG", "risk_percent": 1.0},
    ]
    result = analyse_trade_basket(
        trades=trades,
        frequency="daily",
        lookback=60,
        correlation_provider=FakePhase1Provider(0.25),
    )
    assert result.status == "ok"
    assert result.populated_trade_count == 5
    assert result.pair_count == 10
    assert len(result.pairs) == 10
    # unique unordered pairs
    keys = {
        frozenset(
            [
                (p.trade_a_instrument_id, p.trade_a_direction),
                (p.trade_b_instrument_id, p.trade_b_direction),
            ]
        )
        for p in result.pairs
    }
    assert len(keys) == 10


def test_5_empty_slots_ignored():
    result = analyse_trade_basket(
        trades=[
            {"instrument_id": "Gold", "direction": "LONG", "risk_percent": 1.0},
            None,
            {"instrument_id": "Silver", "direction": "SHORT", "risk_percent": 1.0},
            {},
            {"instrument_id": "Copper / HG", "direction": "LONG", "risk_percent": 1.0},
        ],
        frequency="daily",
        lookback=60,
        correlation_provider=FakePhase1Provider(0.1),
    )
    assert result.status == "ok"
    assert result.populated_trade_count == 3
    assert result.pair_count == 3
    assert len(result.pairs) == 3


def test_6_invalid_direction():
    result = analyse_trade_basket(
        trades=[
            {"instrument_id": "Gold", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "Silver", "direction": "FLAT", "risk_percent": 1.0},
        ],
        frequency="daily",
        lookback=60,
        correlation_provider=FakePhase1Provider(),
    )
    assert result.status == "error"
    assert any("invalid_direction" in e for e in result.errors)


def test_7_duplicate_instrument_same_direction():
    result = analyse_trade_basket(
        trades=[
            {"instrument_id": "Gold", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "Gold", "direction": "LONG", "risk_percent": 1.0},
        ],
        frequency="daily",
        lookback=60,
        correlation_provider=FakePhase1Provider(),
    )
    assert result.status == "error"
    assert any("duplicate_instrument_direction" in e for e in result.errors)


def test_7b_same_instrument_opposite_directions_accepted_phase4():
    """Phase 4: same instrument LONG+SHORT is accepted as offsetting trades."""
    result = analyse_trade_basket(
        trades=[
            {"instrument_id": "Gold", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "Gold", "direction": "SHORT", "risk_percent": 1.0},
        ],
        frequency="daily",
        lookback=60,
        correlation_provider=FakePhase1Provider(),
    )
    assert result.status == "ok"
    assert result.populated_trade_count == 2
    assert result.pair_count == 1
    assert any("offsetting_same_instrument" in w for w in result.warnings)
    pair = result.pairs[0]
    # Same series → raw ≈ +1; LONG×SHORT → adjusted ≈ −1
    assert pair.raw_correlation == pytest.approx(1.0)
    assert pair.direction_adjusted_correlation == pytest.approx(-1.0)


def test_8_phase1_reuse_architecture():
    """Basket calculator must not import Phase 1 return/price/Pearson modules."""
    calc_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "hptl"
        / "trade_basket"
        / "calculator.py"
    )
    tree = ast.parse(calc_path.read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imported.add(node.module)

    forbidden = {
        "hptl.correlation_matrix.returns",
        "hptl.correlation_matrix.prices",
        "hptl.correlation_matrix.methods",
        "hptl.correlation_matrix.alignment",
        "hptl.correlation_matrix.engine",
    }
    assert not (imported & forbidden), f"forbidden imports: {imported & forbidden}"

    # Production provider points at Phase 1 service helper
    provider = Phase1ServiceCorrelationProvider()
    assert "correlation_matrix.service" in provider.source

    # Runtime: basket asks provider, does not invent values
    fake = FakePhase1Provider(0.42)
    result = analyse_trade_basket(
        trades=[
            {"instrument_id": "Gold", "direction": "LONG"},
            {"instrument_id": "Silver", "direction": "SHORT"},
        ],
        correlation_provider=fake,
    )
    assert fake.get_map_calls == 1
    assert result.pairs[0].raw_correlation == 0.42
    assert result.correlation_source == "fake_phase1_provider"


def test_unknown_instrument_rejected():
    result = analyse_trade_basket(
        trades=[
            {"instrument_id": "Not A Real Market", "direction": "LONG"},
            {"instrument_id": "Silver", "direction": "LONG"},
        ],
        correlation_provider=FakePhase1Provider(),
    )
    assert result.status == "error"
    assert any("unknown_instrument_id" in e for e in result.errors)


def test_risk_percent_stored_not_used():
    result = analyse_trade_basket(
        trades=[
            {"instrument_id": "Gold", "direction": "LONG", "risk_percent": 5.0},
            {"instrument_id": "Silver", "direction": "SHORT", "risk_percent": 0.25},
        ],
        correlation_provider=FakePhase1Provider(0.5),
    )
    assert result.status == "ok"
    assert result.trades[0].risk_percent == 5.0
    assert result.trades[1].risk_percent == 0.25
    # Same adjusted result as 1.0 risk would produce
    assert result.pairs[0].direction_adjusted_correlation == pytest.approx(-0.5)


def test_live_phase1_gold_silver_long_short():
    """Integration: real Phase 1 service for validated Gold/Silver pair."""
    payload = build_trade_basket_payload(
        frequency="daily",
        lookback=60,
        trades=[
            {"instrument_id": "Gold", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "Silver", "direction": "SHORT", "risk_percent": 1.0},
        ],
    )
    assert payload["status"] == "ok"
    assert payload["pair_count"] == 1
    pair = payload["pairs"][0]
    assert pair["raw_correlation"] == pytest.approx(GOLD_SILVER_RAW_PHASE1, abs=1e-6)
    assert pair["direction_adjusted_correlation"] == pytest.approx(
        -GOLD_SILVER_RAW_PHASE1, abs=1e-6
    )
    assert pair["overlapping_return_count"] == 60
    assert payload["correlation_source"].endswith("get_instrument_correlation_map")
    assert payload["risk_percent_affects_calculations"] is False
