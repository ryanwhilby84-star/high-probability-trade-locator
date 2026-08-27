"""Unique unordered trade-pair generation."""

from __future__ import annotations

from hptl.trade_basket.models import TradeEntry


class TradePairGenerator:
    """Generate every unique pair once (i < j). Never self-pairs."""

    def generate(self, trades: list[TradeEntry]) -> list[tuple[TradeEntry, TradeEntry]]:
        pairs: list[tuple[TradeEntry, TradeEntry]] = []
        n = len(trades)
        for i in range(n):
            for j in range(i + 1, n):
                a, b = trades[i], trades[j]
                # Exact duplicate (same instrument + same direction) is rejected
                # by the validator. Same instrument opposite directions are
                # valid offsetting trades and must still form a pair (Phase 4).
                if a.instrument_id == b.instrument_id and a.direction == b.direction:
                    continue
                pairs.append((a, b))
        return pairs

    @staticmethod
    def expected_pair_count(populated_trade_count: int) -> int:
        n = populated_trade_count
        return n * (n - 1) // 2 if n >= 2 else 0
