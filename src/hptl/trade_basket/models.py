"""Phase 2A trade-basket models — inputs and results only (no scoring)."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

Direction = Literal["LONG", "SHORT"]

DIRECTION_SIGN: dict[str, int] = {
    "LONG": 1,
    "SHORT": -1,
}

DEFAULT_RISK_PERCENT = 1.0
MAX_BASKET_TRADES = 5


@dataclass(frozen=True)
class TradeEntry:
    instrument_id: str
    direction: Direction
    risk_percent: float = DEFAULT_RISK_PERCENT

    @property
    def direction_sign(self) -> int:
        return DIRECTION_SIGN[self.direction]

    def to_dict(self) -> dict[str, Any]:
        # instrument_pair mirrors instrument_id (Phase 4 FX pair input model).
        return {
            "instrument_id": self.instrument_id,
            "instrument_pair": self.instrument_id,
            "direction": self.direction,
            "risk_percent": self.risk_percent,
        }


@dataclass
class TradeBasketInput:
    trades: list[TradeEntry | None | dict[str, Any] | None] = field(default_factory=list)
    frequency: str = "daily"
    lookback: int = 60


@dataclass(frozen=True)
class TradePairResult:
    trade_a_instrument_id: str
    trade_a_direction: Direction
    trade_b_instrument_id: str
    trade_b_direction: Direction
    raw_correlation: float
    direction_adjusted_correlation: float
    frequency: str
    lookback: int
    overlapping_return_count: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class TradeBasketResult:
    status: str
    populated_trade_count: int
    pair_count: int
    pairs: list[TradePairResult] = field(default_factory=list)
    trades: list[TradeEntry] = field(default_factory=list)
    frequency: str = "daily"
    lookback: int = 60
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    phase1_engine: str | None = None
    correlation_source: str = "hptl.correlation_matrix.service.get_instrument_correlation_map"

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "populated_trade_count": self.populated_trade_count,
            "pair_count": self.pair_count,
            "pairs": [p.to_dict() for p in self.pairs],
            "trades": [t.to_dict() for t in self.trades],
            "frequency": self.frequency,
            "lookback": self.lookback,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "phase1_engine": self.phase1_engine,
            "correlation_source": self.correlation_source,
        }
