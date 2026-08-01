"""Trade basket analysis engine — Phase 2A orchestration."""

from __future__ import annotations

from typing import Any

from hptl.trade_basket.calculator import (
    DirectionAdjustedCorrelationCalculator,
    Phase1CorrelationProvider,
)
from hptl.trade_basket.models import TradeBasketInput, TradeBasketResult
from hptl.trade_basket.pairs import TradePairGenerator
from hptl.trade_basket.validator import TradeBasketValidator

ENGINE_VERSION = "trade_basket_v2a"


class TradeBasketEngine:
    def __init__(
        self,
        *,
        validator: TradeBasketValidator | None = None,
        calculator: DirectionAdjustedCorrelationCalculator | None = None,
        correlation_provider: Phase1CorrelationProvider | None = None,
    ) -> None:
        self._validator = validator or TradeBasketValidator()
        if calculator is not None:
            self._calculator = calculator
        else:
            self._calculator = DirectionAdjustedCorrelationCalculator(
                correlation_provider=correlation_provider
            )

    def analyse(
        self,
        *,
        trades: list[Any],
        frequency: str = "daily",
        lookback: int = 60,
    ) -> TradeBasketResult:
        basket = TradeBasketInput(
            trades=list(trades or []),
            frequency=frequency,
            lookback=lookback,
        )
        populated, errors = self._validator.validate(basket)
        validation_warnings = list(getattr(self._validator, "last_warnings", []) or [])
        freq = str(frequency or "daily").strip().lower()
        try:
            lb = int(lookback)
        except (TypeError, ValueError):
            lb = 0

        expected_pairs = TradePairGenerator.expected_pair_count(len(populated))

        if errors or lb <= 0 or freq not in ("daily", "weekly"):
            return TradeBasketResult(
                status="error",
                populated_trade_count=len(populated),
                pair_count=0,
                pairs=[],
                trades=populated,
                frequency=freq,
                lookback=lb if lb > 0 else 0,
                errors=errors,
                warnings=validation_warnings,
                correlation_source=self._calculator.correlation_source,
            )

        pairs, calc_errors, warnings, phase1_engine = self._calculator.calculate(
            populated, frequency=freq, lookback=lb
        )
        warnings = validation_warnings + list(warnings)
        if calc_errors or len(pairs) != expected_pairs:
            errs = list(calc_errors)
            if len(pairs) != expected_pairs and expected_pairs > 0 and not calc_errors:
                errs.append(
                    f"pair_count_mismatch: got={len(pairs)} expected={expected_pairs}"
                )
            return TradeBasketResult(
                status="error",
                populated_trade_count=len(populated),
                pair_count=expected_pairs,
                pairs=[],
                trades=populated,
                frequency=freq,
                lookback=lb,
                errors=errs,
                warnings=warnings,
                phase1_engine=phase1_engine,
                correlation_source=self._calculator.correlation_source,
            )

        return TradeBasketResult(
            status="ok",
            populated_trade_count=len(populated),
            pair_count=expected_pairs,
            pairs=pairs,
            trades=populated,
            frequency=freq,
            lookback=lb,
            errors=[],
            warnings=warnings,
            phase1_engine=phase1_engine,
            correlation_source=self._calculator.correlation_source,
        )


def analyse_trade_basket(
    *,
    trades: list[Any],
    frequency: str = "daily",
    lookback: int = 60,
    correlation_provider: Phase1CorrelationProvider | None = None,
) -> TradeBasketResult:
    return TradeBasketEngine(correlation_provider=correlation_provider).analyse(
        trades=trades, frequency=frequency, lookback=lookback
    )
