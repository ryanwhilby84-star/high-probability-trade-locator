"""Direction-adjusted correlation.

Formula (Phase 2A):
    direction_adjusted_correlation = raw_correlation × direction_a × direction_b

where LONG = +1 and SHORT = −1.
"""

from __future__ import annotations

import math
from typing import Any, Protocol

from hptl.trade_basket.models import DIRECTION_SIGN, TradeEntry, TradePairResult
from hptl.trade_basket.pairs import TradePairGenerator


class Phase1CorrelationProvider(Protocol):
    """Abstraction over Phase 1 correlation service (injectable for tests)."""

    def get_map(
        self,
        instrument_ids: list[str],
        *,
        frequency: str,
        lookback: int,
    ) -> dict[str, Any]:
        ...

    def lookup_pair(
        self,
        corr_map: dict[str, Any],
        instrument_a: str,
        instrument_b: str,
    ) -> dict[str, Any]:
        ...


class Phase1ServiceCorrelationProvider:
    """Production provider — delegates to Phase 1 service only."""

    source = "hptl.correlation_matrix.service.get_instrument_correlation_map"

    def get_map(
        self,
        instrument_ids: list[str],
        *,
        frequency: str,
        lookback: int,
    ) -> dict[str, Any]:
        from hptl.correlation_matrix.service import get_instrument_correlation_map

        return get_instrument_correlation_map(
            instrument_ids,
            frequency=frequency,
            lookback=lookback,
        )

    def lookup_pair(
        self,
        corr_map: dict[str, Any],
        instrument_a: str,
        instrument_b: str,
    ) -> dict[str, Any]:
        from hptl.correlation_matrix.service import lookup_raw_correlation

        return lookup_raw_correlation(corr_map, instrument_a, instrument_b)


def direction_adjusted_correlation(
    raw_correlation: float,
    direction_a: str,
    direction_b: str,
) -> float:
    """raw × sign(a) × sign(b)."""
    sa = DIRECTION_SIGN[str(direction_a).upper()]
    sb = DIRECTION_SIGN[str(direction_b).upper()]
    return float(raw_correlation) * sa * sb


class DirectionAdjustedCorrelationCalculator:
    """Build pair results using Phase 1 raw correlations + direction signs."""

    def __init__(
        self,
        *,
        correlation_provider: Phase1CorrelationProvider | None = None,
        pair_generator: TradePairGenerator | None = None,
    ) -> None:
        self._provider = correlation_provider or Phase1ServiceCorrelationProvider()
        self._pairs = pair_generator or TradePairGenerator()

    @property
    def correlation_source(self) -> str:
        return getattr(self._provider, "source", type(self._provider).__name__)

    def calculate(
        self,
        trades: list[TradeEntry],
        *,
        frequency: str,
        lookback: int,
    ) -> tuple[list[TradePairResult], list[str], list[str], str | None]:
        """Returns (pairs, errors, warnings, phase1_engine)."""
        errors: list[str] = []
        warnings: list[str] = []
        if len(trades) < 2:
            return [], errors, warnings, None

        ids = [t.instrument_id for t in trades]
        corr_map = self._provider.get_map(ids, frequency=frequency, lookback=lookback)
        if corr_map.get("status") != "ok":
            detail = corr_map.get("error") or corr_map.get("status")
            msg = corr_map.get("message")
            errors.append(
                f"phase1_correlation_unavailable:{detail}"
                + (f":{msg}" if msg else "")
            )
            return [], errors, warnings, corr_map.get("engine")

        warnings.extend(str(w) for w in (corr_map.get("warnings") or []))
        phase1_engine = corr_map.get("engine")

        results: list[TradePairResult] = []
        for a, b in self._pairs.generate(trades):
            looked = self._provider.lookup_pair(
                corr_map, a.instrument_id, b.instrument_id
            )
            if looked.get("status") != "ok" or looked.get("raw_correlation") is None:
                errors.append(
                    f"missing_phase1_correlation:"
                    f"{a.instrument_id}×{b.instrument_id}:"
                    f"{looked.get('status') or 'unknown'}"
                )
                continue
            raw = float(looked["raw_correlation"])
            if not math.isfinite(raw):
                errors.append(
                    f"non_finite_correlation:{a.instrument_id}×{b.instrument_id}"
                )
                continue
            overlap = looked.get("overlapping_return_count")
            if overlap is None:
                errors.append(
                    f"missing_overlap_count:{a.instrument_id}×{b.instrument_id}"
                )
                continue
            try:
                overlap_i = int(overlap)
            except (TypeError, ValueError):
                errors.append(
                    f"invalid_overlap_count:{a.instrument_id}×{b.instrument_id}"
                )
                continue
            if overlap_i < lookback:
                errors.append(
                    f"insufficient_overlap:{a.instrument_id}×{b.instrument_id}:"
                    f"overlap={overlap_i} required={lookback}"
                )
                continue

            adj = direction_adjusted_correlation(raw, a.direction, b.direction)
            results.append(
                TradePairResult(
                    trade_a_instrument_id=a.instrument_id,
                    trade_a_direction=a.direction,
                    trade_b_instrument_id=b.instrument_id,
                    trade_b_direction=b.direction,
                    raw_correlation=raw,
                    direction_adjusted_correlation=adj,
                    frequency=frequency,
                    lookback=lookback,
                    overlapping_return_count=overlap_i,
                )
            )

        return results, errors, warnings, phase1_engine
