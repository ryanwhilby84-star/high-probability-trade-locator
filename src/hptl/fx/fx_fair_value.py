"""FX fair-value regression — PREP ONLY / DISABLED STUB (V1).

.. warning::
    **Not used in production.** Reserved structure for a future historical
    fair-value model. V1 explicitly returns unavailable. Pillar FX valuation
    uses ``hptl.valuation.fx_carry_real_yield_v3`` instead.

This module reserves the structure for the future historical fair-value model:

    spot_price ~ 2Y_yield_differential   (per pair, rolling window)

V1 deliberately does **not** produce fair values. Returning fabricated numbers
without enough validated history would create fake precision and is explicitly
out of scope. When the regression is built and validated, ``estimate_fair_value``
should return a real ``fair_value_estimate`` + ``spot_deviation_pct`` and allow
``confidence`` to reach High in ``fx_valuation``.

TODO(fx-fair-value-v2):
  1. Assemble a per-pair time series of (spot, 2Y_yield_diff[, 10Y_yield_diff]).
     Source spot from the price store weekly bars; source yield diffs from a
     historical foreign-yield feed (not yet available — only US FRED today).
  2. Fit a rolling/expanding OLS: spot ~ a + b * yield_diff (guard min N, R^2).
  3. fair_value_estimate = a + b * current_yield_diff.
  4. spot_deviation_pct = (spot - fair_value_estimate) / fair_value_estimate * 100.
  5. Gate High confidence on sufficient sample size, stable beta, and acceptable
     fit quality; otherwise keep Medium/Low.
  6. Backtest-safety: only use data up to the as-of week (no look-ahead).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

MIN_OBSERVATIONS = 104  # ~2y of weekly data before a fit is even attempted
MIN_R_SQUARED = 0.30


@dataclass(frozen=True)
class FairValueEstimate:
    fair_value_estimate: float | None
    spot_deviation_pct: float | None
    model: str | None
    r_squared: float | None
    observations: int
    sufficient_data: bool
    note: str


def _unavailable(note: str, observations: int = 0) -> FairValueEstimate:
    return FairValueEstimate(
        fair_value_estimate=None,
        spot_deviation_pct=None,
        model=None,
        r_squared=None,
        observations=observations,
        sufficient_data=False,
        note=note,
    )


def estimate_fair_value(
    *,
    spot: float | None,
    yield_diff_history: Sequence[float] | None = None,
    spot_history: Sequence[float] | None = None,
) -> FairValueEstimate:
    """V1 stub: returns an explicit 'not available' estimate.

    Never fabricates a fair value. The signature is the contract the V2
    regression will implement.
    """
    n = 0
    if yield_diff_history and spot_history:
        n = min(len(yield_diff_history), len(spot_history))
    if n < MIN_OBSERVATIONS:
        return _unavailable(
            "Fair-value regression not built (FX Valuation V1). Insufficient validated "
            "historical yield-differential / spot series available.",
            observations=n,
        )
    # NOTE: Even with enough rows, V1 does not fit a model. See module TODO.
    return _unavailable(
        "Fair-value regression not yet enabled — V1 reports yield-differential valuation only.",
        observations=n,
    )


def as_fields(est: FairValueEstimate) -> dict[str, Any]:
    return {
        "fx_fair_value_estimate": est.fair_value_estimate,
        "fx_spot_deviation_pct": est.spot_deviation_pct,
        "fx_fair_value_model": est.model,
        "fx_fair_value_r_squared": est.r_squared,
        "fx_fair_value_observations": est.observations,
        "fx_fair_value_sufficient_data": est.sufficient_data,
        "fx_fair_value_note": est.note,
    }
