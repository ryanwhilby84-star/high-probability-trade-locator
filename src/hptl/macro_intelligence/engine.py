"""Macro Intelligence Engine — aggregates independent contributors.

Phase 5: all contributors are placeholders → overall bias is Neutral.
Future phases may weight available contributors; Unavailable never invents bias.
"""

from __future__ import annotations

from typing import Any, Sequence

from hptl.macro_intelligence.contributors import default_contributors
from hptl.macro_intelligence.models import (
    ENGINE_VERSION,
    MacroBias,
    MacroContributorResult,
    MacroIntelligenceResult,
)


def _known_instrument_ids() -> set[str]:
    from hptl.markets.instrument_registry import LEGACY_COT_MARKETS, all_instrument_ids

    known = set(LEGACY_COT_MARKETS)
    try:
        known.update(all_instrument_ids(tradeable_only=True))
    except Exception:  # noqa: BLE001
        pass
    return known


def aggregate_overall_bias(
    contributors: Sequence[MacroContributorResult],
) -> MacroBias:
    """Deterministic overall bias from contributor statuses.

    Phase 5 rule: if every contributor is Unavailable (or the list is empty),
    return Neutral. No invented bullish/bearish lean from placeholders.
    """
    if not contributors:
        return "Neutral"
    if all(c.status == "Unavailable" for c in contributors):
        return "Neutral"
    # Future phases: map statuses → scores using weights. Reserved path.
    return "Neutral"


class MacroIntelligenceEngine:
    def __init__(self, contributors: Sequence[Any] | None = None) -> None:
        self._contributors = list(contributors) if contributors is not None else default_contributors()

    def analyse(self, instrument_id: str) -> MacroIntelligenceResult:
        iid = str(instrument_id or "").strip()
        errors: list[str] = []
        if not iid:
            return MacroIntelligenceResult(
                instrument_id="",
                overall_macro_bias="Neutral",
                contributors=[],
                status="error",
                errors=["missing_instrument_id"],
                notes=["Macro Intelligence requires an instrument_id."],
            )

        known = _known_instrument_ids()
        warnings: list[str] = []
        if iid not in known:
            # Still allow evaluation so FX pairs / registry IDs work; warn if unknown.
            from hptl.fx.currency_map import parse_fx_pair
            from hptl.markets.instrument_registry import get_instrument

            if get_instrument(iid) is None and parse_fx_pair(iid) is None:
                errors.append(f"unknown_instrument_id={iid!r}")
                return MacroIntelligenceResult(
                    instrument_id=iid,
                    overall_macro_bias="Neutral",
                    contributors=[],
                    status="error",
                    errors=errors,
                )

        results: list[MacroContributorResult] = []
        for contrib in self._contributors:
            results.append(contrib.evaluate(iid))

        bias = aggregate_overall_bias(results)
        return MacroIntelligenceResult(
            instrument_id=iid,
            overall_macro_bias=bias,
            contributors=results,
            status="ok",
            engine=ENGINE_VERSION,
            warnings=warnings,
            notes=[
                "Phase 5 architecture only — contributors are placeholders.",
                "Overall Macro Bias is Neutral until live contributors are connected.",
                "This engine does not generate buy/sell signals.",
            ],
        )


def analyse_macro_intelligence(
    instrument_id: str,
    *,
    contributors: Sequence[Any] | None = None,
) -> MacroIntelligenceResult:
    return MacroIntelligenceEngine(contributors=contributors).analyse(instrument_id)
