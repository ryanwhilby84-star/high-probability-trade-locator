"""Placeholder macro contributors — Phase 5 architecture only.

Each module returns Unavailable with a deterministic summary. No live data.
"""

from __future__ import annotations

from hptl.macro_intelligence.models import MacroContributorResult

PLACEHOLDER_LAST_UPDATED = None


def _unavailable(contributor_id: str, name: str, instrument_id: str) -> MacroContributorResult:
    return MacroContributorResult(
        name=name,
        status="Unavailable",
        strength=None,
        summary=(
            f"{name} contributor is not yet connected for {instrument_id}. "
            "Phase 5 architecture placeholder — no live macro calculation."
        ),
        last_updated=PLACEHOLDER_LAST_UPDATED,
        weight=0.0,
        contributor_id=contributor_id,
    )


class InterestRatesContributor:
    contributor_id = "interest_rates"
    name = "Interest Rates"

    def evaluate(self, instrument_id: str) -> MacroContributorResult:
        return _unavailable(self.contributor_id, self.name, instrument_id)


class InflationContributor:
    contributor_id = "inflation"
    name = "Inflation"

    def evaluate(self, instrument_id: str) -> MacroContributorResult:
        return _unavailable(self.contributor_id, self.name, instrument_id)


class EconomicGrowthContributor:
    contributor_id = "economic_growth"
    name = "Economic Growth"

    def evaluate(self, instrument_id: str) -> MacroContributorResult:
        return _unavailable(self.contributor_id, self.name, instrument_id)


class CommodityExposureContributor:
    contributor_id = "commodity_exposure"
    name = "Commodity Exposure"

    def evaluate(self, instrument_id: str) -> MacroContributorResult:
        return _unavailable(self.contributor_id, self.name, instrument_id)


class RiskSentimentContributor:
    contributor_id = "risk_sentiment"
    name = "Risk Sentiment"

    def evaluate(self, instrument_id: str) -> MacroContributorResult:
        return _unavailable(self.contributor_id, self.name, instrument_id)


class CentralBanksContributor:
    contributor_id = "central_banks"
    name = "Central Banks"

    def evaluate(self, instrument_id: str) -> MacroContributorResult:
        return _unavailable(self.contributor_id, self.name, instrument_id)


class GovernmentBondsContributor:
    contributor_id = "government_bonds"
    name = "Government Bonds"

    def evaluate(self, instrument_id: str) -> MacroContributorResult:
        return _unavailable(self.contributor_id, self.name, instrument_id)


class DollarEnvironmentContributor:
    contributor_id = "dollar_environment"
    name = "Dollar Environment"

    def evaluate(self, instrument_id: str) -> MacroContributorResult:
        return _unavailable(self.contributor_id, self.name, instrument_id)


def default_contributors() -> list[object]:
    """Stable ordered registry of Phase 5 placeholder contributors."""
    return [
        InterestRatesContributor(),
        InflationContributor(),
        EconomicGrowthContributor(),
        CommodityExposureContributor(),
        RiskSentimentContributor(),
        CentralBanksContributor(),
        GovernmentBondsContributor(),
        DollarEnvironmentContributor(),
    ]
