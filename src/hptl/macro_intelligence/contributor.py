"""Macro contributor interface — independent, injectable modules."""

from __future__ import annotations

from typing import Protocol

from hptl.macro_intelligence.models import MacroContributorResult


class MacroContributor(Protocol):
    """Independent macro factor. Must not depend on other contributors."""

    contributor_id: str
    name: str

    def evaluate(self, instrument_id: str) -> MacroContributorResult:
        """Return a deterministic contributor result for ``instrument_id``."""
        ...
