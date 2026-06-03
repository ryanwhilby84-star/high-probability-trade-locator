"""NASDAQ vs DGS10 — thin wrapper for backward compatibility.

Implementation lives in ``fred_relationship_pair``.
"""

from __future__ import annotations

from hptl.macro.fred_relationship_pair import build_nasdaq_dgs10_relationship_payload as build_nasdaq_dgs10_relationship_payload
from hptl.macro.fred_relationship_pair import format_relationship_digest


def _digest(c20: float | None) -> str:
    """Legacy hook for unit tests (wording may differ from historical strings)."""
    return format_relationship_digest(c20, "Nasdaq Composite", "US 10Y Treasury yield", "daily")
