"""Canonical price store and coverage audits."""

from hptl.prices.price_coverage_audit import build_price_coverage_audit, write_price_coverage_audit
from hptl.prices.price_store import load_price_store, write_price_store

__all__ = [
    "UnifiedPriceAdapter",
    "build_price_coverage_audit",
    "write_price_coverage_audit",
    "load_price_store",
    "write_price_store",
]


def __getattr__(name: str):
    # Lazy export avoids circular import: oanda_prices → prices.models → prices.__init__ → unified_adapter → oanda_prices
    if name == "UnifiedPriceAdapter":
        from hptl.prices.unified_adapter import UnifiedPriceAdapter

        return UnifiedPriceAdapter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
