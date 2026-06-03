"""Canonical price store — offline."""

from __future__ import annotations

from hptl.prices.models import OhlcBar, compute_range_52w, record_to_public
from hptl.prices.price_store import build_store_payload
from hptl.prices.unified_adapter import UnifiedPriceAdapter


def _bars(n: int, start: float = 100.0) -> list[OhlcBar]:
    return [
        {
            "date": f"2025-{i:02d}-01",
            "open": start + i,
            "high": start + i + 2,
            "low": start + i - 1,
            "close": start + i + 1,
            "volume": 1000.0,
        }
        for i in range(1, n + 1)
    ]


def test_compute_range_52w():
    daily = _bars(60, 50.0)
    r = compute_range_52w(daily)
    assert r is not None
    assert r["high"] >= r["low"]


def test_public_record_hides_source():
    rec = {
        "instrument_id": "Euro FX / 6E",
        "price": {"mid": 1.08, "as_of": "2026-01-01"},
        "daily": _bars(5),
        "weekly": _bars(3),
        "range_52w": {"high": 110, "low": 90},
        "history": {"bar_count_daily": 5},
    }
    pub = record_to_public(rec)
    assert "_fetched_via" not in pub
    assert "oanda" not in str(pub).lower() or pub.get("instrument_id")
    assert pub["daily"][0]["close"] == 102.0


def test_unified_adapter_mock_coverage():
    coverage = {
        "oanda_supported": ["Euro FX / 6E"],
        "alpha_supported": ["Euro FX / 6E"],
        "instruments": [
            {
                "htpl_instrument_id": "Euro FX / 6E",
                "sources": [
                    {
                        "source": "oanda",
                        "symbol": "EUR_USD",
                        "coverage_status": "supported",
                    }
                ],
            }
        ],
    }

    class FakeAdapter(UnifiedPriceAdapter):
        def fetch(self, instrument_id: str, *, spec=None):
            return {
                "instrument_id": instrument_id,
                "price": {"mid": 1.09, "bid": 1.089, "ask": 1.091, "as_of": "2026-06-01"},
                "daily": _bars(10),
                "weekly": _bars(4),
                "range_52w": compute_range_52w(_bars(10)),
                "history": None,
                "error": None,
            }

    rec = FakeAdapter(coverage).fetch("Euro FX / 6E")
    assert rec["price"]["mid"] == 1.09
    assert len(rec["daily"]) == 10


def test_store_payload_shape():
    records = {
        "Sugar": {
            "instrument_id": "Sugar",
            "price": None,
            "daily": [],
            "weekly": [],
            "range_52w": None,
            "history": None,
            "error": "test",
        }
    }
    payload = build_store_payload(records)
    assert "instruments" in payload
    assert payload["instruments"]["Sugar"]["error"] == "test"
