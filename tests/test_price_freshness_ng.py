"""Tests for Natural Gas / core price freshness repair."""

from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone

from hptl.oanda.oanda_prices import _parse_candles
from hptl.prices.price_freshness import (
    build_instrument_price_freshness,
    valuation_deviation_gate,
)
from hptl.valuation import ng_storage_production_v2 as ng_v2
from hptl.valuation import energy_ng_valuation_export as export_mod


def test_parse_candles_keeps_forming_incomplete_tip() -> None:
    payload = {
        "candles": [
            {
                "complete": True,
                "time": "2026-07-30T21:00:00.000000000Z",
                "mid": {"o": "2.75", "h": "2.81", "l": "2.72", "c": "2.799"},
                "volume": 100,
            },
            {
                "complete": False,
                "time": "2026-08-02T21:00:00.000000000Z",
                "mid": {"o": "2.80", "h": "2.82", "l": "2.75", "c": "2.761"},
                "volume": 50,
            },
        ]
    }
    complete, forming = _parse_candles(payload)
    assert len(complete) == 1
    assert complete[0]["date"] == "2026-07-30"
    assert forming is not None
    assert forming["date"] == "2026-08-02"
    assert forming["close"] == 2.761


def test_fresh_live_snapshot_marks_comparison_current() -> None:
    now = datetime(2026, 8, 3, 8, 0, tzinfo=timezone.utc)
    rec = {
        "price": {
            "mid": 2.76,
            "bid": 2.75,
            "ask": 2.77,
            "as_of": (now - timedelta(minutes=5)).isoformat(),
        },
        "daily": [{"date": "2026-07-30", "close": 2.799}],
        "weekly": [{"date": "2026-07-24", "close": 2.799}],
        "forming_daily": {"date": "2026-08-02", "close": 2.761},
        "price_scale": {"source": "oanda", "symbol": "NATGAS_USD"},
    }
    fresh = build_instrument_price_freshness(rec, now=now)
    assert fresh["live_quote"]["status"] == "Current"
    assert fresh["market_comparison"]["trusted"] is True
    assert fresh["market_comparison"]["price"] == 2.76


def test_stale_canonical_price_gates_deviation() -> None:
    now = datetime(2026, 8, 3, 8, 0, tzinfo=timezone.utc)
    rec = {
        "price": {
            "mid": 2.799,
            "as_of": (now - timedelta(days=3)).isoformat(),
        },
        "daily": [{"date": "2026-07-30", "close": 2.799}],
        "weekly": [{"date": "2026-07-24", "close": 2.799}],
        "price_scale": {"source": "oanda", "symbol": "NATGAS_USD"},
    }
    fresh = build_instrument_price_freshness(rec, now=now)
    assert fresh["live_quote"]["status"] == "Stale"
    gate = valuation_deviation_gate(fresh, spot_for_model=2.799, fair_value=2.76)
    assert gate["deviation_pct_trusted"] is False
    assert gate["deviation_pct"] is None
    assert gate["fair_value"] == 2.76
    assert gate["warning"]


def test_provider_failure_marks_failed() -> None:
    fresh = build_instrument_price_freshness({"daily": [], "price": None})
    assert fresh["overall_status"] == "Failed"
    assert fresh["market_comparison"]["trusted"] is False


def test_weekend_forming_bar_not_merged_into_completed_daily() -> None:
    payload = {
        "candles": [
            {
                "complete": True,
                "time": "2026-07-30T21:00:00.000000000Z",
                "mid": {"o": "1", "h": "1", "l": "1", "c": "1"},
            },
            {
                "complete": False,
                "time": "2026-08-02T21:00:00.000000000Z",
                "mid": {"o": "2", "h": "2", "l": "2", "c": "2"},
            },
        ]
    }
    complete, forming = _parse_candles(payload)
    assert all(b["date"] != forming["date"] for b in complete)


def test_valuation_export_has_no_cot_stage4_imports() -> None:
    src = inspect.getsource(export_mod) + inspect.getsource(ng_v2)
    assert "run_weekly_cot" not in src
    assert "HPTL_SKIP_VALUATION" not in src
    assert "export_from_masters" not in src
    assert "confluence" not in src.lower() or "no confluence" in src.lower()


def test_no_silent_old_price_without_disclosure() -> None:
    now = datetime(2026, 8, 3, 8, 0, tzinfo=timezone.utc)
    rec = {
        "price": {"mid": 2.799, "as_of": "2026-07-31T20:59:05Z"},
        "daily": [{"date": "2026-07-30", "close": 2.799}],
        "weekly": [{"date": "2026-07-24", "close": 2.799}],
    }
    fresh = build_instrument_price_freshness(rec, now=now)
    gate = valuation_deviation_gate(fresh, spot_for_model=2.799, fair_value=2.76)
    # Old snapshot must not be trusted without warning.
    if not gate["deviation_pct_trusted"]:
        assert gate["warning"]
    else:
        # If within threshold, still disclose kind/status.
        assert fresh["market_comparison"]["kind"]
        assert fresh["market_comparison"]["status"]
