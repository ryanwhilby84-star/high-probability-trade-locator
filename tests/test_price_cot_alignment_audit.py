"""Price ↔ COT alignment audit — gap math and markdown overall status."""

from __future__ import annotations

from hptl.prices.price_cot_alignment_audit import (
    MAX_ALIGNMENT_GAP_DAYS,
    _bar_match,
    _gap_days,
    _symbols_equivalent,
    render_markdown,
)


def test_max_gap_constant():
    assert MAX_ALIGNMENT_GAP_DAYS == 5


def test_gap_days():
    assert _gap_days("2026-07-21", "2026-07-23") == 2
    assert _gap_days("2026-07-09", "2026-07-21") == 12


def test_yahoo_prefixed_symbols_match():
    assert _symbols_equivalent("yahoo:KC=F", "KC=F")
    assert _symbols_equivalent("NAS100_USD", "NAS100USD")
    assert not _symbols_equivalent("NATGAS_USD", "XAU_USD")


def test_bar_match_tolerates_tiny_float_noise():
    a = {"date": "2026-07-23", "open": 2.92, "high": 2.989, "low": 2.888, "close": 2.919}
    b = {**a, "close": 2.9190001}
    assert _bar_match(a, b)


def test_render_markdown_ends_with_overall_status():
    md = render_markdown(
        {
            "generated_at": "2026-07-26T00:00:00+00:00",
            "max_alignment_gap_days": 5,
            "summary": {
                "markets_total": 1,
                "pass_count": 0,
                "fail_count": 1,
                "overall_status": "FAIL",
                "gate_open": False,
            },
            "frontend_cache": {
                "status": "PASS",
                "uses_cache_no_store": True,
                "uses_cache_bust_query": True,
            },
            "instruments": [
                {
                    "instrument": "Natural Gas / NG",
                    "provider": "oanda",
                    "symbol": "NATGAS_USD",
                    "raw_daily_date": "2026-06-12",
                    "store_weekly_date": "2026-06-12",
                    "weekly_aggregation_date": "2026-06-12",
                    "workstation_weekly_date": "2026-06-12",
                    "cot_date": "2026-07-21",
                    "gap_days": 39,
                    "gap_weeks": 5.57,
                    "latest_ohlc": {
                        "date": "2026-06-12",
                        "open": 1,
                        "high": 2,
                        "low": 0.5,
                        "close": 1.5,
                    },
                    "status": "FAIL",
                    "failures": ["price/COT gap 39d exceeds max 5d"],
                    "stages": {"pipeline_break": "alignment"},
                }
            ],
            "failing_instruments": ["Natural Gas / NG"],
        }
    )
    assert "OVERALL STATUS" in md
    assert md.strip().endswith("FAIL")
    assert "Natural Gas / NG" in md
