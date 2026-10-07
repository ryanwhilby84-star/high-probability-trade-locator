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


def test_oanda_start_dated_daily_and_weekly_candles_align_without_weakening_ohlc():
    from hptl.prices.workstation_ohlc_export import derive_weekly_ohlc_from_daily
    from hptl.prices.price_cot_alignment_audit import _oanda_week_end_bar, _compare_last_n_weeks
    # A previous Thursday belongs to the previous native Friday-to-Friday week.
    daily = [{'date': '2026-09-24', 'open': 80, 'high': 90, 'low': 70, 'close': 85, 'source': 'oanda'}]
    for i, d in enumerate(['2026-09-25', '2026-09-26', '2026-09-27', '2026-09-28', '2026-09-29', '2026-09-30', '2026-10-01']):
        daily.append({'date': d, 'open': 100+i, 'high': 110+i, 'low': 90+i, 'close': 105+i, 'source': 'oanda'})
    ws = derive_weekly_ohlc_from_daily(daily)
    assert len(ws) == 2
    assert ws[-1]['date'] == '2026-10-01'
    provider = _oanda_week_end_bar({'date': '2026-09-25', 'open': 100, 'high': 116, 'low': 90, 'close': 111})
    assert provider['date'] == '2026-10-02'
    assert _compare_last_n_weeks([provider], ws) == []
    assert _compare_last_n_weeks([{**provider, 'high': 120}], ws)
    # Calendar-date Yahoo feeds must not acquire OANDA's session convention.
    yahoo = derive_weekly_ohlc_from_daily([{**b, 'source': 'yahoo_futures'} for b in daily])
    assert yahoo[-1]['open'] == 103


def test_corn_provider_uses_futures_and_normalizes_cents(monkeypatch):
    from hptl.prices import price_cot_alignment_audit as audit
    from hptl.prices import coffee_foundation_backfill as feed
    assert audit._provider_and_symbol('Corn') == ('yahoo_futures', 'ZC=F')
    monkeypatch.setattr(feed, 'fetch_yahoo_daily', lambda symbol: [
        {'date': '2026-09-28', 'open': 420, 'high': 430, 'low': 410, 'close': 425}
    ])
    bars, mode = audit._fetch_provider_weekly_series('yahoo_futures', 'ZC=F')
    assert mode == 'yahoo_live_weekly_from_daily'
    assert bars[-1]['open'] == 4.2
    assert bars[-1]['close'] == 4.25


def test_recent_cached_native_candles_cannot_override_oanda_daily(monkeypatch):
    from types import SimpleNamespace
    from hptl.prices import workstation_ohlc_export as export
    from hptl.prices import price_store
    monkeypatch.setattr(export, 'resolve_workstation_index_source', lambda market: None)
    bar = SimpleNamespace(date='2026-10-01', open=100, high=110, low=90, close=105, source='oanda')
    timeline = SimpleNamespace(bars=[bar], canonical_source='oanda', canonical_symbol='SUGAR_USD')
    monkeypatch.setattr(export, 'build_canonical_timeline', lambda *a, **kw: timeline)
    monkeypatch.setattr(price_store, 'load_price_store', lambda: {'instruments': {'Sugar': {
        'weekly': [{'date': '2026-09-25', 'open': 80, 'high': 95, 'low': 70, 'close': 85}]
    }}})
    result = export.build_instrument_workstation_ohlc('Sugar')
    assert result['weekly_ohlc'][-1]['date'] == '2026-10-01'
    assert result['weekly_ohlc'][-1]['close'] == 105
