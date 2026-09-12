from __future__ import annotations

from datetime import date, timedelta

from hptl.markets.instrument_registry import LEGACY_COT_MARKETS
from hptl.seasonality_workstation.integrity import audit_daily_series_for_lookback
from hptl.seasonality_workstation.universe_audit import (
    MIN_DENSE_YEARS_FOR_15Y_SCAN,
    audit_seasonality_instrument,
)


def _daily(start: date, end: date, *, start_px: float = 5.0):
    rows = []
    px = start_px
    d = start
    i = 0
    while d <= end:
        if d.weekday() < 5:
            px *= 1.0 + (0.001 if i % 2 == 0 else -0.0008)
            rows.append((d.isoformat(), px))
            i += 1
        d += timedelta(days=1)
    return rows


def test_corn_rejects_alpha_vantage_source_even_when_series_is_smooth():
    daily = _daily(date(2009, 1, 2), date(2026, 1, 2))
    audit = audit_seasonality_instrument(
        "Corn",
        daily=daily,
        source="alpha_vantage",
        today=date(2026, 1, 2),
    )
    assert audit["status"] == "FAIL"
    assert audit["scanner_eligible"] is False
    assert any("invalid_seasonality_source" in issue for issue in audit["issues"])


def test_corn_accepts_yahoo_futures_source_when_structurally_clean_and_dense():
    daily = _daily(date(2009, 1, 2), date(2026, 1, 2))
    audit = audit_seasonality_instrument(
        "Corn",
        daily=daily,
        source="yahoo_futures",
        today=date(2026, 1, 2),
    )
    assert audit["status"] == "PASS"
    assert audit["scanner_eligible"] is True
    assert audit["source_contract"]["passed"] is True
    assert audit["density"]["dense_year_count"] >= MIN_DENSE_YEARS_FOR_15Y_SCAN
    assert audit["series_fingerprint"]
    assert audit["scanner_scope_fingerprint"]


def test_excessive_discontinuities_inside_active_scope_block_edge_publication():
    daily = _daily(date(2009, 1, 2), date(2026, 1, 2))
    corrupt = []
    factor = 1.0
    for i, (d, px) in enumerate(daily):
        if i and i % 250 == 0:
            factor = 10.0 if factor == 1.0 else 1.0
        corrupt.append((d, px * factor))
    audit = audit_seasonality_instrument(
        "Wheat",
        daily=corrupt,
        source="oanda",
        today=date(2026, 1, 2),
    )
    assert audit["status"] == "FAIL"
    assert any("excessive_discontinuities" in issue for issue in audit["issues"])


def test_legacy_discontinuities_outside_15y_do_not_block_current_scanner():
    clean = _daily(date(2009, 1, 2), date(2026, 1, 2))
    legacy = [
        ("1992-01-01", 1.0),
        ("1992-02-01", 100.0),
        ("1992-03-01", 1.0),
        ("1992-04-01", 100.0),
        ("1992-05-01", 1.0),
        ("1992-06-01", 100.0),
        ("1992-07-01", 1.0),
        ("1992-08-01", 100.0),
        ("1992-09-01", 1.0),
        ("1992-10-01", 100.0),
    ]
    audit = audit_seasonality_instrument(
        "Corn",
        daily=legacy + clean,
        source="yahoo_futures",
        today=date(2026, 1, 2),
    )
    assert audit["status"] == "PASS"
    assert audit["full_history_status"] == "FAIL"
    assert audit["full_history_issues"]
    assert any("legacy_out_of_scope_issues" in w for w in audit["warnings"])


def test_scoped_integrity_still_blocks_bad_data_inside_requested_lookback():
    daily = _daily(date(2009, 1, 2), date(2026, 1, 2))
    corrupt = []
    factor = 1.0
    for i, (d, px) in enumerate(daily):
        if i and i % 250 == 0:
            factor = 10.0 if factor == 1.0 else 1.0
        corrupt.append((d, px * factor))
    audit = audit_daily_series_for_lookback(
        "Corn",
        corrupt,
        source="yahoo_futures",
        lookback_years=15,
        asof="2026-01-02",
    )
    assert audit["status"] == "FAIL"


def test_registry_seasonality_universe_is_unique_and_contains_core_markets():
    assert len(LEGACY_COT_MARKETS) == len(set(LEGACY_COT_MARKETS))
    assert "Corn" in LEGACY_COT_MARKETS
    assert "Soybeans" in LEGACY_COT_MARKETS
    assert "Natural Gas / NG" in LEGACY_COT_MARKETS
    assert "US Dollar Index / DX" in LEGACY_COT_MARKETS
