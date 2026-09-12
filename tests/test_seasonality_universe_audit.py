from __future__ import annotations

from datetime import date, timedelta

from hptl.seasonality_workstation.universe_audit import audit_seasonality_instrument


def _daily(start: date, count: int, *, start_px: float = 5.0):
    rows = []
    px = start_px
    d = start
    for i in range(count):
        if d.weekday() < 5:
            px *= 1.0 + (0.001 if i % 2 == 0 else -0.0008)
            rows.append((d.isoformat(), px))
        d += timedelta(days=1)
    return rows


def test_corn_rejects_alpha_vantage_source_even_when_series_is_smooth():
    daily = _daily(date(2018, 1, 2), 8 * 365)
    audit = audit_seasonality_instrument(
        "Corn",
        daily=daily,
        source="alpha_vantage",
        today=date(2026, 1, 2),
    )
    assert audit["status"] == "FAIL"
    assert audit["scanner_eligible"] is False
    assert any("invalid_seasonality_source" in issue for issue in audit["issues"])


def test_corn_accepts_yahoo_futures_source_when_structurally_clean():
    daily = _daily(date(2018, 1, 2), 8 * 365)
    audit = audit_seasonality_instrument(
        "Corn",
        daily=daily,
        source="yahoo_futures",
        today=date(2026, 1, 2),
    )
    assert audit["status"] == "PASS"
    assert audit["scanner_eligible"] is True
    assert audit["source_contract"]["passed"] is True
    assert audit["series_fingerprint"]


def test_excessive_discontinuities_block_edge_publication():
    daily = _daily(date(2018, 1, 2), 8 * 365)
    corrupt = []
    for i, (d, px) in enumerate(daily):
        # Inject repeated 10x/0.1x scale changes similar to a mixed-unit series.
        if i % 100 == 0:
            px *= 10.0
        corrupt.append((d, px))
    audit = audit_seasonality_instrument(
        "Wheat",
        daily=corrupt,
        source="oanda",
        today=date(2026, 1, 2),
    )
    assert audit["status"] == "FAIL"
    assert any("excessive_discontinuities" in issue for issue in audit["issues"])
