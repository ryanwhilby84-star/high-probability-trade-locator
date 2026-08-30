from __future__ import annotations

import importlib.util
from datetime import date, timedelta
from pathlib import Path


def _load_audit_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "audit_seasonality_roadmap.py"
    spec = importlib.util.spec_from_file_location("audit_seasonality_roadmap", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _weekday_series(start_year: int, end_year: int) -> list[tuple[str, float]]:
    rows: list[tuple[str, float]] = []
    seq = 0
    d = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    while d <= end:
        if d.weekday() < 5:
            # Deterministic trend + within-year wave, always positive.
            day = d.timetuple().tm_yday
            close = 9.0 + 0.04 * (d.year - start_year) + 0.003 * day + 0.0002 * seq
            rows.append((d.isoformat(), close))
            seq += 1
        d += timedelta(days=1)
    return rows


def test_forensic_audit_reproduces_production_roadmap(monkeypatch):
    module = _load_audit_module()
    daily = _weekday_series(2008, 2026)
    monkeypatch.setattr(module, "load_daily_closes", lambda _: (daily, "synthetic", None))

    report = module.audit("Soybeans", "2026-08-24", 15)

    assert report["passed"] is True
    assert report["sample_size"] == 15
    assert report["checks"]["raw_roadmap_curve_match"] is True
    assert report["checks"]["forecast_stats_match"] is True
    assert all(
        row["stats_calendar_anchor"] is not None for row in report["year_ledger"]
    )
    assert all(
        set(row["horizons"]).issuperset({"4w", "8w", "12w"})
        for row in report["year_ledger"]
    )


def test_forensic_audit_exposes_calendar_vs_ordinal_alignment_drift(monkeypatch):
    module = _load_audit_module()
    daily = _weekday_series(2008, 2026)

    # Remove one pre-as-of trading bar from a historical sample year. The roadmap
    # aligns by ordinal trading day, while horizon stats align by month/day. The
    # audit must expose that difference rather than silently treating them as the
    # same observation.
    daily = [
        row for row in daily
        if row[0] != "2018-03-12"
    ]
    monkeypatch.setattr(module, "load_daily_closes", lambda _: (daily, "synthetic", None))

    report = module.audit("Soybeans", "2026-08-24", 15)

    assert report["passed"] is True
    assert report["alignment_audit"]["warning"] is True
    assert report["alignment_audit"]["drift_days_by_year"]["2018"] != 0
