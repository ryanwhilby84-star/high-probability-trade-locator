"""Phase 2A — COT pipeline health and failure handling tests."""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from hptl.cot.cot_failures import FAILURES_JSON, log_cot_failure, read_cot_failures
from hptl.cot.pipeline_health import (
    compute_days_stale,
    dashboard_message,
    status_from_health,
)


class TestStalenessRules:
    def test_healthy_within_10_days(self) -> None:
        latest = date(2026, 6, 10)
        expected = date(2026, 6, 16)
        days = compute_days_stale(latest, expected)
        assert days == 6
        assert status_from_health(days_stale=days, download_success=True, ingest_success=True) == "HEALTHY"

    def test_warning_11_to_17_days(self) -> None:
        latest = date(2026, 6, 1)
        expected = date(2026, 6, 16)
        days = compute_days_stale(latest, expected)
        assert days == 15
        assert status_from_health(days_stale=days, download_success=True, ingest_success=True) == "WARNING"
        msg = dashboard_message(
            status="WARNING",
            latest_report_date="2026-06-01",
            days_stale=days,
            download_success=True,
            ingest_success=True,
        )
        assert "15 days stale" in msg

    def test_failure_18_plus_days(self) -> None:
        latest = date(2026, 5, 20)
        expected = date(2026, 6, 16)
        days = compute_days_stale(latest, expected)
        assert days == 27
        assert status_from_health(days_stale=days, download_success=True, ingest_success=True) == "FAILURE"

    def test_failure_on_download_fail(self) -> None:
        assert (
            status_from_health(
                days_stale=0,
                download_success=False,
                ingest_success=True,
            )
            == "FAILURE"
        )

    def test_failure_on_ingest_fail(self) -> None:
        assert (
            status_from_health(
                days_stale=0,
                download_success=True,
                ingest_success=False,
            )
            == "FAILURE"
        )

    def test_failure_on_pipeline_error(self) -> None:
        assert (
            status_from_health(
                days_stale=3,
                download_success=True,
                ingest_success=True,
                pipeline_error="probe failed",
            )
            == "FAILURE"
        )


class TestFailureLog:
    def test_log_and_read_rolling(self, tmp_path, monkeypatch) -> None:
        path = tmp_path / "cot_failures.json"
        monkeypatch.setattr("hptl.cot.cot_failures.FAILURES_JSON", path)
        log_cot_failure(failure_type="download", source="test", error="network timeout", retry_result="attempt 1/3")
        log_cot_failure(failure_type="parse", source="test", error="corrupt zip")
        rows = read_cot_failures()
        assert len(rows) == 2
        assert rows[0]["failure_type"] == "download"
        doc = json.loads(path.read_text(encoding="utf-8"))
        assert doc["failure_count"] == 2


class TestDownloadValidation:
    def test_missing_file_fails(self, tmp_path) -> None:
        from hptl.cot.cot_download_validate import _validate_raw_file

        missing = tmp_path / "missing.zip"
        errs = _validate_raw_file(missing)
        assert any("missing" in e for e in errs)

    def test_empty_file_fails(self, tmp_path) -> None:
        from hptl.cot.cot_download_validate import _validate_raw_file

        empty = tmp_path / "empty.zip"
        empty.write_bytes(b"")
        errs = _validate_raw_file(empty)
        assert any("empty" in e for e in errs)


class TestIngestValidation:
    def test_no_new_week_fails_when_update_expected(self) -> None:
        from hptl.cot.cot_ingest_validate import validate_post_ingest

        result = validate_post_ingest(
            processed_csv=None,
            master_csv=None,
            previous_week="2026-06-10",
            expected_week="2026-06-17",
            update_performed=True,
            rows_added=0,
        )
        assert result.ok is False
        assert result.errors


class TestHealthTransitions:
    """Simulate HEALTHY → WARNING → FAILURE progression."""

    def test_transition_sequence(self) -> None:
        expected = date(2026, 6, 20)
        statuses = []
        for offset in (5, 12, 20):
            latest = expected - timedelta(days=offset)
            days = compute_days_stale(latest, expected)
            statuses.append(
                status_from_health(days_stale=days, download_success=True, ingest_success=True)
            )
        assert statuses == ["HEALTHY", "WARNING", "FAILURE"]
