"""Weekly COT update orchestration (mocked network)."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.cot.pipeline import CotPipelineResult, run_full_pipeline
from hptl.cot.report_dates import CftcProbeResult, tracked_market_week_keys
from hptl.cot.run_weekly_update import run


class TestWeeklyCotUpdate(unittest.TestCase):
    def test_no_new_report_exits_zero(self) -> None:
        local = pd.Timestamp("2026-05-05")
        probe = CftcProbeResult(
            latest_report_date=pd.Timestamp("2026-05-05"),
            dashboard_rows=pd.DataFrame(),
            rows_fetched=10,
            source_urls=("http://example.com/a", "http://example.com/b"),
        )
        ok = CotPipelineResult(
            latest_local_report_date="2026-05-05",
            latest_cftc_report_date="2026-05-05",
            update_needed=False,
            exit_code=0,
        )
        with patch("hptl.cot.run_weekly_update.run_full_pipeline", return_value=ok):
            rc = run()
        self.assertEqual(rc, 0)

    def test_new_report_runs_pipeline(self) -> None:
        ok = CotPipelineResult(
            update_needed=True,
            update_performed=True,
            latest_cftc_report_date="2026-05-12",
            export_latest_cot_week="2026-05-12",
            exit_code=0,
        )
        with patch("hptl.cot.run_weekly_update.run_full_pipeline", return_value=ok) as mock_run:
            rc = run()
        self.assertEqual(rc, 0)
        mock_run.assert_called_once_with(force=False)

    def test_cftc_unavailable_fails_closed(self) -> None:
        fail = CotPipelineResult(error="network down", exit_code=1)
        with patch("hptl.cot.run_weekly_update.run_full_pipeline", return_value=fail):
            rc = run()
        self.assertEqual(rc, 1)

    def test_pipeline_no_new_week_skips_download(self) -> None:
        local = pd.Timestamp("2026-05-12")
        probe = CftcProbeResult(
            latest_report_date=pd.Timestamp("2026-05-12"),
            dashboard_rows=pd.DataFrame(),
            rows_fetched=10,
            source_urls=("http://example.com/a",),
        )
        with patch("hptl.cot.pipeline.persist_weekly_run"):
            with patch("hptl.cot.pipeline._confluence_export_latest_week", return_value="2026-05-12"):
                with patch("hptl.cot.pipeline.get_latest_local_report_date", return_value=local):
                    with patch("hptl.cot.pipeline.probe_cftc_latest_report_date", return_value=probe):
                        with patch("hptl.cot.pipeline.run_workbook_export") as mock_export:
                            result = run_full_pipeline()
        mock_export.assert_not_called()
        self.assertFalse(result.update_needed)
        self.assertIsNone(result.error)

    def test_tracked_keys_dedup_shape(self) -> None:
        cot = pd.DataFrame(
            {
                "market": ["Gold", "Gold"],
                "cot_report_date": ["2026-05-05", "2026-05-05"],
            }
        )
        keys = tracked_market_week_keys(cot)
        self.assertEqual(len(keys), 1)


if __name__ == "__main__":
    unittest.main()
