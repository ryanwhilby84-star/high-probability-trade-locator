"""Regression: upstream COT current must not skip stale dashboard republish."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.cot.pipeline import run_full_pipeline
from hptl.cot.report_dates import CftcProbeResult


def _catch_ok(week: str = "2026-07-14") -> MagicMock:
    m = MagicMock()
    m.confluence_after = week
    m.export_path = str(
        ROOT / "web-dashboard" / "public" / "data" / "confluence_history_latest.json"
    )
    m.error = None
    m.markets_exported = 26
    return m


class TestUpstreamCurrentDownstreamStale(unittest.TestCase):
    def test_pipeline_republishes_when_master_ahead_of_dashboard(self) -> None:
        """CFTC == master == 2026-07-14 but confluence == 2026-07-07 → republish."""
        local = pd.Timestamp("2026-07-14")
        probe = CftcProbeResult(
            latest_report_date=pd.Timestamp("2026-07-14"),
            dashboard_rows=pd.DataFrame(),
            rows_fetched=10,
            source_urls=("https://www.cftc.gov/files/dea/history/deacot2026.zip",),
        )
        with patch("hptl.cot.pipeline.persist_weekly_run"):
            with patch("hptl.cot.pipeline._confluence_export_latest_week", return_value="2026-07-07"):
                with patch("hptl.cot.pipeline.get_latest_local_report_date", return_value=local):
                    with patch("hptl.cot.pipeline.probe_cftc_latest_report_date", return_value=probe):
                        with patch("hptl.cot.pipeline._read_probe_cache", return_value=None):
                            with patch("hptl.cot.pipeline.run_workbook_export") as mock_export:
                                with patch(
                                    "hptl.confluence.export_from_masters.catch_up_confluence_export",
                                    return_value=_catch_ok(),
                                ) as mock_catch:
                                    with patch(
                                        "hptl.cot.pipeline._export_cot_workstation_series",
                                        return_value=Path(
                                            "web-dashboard/public/data/cot_3y_series_latest.json"
                                        ),
                                    ) as mock_cot3:
                                        with patch("hptl.cot.pipeline._sync_confluence_dashboard_exports", return_value=[]):
                                            with patch("hptl.cot.pipeline._mark_confluence_stale_flag"):
                                                with patch("hptl.cot.pipeline._write_probe_cache"):
                                                    result = run_full_pipeline(
                                                        force=False, skip_confluence=False
                                                    )
        mock_export.assert_not_called()
        mock_catch.assert_called_once()
        mock_cot3.assert_called_once()
        self.assertFalse(result.update_needed)
        self.assertTrue(result.update_performed)
        self.assertEqual(result.export_latest_cot_week, "2026-07-14")
        self.assertFalse(result.cot_data_stale)
        self.assertIsNone(result.error)

    def test_pipeline_cached_probe_also_republishes_downstream(self) -> None:
        local = pd.Timestamp("2026-07-14")
        cache = {
            "latest_cftc_report_date": "2026-07-14",
            "probed_at_utc": "2026-07-18T05:00:00+00:00",
            "source_urls": ["https://www.cftc.gov/files/dea/history/fut_disagg_txt_2026.zip"],
        }
        with patch("hptl.cot.pipeline.persist_weekly_run"):
            with patch("hptl.cot.pipeline._confluence_export_latest_week", return_value="2026-07-07"):
                with patch("hptl.cot.pipeline.get_latest_local_report_date", return_value=local):
                    with patch("hptl.cot.pipeline._read_probe_cache", return_value=cache):
                        with patch("hptl.cot.pipeline._probe_cache_is_trusted", return_value=True):
                            with patch("hptl.cot.pipeline.probe_cftc_latest_report_date") as mock_probe:
                                with patch("hptl.cot.pipeline.run_workbook_export") as mock_export:
                                    with patch(
                                        "hptl.confluence.export_from_masters.catch_up_confluence_export",
                                        return_value=_catch_ok(),
                                    ) as mock_catch:
                                        with patch(
                                            "hptl.cot.pipeline._export_cot_workstation_series",
                                            return_value=Path(
                                                "web-dashboard/public/data/cot_3y_series_latest.json"
                                            ),
                                        ):
                                            with patch(
                                                "hptl.cot.pipeline._sync_confluence_dashboard_exports",
                                                return_value=[],
                                            ):
                                                with patch("hptl.cot.pipeline._mark_confluence_stale_flag"):
                                                    result = run_full_pipeline(
                                                        force=False, skip_confluence=False
                                                    )
        mock_probe.assert_not_called()
        mock_export.assert_not_called()
        mock_catch.assert_called_once()
        self.assertTrue(result.update_performed)
        self.assertEqual(result.export_latest_cot_week, "2026-07-14")

    def test_weekly_pull_does_not_skip_confluence_when_dashboard_stale(self) -> None:
        from hptl.dashboard import weekly_refresh as wr

        with patch.object(wr, "_master_max", return_value="2026-07-14"):
            with patch.object(wr, "_confluence_latest", return_value="2026-07-07"):
                with patch.object(wr, "_cot3y_latest", return_value="2026-07-07"):
                    self.assertTrue(wr._dashboard_cot_export_behind_master())
                    with patch("hptl.cot.pipeline.run_full_pipeline") as mock_run:
                        mock_run.return_value = MagicMock(exit_code=0)
                        wr.pull_cot_and_master(force=False)
                    mock_run.assert_called_once()
                    self.assertFalse(mock_run.call_args.kwargs.get("skip_confluence"))


if __name__ == "__main__":
    unittest.main()
