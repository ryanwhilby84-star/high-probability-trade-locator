"""COT feed stale metadata on confluence export."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.confluence.build_decision_table import _build_cot_feed_status


class TestCotFeedStatus(unittest.TestCase):
    def test_stale_when_export_behind_cftc(self) -> None:
        status = _build_cot_feed_status(
            latest_cot_report_date="2026-05-12",
            cot_feed_meta={"latest_cftc_report_date": "2026-05-19", "cot_data_stale": False},
        )
        self.assertTrue(status["is_stale"])

    def test_fresh_when_explicit_not_stale(self) -> None:
        status = _build_cot_feed_status(
            latest_cot_report_date="2026-05-19",
            cot_feed_meta={"latest_cftc_report_date": "2026-05-19", "cot_data_stale": False},
        )
        self.assertFalse(status["is_stale"])


if __name__ == "__main__":
    unittest.main()
