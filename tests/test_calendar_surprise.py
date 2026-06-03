"""calendar_surprise — beat/miss/inline and data_quality."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.news.calendar_surprise import compute_surprise_fields


class TestCalendarSurprise(unittest.TestCase):
    def test_beat_vs_forecast(self) -> None:
        s = compute_surprise_fields(actual=105.0, forecast=100.0, previous=98.0)
        self.assertEqual(s["direction_vs_forecast"], "beat")
        self.assertEqual(s["data_quality"], "complete")

    def test_miss_vs_forecast(self) -> None:
        s = compute_surprise_fields(actual=95.0, forecast=100.0, previous=98.0)
        self.assertEqual(s["direction_vs_forecast"], "miss")

    def test_missing_actual(self) -> None:
        s = compute_surprise_fields(actual=None, forecast=100.0, previous=98.0)
        self.assertEqual(s["data_quality"], "missing_actual")


if __name__ == "__main__":
    unittest.main()
