"""calendar_interpretation — one-line macro context."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.news.calendar_interpretation import interpret_calendar_event


class TestCalendarInterpretation(unittest.TestCase):
    def test_cpi_hot(self) -> None:
        line = interpret_calendar_event(
            {
                "event_name": "US CPI YoY",
                "country": "US",
                "direction_vs_forecast": "beat",
                "magnitude_vs_forecast": "large",
            }
        )
        self.assertIn("USD", line)
        self.assertIn("gold", line.lower())

    def test_crude_draw(self) -> None:
        line = interpret_calendar_event(
            {
                "event_name": "Crude Oil Inventories",
                "direction_vs_forecast": "miss",
            }
        )
        self.assertIn("oil bullish", line.lower())


if __name__ == "__main__":
    unittest.main()
