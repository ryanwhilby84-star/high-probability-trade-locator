"""Live OpenWeather smoke test — skipped when OPENWEATHER_API_KEY is unset."""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from hptl.intelligence.weather_adapter import WEATHER_ENABLED_MARKETS, fetch_weather_summaries


@unittest.skipUnless(os.getenv("OPENWEATHER_API_KEY", "").strip(), "OPENWEATHER_API_KEY not set")
class TestWeatherLive(unittest.TestCase):
    def test_natural_gas_weather_fetch(self) -> None:
        rows, status = fetch_weather_summaries("Natural Gas / NG")
        self.assertEqual(status, "openweather")
        self.assertGreaterEqual(len(rows), 1)
        self.assertIn("summary", rows[0])
        self.assertEqual(rows[0]["source"], "OpenWeather")
        self.assertIn("fetched_at", rows[0])

    def test_all_enabled_markets(self) -> None:
        for market in sorted(WEATHER_ENABLED_MARKETS):
            rows, status = fetch_weather_summaries(market)
            self.assertEqual(status, "openweather", msg=market)
            self.assertGreaterEqual(len(rows), 1, msg=market)


if __name__ == "__main__":
    unittest.main()
