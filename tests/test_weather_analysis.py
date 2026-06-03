"""Weather analysis from synthetic OpenWeather-shaped payloads."""
from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.intelligence.weather_analysis import (
    ForecastSlice,
    analyze_forecast,
    build_trader_summary,
    parse_forecast_slices,
    weather_importance,
)


def _slice(temp: float, *, wid: int = 800, rain: float = 0.0, desc: str = "clear sky") -> ForecastSlice:
    return ForecastSlice(
        dt=datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
        temp_f=temp,
        temp_min_f=temp - 2,
        temp_max_f=temp + 2,
        weather_id=wid,
        description=desc,
        rain_mm_3h=rain,
        snow_mm_3h=0.0,
        pop=0.1,
    )


class TestWeatherAnalysis(unittest.TestCase):
    def test_parse_forecast_slices(self) -> None:
        payload = {
            "list": [
                {
                    "dt": 1710000000,
                    "main": {"temp": 72, "temp_min": 68, "temp_max": 75},
                    "weather": [{"id": 800, "description": "clear sky"}],
                    "rain": {"3h": 0.0},
                }
            ]
        }
        slices = parse_forecast_slices(payload)
        self.assertEqual(len(slices), 1)
        self.assertEqual(slices[0].temp_f, 72.0)

    def test_cold_snap_detection(self) -> None:
        slices = [_slice(18, desc="clear sky") for _ in range(8)]
        sig = analyze_forecast("Chicago demand hub", slices, is_crop_belt=False)
        self.assertTrue(sig.cold_snap or sig.temp_anomaly_low)

    def test_storm_detection(self) -> None:
        slices = [_slice(70, wid=211, rain=18.0, desc="thunderstorm")]
        sig = analyze_forecast("Henry Hub / Gulf Coast", slices)
        self.assertTrue(sig.storm)
        self.assertEqual(weather_importance(sig), "high")

    def test_trader_summary_includes_region(self) -> None:
        slices = [_slice(95, desc="hot")]
        sig = analyze_forecast("Henry Hub / Gulf Coast", slices)
        line = build_trader_summary("Henry Hub / Gulf Coast", sig, slices)
        self.assertIn("Henry Hub", line)


if __name__ == "__main__":
    unittest.main()
