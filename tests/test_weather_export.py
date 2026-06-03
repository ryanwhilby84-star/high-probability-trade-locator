"""Weather export bundle (no live HTTP)."""
from __future__ import annotations

import json
import os
import unittest
from unittest.mock import patch

from hptl.intelligence.weather_export import build_weather_bundle, write_weather_latest_export


class TestWeatherExport(unittest.TestCase):
    def test_build_weather_bundle_skip_live_feeds(self) -> None:
        with patch.dict(
            os.environ,
            {"HPTL_SKIP_LIVE_FEEDS": "1", "OPENWEATHER_API_KEY": "test-key"},
            clear=False,
        ):
            doc = build_weather_bundle()
        self.assertTrue(doc["skip_live_feeds"])
        self.assertFalse(doc["wired"])
        self.assertEqual(doc["provider"], "none")
        self.assertGreater(len(doc["locations_queried"]), 0)

    def test_write_weather_export_with_mock_fetch(self) -> None:
        env = {k: v for k, v in os.environ.items() if k != "HPTL_SKIP_LIVE_FEEDS"}
        env["OPENWEATHER_API_KEY"] = "test-key"
        fake_row = {
            "region": "Kansas City HRW",
            "summary": "Mild week ahead.",
            "importance": "medium",
            "risk_tags": [],
            "signals": {},
            "fetched_at": "2026-05-19T12:00:00+00:00",
            "source": "OpenWeather",
            "provider": "openweather",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch(
                "hptl.intelligence.weather_export.fetch_weather_summaries",
                return_value=([fake_row], "openweather"),
            ):
                import tempfile
                from pathlib import Path

                with tempfile.TemporaryDirectory() as td:
                    out = Path(td) / "weather_latest.json"
                    path = write_weather_latest_export(out)
                    doc = json.loads(path.read_text(encoding="utf-8"))
        self.assertTrue(doc["wired"])
        self.assertEqual(doc["provider"], "openweather")
        self.assertGreaterEqual(doc["forecast_records_loaded"], 1)
        self.assertIn("Wheat", doc["markets"])


if __name__ == "__main__":
    unittest.main()
