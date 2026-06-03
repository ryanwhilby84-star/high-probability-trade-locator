"""Weather context bundle (mocked HTTP)."""
from __future__ import annotations

import json
import os
import unittest
from unittest.mock import patch

from hptl.intelligence.weather_context import build_weather_context_bundle
from hptl.intelligence.weather_context_export import write_weather_context_export


class TestWeatherContext(unittest.TestCase):
    def test_no_key_records_errors(self) -> None:
        with patch.dict(os.environ, {"OPENWEATHER_API_KEY": ""}, clear=False):
            with patch.dict(os.environ, {"HPTL_SKIP_LIVE_FEEDS": "0"}, clear=False):
                doc = build_weather_context_bundle(respect_skip_live=False)
        self.assertEqual(doc["provider"], "none")
        wheat = doc["markets"]["Wheat"]["records"]
        self.assertEqual(len(wheat), 4)
        self.assertTrue(all(r.get("error") for r in wheat))
        regions = {r["region"] for r in wheat}
        self.assertEqual(regions, {"Kansas", "Oklahoma", "Nebraska", "Texas"})

    def test_write_export_path(self) -> None:
        import tempfile
        from pathlib import Path

        fake_payload = {
            "cod": 200,
            "list": [
                {
                    "dt": 1700000000,
                    "main": {"temp": 55, "temp_min": 50, "temp_max": 60},
                    "weather": [{"id": 800, "description": "clear"}],
                    "rain": {},
                    "pop": 0.1,
                }
            ],
        }
        with patch.dict(
            os.environ,
            {"OPENWEATHER_API_KEY": "k", "HPTL_SKIP_LIVE_FEEDS": "0"},
            clear=False,
        ):
            with patch(
                "hptl.intelligence.weather_context.fetch_forecast_payload",
                return_value=(fake_payload, None),
            ):
                with tempfile.TemporaryDirectory() as td:
                    out = Path(td) / "weather_context_latest.json"
                    path = write_weather_context_export(out, respect_skip_live=False)
                    doc = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(doc["forecast_records_ok"], 8)
        self.assertTrue(doc["markets"]["Natural Gas / NG"]["records"][0]["ok"])


if __name__ == "__main__":
    unittest.main()
