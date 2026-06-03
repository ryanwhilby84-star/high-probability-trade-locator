"""FRED API client: connectivity and observation parsing (mocked; optional live)."""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class TestFredApi(unittest.TestCase):
    def test_connectivity_no_key(self) -> None:
        from hptl.macro.fred_api import check_fred_api_connectivity

        ok, msg = check_fred_api_connectivity(api_key="")
        self.assertFalse(ok)
        self.assertIn("source unavailable", msg)

    @patch("hptl.macro.fred_api.requests.get")
    def test_connectivity_ok(self, mock_get: MagicMock) -> None:
        from hptl.macro.fred_api import check_fred_api_connectivity

        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"seriess": [{"id": "DGS10"}]}
        ok, msg = check_fred_api_connectivity(api_key="fake-key")
        self.assertTrue(ok)
        self.assertEqual(msg, "ok")

    @patch("hptl.macro.fred_api._fetch_observations_frame")
    def test_latest_observations_parsed(self, mock_fetch: MagicMock) -> None:
        import pandas as pd

        from hptl.macro.fred_api import fetch_latest_observations

        def fake_frame(sid: str, key: str, observation_start: str = "") -> pd.DataFrame:
            return pd.DataFrame(
                {
                    "date": pd.to_datetime(["2026-05-01", "2026-05-04"]),
                    "value": [4.0, 4.12],
                }
            )

        mock_fetch.side_effect = fake_frame
        out = fetch_latest_observations(api_key="k")
        self.assertTrue(out["api_configured"])
        self.assertEqual(out["series"]["DGS10"]["value"], 4.12)
        self.assertEqual(out["series"]["DGS10"]["date"], "2026-05-04")

    @unittest.skipUnless(os.getenv("FRED_API_KEY", "").strip(), "FRED_API_KEY not set")
    def test_live_connectivity(self) -> None:
        from hptl.macro.fred_api import check_fred_api_connectivity, fetch_latest_observations

        ok, msg = check_fred_api_connectivity()
        self.assertTrue(ok, msg)
        snap = fetch_latest_observations()
        for sid in ("DGS2", "DGS10", "DGS30", "DFF", "T10Y2Y"):
            self.assertIn(sid, snap["series"])
            self.assertIn("value", snap["series"][sid])
            self.assertIsNotNone(snap["series"][sid]["value"], f"missing value for {sid}")
