"""Stage B macro resilience: freshness bands, cache fallback, non-destructive merge."""

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

import pandas as pd

from hptl.macro import fred_client, macro_freshness
from hptl.macro.macro_relationship_maps import merge_macro_relationship_maps


def _iso_days_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


class TestFreshnessBands(unittest.TestCase):
    def test_band_boundaries(self) -> None:
        self.assertEqual(macro_freshness.band_for_age(0), "live")
        self.assertEqual(macro_freshness.band_for_age(7), "live")
        self.assertEqual(macro_freshness.band_for_age(8), "cached")
        self.assertEqual(macro_freshness.band_for_age(30), "cached")
        self.assertEqual(macro_freshness.band_for_age(31), "stale")
        self.assertEqual(macro_freshness.band_for_age(90), "stale")
        self.assertEqual(macro_freshness.band_for_age(91), "warning")
        self.assertEqual(macro_freshness.band_for_age(None), "unknown")

    def test_data_status_missing(self) -> None:
        self.assertEqual(
            macro_freshness.data_status(available=False, refresh_age_days=None, has_data=False),
            "missing",
        )

    def test_age_days_from(self) -> None:
        self.assertEqual(macro_freshness.age_days_from(_iso_days_ago(10)), 10)
        self.assertIsNone(macro_freshness.age_days_from(None))
        self.assertIsNone(macro_freshness.age_days_from("not-a-date"))


class TestNonDestructiveMerge(unittest.TestCase):
    def test_failed_refresh_preserves_previous(self) -> None:
        previous = {
            "Gold": {
                "available": True,
                "market": "Gold",
                "latest_date": "2026-04-30",
                "last_successful_refresh": _iso_days_ago(3),
            }
        }
        new = {"Gold": {"available": False, "market": "Gold", "error": "FRED fetch failed: ReadTimeout"}}
        merged = merge_macro_relationship_maps(new, previous)
        self.assertTrue(merged["Gold"]["available"])
        self.assertTrue(merged["Gold"]["carried_over"])
        self.assertEqual(merged["Gold"]["data_status"], "live")  # 3 days old
        self.assertIn("ReadTimeout", merged["Gold"]["last_refresh_error"])

    def test_fresh_new_replaces_previous(self) -> None:
        previous = {"Gold": {"available": True, "market": "Gold", "latest_date": "2025-01-01"}}
        new = {"Gold": {"available": True, "market": "Gold", "latest_date": "2026-05-30", "data_status": "live"}}
        merged = merge_macro_relationship_maps(new, previous)
        self.assertEqual(merged["Gold"]["latest_date"], "2026-05-30")
        self.assertFalse(merged["Gold"].get("carried_over", False))

    def test_no_previous_marks_missing(self) -> None:
        new = {"Copper / HG": {"available": False, "market": "Copper / HG", "error": "timeout"}}
        merged = merge_macro_relationship_maps(new, {})
        self.assertEqual(merged["Copper / HG"]["data_status"], "missing")


class TestFredClientCache(unittest.TestCase):
    def test_cache_roundtrip_and_offline_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            prev_dir = os.environ.get("HPTL_MACRO_CACHE_DIR")
            prev_skip = os.environ.get("HPTL_SKIP_LIVE_FEEDS")
            os.environ["HPTL_MACRO_CACHE_DIR"] = d
            try:
                df = pd.DataFrame(
                    {
                        "date": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"]),
                        "value": [1.0, 2.0, 3.0],
                    }
                )
                meta = fred_client._save_cache("TESTSER", "2020-01-01", df, http_status=200)
                self.assertEqual(meta["row_count"], 3)
                self.assertEqual(meta["observation_end"], "2026-01-03")

                # Cache-only mode: served from disk, no network.
                os.environ["HPTL_SKIP_LIVE_FEEDS"] = "1"
                out = fred_client.get_series_df("TESTSER", "2020-01-01")
                self.assertEqual(len(out), 3)
                self.assertEqual(fred_client.last_source("TESTSER", "2020-01-01"), "cache")

                # Cache-only with no cache -> raises (no silent blank).
                with self.assertRaises(fred_client.FredUnavailable):
                    fred_client.get_series_df("NOPE", "2020-01-01")
            finally:
                if prev_dir is None:
                    os.environ.pop("HPTL_MACRO_CACHE_DIR", None)
                else:
                    os.environ["HPTL_MACRO_CACHE_DIR"] = prev_dir
                if prev_skip is None:
                    os.environ.pop("HPTL_SKIP_LIVE_FEEDS", None)
                else:
                    os.environ["HPTL_SKIP_LIVE_FEEDS"] = prev_skip


if __name__ == "__main__":
    unittest.main()
