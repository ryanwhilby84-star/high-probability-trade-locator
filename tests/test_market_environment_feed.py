"""market_environment_feed builder — offline shape and filters."""
from __future__ import annotations

import os
import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.intelligence.catalyst_loader import load_catalyst_config
from hptl.intelligence.macro_event_filter import is_macro_calendar_event
from hptl.intelligence.market_environment_feed import (
    FeedBuildCache,
    _filter_events_for_market,
    _headlines_to_records,
    _record_to_event_item,
    _record_to_news_item,
    build_all_market_environment_feeds,
    build_market_environment_feed,
)


class TestMarketEnvironmentFeed(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.cfg = load_catalyst_config()

    def test_macro_event_filter(self) -> None:
        self.assertTrue(is_macro_calendar_event("US CPI YoY"))
        self.assertTrue(is_macro_calendar_event("FOMC Rate Decision"))
        self.assertFalse(is_macro_calendar_event("Local holiday"))

    def test_filter_events_for_gold(self) -> None:
        events = [
            {
                "event_name": "US CPI YoY",
                "importance_rank": 3,
                "affected_markets": ["Gold", "S&P 500 / ES"],
                "event_timestamp": "2026-05-20T12:30:00+00:00",
                "country": "US",
                "source": "finnhub",
            },
            {
                "event_name": "Random local survey",
                "importance_rank": 1,
                "affected_markets": ["Gold"],
                "event_timestamp": "2026-05-21T08:00:00+00:00",
                "country": "US",
                "source": "finnhub",
            },
        ]
        out = _filter_events_for_market(events, "Gold")
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["event_name"], "US CPI YoY")

    def test_record_legacy_mapping(self) -> None:
        rec = {
            "market": "Gold",
            "provider": "finnhub",
            "source": "Finnhub",
            "category": "headline",
            "title": "US CPI preview",
            "summary": "Matched catalyst tags: rates_fed.",
            "importance": "medium",
            "impact_label": "unknown",
            "event_time": "2026-05-17T10:00:00+00:00",
            "fetched_at": "2026-05-17T10:05:00+00:00",
            "url": "https://example.com",
        }
        news = _record_to_news_item(rec, "Gold")
        self.assertEqual(news["headline"], "US CPI preview")
        self.assertEqual(news["classification"], "neutral")
        ev = _record_to_event_item(rec, "Gold")
        self.assertEqual(ev["risk_level"], "moderate")

    def test_headlines_to_records_requires_title(self) -> None:
        rows = _headlines_to_records(
            "Gold",
            [{"title": "", "fetched_at": "2026-05-17T10:00:00+00:00", "date": "2026-05-17T09:00:00+00:00"}],
        )
        self.assertEqual(rows, [])

    def test_skip_live_feeds_env(self) -> None:
        with patch.dict(os.environ, {"HPTL_SKIP_LIVE_FEEDS": "1"}):
            feeds = build_all_market_environment_feeds(["Gold"])
        self.assertEqual(feeds["Gold"], {})

    @patch("hptl.intelligence.market_environment_feed.fetch_enriched_calendar")
    @patch("hptl.intelligence.market_environment_feed.fetch_finnhub_headlines")
    @patch("hptl.intelligence.market_environment_feed.fetch_weather_summaries")
    def test_build_feed_offline_mocks(
        self,
        mock_weather: unittest.mock.MagicMock,
        mock_news: unittest.mock.MagicMock,
        mock_events: unittest.mock.MagicMock,
    ) -> None:
        mock_events.return_value = {
            "wired": True,
            "message": "",
            "events": [
                {
                    "event_name": "US Non Farm Payrolls",
                    "importance_rank": 3,
                    "affected_markets": ["S&P 500 / ES"],
                    "event_timestamp": "2026-05-22T12:30:00+00:00",
                    "date": "2026-05-22",
                    "country": "US",
                    "source": "trading_economics",
                    "released": False,
                }
            ],
            "sources": {},
            "event_risk_by_market": {"S&P 500 / ES": "high_this_week"},
        }
        mock_news.return_value = ([], "finnhub:no_match")
        mock_weather.return_value = ([], "not configured")

        feed = build_market_environment_feed("S&P 500 / ES", catalyst_cfg=self.cfg, cache=FeedBuildCache(catalyst_cfg=self.cfg))
        self.assertIn("records", feed)
        self.assertIn("live_bundle_last_checked_at", feed)
        macro_recs = [r for r in feed["records"] if r["category"] == "macro_event"]
        self.assertEqual(len(macro_recs), 1)
        self.assertEqual(macro_recs[0]["provider"], "trading_economics")


if __name__ == "__main__":
    unittest.main()
