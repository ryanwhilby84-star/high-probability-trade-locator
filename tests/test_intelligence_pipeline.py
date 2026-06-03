"""Intelligence pipeline: config load, explicit not-configured paths, no network when disabled."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.intelligence.catalyst_loader import SOURCE_NOT_CONFIGURED, load_catalyst_config
from hptl.intelligence.event_adapter import affected_markets_for_event, fetch_normalized_events
from hptl.intelligence.impulse_adapter import compute_simple_impulse
from hptl.intelligence.intelligence_engine import build_intelligence_bundle
from hptl.intelligence.news_adapter import fetch_newsapi_headlines


class TestIntelligencePipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.cfg = load_catalyst_config()

    def test_catalyst_config_has_instruments(self) -> None:
        inst = self.cfg.get("instruments")
        self.assertIsInstance(inst, dict)
        self.assertIn("Gold", inst)

    def test_newsapi_not_configured_without_key(self) -> None:
        rows, status = fetch_newsapi_headlines(instrument="Gold", catalyst_cfg=self.cfg)
        self.assertEqual(rows, [])
        self.assertEqual(status, SOURCE_NOT_CONFIGURED)

    def test_events_sources_not_configured_without_keys(self) -> None:
        from datetime import date

        b = fetch_normalized_events(
            start=date(2026, 5, 1),
            end=date(2026, 5, 7),
            catalyst_cfg=self.cfg,
        )
        self.assertEqual(b["sources"]["finnhub"]["status"], SOURCE_NOT_CONFIGURED)
        self.assertEqual(b["sources"]["trading_economics"]["status"], SOURCE_NOT_CONFIGURED)

    def test_impulse_not_configured_without_master_csv(self) -> None:
        out = compute_simple_impulse("Gold", catalyst_cfg=self.cfg, master_csv_path=ROOT / "nonexistent_master.csv")
        self.assertEqual(out["availability"], SOURCE_NOT_CONFIGURED)

    def test_bundle_offline(self) -> None:
        b = build_intelligence_bundle(
            "Gold",
            catalyst_cfg=self.cfg,
            include_rss=False,
            include_newsapi=False,
            include_gdelt=False,
        )
        self.assertEqual(b["instrument"], "Gold")
        self.assertIn("dashboard_fields", b)
        self.assertEqual(b["sentiment_interference"]["availability"], SOURCE_NOT_CONFIGURED)

    def test_affected_markets_keyword_match(self) -> None:
        m = affected_markets_for_event("US CPI year over year", catalyst_cfg=self.cfg)
        self.assertIn("S&P 500 / ES", m)
