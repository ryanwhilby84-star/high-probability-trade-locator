"""News/catalyst builder: config-driven matches, no fabricated articles."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class TestCatalystNewsBuilder(unittest.TestCase):
    @patch("hptl.news.catalyst_news_builder.fetch_gdelt_doc_list")
    def test_filters_to_config_matches_only(self, mock_gdelt):
        from hptl.intelligence.catalyst_loader import load_catalyst_config
        from hptl.news.catalyst_news_builder import build_instrument_gdelt_news_payload

        mock_gdelt.return_value = [
            SimpleNamespace(
                url="https://a.example/x",
                title="EIA storage report shows natural gas draw",
                seendate="20260510000000",
                domain="a.example",
            ),
            SimpleNamespace(
                url="https://b.example/y",
                title="Unrelated equity rally continues",
                seendate="20260509000000",
                domain="b.example",
            ),
        ]
        cfg = load_catalyst_config()
        out = build_instrument_gdelt_news_payload(catalyst_cfg=cfg, instrument="Natural Gas / NG", maxrecords=20)
        self.assertEqual(out["status"], "ok")
        self.assertEqual(len(out["headlines"]), 1)
        self.assertIn("natural gas", out["headlines"][0]["matched_keywords"])
        self.assertEqual(out["headlines"][0]["source"], "a.example")

    def test_not_configured_missing_map(self) -> None:
        from hptl.news.catalyst_news_builder import build_instrument_gdelt_news_payload

        cfg = {"instruments": {"Natural Gas / NG": {"catalyst_keyword_groups": {}}}}
        out = build_instrument_gdelt_news_payload(catalyst_cfg=cfg, instrument="Natural Gas / NG")
        self.assertEqual(out["status"], "not_configured")

    def test_lng_word_boundary(self) -> None:
        from hptl.news.catalyst_news_builder import _phrase_in_title

        self.assertFalse(_phrase_in_title("Belonging to sector funds", "lng"))
        self.assertTrue(_phrase_in_title("US LNG exports rise", "lng"))


if __name__ == "__main__":
    unittest.main()
