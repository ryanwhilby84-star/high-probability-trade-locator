"""Decision table uses per-market COT as-of backward merge on staggered report weeks."""

from __future__ import annotations

import unittest

import pandas as pd

from hptl.confluence.build_decision_table import TARGET_MARKETS, _build_by_market_as_of


class CotAsOfMergeTests(unittest.TestCase):
    def test_build_by_market_as_of_backward_fills_stale_index_week(self) -> None:
        cot = pd.DataFrame(
            [
                {"market": "NASDAQ / NQ", "cot_report_date": pd.Timestamp("2026-05-05"), "long_value": 1.0, "short_value": 2.0, "net_value": -1.0},
                {"market": "Gold", "cot_report_date": pd.Timestamp("2026-05-12"), "long_value": 10.0, "short_value": 5.0, "net_value": 5.0},
            ]
        )
        by = _build_by_market_as_of(cot, pd.Timestamp("2026-05-12"))
        self.assertIn("NASDAQ / NQ", by)
        self.assertEqual(pd.Timestamp(by["NASDAQ / NQ"]["cot_report_date"]), pd.Timestamp("2026-05-05"))
        self.assertIn("Gold", by)
        self.assertEqual(pd.Timestamp(by["Gold"]["cot_report_date"]), pd.Timestamp("2026-05-12"))
        self.assertNotIn("S&P 500 / ES", by)

    def test_target_markets_include_equity_indices(self) -> None:
        self.assertIn("NASDAQ / NQ", TARGET_MARKETS)
        self.assertIn("Dow / YM", TARGET_MARKETS)


if __name__ == "__main__":
    unittest.main()
