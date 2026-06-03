"""Equity index multi-year COT loader."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.cot.contracts import FINANCIAL_INDEX_CODE_TO_TARGET, GOOD_WORKBOOK_MARKET_ORDER
from hptl.cot.financial_index_loader import (
    DASHBOARD_INDEX_MARKETS,
    WORKBOOK_INDEX_MARKETS,
    _cache_is_stale,
    financial_index_cache_path,
    invalidate_financial_index_year_cache,
    load_financial_index_decision_rows,
    load_financial_index_workbook_rows,
)


class TestFinancialIndexLoader(unittest.TestCase):
    def test_contract_codes_locked(self) -> None:
        self.assertEqual(FINANCIAL_INDEX_CODE_TO_TARGET["209742"], "NASDAQ / NQ")
        self.assertEqual(FINANCIAL_INDEX_CODE_TO_TARGET["13874A"], "S&P 500 / ES")
        self.assertEqual(FINANCIAL_INDEX_CODE_TO_TARGET["124603"], "Dow / YM")
        self.assertEqual(FINANCIAL_INDEX_CODE_TO_TARGET["099741"], "Euro FX / 6E")
        self.assertEqual(FINANCIAL_INDEX_CODE_TO_TARGET["112741"], "NZ Dollar / 6N")

    def test_workbook_includes_dow(self) -> None:
        self.assertIn("DOW", GOOD_WORKBOOK_MARKET_ORDER)
        self.assertEqual(len(WORKBOOK_INDEX_MARKETS), 3)
        self.assertEqual(len(DASHBOARD_INDEX_MARKETS), 3)

    def test_decision_rows_have_history(self) -> None:
        df = load_financial_index_decision_rows()
        if df.empty:
            self.skipTest("no financial index caches/network")
        for m in DASHBOARD_INDEX_MARKETS:
            sub = df[df["market"] == m]
            self.assertGreater(len(sub), 40, msg=m)

    def test_cache_stale_when_remote_newer(self) -> None:
        import pandas as pd

        year = 2099
        path = financial_index_cache_path(year)
        path.parent.mkdir(parents=True, exist_ok=True)
        stale = pd.DataFrame(
            {
                "cftc_contract_market_code": ["209742"],
                "report_date_as_yyyy_mm_dd": ["2099-01-01"],
            }
        )
        stale.to_csv(path, index=False)
        remote_latest = {"209742": "2099-02-01"}
        self.assertTrue(_cache_is_stale(year, remote_latest, path))
        invalidate_financial_index_year_cache(year)


if __name__ == "__main__":
    unittest.main()
