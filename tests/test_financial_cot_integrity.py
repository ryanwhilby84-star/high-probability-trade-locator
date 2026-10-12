"""Financial COT integrity checks: do not fabricate missing trader positions."""
import unittest
import pandas as pd
from hptl.cot.parser import financial_history_to_dashboard_rows

class FinancialCotIntegrityTests(unittest.TestCase):
    def test_missing_dealer_is_not_silently_zero(self):
        raw = pd.DataFrame([{
            "report_date": "2026-10-06",
            "market_and_exchange_names": "NASDAQ-100 - CHICAGO MERCANTILE EXCHANGE",
            "cftc_contract_market_code": "209742",
            "dealer_positions_long_all": None,
            "dealer_positions_short_all": None,
            "asset_mgr_positions_long_all": 120,
            "asset_mgr_positions_short_all": 80,
            "lev_money_positions_long_all": 150,
            "lev_money_positions_short_all": 100,
        }])
        row = financial_history_to_dashboard_rows(raw).iloc[0]
        self.assertTrue(pd.isna(row["commercial_long"]))
        self.assertTrue(pd.isna(row["commercial_short"]))
        self.assertTrue(pd.isna(row["commercial_net"]))

if __name__ == "__main__":
    unittest.main()
