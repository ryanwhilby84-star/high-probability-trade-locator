import datetime as dt
import unittest

from hptl.seasonality_workstation.historical_archive import (
    audit_coverage, build_returns, parse_date,
)


class HistoricalArchiveTests(unittest.TestCase):
    def test_dates(self):
        self.assertEqual(parse_date("610104"), dt.date(1961, 1, 4))
        self.assertEqual(parse_date("01/04/1961"), dt.date(1961, 1, 4))
        self.assertEqual(parse_date("2002-09-30"), dt.date(2002, 9, 30))

    def test_roll_uses_same_contract_prices(self):
        a, b, c = (dt.date(2001, 1, d) for d in (2, 3, 4))
        contracts = {
            "SB01H": ((2001, 3), {a: (10, 10, 10, 10, 1, 1),
                                   b: (11, 11, 11, 11, 1, 1)}),
            "SB01K": ((2001, 5), {a: (20, 20, 20, 20, 1, 1),
                                   b: (21, 21, 21, 21, 1, 1),
                                   c: (22, 22, 22, 22, 1, 1)}),
        }
        result = build_returns(contracts)
        self.assertAlmostEqual(result["returns"][0]["daily_return"], .1)
        self.assertAlmostEqual(result["returns"][1]["daily_return"], 22 / 21 - 1)
        self.assertEqual(len(result["rolls"]), 1)

    def test_missing_year_not_filled(self):
        a, b = dt.date(2001, 1, 2), dt.date(2009, 1, 2)
        contracts = {"SB10H": ((2010, 3), {
            a: (10, 10, 10, 10, 1, 1), b: (20, 20, 20, 20, 1, 1)})}
        result = build_returns(contracts)
        self.assertEqual(result["returns"], [])
        self.assertEqual(result["breaks"][0]["reason"], "missing_sessions")

    def test_coverage(self):
        result = audit_coverage([{"date": "2001-01-02"}, {"date": "2003-01-02"}])
        self.assertEqual(result["missing_years"], [2002])


if __name__ == "__main__":
    unittest.main()
