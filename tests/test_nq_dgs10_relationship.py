"""Unit tests for NASDAQ vs DGS10 relationship builder (no network)."""

from __future__ import annotations

import unittest

import pandas as pd

from hptl.macro import nq_dgs10_relationship as mod


class TestNqDgs10Digest(unittest.TestCase):
    def test_digest_weak(self) -> None:
        t = mod._digest(0.05)
        self.assertTrue("loose" in t.lower() or "weak" in t.lower())

    def test_digest_negative(self) -> None:
        t = mod._digest(-0.4)
        self.assertIn("opposite", t.lower())


class TestRollingCorrSynthetic(unittest.TestCase):
    def test_rolling_corr_length(self) -> None:
        n = 60
        dates = pd.date_range("2020-01-01", periods=n, freq="B")
        nasdaq = pd.Series(range(100, 100 + n), dtype=float) + pd.Series(range(n)).apply(lambda i: (i % 5) * 0.1)
        dgs10 = pd.Series(2.0, index=range(n), dtype=float) + pd.Series(range(n)).apply(lambda i: i * 0.001)
        df = pd.DataFrame({"date": dates, "nasdaq": nasdaq.values, "dgs10": dgs10.values})
        nq_ret = df["nasdaq"].pct_change()
        y10_d = df["dgs10"].diff()
        c20 = nq_ret.rolling(20, min_periods=15).corr(y10_d)
        self.assertEqual(len(c20), n)
        self.assertTrue(pd.isna(c20.iloc[10]))  # early warm-up
