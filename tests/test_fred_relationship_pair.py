"""Tests for generic FRED macro relationship builder (no live FRED calls)."""

from __future__ import annotations

import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from hptl.macro import fred_relationship_pair as fr


class TestFormatRelationshipDigest(unittest.TestCase):
    def test_none(self) -> None:
        t = fr.format_relationship_digest(None, "Gold", "Dollar index", "monthly")
        self.assertIn("filling in", t.lower())

    def test_loose(self) -> None:
        t = fr.format_relationship_digest(0.05, "Silver", "Gold", "monthly")
        self.assertTrue("loose" in t.lower() or "dominating" in t.lower())

    def test_negative_strong(self) -> None:
        t = fr.format_relationship_digest(-0.4, "Nasdaq", "US 10Y", "daily")
        self.assertTrue("opposite" in t.lower() or "other" in t.lower())


class TestCorrRegime(unittest.TestCase):
    def test_weak_short_series(self) -> None:
        s = pd.Series([0.2, 0.15, 0.1] * 4)
        self.assertEqual(fr._corr_regime(s), "weak")

    def test_active_stable(self) -> None:
        rng = np.random.default_rng(42)
        s = pd.Series(0.35 + rng.normal(0, 0.02, 40))
        r = fr._corr_regime(s)
        self.assertIn(r, ("active", "weak", "unstable", "diverging"))


class TestBuildRelationshipPayloadMocked(unittest.TestCase):
    def test_daily_payload_shape(self) -> None:
        n = 300
        dates = pd.bdate_range("2018-06-01", periods=n)
        price = pd.DataFrame({"date": dates, "value": np.linspace(7000.0, 9000.0, n)})
        driver = pd.DataFrame({"date": dates, "value": np.linspace(2.5, 3.2, n)})
        prof: fr.RelationshipProfile = {
            "market": "NASDAQ / NQ",
            "price_fred_id": "NASDAQCOM",
            "price_display": "Nasdaq Composite",
            "driver_fred_id": "DGS10",
            "driver_id": "dgs10",
            "driver_display": "US 10Y Treasury yield",
            "driver_is_yield": True,
            "cadence": "daily",
            "observation_start": "2018-01-01",
            "rolling_primary": 20,
            "rolling_secondary": 30,
            "rolling_tertiary": 60,
        }
        with patch.object(fr, "_fred_series_csv", side_effect=[price, driver]):
            out = fr.build_relationship_payload(prof)
        self.assertTrue(out.get("available"))
        self.assertEqual(out.get("market"), "NASDAQ / NQ")
        self.assertEqual(len(out.get("dates", [])), len(out.get("price_rebased_pct", [])))
        self.assertIn("nasdaq_rebased_pct", out)
        self.assertIn("digest", out)
        self.assertIn("correlation_regime", out)
