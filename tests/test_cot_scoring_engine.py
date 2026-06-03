"""COT scoring fault tolerance and Corn audit."""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

os.environ["HPTL_SKIP_PRICE_SCORING"] = "1"

from hptl.cot.scoring_engine import (
    apply_probabilistic_cot_scoring,
    score_cot_row,
    score_cot_row_with_diagnostics,
)


class TestCotScoringEngine(unittest.TestCase):
    def test_bearish_flow_against_price_low_confidence(self) -> None:
        res = score_cot_row(
            net=-40_000.0,
            w1=-8_000.0,
            w4=-20_000.0,
            long_w1=-2_000.0,
            short_w1=5_000.0,
            persist={"aligned_weeks": 3.0, "opposed_weeks": 0.0, "accel_ratio": -0.5, "participation_expansion": 1.0},
            price_week_pct=2.5,
        )
        self.assertIn("Bearish", res.cot_bias)
        self.assertGreater(res.signal_strength, 4.0)
        self.assertLess(res.score_confidence, 0.5)
        self.assertIn(res.market_state, ("squeeze_risk", "positioning_failure", "distribution_during_strength"))

    def test_scores_are_not_only_even_integers(self) -> None:
        rows = []
        for w1 in (-12000, -6000, 3000, 8000, 15000):
            rows.append(
                score_cot_row(
                    net=-25_000.0,
                    w1=float(w1),
                    w4=float(w1) * 2.2,
                    long_w1=500.0,
                    short_w1=-200.0,
                    persist={"aligned_weeks": 2.0, "opposed_weeks": 0.0, "accel_ratio": 0.0, "participation_expansion": 0.0},
                    price_week_pct=None,
                ).cot_score
            )
        unique = set(rows)
        self.assertGreater(len(unique), 2)
        self.assertTrue(any(abs(s - round(s)) > 0.01 or s != round(s) for s in rows if s > 0))

    def test_persistence_raises_signal(self) -> None:
        low = score_cot_row(
            net=-30_000.0,
            w1=-5_000.0,
            w4=-10_000.0,
            long_w1=None,
            short_w1=None,
            persist={"aligned_weeks": 0.0, "opposed_weeks": 0.0, "accel_ratio": 0.0, "participation_expansion": 0.0},
            price_week_pct=-1.0,
        )
        high = score_cot_row(
            net=-30_000.0,
            w1=-5_000.0,
            w4=-10_000.0,
            long_w1=None,
            short_w1=None,
            persist={"aligned_weeks": 4.0, "opposed_weeks": 0.0, "accel_ratio": -0.3, "participation_expansion": 2.0},
            price_week_pct=-1.0,
        )
        self.assertGreater(high.signal_strength, low.signal_strength)
        self.assertGreater(high.score_confidence, low.score_confidence)

    def test_apply_on_frame(self) -> None:
        cot = pd.DataFrame(
            {
                "market": ["Gold", "Gold"],
                "cot_report_date": pd.to_datetime(["2026-05-05", "2026-05-12"]),
                "net_value": [-20_000.0, -25_000.0],
                "weekly_change": [-3_000.0, -5_000.0],
                "four_week_change": [-8_000.0, -12_000.0],
                "long_weekly_change": [100.0, 200.0],
                "short_weekly_change": [-500.0, -800.0],
            }
        )
        out = apply_probabilistic_cot_scoring(cot)
        self.assertIn("signal_strength", out.columns)
        self.assertIn("score_confidence", out.columns)
        self.assertTrue(len(str(out.iloc[-1]["cot_summary"])) > 40)

    def test_invalid_strings_do_not_crash(self) -> None:
        diag = score_cot_row_with_diagnostics(
            net="N/A",
            w1="-56100",
            w4="bad",
            long_w1=None,
            short_w1="8136",
            persist={"aligned_weeks": 3.0, "opposed_weeks": 2.0, "accel_ratio": -2.0, "participation_expansion": 1.0},
            price_week_pct=None,
        )
        self.assertIsNotNone(diag.result.cot_bias)
        self.assertFalse(diag.validation.fields[0].valid)  # net_value invalid
        self.assertTrue(diag.validation.fields[1].valid)  # weekly_change valid string

    def test_corn_example_20260526(self) -> None:
        persist = {
            "aligned_weeks": 3.0,
            "opposed_weeks": 2.0,
            "accel_ratio": -1.078,
            "participation_expansion": 1.0,
        }
        diag = score_cot_row_with_diagnostics(
            net=302_002,
            w1=-56_100,
            w4=-38_738,
            long_w1=-47_964,
            short_w1=8_136,
            persist=persist,
            price_week_pct=None,
        )
        res = diag.result
        self.assertEqual(res.cot_directional_bias, "Bullish")
        self.assertEqual(res.cot_bias, "Bullish / Weakening")
        self.assertAlmostEqual(res.signal_strength, 4.4, places=1)
        components = {c.name: c for c in diag.components}
        self.assertEqual(components["weekly_momentum"].status, "OK")
        self.assertAlmostEqual(components["weekly_momentum"].score, 0.0, places=2)
        self.assertAlmostEqual(components["trend_persistence"].score, 0.575, places=3)


if __name__ == "__main__":
    unittest.main()
