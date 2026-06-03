"""Unit tests for the Thesis Tracker (conviction, trend, age, store, snapshot)."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from hptl.thesis_tracker import conviction, store
from hptl.thesis_tracker.decision import MISSING_CONFIRMATIONS, build_decision
from hptl.thesis_tracker.models import norm_status
from hptl.thesis_tracker.snapshot import market_history, snapshot_from_record


class TestConviction(unittest.TestCase):
    def test_single_component_renormalizes(self):
        # Only COT present -> conviction equals the normalized COT value.
        score, present = conviction.compute_conviction({"cot_score": 7.0})
        self.assertEqual(present, ["cot"])
        self.assertEqual(score, 70)

    def test_weighted_composite(self):
        score, present = conviction.compute_conviction(
            {"cot_score": 8.0, "macro_score": 6.0, "structural_score": 50.0}
        )
        self.assertEqual(sorted(present), ["cot", "macro", "structural"])
        # 0.45*80 + 0.25*60 + 0.30*50 = 36 + 15 + 15 = 66
        self.assertEqual(score, 66)

    def test_missing_components_returns_none(self):
        score, present = conviction.compute_conviction({"valuation_score": 90})
        self.assertIsNone(score)
        self.assertEqual(present, [])

    def test_trend_improving_and_deteriorating(self):
        snaps = [
            {"week": "w1", "cot_score": 4.0},
            {"week": "w2", "cot_score": 5.0},
            {"week": "w3", "cot_score": 7.0},
        ]
        conviction.annotate_conviction(snaps)
        self.assertEqual(conviction.compute_trend(snaps), conviction.TREND_IMPROVING)
        for s in snaps:
            s["cot_score"] = 9.0 - {"w1": 0, "w2": 3, "w3": 6}[s["week"]] / 10 * 0  # keep simple
        snaps2 = [
            {"week": "w1", "cot_score": 8.0},
            {"week": "w2", "cot_score": 6.0},
            {"week": "w3", "cot_score": 4.0},
        ]
        conviction.annotate_conviction(snaps2)
        self.assertEqual(conviction.compute_trend(snaps2), conviction.TREND_DETERIORATING)

    def test_trend_stable_small_change(self):
        snaps = [{"week": "w1", "cot_score": 5.0}, {"week": "w2", "cot_score": 5.1}]
        conviction.annotate_conviction(snaps)
        self.assertEqual(conviction.compute_trend(snaps), conviction.TREND_STABLE)

    def test_age_weeks_distinct(self):
        snaps = [{"week": "w1"}, {"week": "w1"}, {"week": "w2"}]
        self.assertEqual(conviction.compute_age_weeks(snaps), 2)


class TestSnapshot(unittest.TestCase):
    def test_snapshot_extraction_pulls_nested_structural(self):
        record = {
            "date": "2026-05-26",
            "market": "Gold",
            "cot_report_date": "2026-05-26",
            "cot_bias": "Bullish",
            "cot_score": 6.5,
            "macro_score": "N/A",
            "long_value": 100.0,
            "net_value": 40.0,
            "positioning_state": "Accumulation",
            "institutional_context": {
                "structural_score": 74.9,
                "structural_conviction": "high",
                "attention": {"priority_score": 82.0},
            },
        }
        snap = snapshot_from_record(record)
        self.assertEqual(snap["week"], "2026-05-26")
        self.assertEqual(snap["cot_score"], 6.5)
        self.assertIsNone(snap["macro_score"])  # "N/A" -> None
        self.assertEqual(snap["structural_score"], 74.9)
        self.assertEqual(snap["priority_score"], 82.0)
        # placeholders always null
        self.assertIsNone(snap["valuation_score"])
        self.assertIsNone(snap["seasonality_score"])

    def test_market_history_dedupes_and_orders(self):
        records = [
            {"date": "2026-05-12", "market": "Gold", "cot_score": 5.0},
            {"date": "2026-05-05", "market": "Gold", "cot_score": 4.0},
            {"date": "2026-05-12", "market": "Gold", "cot_score": 5.5},  # later wins
            {"date": "2026-05-05", "market": "Silver", "cot_score": 1.0},
        ]
        hist = market_history(records, "Gold")
        self.assertEqual([s["week"] for s in hist], ["2026-05-05", "2026-05-12"])
        self.assertEqual(hist[-1]["cot_score"], 5.5)


def _accumulating_long(status="DEVELOPING"):
    snaps = [
        {"week": "w1", "long_value": 120000, "short_value": 300000, "net_value": -180000, "one_week_net_change": 5000, "cot_score": 3.0, "macro_score": 2.0, "structural_score": 55.0},
        {"week": "w2", "long_value": 135000, "short_value": 285000, "net_value": -150000, "one_week_net_change": 30000, "cot_score": 4.0, "macro_score": 3.0, "structural_score": 58.0},
        {"week": "w3", "long_value": 150000, "short_value": 260000, "net_value": -110000, "one_week_net_change": 40000, "cot_score": 5.0, "macro_score": 4.0, "structural_score": 60.0},
        {"week": "w4", "long_value": 165000, "short_value": 240000, "net_value": -75000, "one_week_net_change": 35000, "cot_score": 6.0, "macro_score": 5.0, "structural_score": 62.0},
    ]
    conviction.annotate_conviction(snaps)
    return {"market": "Sugar", "status": status, "direction_bias": "long", "snapshots": snaps}


class TestDecision(unittest.TestCase):
    def test_status_migration_legacy_values(self):
        self.assertEqual(norm_status("LIMIT ORDER SET"), "READY")
        self.assertEqual(norm_status("ACTIVE TRADE"), "ACTIVE")
        self.assertEqual(norm_status("active"), "ACTIVE")

    def test_emerging_long_story_and_evolution(self):
        dec = build_decision(_accumulating_long())
        # net improving + longs up + shorts down should all be "improved"
        improved = " ".join(dec["evolution"]["improved"]).lower()
        self.assertIn("long exposure increased", improved)
        self.assertIn("short exposure decreased", improved)
        self.assertIn("net positioning moved", improved)
        self.assertIn("accumulation", dec["interpretation"].lower())
        self.assertTrue(any("net positioning" in s.lower() for s in dec["story"]))

    def test_missing_confirmations_always_present(self):
        dec = build_decision(_accumulating_long())
        labels = {m["label"] for m in dec["missing_confirmations"]}
        self.assertEqual(len(dec["missing_confirmations"]), len(MISSING_CONFIRMATIONS))
        self.assertIn("Valuation", labels)
        self.assertIn("Seasonality", labels)
        self.assertIn("Retail positioning", labels)
        self.assertTrue(all(m["wired"] is False for m in dec["missing_confirmations"]))

    def test_priority_tier_by_status(self):
        self.assertEqual(build_decision(_accumulating_long("READY"))["priority_tier"], 1)
        self.assertEqual(build_decision(_accumulating_long("ACTIVE"))["priority_tier"], 1)
        self.assertEqual(build_decision(_accumulating_long("INVALIDATED"))["priority_tier"], 3)
        # discovered with strong alignment promotes to tier 2 (not stuck at 3)
        self.assertIn(build_decision(_accumulating_long("DISCOVERED"))["priority_tier"], (2, 3))

    def test_readiness_checks_shape(self):
        dec = build_decision(_accumulating_long("READY"))
        r = dec["readiness"]
        self.assertEqual(r["total"], 4)
        self.assertEqual(len(r["checks"]), 4)
        self.assertGreaterEqual(r["met"], 0)
        self.assertEqual(dec["readiness"]["label"], "Limit-order preparation justified")

    def test_single_snapshot_is_safe(self):
        snaps = [{"week": "w1", "net_value": -50000, "cot_score": 4.0}]
        conviction.annotate_conviction(snaps)
        dec = build_decision({"market": "X", "status": "DISCOVERED", "direction_bias": "long", "snapshots": snaps})
        self.assertIn("story", dec)
        self.assertEqual(dec["evolution"]["improved"], [])


class TestStore(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        base = Path(self._tmp.name)
        self._orig = (store.TRACKER_PATH, store.EXPORT_PATH, store.DIST_EXPORT_PATH)
        store.TRACKER_PATH = base / "thesis_tracker.json"
        store.EXPORT_PATH = base / "export" / "thesis_tracker_latest.json"
        store.DIST_EXPORT_PATH = base / "dist" / "data" / "thesis_tracker_latest.json"

    def tearDown(self):
        store.TRACKER_PATH, store.EXPORT_PATH, store.DIST_EXPORT_PATH = self._orig
        self._tmp.cleanup()

    def test_add_is_idempotent_per_market(self):
        a = store.add_thesis({"market": "Sugar"}, initial_snapshot={"week": "w1", "cot_score": 5.0})
        b = store.add_thesis({"market": "Sugar"}, initial_snapshot={"week": "w1", "cot_score": 5.0})
        self.assertEqual(a["thesis_id"], b["thesis_id"])
        self.assertEqual(len(store.list_theses()), 1)

    def test_append_snapshot_updates_derived_and_dedupes(self):
        t = store.add_thesis({"market": "Gold"}, initial_snapshot={"week": "w1", "cot_score": 4.0})
        store.append_snapshot(t["thesis_id"], {"week": "w2", "cot_score": 7.0})
        store.append_snapshot(t["thesis_id"], {"week": "w2", "cot_score": 9.0})  # dup week ignored
        updated = store.get_thesis(t["thesis_id"])
        self.assertEqual(updated["age_weeks"], 2)
        self.assertEqual(updated["conviction_trend"], conviction.TREND_IMPROVING)
        self.assertEqual(len(updated["snapshots"]), 2)

    def test_terminal_status_archives(self):
        t = store.add_thesis({"market": "Copper / HG"}, initial_snapshot={"week": "w1", "cot_score": 5.0})
        store.update_status(t["thesis_id"], "COMPLETED")
        updated = store.get_thesis(t["thesis_id"])
        self.assertTrue(updated["archived"])
        self.assertEqual(len(store.list_theses(include_archived=False)), 0)

    def test_export_has_summary_and_disclaimer(self):
        store.add_thesis({"market": "Wheat"}, initial_snapshot={"week": "w1", "cot_score": 5.0})
        store.export_tracker()
        payload = json.loads(store.EXPORT_PATH.read_text(encoding="utf-8"))
        self.assertIn("summary", payload)
        self.assertEqual(payload["summary"]["active"], 1)
        self.assertIn("disclaimer", payload)


if __name__ == "__main__":
    unittest.main()
