"""Regression tests for fail-closed dashboard data packaging."""
import json
from datetime import date, timedelta
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from scripts.package_dashboard_data_release import REQUIRED, main, validate


class DataReleaseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.data = self.root / "data"
        self.data.mkdir()
        for name, keys in REQUIRED.items():
            doc = {key: [{"sample": True}] for key in keys}
            if name == "confluence_history_latest.json":
                doc["latest_cot_report_date"] = date.today().isoformat()
            (self.data / name).write_text(json.dumps(doc), encoding="utf-8")

    def test_complete_snapshot_validates(self):
        errors, files = validate(self.data)
        self.assertEqual(errors, [])
        self.assertEqual(set(files), set(REQUIRED))

    def test_missing_data_blocks_release(self):
        (self.data / "prices_latest.json").unlink()
        errors, _ = validate(self.data)
        self.assertTrue(any("prices_latest.json: missing" in e for e in errors))

    def test_empty_price_snapshot_blocks_release(self):
        (self.data / "prices_latest.json").write_text("{}")
        errors, _ = validate(self.data)
        self.assertTrue(any("empty price snapshot" in error for error in errors))

    def test_empty_confluence_blocks_release(self):
        (self.data / "confluence_history_latest.json").write_text(
            json.dumps({"records": [], "latest_cot_report_date": date.today().isoformat()})
        )
        errors, _ = validate(self.data)
        self.assertTrue(any("records missing or empty" in e for e in errors))

    def test_stale_cot_blocks_release(self):
        path = self.data / "confluence_history_latest.json"
        doc = json.loads(path.read_text())
        doc["latest_cot_report_date"] = (date.today() - timedelta(days=30)).isoformat()
        path.write_text(json.dumps(doc))
        errors, _ = validate(self.data, max_cot_age_days=21)
        self.assertTrue(any("report date age" in e for e in errors))

    def test_future_cot_blocks_release(self):
        path = self.data / "confluence_history_latest.json"
        doc = json.loads(path.read_text())
        doc["latest_cot_report_date"] = (date.today() + timedelta(days=1)).isoformat()
        path.write_text(json.dumps(doc))
        errors, _ = validate(self.data, max_cot_age_days=21)
        self.assertTrue(any("report date age" in e for e in errors))

    def test_invalid_json_blocks_release(self):
        (self.data / "prices_latest.json").write_text("{broken")
        errors, _ = validate(self.data)
        self.assertTrue(any("prices_latest.json" in e for e in errors))

    def test_successful_release_has_manifest_and_checksums(self):
        out = self.root / "release.zip"
        with patch("sys.argv", ["package_dashboard_data_release.py", "--data-dir", str(self.data), "--output", str(out)]):
            self.assertEqual(main(), 0)
        with zipfile.ZipFile(out) as z:
            manifest = json.loads(z.read("manifest.json"))
            self.assertEqual(set(manifest["files"]), set(REQUIRED))
            for name, meta in manifest["files"].items():
                self.assertEqual(len(meta["sha256"]), 64)
                self.assertEqual(z.read("data/" + name), (self.data / name).read_bytes())

    def test_failed_release_does_not_create_archive(self):
        (self.data / "prices_latest.json").unlink()
        out = self.root / "release.zip"
        with patch("sys.argv", ["package_dashboard_data_release.py", "--data-dir", str(self.data), "--output", str(out)]):
            self.assertEqual(main(), 1)
        self.assertFalse(out.exists())


if __name__ == "__main__":
    unittest.main()
