"""Weekly run log persistence."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.cot import weekly_run_log as wrl


class TestWeeklyRunLog(unittest.TestCase):
    def test_persist_writes_latest_and_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            wrl.EXPORTS_DIR = base
            wrl.WEEKLY_TEXT_LOG = base / "weekly_cot_update.log"
            wrl.WEEKLY_JSON_LATEST = base / "weekly_cot_update_latest.json"
            wrl.WEEKLY_JSON_HISTORY = base / "weekly_cot_update_history.jsonl"

            payload = {
                "run_timestamp_utc": "2026-05-17T12:00:00+00:00",
                "update_performed": True,
                "exit_code": 0,
            }
            wrl.persist_weekly_run(payload, human_lines=["update performed: True"])

            self.assertTrue(wrl.WEEKLY_TEXT_LOG.exists())
            latest = json.loads(wrl.WEEKLY_JSON_LATEST.read_text(encoding="utf-8"))
            self.assertTrue(latest["update_performed"])
            lines = wrl.WEEKLY_JSON_HISTORY.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 1)


if __name__ == "__main__":
    unittest.main()
