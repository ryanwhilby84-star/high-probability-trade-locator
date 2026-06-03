"""Trade journal store and webhook (no network)."""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from hptl.journal import store
from hptl.journal.webhook import handle_webhook_body, verify_webhook_secret


class TestTradeJournal(unittest.TestCase):
    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self._journal = Path(self._td.name) / "trade_journal.json"
        self._export = Path(self._td.name) / "export.json"
        patcher_j = patch.object(store, "JOURNAL_PATH", self._journal)
        patcher_e = patch.object(store, "EXPORT_PATH", self._export)
        patcher_j.start()
        patcher_e.start()
        self.addCleanup(patcher_j.stop)
        self.addCleanup(patcher_e.stop)

    def test_create_and_export(self) -> None:
        entry = store.create_entry(
            {"market": "Wheat", "direction": "long", "status": "idea", "entry_price": 6.5},
            source="test",
        )
        self.assertTrue(entry["trade_id"])
        path = store.export_journal()
        doc = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(len(doc["entries"]), 1)

    def test_webhook_secret(self) -> None:
        with patch.dict(os.environ, {"TRADINGVIEW_WEBHOOK_SECRET": "sekrit"}, clear=False):
            self.assertTrue(verify_webhook_secret({"x-tradingview-webhook-secret": "sekrit"}, {}))
            self.assertFalse(verify_webhook_secret({"x-tradingview-webhook-secret": "wrong"}, {}))

    def test_webhook_upsert(self) -> None:
        body = json.dumps(
            {
                "market": "Wheat",
                "symbol": "WHEATUSD",
                "direction": "short",
                "entry_price": 6.62,
                "stop_loss": 6.8,
                "status": "planned",
            }
        ).encode()
        first = handle_webhook_body(body)
        second = handle_webhook_body(
            json.dumps({**json.loads(body.decode()), "trade_id": first["trade_id"], "status": "order_set"}).encode()
        )
        self.assertEqual(first["trade_id"], second["trade_id"])
        self.assertEqual(second["status"], "order_set")


if __name__ == "__main__":
    unittest.main()
