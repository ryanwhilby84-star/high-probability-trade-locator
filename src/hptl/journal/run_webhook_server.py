"""Local HTTP server for TradingView webhooks and manual journal POSTs (logging only)."""
from __future__ import annotations

import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.journal.store import create_entry, export_journal, list_entries
from hptl.journal.webhook import handle_webhook_body, verify_webhook_secret


class JournalHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: object) -> None:
        print(f"[journal] {self.address_string()} - {fmt % args}")

    def _read_json(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length > 0 else b""

    def _headers_lower(self) -> dict[str, str]:
        return {k.lower(): v for k, v in self.headers.items()}

    def _query(self) -> dict[str, str]:
        parsed = urlparse(self.path)
        flat: dict[str, str] = {}
        for k, vals in parse_qs(parsed.query).items():
            if vals:
                flat[k] = vals[0]
        return flat

    def _path(self) -> str:
        return urlparse(self.path).path.rstrip("/") or "/"

    def _send(self, code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-TradingView-Webhook-Secret, X-Webhook-Secret, Authorization")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:
        self._send(204, {})

    def do_GET(self) -> None:
        path = self._path()
        if path in ("/health", "/journal/health"):
            self._send(200, {"ok": True, "service": "hptl-trade-journal", "execution": False})
            return
        if path == "/journal/entries":
            q = self._query()
            rows = list_entries(status=q.get("status"), market=q.get("market"))
            self._send(200, {"entries": rows, "count": len(rows)})
            return
        self._send(404, {"error": "not found"})

    def do_POST(self) -> None:
        headers = self._headers_lower()
        query = self._query()
        path = self._path()
        body = self._read_json()

        if path in ("/webhook/tradingview", "/journal/webhook/tradingview"):
            if not verify_webhook_secret(headers, query):
                self._send(401, {"error": "invalid or missing TRADINGVIEW_WEBHOOK_SECRET"})
                return
            try:
                entry = handle_webhook_body(body)
            except ValueError as exc:
                self._send(400, {"error": str(exc)})
                return
            export_path = export_journal()
            self._send(200, {"ok": True, "entry": entry, "export_path": str(export_path)})
            return

        if path == "/journal/entries":
            if not verify_webhook_secret(headers, query):
                self._send(401, {"error": "invalid or missing TRADINGVIEW_WEBHOOK_SECRET"})
                return
            try:
                data = json.loads(body.decode("utf-8") if body else "{}")
            except json.JSONDecodeError:
                self._send(400, {"error": "invalid JSON"})
                return
            if not isinstance(data, dict):
                self._send(400, {"error": "payload must be object"})
                return
            try:
                if data.get("trade_id"):
                    from hptl.journal.store import upsert_entry

                    entry = upsert_entry(data, source="dashboard_manual")
                else:
                    entry = create_entry(data, source="dashboard_manual")
            except ValueError as exc:
                self._send(400, {"error": str(exc)})
                return
            export_path = export_journal()
            self._send(200, {"ok": True, "entry": entry, "export_path": str(export_path)})
            return

        self._send(404, {"error": "not found"})


def main() -> int:
    port = int(__import__("os").getenv("HPTL_JOURNAL_PORT", "8787"))
    host = __import__("os").getenv("HPTL_JOURNAL_HOST", "127.0.0.1")
    server = ThreadingHTTPServer((host, port), JournalHandler)
    print(f"Trade journal server (logging only) http://{host}:{port}")
    print("  POST /webhook/tradingview  — TradingView alerts")
    print("  POST /journal/entries     — manual dashboard saves")
    print("  GET  /journal/entries     — list entries")
    print("Set TRADINGVIEW_WEBHOOK_SECRET in .env")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
