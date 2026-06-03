"""Validate trade journal: manual entry, webhook entry, JSON export."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.journal.store import EXPORT_PATH, JOURNAL_PATH, create_entry, export_journal, list_entries
from hptl.journal.webhook import handle_webhook_body


def main() -> int:
    print("=== Trade journal validation (logging only) ===\n")

    manual = create_entry(
        {
            "market": "Wheat",
            "symbol": "ZW",
            "direction": "long",
            "status": "idea",
            "entry_price": 6.45,
            "stop_loss": 6.2,
            "target_1": 6.8,
            "setup_type": "COT confluence test",
            "notes": "Sample manual trade idea",
            "cot_bias": "bullish",
            "cot_score": 72,
            "macro_bias": "risk-on",
            "weather_bias": "Mixed",
            "catalyst_risk": "clean",
            "dashboard_snapshot": {"cot_week": "2026-01-14", "source": "validate_cli"},
        },
        source="validate_manual",
    )
    print(f"Manual entry: {manual['trade_id']} ({manual['market']} {manual['direction']})")

    webhook_payload = {
        "market": "Natural Gas / NG",
        "symbol": "NG",
        "direction": "short",
        "entry_price": 3.12,
        "stop_loss": 3.35,
        "target_1": 2.85,
        "target_2": 2.7,
        "setup_type": "HTF supply rejection",
        "status": "planned",
        "notes": "Limit order idea from weekly supply",
    }
    webhook = handle_webhook_body(json.dumps(webhook_payload).encode("utf-8"))
    print(f"Webhook entry: {webhook['trade_id']} ({webhook['market']} {webhook['status']})")

    export_path = export_journal()
    print(f"\nStored: {JOURNAL_PATH.resolve()}")
    print(f"Export: {export_path.resolve()}")
    print(f"Entries: {len(list_entries())}")

    doc = json.loads(export_path.read_text(encoding="utf-8"))
    if not doc.get("entries"):
        print("ERROR: export has no entries", file=sys.stderr)
        return 1
    print("\nOK — trade journal logging only (no broker execution).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
