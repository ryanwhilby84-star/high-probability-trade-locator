"""Re-attach metals valuation fields on existing confluence export rows (no full rebuild)."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from hptl.confluence.dashboard_export import OUT_PATH, write_dashboard_exports
from hptl.pillars.confluence_attach import _metals_valuation_pillar_overlay
from hptl.valuation.metals_valuation_v1 import METALS_MARKETS

ROOT = Path(__file__).resolve().parents[1]


def refresh_metals_confluence_valuation(*, confluence_path: Path | None = None) -> dict:
    path = confluence_path or OUT_PATH
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = list(payload.get("records") or [])
    cache: dict[tuple[str, str], dict] = {}
    updated = 0
    wired = 0

    for rec in records:
        market = str(rec.get("market") or "")
        if market not in METALS_MARKETS:
            continue
        week = str(rec.get("date") or "")[:10]
        if not week:
            continue
        key = (market, week)
        if key not in cache:
            cache[key] = _metals_valuation_pillar_overlay(market, week)
        overlay = cache[key]
        if not overlay:
            continue
        rec.update(overlay)
        updated += 1
        if overlay.get("valuation_wired") is True:
            wired += 1

    payload["records"] = records
    payload["generated_at"] = datetime.now(timezone.utc).isoformat()
    write_dashboard_exports(payload)
    return {
        "records_scanned": sum(1 for r in records if r.get("market") in METALS_MARKETS),
        "records_updated": updated,
        "records_wired": wired,
        "unique_week_calls": len(cache),
    }


def main() -> int:
    result = refresh_metals_confluence_valuation()
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
