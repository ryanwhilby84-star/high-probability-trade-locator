"""Core-universe price freshness audit (LEGACY_COT_MARKETS).

Usage:
  python scripts/run_core_price_freshness_audit.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.markets.instrument_registry import LEGACY_COT_MARKETS, get_instrument
from hptl.prices.price_freshness import build_instrument_price_freshness
from hptl.prices.price_store import load_instrument_record_internal

OUT_JSON = ROOT / "data" / "audits" / "core_price_freshness_audit.json"
OUT_MD = ROOT / "data" / "audits" / "core_price_freshness_audit.md"


def main() -> int:
    now = datetime.now(timezone.utc)
    rows = []
    for mid in LEGACY_COT_MARKETS:
        spec = get_instrument(mid)
        rec = load_instrument_record_internal(mid) or {}
        scale = rec.get("price_scale") or {}
        provider = scale.get("source") or (getattr(spec, "price_provider", None) if spec else None)
        symbol = scale.get("symbol") or (getattr(spec, "oanda_symbol", None) if spec else None)
        if not rec.get("daily") and not rec.get("price"):
            status = "unsupported" if not symbol else "failed"
            rows.append(
                {
                    "instrument_id": mid,
                    "provider": provider,
                    "symbol": symbol,
                    "status": status,
                    "overall_status": status.title() if status != "unsupported" else "Unsupported",
                }
            )
            continue
        fresh = build_instrument_price_freshness(
            rec, now=now, provider=provider, symbol=symbol
        )
        overall = fresh.get("overall_status") or "Failed"
        bucket = (
            "current"
            if overall == "Current"
            else "stale"
            if overall == "Stale"
            else "failed"
        )
        rows.append(
            {
                "instrument_id": mid,
                "provider": provider,
                "symbol": symbol,
                "status": bucket,
                "overall_status": overall,
                "live": fresh.get("live_quote"),
                "completed_daily": fresh.get("latest_completed_daily"),
                "completed_weekly": fresh.get("latest_completed_weekly"),
                "forming_daily": fresh.get("forming_daily"),
                "market_comparison": fresh.get("market_comparison"),
            }
        )

    summary = {
        "current": sum(1 for r in rows if r["status"] == "current"),
        "stale": sum(1 for r in rows if r["status"] == "stale"),
        "failed": sum(1 for r in rows if r["status"] == "failed"),
        "unsupported": sum(1 for r in rows if r["status"] == "unsupported"),
        "n": len(rows),
    }
    payload = {
        "generated_at": now.isoformat(),
        "universe": "LEGACY_COT_MARKETS",
        "summary": summary,
        "instruments": rows,
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    lines = [
        "# Core Price Freshness Audit",
        "",
        f"Generated: `{payload['generated_at']}`",
        "",
        f"- Current: **{summary['current']}**",
        f"- Stale: **{summary['stale']}**",
        f"- Failed: **{summary['failed']}**",
        f"- Unsupported: **{summary['unsupported']}**",
        "",
        "| Instrument | Status | Live | Completed daily | Weekly |",
        "|---|---|---:|---|---|",
    ]
    for r in rows:
        live = (r.get("live") or {}).get("price")
        live_st = (r.get("live") or {}).get("status")
        d = r.get("completed_daily") or {}
        w = r.get("completed_weekly") or {}
        lines.append(
            f"| {r['instrument_id']} | {r['overall_status']} | "
            f"{live} ({live_st}) | {d.get('date')} {d.get('close')} | "
            f"{w.get('date')} {w.get('close')} |"
        )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"wrote {OUT_JSON}")
    print(f"wrote {OUT_MD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
