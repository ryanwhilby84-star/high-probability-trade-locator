"""Validate economic calendar ingestion (provider, keys, sample events)."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.confluence.build_decision_table import TARGET_MARKETS
from hptl.intelligence.catalyst_loader import load_catalyst_config
from hptl.news.economic_calendar_engine import build_calendar_views, fetch_enriched_calendar
from hptl.news.economic_calendar_provider import provider_api_keys_status


def _fmt_event(ev: dict) -> str:
    name = ev.get("event_name", "")
    d = ev.get("date", "")
    imp = ev.get("importance", "")
    act = ev.get("actual")
    fc = ev.get("forecast")
    prev = ev.get("previous")
    unit = ev.get("unit") or ""
    direction = ev.get("direction_vs_forecast")
    mkts = ", ".join((ev.get("affected_markets") or [])[:6])
    line = f"  {d} | {name} [{imp}]"
    if act is not None or fc is not None or prev is not None:
        line += f" | actual={act} forecast={fc} previous={prev}"
        if unit:
            line += f" ({unit})"
    if direction:
        line += f" | {direction} ({ev.get('magnitude_vs_forecast', '')})"
    if mkts:
        line += f"\n    markets: {mkts}"
    interp = ev.get("interpretation")
    if interp:
        line += f"\n    → {interp}"
    return line


def main() -> int:
    argparse.ArgumentParser(description="Validate economic calendar ingestion.").parse_args()

    status = provider_api_keys_status()
    print("=== Economic calendar validation ===\n")
    print(f"Provider used (resolved): {status['provider']}")
    print(f"FINNHUB_API_KEY detected: {status['finnhub_key']}")
    print(f"TRADING_ECONOMICS / TRADINGECONOMICS key detected: {status['trading_economics_key']}")
    print(f"HPTL_SKIP_LIVE_FEEDS: {status['skip_live_feeds']}")

    cfg = load_catalyst_config()
    bundle = fetch_enriched_calendar(catalyst_cfg=cfg)
    views = build_calendar_views(bundle, markets=list(TARGET_MARKETS))

    print(f"\nWired: {bundle.get('wired')}")
    if not bundle.get("wired"):
        print(f"Message: {bundle.get('message')}")
        return 0

    events = bundle.get("events") or []
    print(f"Events loaded: {len(events)}")
    if bundle.get("errors"):
        print(f"Upstream errors: {bundle['errors']}")

    print("\n--- Next 10 high-impact (upcoming) ---")
    for ev in (views.get("upcoming_high_impact") or [])[:10]:
        print(_fmt_event(ev))
    if not views.get("upcoming_high_impact"):
        print("  (none in window)")

    print("\n--- Latest 10 released (with actuals) ---")
    for ev in (views.get("latest_released") or [])[:10]:
        print(_fmt_event(ev))
    if not views.get("latest_released"):
        print("  (none in window)")

    risk = views.get("event_risk_by_market") or {}
    if risk:
        print("\n--- Event-risk badges (sample) ---")
        for m in list(TARGET_MARKETS)[:8]:
            print(f"  {m}: {risk.get(m, '—')}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
