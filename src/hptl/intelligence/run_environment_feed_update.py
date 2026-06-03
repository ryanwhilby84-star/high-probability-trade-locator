"""Refresh ``market_environment_feed`` on the latest confluence export (optional standalone run)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

from hptl.confluence.build_decision_table import OUT_PATH, TARGET_MARKETS
from hptl.config import get_finnhub_api_key, get_openweather_api_key
from hptl.intelligence.market_environment_feed import attach_feeds_to_latest_records
from hptl.intelligence.weather_context_export import write_weather_context_export
from hptl.news.economic_calendar_engine import write_economic_calendar_export
from hptl.news.economic_calendar_provider import live_feeds_disabled, provider_api_keys_status


def _feed_populated(feed: object) -> bool:
    if not isinstance(feed, dict) or not feed:
        return False
    if feed.get("news_items") or feed.get("event_items"):
        return True
    cal = feed.get("calendar_catalysts")
    if isinstance(cal, dict) and cal.get("wired"):
        return True
    if feed.get("weather_feed_connected"):
        return True
    if feed.get("records"):
        return True
    return False


def main() -> int:
    path = Path(OUT_PATH)
    if not path.exists():
        print(f"Missing export: {path}", file=sys.stderr)
        return 1

    status = provider_api_keys_status()
    print("=== Environment feed update ===")
    print(json.dumps(status, indent=2))
    print(f"FINNHUB_API_KEY loaded from .env: {'yes' if get_finnhub_api_key() else 'no'}")
    print(f"OPENWEATHER_API_KEY loaded from .env: {'yes' if get_openweather_api_key() else 'no'}")
    print(f"HPTL_SKIP_LIVE_FEEDS: {status['skip_live_feeds']}")

    if live_feeds_disabled():
        print("ERROR: HPTL_SKIP_LIVE_FEEDS is enabled — set to 0 or unset before live refresh.", file=sys.stderr)
        return 2
    if status["provider"] == "none":
        print(
            "ERROR: No economic calendar provider — set FINNHUB_API_KEY and/or TRADINGECONOMICS_API_KEY in .env.",
            file=sys.stderr,
        )
        return 2
    if not get_finnhub_api_key():
        print("WARNING: FINNHUB_API_KEY not detected — calendar/news feeds will stay NOT WIRED.", file=sys.stderr)

    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload.get("records")
    if not isinstance(records, list):
        print("Invalid confluence payload: no records array", file=sys.stderr)
        return 1

    latest_date = max((str(r.get("date") or "") for r in records), default="")
    attach_feeds_to_latest_records(records, markets=list(TARGET_MARKETS))

    cal_path = write_economic_calendar_export(markets=list(TARGET_MARKETS))
    print(f"Wrote {cal_path}")
    wx_path = write_weather_context_export(respect_skip_live=False)
    print(f"Wrote {wx_path}")

    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Updated {path}")

    latest_rows = [r for r in records if str(r.get("date") or "") == latest_date]
    populated = sum(1 for r in latest_rows if _feed_populated(r.get("market_environment_feed")))
    with_news = sum(
        1
        for r in latest_rows
        if isinstance(r.get("market_environment_feed"), dict)
        and (r["market_environment_feed"].get("news_items") or [])
    )
    print(f"Latest COT week: {latest_date}")
    print(f"Rows on latest week: {len(latest_rows)}")
    print(f"Rows with populated market_environment_feed: {populated}/{len(latest_rows)}")
    print(f"Rows with Finnhub news_items: {with_news}/{len(latest_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
