"""Fetch OpenWeather context and write ``weather_context_latest.json``."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.config import get_openweather_api_key
from hptl.intelligence.weather_context_export import write_weather_context_export
from hptl.news.economic_calendar_provider import live_feeds_disabled


def _safe_print(line: str) -> None:
    enc = getattr(sys.stdout, "encoding", None) or "utf-8"
    try:
        print(line)
    except UnicodeEncodeError:
        print(line.encode(enc, errors="replace").decode(enc, errors="replace"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Update weather_context_latest.json from OpenWeather.")
    parser.add_argument(
        "--ignore-skip-flag",
        action="store_true",
        help="Fetch even when HPTL_SKIP_LIVE_FEEDS is set",
    )
    args = parser.parse_args()

    _safe_print("=== Weather context update ===\n")
    _safe_print(f"Provider: openweather")
    _safe_print(f"OPENWEATHER_API_KEY detected: {'yes' if get_openweather_api_key() else 'no'}")
    _safe_print(f"HPTL_SKIP_LIVE_FEEDS: {live_feeds_disabled()}")

    out_path = write_weather_context_export(respect_skip_live=not args.ignore_skip_flag)
    doc = json.loads(out_path.read_text(encoding="utf-8"))

    _safe_print(f"\nLocations queried: {len(doc.get('locations_queried') or [])}")
    for loc in doc.get("locations_queried") or []:
        _safe_print(f"  {loc.get('market')} - {loc.get('region')} ({loc.get('lat')}, {loc.get('lon')})")

    _safe_print(f"\nForecast records fetched: {doc.get('forecast_records_loaded', 0)}")
    _safe_print(f"Forecast records OK: {doc.get('forecast_records_ok', 0)}")
    _safe_print(f"Output file: {out_path.resolve()}")

    if doc.get("error"):
        _safe_print(f"Bundle note: {doc.get('error')}")

    for market, block in (doc.get("markets") or {}).items():
        records = block.get("records") or []
        _safe_print(f"\n--- {market} ({block.get('status')}) ---")
        for row in records:
            if row.get("ok"):
                _safe_print(
                    f"  {row.get('region')}: {row.get('temperature_display')} | "
                    f"{row.get('precipitation_display')} | {row.get('forecast_summary')}"
                )
            else:
                _safe_print(f"  {row.get('region')}: ERROR — {row.get('error')}")

    ok = int(doc.get("forecast_records_ok") or 0)
    return 0 if ok > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
