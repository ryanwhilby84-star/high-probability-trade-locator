"""Validate OpenWeather ingestion and write ``weather_latest.json``."""
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
from hptl.intelligence.weather_export import write_weather_latest_export
from hptl.news.economic_calendar_provider import live_feeds_disabled


def _safe_print(line: str) -> None:
    """Avoid Windows console UnicodeEncodeError on region labels."""
    enc = getattr(sys.stdout, "encoding", None) or "utf-8"
    try:
        print(line)
    except UnicodeEncodeError:
        print(line.encode(enc, errors="replace").decode(enc, errors="replace"))


def main() -> int:
    argparse.ArgumentParser(description="Validate OpenWeather weather feed.").parse_args()

    _safe_print("=== Weather feed validation ===\n")
    _safe_print("Provider used: openweather (when wired)")
    _safe_print(f"OPENWEATHER_API_KEY detected: {'yes' if get_openweather_api_key() else 'no'}")
    _safe_print(f"HPTL_SKIP_LIVE_FEEDS: {live_feeds_disabled()}")

    out_path = write_weather_latest_export()
    doc = json.loads(out_path.read_text(encoding="utf-8"))

    _safe_print(f"\nLocations queried ({len(doc.get('locations_queried') or [])}):")
    for loc in doc.get("locations_queried") or []:
        _safe_print(f"  {loc.get('market')} - {loc.get('region')} ({loc.get('lat')}, {loc.get('lon')})")

    _safe_print(f"\nForecast records loaded: {doc.get('forecast_records_loaded', 0)}")
    _safe_print(f"Output file: {out_path.resolve()}")
    _safe_print(f"Wired: {doc.get('wired')}")
    if doc.get("message"):
        _safe_print(f"Message: {doc.get('message')}")

    for market, block in (doc.get("markets") or {}).items():
        regions = block.get("regions_loaded") or []
        if regions:
            _safe_print(f"\n--- {market} ---")
            for row in block.get("summaries") or []:
                _safe_print(f"  {row.get('region')}: {row.get('summary')}")

    return 0 if doc.get("wired") else 1


if __name__ == "__main__":
    raise SystemExit(main())
