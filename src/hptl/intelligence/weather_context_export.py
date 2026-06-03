"""Write ``weather_context_latest.json`` for the dashboard instrument page."""
from __future__ import annotations

import json
from pathlib import Path

from hptl.config import PROJECT_ROOT
from hptl.intelligence.weather_context import build_weather_context_bundle


def default_export_path() -> Path:
    return PROJECT_ROOT / "web-dashboard" / "public" / "data" / "weather_context_latest.json"


def write_weather_context_export(path: Path | None = None, *, respect_skip_live: bool = True) -> Path:
    doc = build_weather_context_bundle(respect_skip_live=respect_skip_live)
    p = path or default_export_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    return p
