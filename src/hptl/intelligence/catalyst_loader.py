"""Load and validate ``catalyst_config.json`` (keywords, related markets, sensitivities)."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hptl.config import DATA_DIR

SOURCE_NOT_CONFIGURED = "not available — source not configured"


def default_catalyst_config_path() -> Path:
    return DATA_DIR / "config" / "catalyst_config.json"


def load_catalyst_config(path: Path | str | None = None) -> dict[str, Any]:
    """Load catalyst JSON; raises ``FileNotFoundError`` if missing."""
    p = Path(path) if path is not None else default_catalyst_config_path()
    if not p.exists():
        raise FileNotFoundError(f"Catalyst config not found: {p}")
    with p.open(encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("catalyst_config.json must be a JSON object")
    inst = data.get("instruments")
    if not isinstance(inst, dict):
        raise ValueError("catalyst_config.json requires object 'instruments'")
    return data


def instrument_profile(cfg: dict[str, Any], instrument: str) -> dict[str, Any] | None:
    inst = cfg.get("instruments") if isinstance(cfg.get("instruments"), dict) else {}
    prof = inst.get(instrument)
    return prof if isinstance(prof, dict) else None
