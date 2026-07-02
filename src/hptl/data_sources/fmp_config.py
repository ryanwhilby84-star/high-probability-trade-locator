"""FMP provider configuration — audit-only; never replaces primary feeds."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hptl.config import DATA_DIR, get_fmp_api_key

CONFIG_PATH = DATA_DIR / "config" / "fmp_provider.json"
DEFAULT_MODE = "audit_only"


@dataclass(frozen=True)
class FmpProviderConfig:
    """Runtime FMP provider settings."""

    enabled: bool
    mode: str
    primary_providers: tuple[str, ...]
    timeout_seconds: int
    inter_request_delay_seconds: float
    max_retries: int
    audit_json_path: Path
    audit_markdown_path: Path
    api_key_configured: bool

    @property
    def is_audit_only(self) -> bool:
        return self.mode == DEFAULT_MODE


def load_fmp_provider_config(*, config_path: Path | None = None) -> FmpProviderConfig:
    """Load provider config. ``enabled`` is True only when ``FMP_API_KEY`` is set."""
    path = config_path or CONFIG_PATH
    raw: dict[str, Any] = {}
    if path.exists():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            raw = {}

    key = get_fmp_api_key()
    req = raw.get("request") or {}
    audit_out = raw.get("audit_output") or {}

    def _audit_path(key: str, default: str) -> Path:
        rel = str(audit_out.get(key) or default).replace("\\", "/")
        if rel.startswith("data/"):
            rel = rel[5:]
        return DATA_DIR / rel

    return FmpProviderConfig(
        enabled=bool(key),
        mode=str(raw.get("mode") or DEFAULT_MODE),
        primary_providers=tuple(raw.get("primary_providers") or ()),
        timeout_seconds=max(5, int(req.get("timeout_seconds") or 30)),
        inter_request_delay_seconds=max(0.0, float(req.get("inter_request_delay_seconds") or 0.35)),
        max_retries=max(1, int(req.get("max_retries") or 3)),
        audit_json_path=_audit_path("json", "audits/fmp_endpoint_audit.json"),
        audit_markdown_path=_audit_path("markdown", "audits/fmp_endpoint_audit.md"),
        api_key_configured=bool(key),
    )
