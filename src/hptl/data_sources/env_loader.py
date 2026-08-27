"""Load project .env into os.environ (never overwrites existing vars)."""

from __future__ import annotations

import os
from pathlib import Path


def load_project_dotenv(*, keys: tuple[str, ...] | None = None) -> dict[str, bool]:
    """Load ``PROJECT_ROOT/.env``. Returns {key: present} for requested keys."""
    try:
        from hptl.config import PROJECT_ROOT

        env_path = PROJECT_ROOT / ".env"
    except Exception:
        env_path = Path(".env")

    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip().strip('"').strip("'")
            if not k:
                continue
            if k not in os.environ or not str(os.environ.get(k, "")).strip():
                if v:
                    os.environ[k] = v

    check = keys or ("EIA_API_KEY", "NOAA_API_TOKEN", "FRED_API_KEY")
    return {k: bool((os.environ.get(k) or "").strip()) for k in check}
