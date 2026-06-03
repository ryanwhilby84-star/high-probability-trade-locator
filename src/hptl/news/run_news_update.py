"""CLI: refresh GDELT-backed news/catalyst JSON for the dashboard."""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from hptl.config import PROJECT_ROOT
from hptl.intelligence.catalyst_loader import load_catalyst_config
from hptl.news.catalyst_news_builder import write_news_catalysts_json


def ensure_env_from_example() -> None:
    """If ``.env`` is missing, copy from ``.env.example``. Real keys belong only in ``.env``."""
    env_path = PROJECT_ROOT / ".env"
    example = PROJECT_ROOT / ".env.example"
    if env_path.exists():
        return
    if not example.exists():
        print("WARNING: .env missing and .env.example not found — create .env manually if needed.", file=sys.stderr)
        return
    shutil.copyfile(example, env_path)
    print(f"Created {env_path} from .env.example (fill API keys in .env only; keep .env.example as template).")


def main() -> int:
    ensure_env_from_example()
    ap = argparse.ArgumentParser(description="Build news_catalysts.json from GDELT + catalyst_config.")
    ap.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output JSON path (default: web-dashboard/public/data/news_catalysts.json)",
    )
    args = ap.parse_args()
    cfg = load_catalyst_config()
    out = write_news_catalysts_json(path=args.out, catalyst_cfg=cfg)
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
