"""Weekly COT auto-update — delegates to ``hptl.cot.pipeline`` (same as ``python -m hptl.cot.run_update``)."""
from __future__ import annotations

import sys

from hptl.cot.pipeline import run_full_pipeline


def run(*, force: bool = False) -> int:
    result = run_full_pipeline(force=force)
    return 0 if result.error is None else (result.exit_code or 1)


def main() -> None:
    force = "--force" in sys.argv
    raise SystemExit(run(force=force))


if __name__ == "__main__":
    main()
