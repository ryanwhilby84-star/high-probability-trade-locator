"""HPTL COT update — single command for weekly refresh (download → master → confluence JSON)."""
from __future__ import annotations

import sys

from hptl.cot.pipeline import main as pipeline_main
from hptl.cot.workbook_export import run_workbook_export

__all__ = ["run", "run_workbook_export", "main"]


def run(argv: list[str] | None = None) -> int:
    return pipeline_main(argv if argv is not None else sys.argv[1:])


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
