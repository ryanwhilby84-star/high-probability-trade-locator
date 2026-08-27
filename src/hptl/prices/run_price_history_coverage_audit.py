"""Run full HPTL price-vs-COT history coverage audit.

Usage:
    python -m hptl.prices.run_price_history_coverage_audit
"""

from __future__ import annotations

import sys

from hptl.prices.price_history_coverage_audit import run


def main() -> int:
    try:
        run()
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
