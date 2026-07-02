"""CLI: build the 3Y COT-vs-price series snapshot.

    python -m hptl.cot.run_cot_3y_series
"""

from __future__ import annotations

from hptl.cot.cot_3y_series_export import run


def main() -> int:
    import sys

    audit = sys.argv[1] if len(sys.argv) > 1 else None
    run(audit=audit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
