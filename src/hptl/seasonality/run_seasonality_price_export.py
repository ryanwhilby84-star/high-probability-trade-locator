"""CLI: build seasonality vs current price chart snapshot.

    python -m hptl.seasonality.run_seasonality_price_export
    python -m hptl.seasonality.run_seasonality_price_export "Copper / HG"
"""

from __future__ import annotations

import sys

from hptl.seasonality.seasonality_price_export import run


def main() -> int:
    audit = sys.argv[1] if len(sys.argv) > 1 else None
    run(audit=audit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
