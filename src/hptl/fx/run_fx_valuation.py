"""CLI: build the standalone FX valuation snapshot.

    python -m hptl.fx.run_fx_valuation
"""

from __future__ import annotations

from hptl.fx.fx_valuation_export import run


def main() -> int:
    run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
