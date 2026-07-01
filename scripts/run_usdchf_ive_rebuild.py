"""CLI: Phase 1B USD/CHF IVE rebuild investigation."""
from __future__ import annotations

from hptl.valuation.usdchf_ive_rebuild import write_usdchf_ive_rebuild_artifacts


def main() -> int:
    paths = write_usdchf_ive_rebuild_artifacts()
    print(f"Wrote {paths['md']}")
    print(f"Wrote {paths['json']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
