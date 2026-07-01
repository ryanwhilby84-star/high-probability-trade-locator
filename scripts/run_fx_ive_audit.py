"""CLI: Phase 1A FX IVE institutional audit."""
from __future__ import annotations

from hptl.valuation.fx_ive_audit import write_fx_ive_audit_artifacts


def main() -> int:
    paths = write_fx_ive_audit_artifacts(refresh_caches=False)
    print(f"Wrote {paths['audit_md']}")
    print(f"Wrote {paths['audit_json']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
