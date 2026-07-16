"""CLI: rebuild valuation_latest.json."""
from __future__ import annotations

from hptl.valuation.export import build_valuation_latest, write_valuation_exports
from hptl.valuation.metals_valuation_export import write_metals_valuation_exports
from hptl.valuation.energy_ng_valuation_export import write_natural_gas_valuation_exports


def main() -> int:
    payload = build_valuation_latest()
    paths = write_valuation_exports(payload)

    metals_paths = write_metals_valuation_exports()
    ng_paths = write_natural_gas_valuation_exports()

    s = payload["summary"]
    print(f"Wrote {paths['public']}")
    print(f"Wrote metals valuation {metals_paths['metals_valuation']}")
    print(f"Wrote NG valuation {ng_paths['data']}")
    print(f"Wired={s['wired_count']}/{s['total_instruments']} unavailable={s['unavailable_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())