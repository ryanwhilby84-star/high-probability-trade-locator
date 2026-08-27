"""Run metals valuation V1 export + audit artifacts."""

from __future__ import annotations

from hptl.valuation.export import write_valuation_exports
from hptl.valuation.metals_valuation_export import write_metals_valuation_exports


def main() -> None:
    write_metals_valuation_exports()
    paths = write_valuation_exports()
    print("Metals valuation V1 export complete:")
    for k, p in paths.items():
        print(f"  {k}: {p}")


if __name__ == "__main__":
    main()
