"""Run agriculture valuation export + audits."""

from __future__ import annotations

from hptl.valuation.agri_valuation_export import merge_agri_into_valuation_latest, write_agri_valuation_exports
from hptl.valuation.export import build_valuation_latest, write_valuation_exports


def main() -> None:
    agri_paths = write_agri_valuation_exports()
    for label, path in agri_paths.items():
        print(f"Wrote {label}: {path}")

    val_doc = build_valuation_latest()
    merged = merge_agri_into_valuation_latest(val_doc)
    val_paths = write_valuation_exports(merged)
    for label, path in val_paths.items():
        print(f"Wrote valuation_{label}: {path}")


if __name__ == "__main__":
    main()
