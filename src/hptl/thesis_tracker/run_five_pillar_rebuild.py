"""Full rebuild: valuation → seasonality → confluence → thesis → scanner → distribution report."""
from __future__ import annotations

import argparse


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Rebuild all five opportunity pillars and exports.")
    parser.add_argument("--skip-baseline", action="store_true", help="Do not capture 3-pillar baseline first.")
    parser.add_argument("--skip-confluence", action="store_true", help="Skip confluence rebuild (faster dev).")
    parser.add_argument("--weeks", type=int, default=13, help="Thesis seed history weeks.")
    args = parser.parse_args(argv)

    if not args.skip_baseline:
        from hptl.thesis_tracker.opportunity_baseline import write_baseline

        p = write_baseline()
        print(f"Captured 3-pillar baseline: {p}")

    from hptl.valuation.export import build_valuation_latest, write_valuation_exports

    val = build_valuation_latest()
    write_valuation_exports(val)
    print(f"Valuation wired {val['summary']['wired_count']}/{val['summary']['total_instruments']}")

    from hptl.seasonality.export import build_seasonality_latest, write_seasonality_exports

    sea = build_seasonality_latest()
    write_seasonality_exports(sea)
    print(f"Seasonality wired {sea['summary']['wired_count']}/{sea['summary']['total_instruments']}")

    if not args.skip_confluence:
        from hptl.confluence.build_decision_table import run as run_confluence

        run_confluence()
        print("Confluence history rebuilt.")

    from hptl.thesis_tracker.run_thesis_seed import main as run_seed

    rc = run_seed(["--reset", f"--weeks={args.weeks}"])
    if rc != 0:
        return rc
    print("Thesis tracker re-seeded.")

    from hptl.thesis_tracker.opportunity_distribution_report import (
        build_distribution_report,
        write_reports,
        write_scanner_latest,
    )

    write_scanner_latest()
    paths = write_reports(build_distribution_report())
    print(f"Distribution report: {paths['markdown']}")
    print(f"JSON: {paths['json']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
