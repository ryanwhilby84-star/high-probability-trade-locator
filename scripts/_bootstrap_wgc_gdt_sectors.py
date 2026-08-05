"""Bootstrap official WGC GDT supply/demand history from XLSX workbooks.

HTML table scraping is intentionally disabled.

Usage:
  python scripts/_bootstrap_wgc_gdt_sectors.py
  python scripts/_bootstrap_wgc_gdt_sectors.py --xlsx /path/to/GDT_Tables.xlsx
  python scripts/_bootstrap_wgc_gdt_sectors.py --xlsx-dir data/raw/wgc_gdt/
  python scripts/_bootstrap_wgc_gdt_sectors.py --no-download
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.data_sources.wgc_gdt_xlsx_ingest import run_bootstrap  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Ingest official WGC GDT XLSX supply/demand history (no HTML scraping)."
    )
    parser.add_argument(
        "--xlsx",
        type=Path,
        default=None,
        help="Path to a manually downloaded GDT Tables XLSX",
    )
    parser.add_argument(
        "--xlsx-dir",
        type=Path,
        default=None,
        help="Directory of GDT XLSX workbooks (also scans data/raw/wgc_gdt/)",
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Do not attempt Goldhub download (manual files only)",
    )
    args = parser.parse_args(argv)

    # Valid --xlsx is always local-only: never attempt Goldhub HTTP.
    try_download = False if args.xlsx is not None else (not args.no_download)

    result = run_bootstrap(
        xlsx=args.xlsx,
        xlsx_dir=args.xlsx_dir,
        try_download=try_download,
    )

    print("WGC GDT XLSX bootstrap")
    print(f"  ok={result.ok}")
    if result.auth_required:
        print("  auth_required=True (Goldhub login / WGC_GOLDHUB_COOKIE / --xlsx)")
    print(f"  earliest={result.earliest}")
    print(f"  latest={result.latest}")
    print(f"  n_quarters={result.n_quarters}")
    print(f"  n_revisions={result.n_revisions}")
    print(f"  n_recon_failures={result.n_recon_failures}")
    print("  populated_by_sector:")
    for k, n in sorted((result.populated or {}).items()):
        print(f"    {k}: {n}")
    if result.sources_used:
        print(f"  sources={result.sources_used}")
    if result.error:
        print(f"  ERROR: {result.error}")
    for label, path in (result.paths or {}).items():
        print(f"  wrote {label}: {path}")

    # Loud failure if history / auth gates not met
    if not result.ok:
        return 1
    if result.n_quarters < 40:
        print("  ERROR: minimum-history gate failed (<40 quarters)")
        return 1
    if result.n_quarters < 60:
        print("  ERROR: history target gate failed (<60 quarters)")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
