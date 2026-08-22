"""Run GDT bootstrap against the local Downloads workbook (no HTTP)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.data_sources.wgc_gdt_xlsx_ingest import run_bootstrap  # noqa: E402

XLSX = Path.home() / "Documents" / "Downloads" / "GDT_Tables_Q2'26_EN.xlsx"


def main() -> int:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else XLSX
    print(f"local_xlsx={path}")
    print(f"exists={path.is_file()} try_download=False")
    result = run_bootstrap(xlsx=path, try_download=False)
    print("ok", result.ok)
    print("earliest", result.earliest)
    print("latest", result.latest)
    print("n_quarters", result.n_quarters)
    print("n_revisions", result.n_revisions)
    print("n_recon_failures", result.n_recon_failures)
    print("auth_required", result.auth_required)
    print("sources", result.sources_used)
    print("populated", result.populated)
    print("error", result.error)
    print("paths", result.paths)
    return 0 if result.ok and result.n_quarters >= 60 else 1


if __name__ == "__main__":
    raise SystemExit(main())
