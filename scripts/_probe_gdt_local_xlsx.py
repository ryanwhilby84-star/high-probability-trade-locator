"""Probe local GDT workbook structure (apostrophe-safe path)."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "audits" / "_gdt_xlsx_probe.txt"
DEFAULT = Path.home() / "Documents" / "Downloads" / "GDT_Tables_Q2'26_EN.xlsx"


def main() -> None:
    p = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT
    lines: list[str] = []
    lines.append(f"path={p}")
    lines.append(f"exists={p.is_file()} size={p.stat().st_size if p.is_file() else None}")
    xl = pd.ExcelFile(p)
    lines.append(f"sheets={xl.sheet_names}")
    for s in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=s, header=None)
        lines.append(f"\n=== {s} shape={df.shape} ===")
        # Show first 25 rows, truncate cell text
        for i in range(min(25, len(df))):
            cells = []
            for j in range(min(20, df.shape[1])):
                v = df.iat[i, j]
                if pd.isna(v):
                    cells.append("")
                else:
                    t = str(v).replace("\n", " ")[:80]
                    cells.append(t)
            # strip trailing empties
            while cells and cells[-1] == "":
                cells.pop()
            if any(cells):
                lines.append(f"R{i}: " + " | ".join(cells))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
