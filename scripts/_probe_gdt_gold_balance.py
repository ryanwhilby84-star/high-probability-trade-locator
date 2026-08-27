"""Dump Gold Balance header + row labels for parser design."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "audits" / "_gdt_gold_balance_probe.txt"
XLSX = Path.home() / "Documents" / "Downloads" / "GDT_Tables_Q2'26_EN.xlsx"


def main() -> None:
    df = pd.read_excel(XLSX, sheet_name="Gold Balance", header=None)
    lines = [f"shape={df.shape}"]
    # Header-ish rows 0-6 across ALL columns
    for r in range(min(6, len(df))):
        vals = []
        for c in range(df.shape[1]):
            v = df.iat[r, c]
            if pd.isna(v):
                continue
            vals.append(f"C{c}={v!r}"[:120])
        lines.append(f"R{r}: " + " ; ".join(vals))
    lines.append("\nROW LABELS (col 1):")
    for r in range(len(df)):
        v = df.iat[r, 1] if df.shape[1] > 1 else None
        if pd.notna(v):
            lines.append(f"R{r}: {str(v)[:100]!r}")
    # Count quarterly-looking headers in row 4
    q_cols = []
    for c in range(df.shape[1]):
        v = df.iat[4, c]
        if pd.isna(v):
            # try row 3
            v = df.iat[3, c] if df.shape[0] > 3 else None
        s = str(v) if pd.notna(v) else ""
        if "Q" in s.upper() or (isinstance(v, (int, float)) and 2000 <= float(v) <= 2030):
            q_cols.append((c, s))
    lines.append(f"\nPERIOD-LIKE cols on R4/nearby: n={len(q_cols)}")
    for c, s in q_cols:
        lines.append(f"  C{c}: {s}")

    # Also Gold Prices sheet
    lines.append("\n=== Gold Prices ===")
    gp = pd.read_excel(XLSX, sheet_name="Gold Prices", header=None)
    lines.append(f"shape={gp.shape}")
    for r in range(min(10, len(gp))):
        vals = []
        for c in range(min(30, gp.shape[1])):
            v = gp.iat[r, c]
            if pd.notna(v):
                vals.append(f"C{c}={str(v)[:40]}")
        if vals:
            lines.append(f"R{r}: " + " ; ".join(vals))

    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
