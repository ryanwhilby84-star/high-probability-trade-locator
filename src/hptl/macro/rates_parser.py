from __future__ import annotations

from pathlib import Path

import pandas as pd

RAW = Path("data/raw/macro/rates_raw.csv")
OUT = Path("data/processed/macro/rates_clean.csv")
CORE_REQUIRED = ["dgs2", "dgs10", "dgs30"]


def process_rates(raw_path: Path = RAW, output_path: Path = OUT) -> pd.DataFrame:
    """Clean, align, and calculate rate changes.

    FRED Treasury yield units are percentage points, so:
    - 10 bps = 0.10
    - 5 bps = 0.05
    """
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw macro file not found: {raw_path}")

    df = pd.read_csv(raw_path, parse_dates=["date"])
    df = df.sort_values("date").reset_index(drop=True)

    for col in ["dgs2", "dgs10", "dgs30", "fed_funds", "t10y2y"]:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors="coerce")

    calculated_curve = df["dgs10"] - df["dgs2"]
    df["yield_curve_10y2y"] = df["t10y2y"].where(df["t10y2y"].notna(), calculated_curve)

    for col in ["dgs2", "dgs10", "dgs30", "fed_funds", "yield_curve_10y2y"]:
        df[f"{col}_1w_change"] = df[col] - df[col].shift(5)
        df[f"{col}_4w_change"] = df[col] - df[col].shift(20)

    df["core_rates_complete"] = df[CORE_REQUIRED].notna().all(axis=1)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Clean macro rows processed: {len(df)}")
    print(f"Clean macro file saved: {output_path}")
    return df
