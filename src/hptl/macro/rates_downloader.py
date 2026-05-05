from __future__ import annotations

from io import StringIO
from pathlib import Path

import pandas as pd
import requests

BASE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id="
START_DATE = "2025-01-01"

SERIES = {
    "dgs2": "DGS2",
    "dgs10": "DGS10",
    "dgs30": "DGS30",
    # DFF is the daily effective federal funds rate. It is historical/effective
    # rate data only; it is not a real-time policy expectations series.
    "fed_funds": "DFF",
    "t10y2y": "T10Y2Y",
}

RAW_PATH = Path("data/raw/macro")


def download_series(name: str, code: str) -> pd.DataFrame:
    """Download one FRED CSV series and return date plus renamed value column."""
    print(f"Fetching FRED series {code} -> {name}")
    response = requests.get(BASE_URL + code, timeout=30)
    try:
        response.raise_for_status()
    except Exception as exc:
        print(f"ERROR fetching FRED series {code}: {type(exc).__name__}: {exc}")
        raise

    df = pd.read_csv(StringIO(response.text))
    if df.shape[1] < 2:
        raise ValueError(f"Unexpected FRED response for {code}: expected at least 2 columns")

    df = df.iloc[:, :2].copy()
    df.columns = ["date", name]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df[name] = pd.to_numeric(df[name].replace(".", pd.NA), errors="coerce")
    df = df.dropna(subset=["date"])
    df = df[df["date"] >= pd.Timestamp(START_DATE)]
    return df


def download_all() -> pd.DataFrame:
    """Download required macro/rates series from 2025-01-01 to latest available."""
    print("=" * 70)
    print("Macro rates download started")
    print(f"Pulled date range: {START_DATE} -> latest available")
    print(f"FRED series requested: {', '.join(SERIES.values())}")
    print("=" * 70)

    RAW_PATH.mkdir(parents=True, exist_ok=True)
    merged: pd.DataFrame | None = None

    for name, code in SERIES.items():
        series_df = download_series(name, code)
        merged = series_df if merged is None else merged.merge(series_df, on="date", how="outer")

    if merged is None or merged.empty:
        raise ValueError("No macro/rates data was downloaded from FRED")

    merged = merged.sort_values("date").reset_index(drop=True)
    raw_file = RAW_PATH / "rates_raw.csv"
    merged.to_csv(raw_file, index=False)
    latest_available = merged["date"].max()

    print(f"Loaded macro rows: {len(merged)}")
    print(f"Latest available date in pulled data: {latest_available.date() if pd.notna(latest_available) else 'UNKNOWN'}")
    print(f"Raw macro file saved: {raw_file}")
    return merged
