"""One-off: list CFTC names/codes for market expansion (run from repo root)."""
from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"

SEARCH = [
    "EURO", "POUND", "YEN", "FRANC", "AUSTRALIAN", "CANADIAN", "NZ DOLLAR", "DOLLAR INDEX",
    "RUSSELL", "NIKKEI", "DAX", "FTSE", "STOXX", "HANG SENG",
    "PLATINUM", "PALLADIUM", "HEATING", "GASOLINE", "RBOB", "BRENT",
    "COTTON", "SUGAR", "ORANGE", "SOYBEAN MEAL", "SOYBEAN OIL", "RICE", "OATS", "CANOLA",
    "2-YEAR", "5-YEAR", "10-YEAR", "30-YEAR", "ULTRA", "EURODOLLAR", "SOFR",
    "TREASURY", "NOTE", "BOND",
]


def scan_disagg():
    p = sorted(PROCESSED.glob("cot_cleaned_*.csv"))[-1]
    df = pd.read_csv(p, low_memory=False)
    name = "market_and_exchange_names" if "market_and_exchange_names" in df.columns else "market_name_clean"
    code = "cftc_contract_market_code"
    for kw in SEARCH:
        sub = df[df[name].astype(str).str.contains(kw, case=False, na=False)]
        if sub.empty:
            continue
        g = sub.groupby([code, name]).size().reset_index(name="n")
        print(f"\n[DISAGG] {kw}")
        for _, r in g.head(5).iterrows():
            print(f"  {r[code]!s:10} {r[name]}")


def scan_fin(year=2026):
    url = f"https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip"
    r = requests.get(url, timeout=120)
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    inner = [n for n in zf.namelist() if n.lower().endswith((".txt", ".csv"))][0]
    df = pd.read_csv(zf.open(inner), low_memory=False)
    name = "Market_and_Exchange_Names"
    code = "CFTC_Contract_Market_Code"
    for kw in SEARCH:
        sub = df[df[name].astype(str).str.contains(kw, case=False, na=False)]
        if sub.empty:
            continue
        g = sub.groupby([code, name]).size().reset_index(name="n")
        print(f"\n[FIN] {kw}")
        for _, row in g.head(5).iterrows():
            print(f"  {row[code]!s:10} {row[name]}")


if __name__ == "__main__":
    print("=== disaggregated ===")
    scan_disagg()
    print("\n=== financial ===")
    scan_fin()
