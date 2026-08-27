"""Probe per-country IMF IRFCL monetary gold series."""
from __future__ import annotations

import io

import pandas as pd
import requests

BASE = "https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.STA/IRFCL"
UA = {"User-Agent": "HPTL/cb-gold-probe", "Accept": "text/csv"}


def try_query(label: str, params: dict) -> None:
    q = "&".join(f"c[{k}]={v}" for k, v in params.items())
    url = f"{BASE}/~/*?{q}&startPeriod=2016&endPeriod=2025&format=csv"
    r = requests.get(url, headers=UA, timeout=120)
    print(label, r.status_code, len(r.content))
    if r.status_code == 200 and len(r.content) > 400 and "TIME_PERIOD" in r.text:
        df = pd.read_csv(io.StringIO(r.text))
        df = df.dropna(subset=["OBS_VALUE"])
        print(df[["COUNTRY", "INDICATOR", "TIME_PERIOD", "OBS_VALUE", "UNIT"]].tail(5).to_string())
        print("rows", len(df))


if __name__ == "__main__":
    for country in ("USA", "CHN", "IND", "DEU", "W00"):
        for ind in ("GOLDV", "GOLD", "GOLDH", "F11", "F11A"):
            try_query(f"{country}/{ind}", {"COUNTRY": country, "INDICATOR": ind, "FREQUENCY": "M"})
