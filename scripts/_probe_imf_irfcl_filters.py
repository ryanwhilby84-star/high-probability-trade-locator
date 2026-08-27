"""Probe IMF IRFCL section/unit filters for monetary gold."""
from __future__ import annotations

import io

import pandas as pd
import requests

BASE = "https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.STA/IRFCL"
UA = {"User-Agent": "HPTL/cb-gold-probe", "Accept": "text/csv"}


def try_q(label: str, extra: str) -> None:
    url = f"{BASE}/~/*?{extra}&startPeriod=2020&endPeriod=2024&format=csv"
    r = requests.get(url, headers=UA, timeout=120)
    print(label, r.status_code, len(r.content))
    if r.status_code == 200 and len(r.content) > 500 and "OBS_VALUE" in r.text:
        df = pd.read_csv(io.StringIO(r.text))
        df = df.dropna(subset=["OBS_VALUE"])
        if df.empty:
            print("  empty after dropna")
            return
        print(df.head(2).to_string())
        print("rows", len(df), "countries", df["COUNTRY"].nunique() if "COUNTRY" in df.columns else "?")


if __name__ == "__main__":
    filters = [
        ("section F11", "c[IRFCL_SECTION]=F11&c[FREQUENCY]=M"),
        ("section f11", "c[IRFCL_SECTION]=f11&c[FREQUENCY]=M"),
        ("flow stock stock", "c[FLOW_STOCK_ENTRY]=STOCK&c[FREQUENCY]=M"),
        ("unit TON", "c[UNIT]=TON&c[FREQUENCY]=M"),
        ("unit TNE", "c[UNIT]=TNE&c[FREQUENCY]=M"),
        ("country USA only", "c[COUNTRY]=USA&c[FREQUENCY]=M"),
        ("indicator wildcard", "c[INDICATOR]=IRFCL*&c[COUNTRY]=USA&c[FREQUENCY]=M"),
    ]
    for label, extra in filters:
        try_q(label, extra)
