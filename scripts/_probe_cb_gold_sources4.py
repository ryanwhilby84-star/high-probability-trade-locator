"""Probe IMF IFS / IRFCL indicator codes for monetary gold."""
from __future__ import annotations

import io
import json
import re
import sys
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
UA = {"User-Agent": "HPTL/cb-gold-probe"}
BASE = "https://api.imf.org/external/sdmx/3.0"


def find_gold_indicators() -> list[str]:
    r = requests.get(
        f"{BASE}/structure/dataflow/IMF.STA/IRFCL?references=all",
        headers=UA,
        timeout=120,
    )
    text = r.text
    # indicator ids in structure
    ids = set(re.findall(r'"id":"([^"]*GOLD[^"]*)"', text, re.I))
    ids |= set(re.findall(r'"id":"([^"]*F11[^"]*)"', text, re.I))
    ids |= set(re.findall(r'"id":"(IRFCL[^"]*GOLD[^"]*)"', text, re.I))
    return sorted(ids)


def try_indicator(ind: str) -> None:
    url = (
        f"{BASE}/data/dataflow/IMF.STA/IRFCL/~/*"
        f"?c[INDICATOR]={ind}"
        f"&c[FREQUENCY]=M"
        f"&startPeriod=2018"
        f"&endPeriod=2025"
        f"&format=csv"
    )
    r = requests.get(url, headers={**UA, "Accept": "text/csv"}, timeout=120)
    if r.status_code != 200 or len(r.content) < 400:
        return
    if "TIME_PERIOD" not in r.text:
        return
    df = pd.read_csv(io.StringIO(r.text))
    df = df.dropna(subset=["OBS_VALUE"], how="all")
    if df.empty:
        return
    countries = df["COUNTRY"].nunique() if "COUNTRY" in df.columns else 0
    print(f"  HIT {ind}: rows={len(df)} countries={countries}")
    print(df[["COUNTRY", "TIME_PERIOD", "OBS_VALUE"]].tail(3).to_string())


def try_ifs_indicators() -> None:
    for ind in ("RAXGOL", "RAFA_GOLD", "GOLD", "ENDGOLD"):
        url = (
            f"{BASE}/data/dataflow/IMF.STA/IFS/~/*"
            f"?c[INDICATOR]={ind}"
            f"&c[FREQUENCY]=M"
            f"&startPeriod=2018"
            f"&endPeriod=2025"
            f"&format=csv"
        )
        r = requests.get(url, headers={**UA, "Accept": "text/csv"}, timeout=120)
        print(f"IFS {ind}: {r.status_code} {len(r.content)}")
        if r.status_code == 200 and len(r.content) > 400 and "TIME_PERIOD" in r.text:
            df = pd.read_csv(io.StringIO(r.text))
            print(df.head(2).to_string())
            print("rows", len(df))


def probe_wgc_static() -> None:
    r = requests.get(
        "https://www.gold.org/goldhub/data/gold-reserves-by-country",
        headers=UA,
        timeout=60,
    )
    files = sorted(set(re.findall(r"https://www\.gold\.org/sites/default/files/[^\"'\s>]+", r.text)))
    print("static files", len(files))
    for f in files:
        rr = requests.head(f, headers=UA, timeout=30, allow_redirects=True)
        print(f"  {rr.status_code} {f.split('/')[-1][:60]}")


if __name__ == "__main__":
    print("=== WGC static ===")
    probe_wgc_static()
    print("\n=== IRFCL gold indicator ids ===")
    inds = find_gold_indicators()
    print("found", len(inds), inds[:25])
    for ind in inds[:15]:
        try_indicator(ind)
    print("\n=== IFS ===")
    try_ifs_indicators()
