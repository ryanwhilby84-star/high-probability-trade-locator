"""Probe WGC file downloads and IMF filtered gold reserves."""
from __future__ import annotations

import io
import sys
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

UA = {"User-Agent": "Mozilla/5.0 (compatible; HPTL/cb-gold-probe)"}
BASE = "https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.STA/IRFCL"


def probe_wgc_downloads() -> None:
    ids = [7739, 7741, 7745, 8052, 12491, 16208]
    for fid in ids:
        for path in (f"https://www.gold.org/download/file/{fid}", f"https://www.gold.org/download/{fid}"):
            r = requests.get(path, headers=UA, timeout=60, allow_redirects=True)
            ct = r.headers.get("content-type", "")
            is_xlsx = r.content[:4] == b"PK\x03\x04"
            print(f"file/{fid} {path.split('gold.org')[1]} -> {r.status_code} {len(r.content)} xlsx={is_xlsx} ct={ct[:50]}")
            if is_xlsx:
                xl = pd.ExcelFile(io.BytesIO(r.content))
                print("  sheets", xl.sheet_names[:5])
                for sh in xl.sheet_names[:2]:
                    df = pd.read_excel(xl, sheet_name=sh, header=None)
                    print(f"  {sh} shape", df.shape)
                    print(df.iloc[:6, :6].to_string())


def probe_imf_gold() -> None:
    indicators = [
        "GOLD",
        "GOLDV",
        "GOLDH",
        "F11",
        "F11A",
        "RAFA_GOLD_TON",
        "RAXGOL_TON",
        "IRFCLDT1_IRFCL111_GOLD",
    ]
    for ind in indicators:
        url = (
            f"{BASE}/~/*"
            f"?c[COUNTRY]=W00"
            f"&c[INDICATOR]={ind}"
            f"&c[FREQUENCY]=M"
            f"&startPeriod=2018"
            f"&endPeriod=2025"
            f"&format=csv"
        )
        r = requests.get(url, headers={**UA, "Accept": "text/csv"}, timeout=120)
        print(f"IMF W00 {ind} M -> {r.status_code} {len(r.content)}")
        if r.status_code == 200 and len(r.content) > 200 and "TIME_PERIOD" in r.text:
            df = pd.read_csv(io.StringIO(r.text))
            print("  rows", len(df), "periods", df["TIME_PERIOD"].tail(3).tolist())
            print("  values", df["OBS_VALUE"].tail(3).tolist())


if __name__ == "__main__":
    print("=== WGC downloads ===")
    probe_wgc_downloads()
    print("\n=== IMF gold filters ===")
    probe_imf_gold()
