"""Probe institutional sources for central-bank gold net purchases."""
from __future__ import annotations

import io
import json
import re
import sys
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

UA = {"User-Agent": "Mozilla/5.0 (compatible; HPTL/cb-gold-probe)"}


def probe_wgc_page() -> None:
    url = "https://www.gold.org/goldhub/data/gold-reserves-by-country"
    r = requests.get(url, timeout=60, headers=UA)
    print("WGC page", r.status_code, len(r.text))
    patterns = [
        r"https://www\.gold\.org/download/file/\d+/[^\"'\s<>]+",
        r"/sites/default/files/[^\"'\s<>]+\.xlsx",
        r"gold-reserves[^\"'\s<>]*\.xlsx",
    ]
    for pat in patterns:
        found = sorted(set(re.findall(pat, r.text, flags=re.I)))
        print(f" pattern {pat[:40]}... -> {len(found)}")
        for x in found[:8]:
            print("  ", x)


def probe_wgc_known_ids() -> None:
    # Common WGC Drupal file IDs (discovered from historical exports / sitemaps)
    candidates = [
        16681,
        16682,
        16683,
        16700,
        16701,
        15000,
        14000,
        12000,
        10000,
        8000,
        5000,
    ]
    names = [
        "Changes-in-World-Official-Gold-Reserves.xlsx",
        "Quarterly-times-series-on-World-Official-Gold-Reserves-since-2000.xlsx",
        "Latest-World-Official-Gold-Reserves.xlsx",
    ]
    for fid in candidates:
        for name in names:
            url = f"https://www.gold.org/download/file/{fid}/{name}"
            try:
                r = requests.head(url, timeout=15, headers=UA, allow_redirects=True)
                if r.status_code == 200:
                    print("HIT", url, r.headers.get("content-type"), r.headers.get("content-length"))
            except Exception:
                pass


def probe_imf_sdmx3() -> None:
    # IMF PCPS world gold price; try IFS gold reserves via SDMX 3
    urls = [
        "https://api.imf.org/external/sdmx/3.0/structure/dataflow",
        "https://dataservices.imf.org/REST/SDMX_JSON.svc/Dataflow",
    ]
    for url in urls:
        try:
            r = requests.get(url, timeout=30, headers=UA)
            print("IMF", url, r.status_code, len(r.content))
        except Exception as e:
            print("IMF fail", url, e)

    # PCPS commodity prices - gold
    pcps = (
        "https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.RES/PCPS/~/"
        "W00.PGOLD.?c[TIME_PERIOD]=ge:2016&attributes=dsd&measures=all"
    )
    try:
        r = requests.get(pcps, headers={**UA, "Accept": "text/csv"}, timeout=60)
        print("PCPS PGOLD", r.status_code, r.text[:300] if r.text else "")
    except Exception as e:
        print("PCPS fail", e)


def probe_fred() -> None:
    from hptl.macro import fred_client

    for sid in ("GOLDAMGBD228NLBM", "IR14270", "IQ12260"):
        try:
            df = fred_client.get_series_df(sid, observation_start="2016-01-01")
            print("FRED", sid, "rows", len(df), "latest", df.index.max() if len(df) else None)
        except Exception as e:
            print("FRED", sid, "err", e)


if __name__ == "__main__":
    probe_wgc_page()
    print()
    probe_wgc_known_ids()
    print()
    probe_imf_sdmx3()
    print()
    probe_fred()
