"""Probe WGC/IMF for automatable CB gold purchase sources."""
from __future__ import annotations

import io
import re
import sys
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

UA = {"User-Agent": "Mozilla/5.0 (compatible; HPTL/cb-gold-probe)"}


def probe_wgc_page() -> None:
    r = requests.get(
        "https://www.gold.org/goldhub/data/gold-reserves-by-country",
        headers=UA,
        timeout=60,
    )
    print("WGC page", r.status_code, len(r.text))
    for pat in (
        r"https://[^\"'\s>]+\.xlsx",
        r"/download/file/\d+",
        r"/download/\d+",
        r"api\.gold\.org[^\"'\s>]+",
        r"goldhub[^\"'\s>]*api[^\"'\s>]*",
    ):
        hits = sorted(set(re.findall(pat, r.text, re.I)))
        print(f"  pattern {pat[:30]} -> {len(hits)}")
        for h in hits[:8]:
            print(f"    {h[:120]}")

    for url in (
        "https://www.gold.org/api/goldhub/data/gold-reserves-by-country",
        "https://www.gold.org/goldhub/api/data/gold-reserves",
    ):
        rr = requests.get(url, headers=UA, timeout=30, allow_redirects=True)
        print(f"  try {url} -> {rr.status_code} {len(rr.content)}")


def probe_imf_sdmx3() -> None:
    base = "https://api.imf.org/external/sdmx/3.0"
    r = requests.get(f"{base}/structure/dataflow/IMF.STA/IRFCL", headers=UA, timeout=60)
    print("IRFCL meta", r.status_code, r.text[:800] if r.text else "")

    # SDMX 3.0 data query variants
    queries = [
        f"{base}/data/dataflow/IMF.STA/IRFCL",
        f"{base}/data/dataflow/IMF.STA/IRFCL/*",
        f"{base}/data/dataflow/IMF.STA/IRFCL/~/*",
    ]
    params = {
        "startPeriod": "2018",
        "endPeriod": "2025",
        "attributes": "dsd",
        "measures": "all",
        "format": "csv",
    }
    for url in queries:
        rr = requests.get(url, params=params, headers={**UA, "Accept": "text/csv"}, timeout=90)
        print(f"  IMF {url.split('IRFCL')[-1]} -> {rr.status_code} {len(rr.content)}")
        if rr.status_code == 200 and len(rr.content) > 300:
            print(rr.text[:400])

    # dimension-filtered
    filt = (
        f"{base}/data/dataflow/IMF.STA/IRFCL/*/*.*.*.*"
        "?c[COUNTRY]=W00&c[INDICATOR]=GOLD&startPeriod=2018&endPeriod=2025&format=csv"
    )
    rr = requests.get(filt, headers={**UA, "Accept": "text/csv"}, timeout=90)
    print("  IMF filtered", rr.status_code, len(rr.content))
    if rr.status_code == 200 and "TIME_PERIOD" in rr.text:
        df = pd.read_csv(io.StringIO(rr.text))
        print(df.tail(3))


if __name__ == "__main__":
    probe_wgc_page()
    print()
    probe_imf_sdmx3()
