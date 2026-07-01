"""Probe IMF SDMX 3.0 for official gold reserves series."""
from __future__ import annotations

import io
import sys
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

UA = {"User-Agent": "Mozilla/5.0 (compatible; HPTL/imf-gold-probe)"}


def try_url(url: str, label: str) -> None:
    try:
        r = requests.get(url, headers={**UA, "Accept": "text/csv"}, timeout=90)
        print(label, "status", r.status_code, "bytes", len(r.content))
        if r.status_code == 200 and r.text and "OBS_VALUE" in r.text[:500]:
            df = pd.read_csv(io.StringIO(r.text), low_memory=False)
            print("  cols", list(df.columns)[:12])
            print("  rows", len(df))
            if "TIME_PERIOD" in df.columns:
                print("  periods", df["TIME_PERIOD"].dropna().astype(str).tail(5).tolist())
            if "OBS_VALUE" in df.columns:
                print("  last values", df["OBS_VALUE"].dropna().tail(3).tolist())
    except Exception as e:
        print(label, "ERR", e)


def main() -> None:
    # Browse dataflows containing GOLD or RESERVE
    r = requests.get(
        "https://api.imf.org/external/sdmx/3.0/structure/dataflow",
        headers=UA,
        timeout=60,
    )
    text = r.text.lower()
    for token in ("gold", "reserve", "cofer", "irfcl", "ifs"):
        print("dataflow contains", token, token in text)

    candidates = [
        (
            "IRFCL world gold tonnes Q",
            "https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.STA/IRFCL/~/"
            "W00.RES_GOLD_TON_Q.?c[TIME_PERIOD]=ge:2016&attributes=dsd&measures=all",
        ),
        (
            "IRFCL world gold Q alt",
            "https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.STA/IRFCL/~/"
            "1C_W00.RAXGOL_TON_Q.?c[TIME_PERIOD]=ge:2016&attributes=dsd&measures=all",
        ),
        (
            "IFS gold",
            "https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.STA/IFS/~/"
            "W00..?c[TIME_PERIOD]=ge:2016&attributes=dsd&measures=all",
        ),
    ]
    for label, url in candidates:
        try_url(url, label)


if __name__ == "__main__":
    main()
