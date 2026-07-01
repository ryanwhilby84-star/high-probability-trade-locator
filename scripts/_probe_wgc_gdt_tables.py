"""Extract supply/demand tables from WGC GDT HTML pages."""
from __future__ import annotations

import re

import pandas as pd
import requests
from io import StringIO

UA = {"User-Agent": "Mozilla/5.0 (compatible; HPTL/gdt-table)"}

PAGES = [
    "https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-q1-2025",
    "https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-q2-2025",
    "https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-q3-2025",
    "https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-full-year-2024",
    "https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-full-year-2023",
    "https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-full-year-2022",
]


def main() -> None:
    for url in PAGES:
        r = requests.get(url, headers=UA, timeout=60)
        if r.status_code != 200:
            print(url, "status", r.status_code)
            continue
        tables = pd.read_html(StringIO(r.text))
        print("===", url.split("/")[-1], "tables", len(tables))
        for i, df in enumerate(tables):
            flat = " ".join(str(x) for x in df.values.flatten() if str(x) != "nan").lower()
            if "central bank" in flat or "official" in flat:
                print(f" table {i} shape {df.shape}")
                print(df.to_string()[:1200])


if __name__ == "__main__":
    main()
