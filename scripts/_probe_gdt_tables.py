"""Extract central bank quarterly tonnes from WGC GDT HTML tables."""
from __future__ import annotations

import re
from io import StringIO

import pandas as pd
import requests

UA = {"User-Agent": "Mozilla/5.0"}


def extract_from_url(url: str) -> None:
    r = requests.get(url, headers=UA, timeout=60)
    print("===", url.split("/")[-1], r.status_code)
    try:
        tables = pd.read_html(StringIO(r.text), flavor="html5lib")
    except Exception as exc:
        print(" read_html failed", exc)
        return
    for i, df in enumerate(tables):
        flat = df.astype(str).apply(lambda col: col.str.lower()).values.flatten()
        if any("central bank" in str(x) for x in flat):
            print(f"table {i} shape {df.shape}")
            print(df.to_string()[:2000])


def main() -> None:
    urls = [
        "https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-q1-2025",
        "https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-full-year-2024",
        "https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-full-year-2023",
    ]
    for u in urls:
        extract_from_url(u)


if __name__ == "__main__":
    main()
