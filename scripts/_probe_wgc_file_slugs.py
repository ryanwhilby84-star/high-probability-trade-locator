"""Try WGC download/file URLs with slugs from page HTML."""
from __future__ import annotations

import io
import re

import pandas as pd
import requests

UA = {"User-Agent": "Mozilla/5.0 (compatible; HPTL/cb-gold-probe)"}


def main() -> None:
    page = requests.get(
        "https://www.gold.org/goldhub/data/gold-reserves-by-country",
        headers=UA,
        timeout=60,
    )
    # href="/download/file/7745/changes-in-world-official-gold-reserves.xlsx"
    links = re.findall(
        r'href="(/download/file/\d+/[^"]+)"',
        page.text,
        flags=re.I,
    )
    print("links", len(links))
    for rel in links:
        url = "https://www.gold.org" + rel.replace("&amp;", "&")
        r = requests.get(url, headers=UA, timeout=60, allow_redirects=True)
        is_xlsx = r.content[:4] == b"PK\x03\x04"
        print(f"{rel[:70]} -> {r.status_code} {len(r.content)} xlsx={is_xlsx}")
        if is_xlsx:
            xl = pd.ExcelFile(io.BytesIO(r.content))
            print(" sheets", xl.sheet_names)
            df = pd.read_excel(xl, sheet_name=xl.sheet_names[0], header=None)
            print(df.iloc[:8, :5].to_string())


if __name__ == "__main__":
    main()
