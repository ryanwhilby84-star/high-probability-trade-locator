"""Probe Goldhub pages for official GDT XLSX download links."""

from __future__ import annotations

import re

import requests

UA = {"User-Agent": "Mozilla/5.0 (compatible; HPTL/gdt-xlsx-probe)"}
URLS = [
    "https://www.gold.org/goldhub/data/gold-demand-by-country",
    "https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-q2-2026",
    "https://www.gold.org/goldhub/research/gold-demand-trends",
]


def main() -> None:
    for url in URLS:
        r = requests.get(url, headers=UA, timeout=60)
        print("===", url, r.status_code, r.headers.get("content-type"), len(r.content))
        hrefs = re.findall(r'href=["\']([^"\']+)["\']', r.text, flags=re.I)
        interesting = [
            h
            for h in hrefs
            if any(x in h.lower() for x in ("xlsx", "download", "gdt", "demand", "file/"))
        ]
        for h in sorted(set(interesting)):
            print(" ", h)


if __name__ == "__main__":
    main()
