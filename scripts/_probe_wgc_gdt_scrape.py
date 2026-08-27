"""Scrape WGC GDT pages for quarterly central bank net purchase figures."""
from __future__ import annotations

import re

import requests

UA = {"User-Agent": "Mozilla/5.0 (compatible; HPTL/cb-gold-scrape)"}

URLS = [
    "https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-q1-2025",
    "https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-q2-2025",
    "https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-q3-2025",
    "https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-q4-2025",
    "https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-full-year-2024",
    "https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-full-year-2023",
]


def main() -> None:
    for url in URLS:
        r = requests.get(url, headers=UA, timeout=60)
        text = r.text
        print("===", url.split("/")[-1], r.status_code, len(text))
        # table rows with central bank
        for m in re.finditer(r"Central banks[^<]{0,120}", text, re.I):
            s = re.sub(r"<[^>]+>", " ", m.group(0))
            s = re.sub(r"\s+", " ", s).strip()
            if len(s) > 20:
                print(" ", s[:120])
        # highcharts data
        for m in re.finditer(r"data-series=\"([^\"]+)\"", text):
            print(" series attr", m.group(1)[:80])
        for m in re.finditer(r"\"name\":\"[^\"]*[Cc]entral[^\"]*\"[^}]{0,200}", text):
            print(" json", m.group(0)[:120])


if __name__ == "__main__":
    main()
