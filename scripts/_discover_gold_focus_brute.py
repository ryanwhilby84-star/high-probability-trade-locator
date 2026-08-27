"""Brute discover gold focus CB stats URLs (HEAD)."""
from __future__ import annotations

import concurrent.futures
import re

import requests

UA = {"User-Agent": "Mozilla/5.0"}
SLUGS = [
    "central-bank-gold-statistics",
    "central-bank-gold-statistics-january",
    "central-bank-gold-statistics-february",
    "central-bank-gold-statistics-march",
    "central-bank-gold-statistics-april",
    "central-bank-gold-statistics-may",
    "central-bank-gold-statistics-june",
    "central-bank-gold-statistics-july",
    "central-bank-gold-statistics-august",
    "central-bank-gold-statistics-september",
    "central-bank-gold-statistics-october",
    "central-bank-gold-statistics-november",
    "central-bank-gold-statistics-december",
]


def check(url: str) -> str | None:
    try:
        r = requests.head(url, headers=UA, timeout=15, allow_redirects=True)
        if r.status_code == 200:
            return url
    except requests.RequestException:
        pass
    return None


def main() -> None:
    urls: list[str] = []
    for year in range(2022, 2027):
        for month in range(1, 13):
            mm = f"{month:02d}"
            for slug in SLUGS:
                urls.append(f"https://www.gold.org/goldhub/gold-focus/{year}/{mm}/{slug}")
                urls.append(f"https://www.gold.org/goldhub/gold-focus/{year}/{mm}/{slug}-{year}")

    hits: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
        for result in ex.map(check, urls):
            if result:
                hits.append(result)
    print("hits", len(hits))
    for h in sorted(set(hits)):
        print(h)


if __name__ == "__main__":
    main()
