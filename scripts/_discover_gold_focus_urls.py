"""Discover central-bank gold focus URLs from gold.org listing pages."""
from __future__ import annotations

import re

import requests

UA = {"User-Agent": "Mozilla/5.0"}


def main() -> None:
    seeds = [
        "https://www.gold.org/goldhub/gold-focus",
        "https://www.gold.org/goldhub/research/central-banks",
    ]
    found: set[str] = set()
    for seed in seeds:
        r = requests.get(seed, headers=UA, timeout=60)
        print(seed, r.status_code, len(r.text))
        for m in re.findall(r'href="(/goldhub/gold-focus/[^"]+)"', r.text):
            if "central-bank" in m.lower() and "gold" in m.lower():
                found.add("https://www.gold.org" + m)
    print("found", len(found))
    for u in sorted(found)[:40]:
        print(u)


if __name__ == "__main__":
    main()
