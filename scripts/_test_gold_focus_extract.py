"""Test gold focus extraction on known URLs."""
from __future__ import annotations

import re
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bootstrap_cb_gold_from_gold_focus import _extract_from_html  # noqa: E402

URLS = [
    "https://www.gold.org/goldhub/gold-focus/2026/06/central-bank-gold-statistics-central-banks-resume-net-buying-april",
    "https://www.gold.org/goldhub/gold-focus/2026/04/central-bank-gold-statistics-central-banks-stay-course-gold-february",
    "https://www.gold.org/goldhub/gold-focus/2026/03/central-bank-gold-statistics-momentum-eases-january-while-demand-base",
    "https://www.gold.org/goldhub/gold-focus/2026/05/central-bank-gold-statistics-march-2026",
]

UA = {"User-Agent": "Mozilla/5.0"}


def main() -> None:
    for url in URLS:
        r = requests.get(url, headers=UA, timeout=60)
        print(url.split("/")[-1], r.status_code)
        hit = _extract_from_html(r.text, url)
        print(" ", hit)
        if not hit:
            text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", r.text))
            for pat in [r"net \d+t", r"bought \d+t", r"\d+t in"]:
                m = re.search(pat, text, re.I)
                if m:
                    print("  snippet", m.group(0), text[m.start() - 40 : m.end() + 40][:120])


if __name__ == "__main__":
    main()
