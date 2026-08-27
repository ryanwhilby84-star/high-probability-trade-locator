"""Merge GDT quarterly + Gold Focus monthly into gold_cb_purchases.csv."""
from __future__ import annotations

import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bootstrap_cb_gold_from_gdt import _extract_cb_from_page, _gdt_urls, _quarter_end  # noqa: E402
from scripts._test_gold_focus_extract import _extract_from_html  # type: ignore  # noqa: E402

# reuse brute discovery hits from prior run
GOLD_FOCUS_URLS = [
    "https://www.gold.org/goldhub/gold-focus/2024/03/central-bank-gold-statistics-january-2024",
    "https://www.gold.org/goldhub/gold-focus/2024/05/central-bank-gold-statistics-march-2024",
    "https://www.gold.org/goldhub/gold-focus/2024/07/central-bank-gold-statistics-july-2024",
    "https://www.gold.org/goldhub/gold-focus/2024/07/central-bank-gold-statistics-june-2024",
    "https://www.gold.org/goldhub/gold-focus/2024/07/central-bank-gold-statistics-may-2024",
    "https://www.gold.org/goldhub/gold-focus/2024/08/central-bank-gold-statistics-june-2024",
    "https://www.gold.org/goldhub/gold-focus/2024/09/central-bank-gold-statistics-july-2024",
    "https://www.gold.org/goldhub/gold-focus/2024/11/central-bank-gold-statistics-november-2024",
    "https://www.gold.org/goldhub/gold-focus/2024/11/central-bank-gold-statistics-september-2024",
    "https://www.gold.org/goldhub/gold-focus/2024/12/central-bank-gold-statistics-october-2024",
    "https://www.gold.org/goldhub/gold-focus/2025/05/central-bank-gold-statistics-march-2025",
    "https://www.gold.org/goldhub/gold-focus/2025/06/central-bank-gold-statistics-april-2025",
    "https://www.gold.org/goldhub/gold-focus/2025/08/central-bank-gold-statistics-june-2025",
    "https://www.gold.org/goldhub/gold-focus/2025/11/central-bank-gold-statistics-september-2025",
    "https://www.gold.org/goldhub/gold-focus/2026/05/central-bank-gold-statistics-march-2026",
    "https://www.gold.org/goldhub/gold-focus/2026/06/central-bank-gold-statistics-central-banks-resume-net-buying-april",
    "https://www.gold.org/goldhub/gold-focus/2026/04/central-bank-gold-statistics-central-banks-stay-course-gold-february",
    "https://www.gold.org/goldhub/gold-focus/2026/03/central-bank-gold-statistics-momentum-eases-january-while-demand-base",
]

UA = {"User-Agent": "Mozilla/5.0"}
OUT = ROOT / "data" / "manual" / "metals" / "gold_cb_purchases.csv"


def main() -> None:
    merged: dict[str, float] = {}
    for url in _gdt_urls():
        merged.update(_extract_cb_from_page(url))
    for url in GOLD_FOCUS_URLS:
        r = requests.get(url, headers=UA, timeout=60)
        if r.status_code != 200:
            continue
        hit = _extract_from_html(r.text, url)
        if hit:
            merged[hit[0]] = hit[1]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "date,value",
        "# tonnes net; WGC GDT quarterly + Gold Focus monthly (public pages)",
        "",
    ]
    for dt in sorted(merged):
        lines.append(f"{dt},{merged[dt]:g}")
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {len(merged)} rows to {OUT}")


if __name__ == "__main__":
    main()
