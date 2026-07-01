"""Extract GDT supply/demand table central bank row from full-year pages."""
from __future__ import annotations

import re

import requests

UA = {"User-Agent": "Mozilla/5.0"}


def extract_cb_row(html: str) -> list[tuple[str, float]]:
    # find table row: Central banks & other inst. | q1 | q2 | ...
    text = re.sub(r"<[^>]+>", "|", html)
    chunks = [c.strip() for c in text.split("|") if c.strip()]
    out: list[tuple[str, float]] = []
    for i, c in enumerate(chunks):
        if "central bank" in c.lower() and "other inst" in c.lower():
            vals = []
            for j in range(i + 1, min(i + 20, len(chunks))):
                try:
                    vals.append(float(chunks[j].replace(",", "")))
                except ValueError:
                    if vals:
                        break
            print("row values", vals[:8])
    return out


def main() -> None:
    for year in (2024, 2023, 2022):
        url = f"https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-full-year-{year}"
        r = requests.get(url, headers=UA, timeout=60)
        print("===", year, r.status_code)
        if r.status_code == 200:
            extract_cb_row(r.text)


if __name__ == "__main__":
    main()
