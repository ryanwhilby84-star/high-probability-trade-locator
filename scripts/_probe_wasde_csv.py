"""Try WASDE historical CSV downloads."""
from __future__ import annotations

import requests

URLS = [
    "https://www.usda.gov/sites/default/files/documents/wasde0120.csv",
    "https://www.usda.gov/sites/default/files/documents/wasde0124.csv",
    "https://www.usda.gov/sites/default/files/documents/wasde1223.csv",
    "https://usda.library.cornell.edu/concern/publications/3t945q76s?locale=en",
]


def main() -> None:
    for url in URLS:
        try:
            r = requests.get(
                url,
                timeout=120,
                headers={"User-Agent": "Mozilla/5.0 (compatible; HPTL/1.0)"},
                allow_redirects=True,
            )
            print(url[:70], r.status_code, len(r.content), r.headers.get("content-type", "")[:40])
            if r.ok and len(r.content) > 1000 and "csv" in url:
                print(r.text[:400])
        except Exception as exc:
            print(url[:70], "ERR", exc)


if __name__ == "__main__":
    main()
