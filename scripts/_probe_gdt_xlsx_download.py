"""Try downloading the official GDT Tables XLSX."""

from __future__ import annotations

import os
from pathlib import Path

import requests

UA = {"User-Agent": "Mozilla/5.0 (compatible; HPTL/gdt-xlsx)"}
CANDIDATES = [
    "https://www.gold.org/download/file/20975/GDT_Tables_Q2%2726_EN.xlsx",
    "https://www.gold.org/download/file/20975/GDT_Tables_Q2'26_EN.xlsx",
    "https://www.gold.org/download/file/20975/GDT_Tables_Q226_EN.xlsx",
]
OUT = Path("data/raw/wgc_gdt/_probe_download.bin")


def main() -> None:
    headers = dict(UA)
    cookie = os.environ.get("WGC_GOLDHUB_COOKIE", "").strip()
    print("cookie_present", bool(cookie), "len", len(cookie))
    if cookie:
        headers["Cookie"] = cookie
    OUT.parent.mkdir(parents=True, exist_ok=True)
    for url in CANDIDATES:
        r = requests.get(url, headers=headers, timeout=90, allow_redirects=True)
        magic = r.content[:4]
        is_xlsx = magic == b"PK\x03\x04"
        is_html = b"<html" in r.content[:500].lower() or b"<!doctype" in r.content[:500].lower()
        print(
            Path(url).name,
            "status",
            r.status_code,
            "ctype",
            r.headers.get("content-type"),
            "n",
            len(r.content),
            "xlsx",
            is_xlsx,
            "html",
            is_html,
            "final",
            r.url[:100],
        )
        if is_xlsx:
            path = Path("data/raw/wgc_gdt/GDT_Tables_Q2_2026_EN.xlsx")
            path.write_bytes(r.content)
            print("wrote", path)
            return
        OUT.write_bytes(r.content[:2000])
    print("download failed; wrote probe snippet to", OUT)


if __name__ == "__main__":
    main()
