"""Bootstrap monthly CB net purchases from public WGC Gold Focus posts (2020+)."""
from __future__ import annotations

import re
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

UA = {"User-Agent": "Mozilla/5.0 (compatible; HPTL/gold-focus-bootstrap)"}

MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


def _month_end(year: int, month: int) -> str:
    import pandas as pd

    return (pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")


def _extract_from_html(html: str, url: str) -> tuple[str, float] | None:
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text)

    # Title hints: "... March 2026", "... in April", "... February"
    title_month = None
    title_year = None
    for mname, mnum in MONTHS.items():
        if re.search(rf"\b{mname}\b", url, re.I) or re.search(rf"\b{mname}\s+20\d{{2}}\b", text[:500], re.I):
            title_month = mnum
        ym = re.search(rf"\b{mname}\s+(20\d{{2}})\b", text[:800], re.I)
        if ym:
            title_month = mnum
            title_year = int(ym.group(1))

    patterns = [
        r"bought a net\s+(-?\d+(?:\.\d+)?)\s*t(?:onne[s]?)?\s+in\s+(January|February|March|April|May|June|July|August|September|October|November|December)",
        r"net purchases total(?:led|led)?\s+(-?\d+(?:\.\d+)?)\s*t(?:onne[s]?)?\s+in\s+(January|February|March|April|May|June|July|August|September|October|November|December)",
        r"having bought\s+(-?\d+(?:\.\d+)?)\s*t(?:onne[s]?)?\.?\s+This was",
        r"resumed net gold purchases in\s+(January|February|March|April|May|June|July|August|September|October|November|December), having bought\s+(-?\d+(?:\.\d+)?)\s*t",
        r"net sales reported in\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+\((-?\d+(?:\.\d+)?)\s*t\)",
        r"Central banks bought a net\s+(-?\d+(?:\.\d+)?)\s*t(?:onne[s]?)?\s+in\s+(January|February|March|April|May|June|July|August|September|October|November|December)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.I)
        if not m:
            continue
        groups = m.groups()
        if len(groups) == 2:
            if groups[0].replace(".", "", 1).isdigit() or groups[0].lstrip("-").replace(".", "", 1).isdigit():
                val, mname = float(groups[0]), groups[1]
            else:
                mname, val = groups[0], float(groups[1])
        else:
            continue
        month = MONTHS[mname.lower()]
        year = title_year
        if year is None:
            ym = re.search(rf"{mname}\s+(20\d{{2}})", text[:1200], re.I)
            year = int(ym.group(1)) if ym else None
        if year is None:
            # infer from URL /gold-focus/2026/06/...
            um = re.search(r"/gold-focus/(20\d{2})/(\d{2})/", url)
            if um:
                year = int(um.group(1))
                pub_month = int(um.group(2))
                # report month typically 2 months before publish
                month = month if month <= pub_month else month
        if year is None:
            continue
        return _month_end(year, month), val
    return None


def discover_urls() -> list[str]:
    urls: list[str] = []
    for year in range(2020, 2027):
        for month in range(1, 13):
            mm = f"{month:02d}"
            for slug in (
                f"central-bank-gold-statistics-{list(MONTHS.keys())[month-1]}-{year}",
                f"central-bank-gold-statistics-{list(MONTHS.keys())[month-1]}",
            ):
                urls.append(f"https://www.gold.org/goldhub/gold-focus/{year}/{mm}/{slug}")
            urls.append(
                f"https://www.gold.org/goldhub/gold-focus/{year}/{mm}/central-bank-gold-statistics"
            )
    return urls


def main() -> int:
    found: dict[str, float] = {}
    for url in discover_urls():
        try:
            r = requests.get(url, headers=UA, timeout=20)
        except requests.RequestException:
            continue
        if r.status_code != 200 or "central bank" not in r.text.lower():
            continue
        hit = _extract_from_html(r.text, url)
        if hit:
            dt, val = hit
            found[dt] = val
            print("hit", dt, val, url.split("/")[-1])

    print("total months", len(found))
    if len(found) < 36:
        print("insufficient for bootstrap (need 36+ monthly obs)")
        return 1

    out = ROOT / "data" / "manual" / "metals" / "gold_cb_purchases.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    lines = ["date,value  # tonnes net; source=WGC Gold Focus public posts\n"]
    for dt in sorted(found):
        lines.append(f"{dt},{found[dt]:g}\n")
    out.write_text("".join(lines), encoding="utf-8")
    print(f"Wrote {out} ({len(found)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
