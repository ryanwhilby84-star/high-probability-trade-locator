"""Fetch quarterly central bank net purchases from public WGC GDT HTML tables."""

from __future__ import annotations

import re
from io import StringIO
from typing import Any

import pandas as pd
import requests

USER_AGENT = "Mozilla/5.0 (compatible; HPTL/gdt-cb-gold)"


def _quarter_end(label: str) -> str | None:
    s = str(label).strip().replace("'", "'")
    m = re.match(r"^Q([1-4])'(\d{2,4})$", s, re.I)
    if not m:
        return None
    q, y = int(m.group(1)), int(m.group(2))
    if y < 100:
        y += 2000
    month = q * 3
    return (pd.Timestamp(year=y, month=month, day=1) + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")


def _gdt_urls() -> list[str]:
    urls: list[str] = []
    for year in range(2016, 2027):
        urls.append(
            f"https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-full-year-{year}"
        )
        for q in (1, 2, 3, 4):
            urls.append(
                f"https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-q{q}-{year}"
            )
    return urls


def _extract_cb_from_page(url: str) -> dict[str, float]:
    r = requests.get(url, timeout=60, headers={"User-Agent": USER_AGENT})
    if r.status_code != 200:
        return {}
    try:
        tables = pd.read_html(StringIO(r.text), flavor="html5lib")
    except ValueError:
        return {}
    if not tables:
        return {}
    df = tables[0]
    header = df.iloc[0].tolist()
    cb_row = None
    for _, row in df.iterrows():
        if "central bank" in str(row.iloc[0]).lower():
            cb_row = row.tolist()
            break
    if not cb_row:
        return {}
    out: dict[str, float] = {}
    for col_label, val in zip(header, cb_row):
        dt = _quarter_end(col_label)
        if not dt:
            continue
        try:
            out[dt] = float(str(val).replace(",", ""))
        except (TypeError, ValueError):
            continue
    return out


def fetch_gdt_quarterly_cb_purchases() -> list[dict[str, Any]]:
    merged: dict[str, float] = {}
    for url in _gdt_urls():
        merged.update(_extract_cb_from_page(url))
    obs = [{"date": dt, "value": merged[dt]} for dt in sorted(merged)]
    return obs
