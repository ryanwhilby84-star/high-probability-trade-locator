"""Scrape WGC and LME download links."""
import re
import requests

r = requests.get(
    "https://www.gold.org/goldhub/data/gold-demand-by-country",
    timeout=30,
    headers={"User-Agent": "Mozilla/5.0"},
)
links = set(re.findall(r"https://www\.gold\.org/download/file/\d+/[^\"'\s>]+", r.text))
print("wgc links", len(links))
for l in sorted(links)[:15]:
    print(l)

r2 = requests.get(
    "https://www.lme.com/en/Market-data/Reports-and-data/Warehouse-and-stock-reports",
    timeout=30,
    headers={"User-Agent": "Mozilla/5.0"},
)
print("lme page", r2.status_code, len(r2.text))
for m in re.findall(r'href="([^"]+)"', r2.text):
    if "stock" in m.lower() or "warehouse" in m.lower() or "copper" in m.lower():
        print(m[:120])
