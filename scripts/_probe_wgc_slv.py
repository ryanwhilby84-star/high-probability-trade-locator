"""Probe WGC reserves xlsx links."""
import re
import requests

for page in [
    "https://www.gold.org/goldhub/data/gold-reserves-by-country",
    "https://www.gold.org/goldhub/data/gold-etfs-holdings-and-flows",
]:
    r = requests.get(page, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
    print(page, r.status_code)
    links = re.findall(r"https://www\.gold\.org/download/file/\d+/[^\"'\s>]+", r.text)
    for l in sorted(set(links))[:20]:
        print(" ", l)

import yfinance as yf

t = yf.Ticker("SLV")
sh = t.get_shares_full(start="2016-01-01")
print("SLV shares", None if sh is None else len(sh), sh.tail(3) if sh is not None and len(sh) else "")
