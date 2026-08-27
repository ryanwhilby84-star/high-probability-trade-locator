"""Temporary probe — not part of production ingest."""
import re
import requests

for sym in ["gld", "iau"]:
    for pat in [
        f"https://www.ssga.com/library-content/products/fund-data/etfs/us/navhist-us-en-{sym}.xlsx",
        f"https://www.ssga.com/library-content/products/fund-data/etfs/us/hist-nav-us-en-{sym}.xlsx",
    ]:
        r = requests.get(pat, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200 and len(r.content) > 1000:
            print("HIT", pat, len(r.content))

r = requests.get(
    "https://www.spdrgoldshares.com/usa/industry-research/",
    timeout=30,
    headers={"User-Agent": "Mozilla/5.0"},
)
links = set(re.findall(r'href=["\']([^"\']+\.(?:csv|xlsx|xls))["\']', r.text, re.I))
print("spdr links", len(links))
for l in sorted(links)[:20]:
    print(l)

for sid in [
    "CHNBCICPAINAAI",
    "BSCICP02CNM460S",
    "PRINTO01CNM661N",
    "CHNPRINTO01IXOBM",
    "NAEXKP01CNM661S",
    "CHNPRMNTO01IXOBM",
    "CHNPRMNTO01GYM",
    "CHNPRINTO01GYM",
    "CHNPROINDMISMEI",
]:
    rr = requests.get(
        f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}",
        timeout=15,
        headers={"User-Agent": "HPTL"},
    )
    n = max(0, len(rr.text.splitlines()) - 1) if rr.ok else 0
    if n > 10:
        print("FRED", sid, n, rr.text.splitlines()[-1][:80])
