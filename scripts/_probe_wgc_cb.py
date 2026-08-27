import re
import sys
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

page = requests.get(
    "https://www.gold.org/goldhub/data/gold-reserves-by-country",
    timeout=60,
    headers={"User-Agent": "Mozilla/5.0"},
)
print("page", page.status_code)
links = re.findall(
    r'https://www\.gold\.org/download/file/\d+/[^"\'\s>]+\.xlsx',
    page.text,
    flags=re.I,
)
print("links", len(links))
for l in links:
    print(l)

for l in links:
    r = requests.get(l, timeout=90, headers={"User-Agent": "Mozilla/5.0"})
    ok = r.status_code == 200 and r.content[:2] == b"PK"
    name = l.split("/")[-1]
    print("fetch", name, ok, r.status_code, len(r.content))
    if not ok:
        continue
    xl = pd.ExcelFile(BytesIO(r.content))
    print("  sheets", xl.sheet_names)
    for sheet in xl.sheet_names[:3]:
        df = pd.read_excel(xl, sheet_name=sheet)
        print("  ", sheet, df.shape, list(df.columns)[:6])
