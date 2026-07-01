"""Sample US corn PSD rows for one release month."""
from __future__ import annotations

import xml.etree.ElementTree as ET
from collections import defaultdict

import requests

SOAP_URL = "https://apps.fas.usda.gov/PSDExternalAPIService/svcPSD_AMIS.asmx"


def fetch(code: str) -> ET.Element:
    body = (
        f'<getDatabyCommodity xmlns="http://www.fas.usda.gov/wsfaspsd/">'
        f"<strCommodityCode>{code}</strCommodityCode></getDatabyCommodity>"
    )
    envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>{body}</soap:Body>
</soap:Envelope>"""
    r = requests.post(
        SOAP_URL,
        data=envelope.encode("utf-8"),
        headers={
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": "http://www.fas.usda.gov/wsfaspsd/getDatabyCommodity",
        },
        timeout=180,
    )
    r.raise_for_status()
    root = ET.fromstring(r.content)
    body_el = root.find("{http://schemas.xmlsoap.org/soap/envelope/}Body")
    return body_el.find(".//{http://www.fas.usda.gov/wsfaspsd/}getDatabyCommodityResult")


def main() -> None:
    result = fetch("0440000")
    by_key: dict[tuple, dict[str, float]] = defaultdict(dict)
    for comm in result.findall(".//Commodity"):
        if (comm.findtext("Country_Code") or "").strip() != "US":
            continue
        cy = comm.findtext("Calendar_Year")
        mo = comm.findtext("Month")
        my = comm.findtext("Market_Year")
        aid = comm.findtext("Attribute_Id")
        val = comm.findtext("Value")
        key = (cy, mo, my)
        by_key[key][aid] = float(val)
    # latest calendar year/month
    keys = sorted(by_key.keys())
    for key in keys[-5:]:
        vals = by_key[key]
        print("key", key)
        for aid in sorted(vals, key=lambda x: int(x)):
            print(" ", aid, vals[aid])
        es = vals.get("176")
        tc = vals.get("125")
        ex = vals.get("113")
        td = vals.get("178")
        bs = vals.get("20")
        prod = vals.get("28")
        if es and tc and ex:
            print("  stu_end/total_cons", es / tc if tc else None)
            print("  stu_end/(cons+exp)", es / (tc + ex) if (tc + ex) else None)
        print()


if __name__ == "__main__":
    main()
