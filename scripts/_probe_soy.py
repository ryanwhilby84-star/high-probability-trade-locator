"""Soybean PSD attribute sample."""
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
    result = fetch("2222000")
    attrs: dict[str, str] = {}
    for comm in result.findall(".//Commodity"):
        if (comm.findtext("Country_Code") or "").strip() != "US":
            continue
        aid = comm.findtext("Attribute_Id")
        adesc = (comm.findtext("Attribute_Description") or "").strip()
        if aid:
            attrs[aid] = adesc
    for aid, adesc in sorted(attrs.items(), key=lambda x: int(x[0])):
        print(aid, adesc)

    by_key: dict[tuple, dict[str, float]] = defaultdict(dict)
    for comm in result.findall(".//Commodity"):
        if (comm.findtext("Country_Code") or "").strip() != "US":
            continue
        key = (comm.findtext("Calendar_Year"), comm.findtext("Month"), comm.findtext("Market_Year"))
        by_key[key][comm.findtext("Attribute_Id")] = float(comm.findtext("Value"))
    print("keys", len(by_key))
    key = sorted(by_key.keys())[-1]
    vals = by_key[key]
    print("latest", key)
    es = vals.get("176")
    tc = vals.get("125")
    ex = vals.get("110") or vals.get("113")
    ts = vals.get("86")
    print("ending", es, "dom", tc, "exp", ex, "supply", ts)
    if es and tc and ex:
        print("stu cons+exp", es / (tc + ex))


if __name__ == "__main__":
    main()
