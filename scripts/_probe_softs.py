"""Sugar and cotton PSD probe."""
from __future__ import annotations

import xml.etree.ElementTree as ET
from collections import defaultdict

import requests

SOAP_URL = "https://apps.fas.usda.gov/PSDExternalAPIService/svcPSD_AMIS.asmx"


def fetch(code: str) -> ET.Element | None:
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


def show(name: str, code: str) -> None:
    result = fetch(code)
    if result is None or result.find(".//ERROR") is not None:
        print(name, code, "ERROR")
        return
    attrs: dict[str, str] = {}
    keys: set[tuple] = set()
    for comm in result.findall(".//Commodity"):
        if (comm.findtext("Country_Code") or "").strip() != "US":
            continue
        keys.add((comm.findtext("Calendar_Year"), comm.findtext("Month"), comm.findtext("Market_Year")))
        aid = comm.findtext("Attribute_Id")
        adesc = (comm.findtext("Attribute_Description") or "").strip()
        if aid:
            attrs[aid] = adesc
    print(name, code, "keys", len(keys))
    for aid, adesc in sorted(attrs.items(), key=lambda x: int(x[0])):
        print(" ", aid, adesc)


def main() -> None:
    show("Sugar", "0612000")
    show("Cotton", "2631000")


if __name__ == "__main__":
    main()
