"""Count release points for all priority commodities."""
from __future__ import annotations

import xml.etree.ElementTree as ET
from collections import defaultdict

import requests

SOAP_URL = "https://apps.fas.usda.gov/PSDExternalAPIService/svcPSD_AMIS.asmx"

COMMODITIES = {
    "Corn": "0440000",
    "Soybeans": "2222000",
    "Wheat": "0410000",
    "Cotton": "0811000",
    "Sugar": "0610000",
}


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
    for name, code in COMMODITIES.items():
        result = fetch(code)
        if result.find(".//ERROR") is not None:
            print(name, code, "ERROR", result.find(".//ERROR").findtext("ERROR"))
            continue
        keys = set()
        attrs: set[str] = set()
        for comm in result.findall(".//Commodity"):
            if (comm.findtext("Country_Code") or "").strip() != "US":
                continue
            keys.add((comm.findtext("Calendar_Year"), comm.findtext("Month")))
            attrs.add((comm.findtext("Attribute_Id"), (comm.findtext("Attribute_Description") or "").strip()))
        print(name, code, "US releases", len(keys), "attrs", len(attrs))
        if attrs:
            print("  sample attrs", sorted(attrs)[:8])


if __name__ == "__main__":
    main()
