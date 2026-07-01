"""List all US corn PSD release keys."""
from __future__ import annotations

import xml.etree.ElementTree as ET

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
    keys = sorted(
        {
            (
                comm.findtext("Calendar_Year"),
                comm.findtext("Month"),
                comm.findtext("Market_Year"),
            )
            for comm in result.findall(".//Commodity")
            if (comm.findtext("Country_Code") or "").strip() == "US"
        }
    )
    print("count", len(keys))
    for k in keys:
        print(k)


if __name__ == "__main__":
    main()
