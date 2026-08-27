"""Probe USDA PSD attributes for US corn."""
from __future__ import annotations

import xml.etree.ElementTree as ET
from collections import defaultdict

import requests

SOAP_URL = "https://apps.fas.usda.gov/PSDExternalAPIService/svcPSD_AMIS.asmx"
CODE = "0440000"


def fetch_commodity(code: str) -> bytes:
    body = (
        f'<getDatabyCommodity xmlns="http://www.fas.usda.gov/wsfaspsd/">'
        f"<strCommodityCode>{code}</strCommodityCode></getDatabyCommodity>"
    )
    envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <soap:Body>{body}</soap:Body>
</soap:Envelope>"""
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "http://www.fas.usda.gov/wsfaspsd/getDatabyCommodity",
    }
    r = requests.post(SOAP_URL, data=envelope.encode("utf-8"), headers=headers, timeout=180)
    r.raise_for_status()
    return r.content


def main() -> None:
    root = ET.fromstring(fetch_commodity(CODE))
    ns = {"s": "http://schemas.xmlsoap.org/soap/envelope/"}
    body = root.find("s:Body", ns)
    assert body is not None
    result = body.find(".//{http://www.fas.usda.gov/wsfaspsd/}getDatabyCommodityResult")
    attrs: dict[str, str] = {}
    us_rows = 0
    for comm in result.findall(".//Commodity"):
        country = (comm.findtext("Country_Code") or "").strip()
        if country != "US":
            continue
        us_rows += 1
        aid = comm.findtext("Attribute_Id")
        adesc = (comm.findtext("Attribute_Description") or "").strip()
        if aid:
            attrs[aid] = adesc
    print("US rows", us_rows)
    for aid, adesc in sorted(attrs.items(), key=lambda x: int(x[0])):
        print(aid, adesc)


if __name__ == "__main__":
    main()
