"""Find PSD commodity codes for cotton and sugar."""
from __future__ import annotations

import xml.etree.ElementTree as ET

import requests

SOAP_URL = "https://apps.fas.usda.gov/PSDExternalAPIService/svcPSD_AMIS.asmx"
CANDIDATES = [
    "0811000",
    "0813100",
    "0813200",
    "0810000",
    "0610000",
    "0612000",
    "0613000",
    "0614000",
    "0615000",
]


def try_code(code: str) -> str:
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
        timeout=120,
    )
    root = ET.fromstring(r.content)
    result = root.find(".//{http://www.fas.usda.gov/wsfaspsd/}getDatabyCommodityResult")
    err = result.find(".//ERROR")
    if err is not None:
        return f"ERROR: {(err.findtext('ERROR') or err.text or 'unknown')[:80]}"
    comm = result.find(".//Commodity")
    if comm is None:
        return "no rows"
    desc = (comm.findtext("Commodity_Description") or "").strip()
    us = sum(
        1
        for c in result.findall(".//Commodity")
        if (c.findtext("Country_Code") or "").strip() == "US"
    )
    return f"OK desc={desc!r} us_rows={us}"


def main() -> None:
    for code in CANDIDATES:
        print(code, try_code(code))


if __name__ == "__main__":
    main()
