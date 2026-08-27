"""Temporary probe for USDA PSD endpoints."""
from __future__ import annotations

import requests

SOAP_URL = "https://apps.fas.usda.gov/PSDExternalAPIService/svcPSD_AMIS.asmx"


def soap_call(action: str, body_inner: str) -> requests.Response:
    envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
 xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>{body_inner}</soap:Body>
</soap:Envelope>"""
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": f"http://www.fas.usda.gov/wsfaspsd/{action}",
    }
    return requests.post(SOAP_URL, data=envelope.encode("utf-8"), headers=headers, timeout=120)


def main() -> None:
    for action, body in [
        (
            "getCommodityList",
            '<getCommodityList xmlns="http://www.fas.usda.gov/wsfaspsd/" />',
        ),
        (
            "getDatabyCommodity",
            '<getDatabyCommodity xmlns="http://www.fas.usda.gov/wsfaspsd/">'
            "<strCommodityCode>0440000</strCommodityCode></getDatabyCommodity>",
        ),
        (
            "getDatabyCommodity",
            '<getDatabyCommodity xmlns="http://www.fas.usda.gov/wsfaspsd/">'
            "<strCommodityCode>CORN</strCommodityCode></getDatabyCommodity>",
        ),
    ]:
        r = soap_call(action, body)
        print("===", action, r.status_code, len(r.text))
        print(r.text[:2000])
        print()


if __name__ == "__main__":
    main()
