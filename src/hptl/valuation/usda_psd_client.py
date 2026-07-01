"""USDA FAS PSD client — SOAP fetch with on-disk cache."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import requests

from hptl.config import PROJECT_ROOT

SOAP_URL = "https://apps.fas.usda.gov/PSDExternalAPIService/svcPSD_AMIS.asmx"
DEFAULT_CACHE_DIR = PROJECT_ROOT / "data" / "raw" / "usda_psd"
DEFAULT_TIMEOUT = int(os.environ.get("USDA_PSD_TIMEOUT_SECONDS", "180"))


class UsdaPsdError(RuntimeError):
    pass


def _cache_path(commodity_code: str, *, cache_dir: Path | None = None) -> Path:
    root = cache_dir or DEFAULT_CACHE_DIR
    return root / f"{commodity_code}.xml"


def fetch_psd_commodity_xml(
    commodity_code: str,
    *,
    cache_dir: Path | None = None,
    force_refresh: bool = False,
    timeout: int = DEFAULT_TIMEOUT,
) -> bytes:
    """Download PSD commodity dataset via SOAP; cache raw XML under data/raw/usda_psd/."""
    path = _cache_path(commodity_code, cache_dir=cache_dir)
    if path.exists() and not force_refresh:
        return path.read_bytes()

    body = (
        f'<getDatabyCommodity xmlns="http://www.fas.usda.gov/wsfaspsd/">'
        f"<strCommodityCode>{commodity_code}</strCommodityCode>"
        f"</getDatabyCommodity>"
    )
    envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <soap:Body>{body}</soap:Body>
</soap:Envelope>"""
    response = requests.post(
        SOAP_URL,
        data=envelope.encode("utf-8"),
        headers={
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": "http://www.fas.usda.gov/wsfaspsd/getDatabyCommodity",
        },
        timeout=timeout,
    )
    response.raise_for_status()
    content = response.content
    root = ET.fromstring(content)
    err = root.find(".//ERROR")
    if err is not None:
        msg = err.findtext("ERROR") or ET.tostring(err, encoding="unicode")
        raise UsdaPsdError(f"PSD SOAP error for {commodity_code}: {msg}")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return content


def parse_psd_rows(xml_bytes: bytes, *, country_code: str = "US") -> list[dict[str, Any]]:
    """Parse SOAP XML into flat PSD rows."""
    root = ET.fromstring(xml_bytes)
    body = root.find("{http://schemas.xmlsoap.org/soap/envelope/}Body")
    if body is None:
        raise UsdaPsdError("Missing SOAP body")
    result = body.find(".//{http://www.fas.usda.gov/wsfaspsd/}getDatabyCommodityResult")
    if result is None:
        raise UsdaPsdError("Missing getDatabyCommodityResult")

    rows: list[dict[str, Any]] = []
    for comm in result.findall(".//Commodity"):
        cc = (comm.findtext("Country_Code") or "").strip()
        if cc != country_code:
            continue
        rows.append(
            {
                "commodity_code": (comm.findtext("Commodity_code") or "").strip(),
                "commodity_description": (comm.findtext("Commodity_Description") or "").strip(),
                "country_code": cc,
                "calendar_year": (comm.findtext("Calendar_Year") or "").strip(),
                "month": (comm.findtext("Month") or "").strip(),
                "market_year": (comm.findtext("Market_Year") or "").strip(),
                "attribute_id": (comm.findtext("Attribute_Id") or "").strip(),
                "attribute_description": (comm.findtext("Attribute_Description") or "").strip(),
                "unit_description": (comm.findtext("Unit_Description") or "").strip(),
                "value": comm.findtext("Value"),
            }
        )
    return rows
