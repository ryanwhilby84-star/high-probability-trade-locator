"""Official EIA dnav hist_xls downloads (no API key required)."""

from __future__ import annotations

import urllib.request
from datetime import datetime, timedelta
from typing import Any

import xlrd

UA = {"User-Agent": "Mozilla/5.0 HPTL/1.0"}

EIA_XLS = {
    "working_gas_storage": {
        "url": "https://www.eia.gov/dnav/ng/hist_xls/NW2_EPG0_SWO_R48_BCFw.xls",
        "series_id": "NW2_EPG0_SWO_R48_BCF_W",
        "unit_scale": 1.0,  # Bcf
    },
    "dry_gas_production": {
        "url": "https://www.eia.gov/dnav/ng/hist_xls/N9070US2m.xls",
        "series_id": "N9070US2",
        "unit_scale": 1.0 / 1000.0 / 30.437,  # MMcf/month -> Bcf/d
    },
    "lng_exports": {
        "url": "https://www.eia.gov/dnav/ng/hist_xls/N9133US2m.xls",
        "series_id": "N9133US2",
        "unit_scale": 1.0 / 1000.0 / 30.437,  # MMcf/month -> Bcf/d
    },
}


def _excel_serial_to_date(serial: float) -> str | None:
    try:
        # Excel 1900 date system (xlrd)
        base = datetime(1899, 12, 30)
        dt = base + timedelta(days=float(serial))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def fetch_eia_xls_observations(driver_key: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return ([{date,value}], meta) for an EIA hist_xls workbook Data 1 sheet."""
    spec = EIA_XLS[driver_key]
    req = urllib.request.Request(spec["url"], headers=UA)
    with urllib.request.urlopen(req, timeout=90) as resp:
        data = resp.read()

    book = xlrd.open_workbook(file_contents=data)
    sheet = book.sheet_by_name("Data 1") if "Data 1" in book.sheet_names() else book.sheet_by_index(1)
    scale = float(spec["unit_scale"])
    rows: list[dict[str, Any]] = []
    for r in range(3, sheet.nrows):
        dcell = sheet.cell_value(r, 0)
        vcell = sheet.cell_value(r, 1)
        if dcell in ("", None) or vcell in ("", None):
            continue
        if isinstance(dcell, (int, float)):
            date = _excel_serial_to_date(float(dcell))
        else:
            date = str(dcell)[:10]
        try:
            value = float(vcell) * scale
        except (TypeError, ValueError):
            continue
        if not date or value != value:
            continue
        rows.append({"date": date, "value": value})
    rows.sort(key=lambda x: x["date"])
    meta = {
        "official_source": "EIA dnav hist_xls (official public download)",
        "source_url": spec["url"],
        "series_identifier": spec["series_id"],
    }
    return rows, meta
