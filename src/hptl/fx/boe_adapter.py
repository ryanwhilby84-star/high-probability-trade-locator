"""GBP currency-rate adapter — Bank of England (first-party).

Fields
------
* Policy rate -> Bank Rate (``IUDBEDR``) via the BoE Interactive Database CSV
  export. Reliable, dated, no key.
* 2Y / 10Y -> BoE government (nominal) spot yield curve. The BoE publishes a
  daily "GLC Nominal daily data" workbook inside
  ``latest-yield-curve-data.zip``; sheet "4. spot curve" gives the fitted spot
  rate at every maturity (0.5y steps). We read the 2.0y and 10.0y columns of
  the latest dated row. This is the BoE's own commercial-grade gilt curve.
"""

from __future__ import annotations

import csv
import io
import zipfile
from datetime import date, timedelta

import pandas as pd

from hptl.fx.rate_adapter_base import (
    FieldValue,
    NormalizedRate,
    fetch_bytes,
    fetch_text,
    to_float,
)

CURRENCY = "GBP"
CENTRAL_BANK = "Bank of England"

YIELD_CURVE_URL = (
    "https://www.bankofengland.co.uk/-/media/boe/files/statistics/yield-curves/"
    "latest-yield-curve-data.zip"
)
_GLC_NOMINAL_FILE = "GLC Nominal daily data current month.xlsx"
_SPOT_SHEET = "4. spot curve"
_YIELD_SOURCE = "Bank of England (GLC nominal spot curve)"


def _bank_rate_url() -> str:
    today = date.today()
    start = today - timedelta(days=120)
    return (
        "https://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp"
        "?csv.x=yes"
        f"&Datefrom={start.strftime('%d/%b/%Y')}"
        f"&Dateto={today.strftime('%d/%b/%Y')}"
        "&SeriesCodes=IUDBEDR&CSVF=TN&UsingCodes=Y&VPD=Y&VFD=N"
    )


def _fetch_bank_rate() -> FieldValue:
    try:
        raw = fetch_text(_bank_rate_url(), cache_key="gbp_bank_rate")
        reader = csv.DictReader(io.StringIO(raw))
        rows = [r for r in reader if (r.get("IUDBEDR") or "").strip()]
        if not rows:
            raise ValueError("no Bank Rate observations")
        last = rows[-1]
        as_of = None
        d = (last.get("DATE") or "").strip()
        if d:
            try:
                as_of = date.fromisoformat(d).isoformat()
            except ValueError:
                from datetime import datetime

                as_of = datetime.strptime(d, "%d %b %Y").date().isoformat()
        return FieldValue(
            value=to_float(last.get("IUDBEDR")),
            as_of=as_of,
            source="Bank of England (Bank Rate, IUDBEDR)",
        )
    except Exception as exc:  # noqa: BLE001
        return FieldValue(error=f"{type(exc).__name__}: {exc}")


def _parse_spot_curve(content: bytes) -> dict[str, FieldValue]:
    z = zipfile.ZipFile(io.BytesIO(content))
    names = z.namelist()
    target = next((n for n in names if n == _GLC_NOMINAL_FILE), None)
    if target is None:
        target = next((n for n in names if "GLC Nominal" in n and "daily" in n.lower()), None)
    if target is None:
        raise ValueError(f"GLC Nominal daily file not found in zip ({names})")

    df = pd.read_excel(io.BytesIO(z.read(target)), sheet_name=_SPOT_SHEET, header=None)
    # Locate the maturity header row (col 0 == "years:").
    hdr_idx = None
    for i in range(min(10, len(df))):
        if str(df.iloc[i, 0]).strip().lower().startswith("years"):
            hdr_idx = i
            break
    if hdr_idx is None:
        raise ValueError("no maturity ('years:') header row in spot curve sheet")
    maturities = df.iloc[hdr_idx].tolist()

    def col_for(target_years: float) -> int | None:
        for j, v in enumerate(maturities):
            try:
                if abs(float(v) - target_years) < 1e-6:
                    return j
            except (TypeError, ValueError):
                continue
        return None

    c2, c10 = col_for(2.0), col_for(10.0)
    if c2 is None or c10 is None:
        raise ValueError("2.0y / 10.0y maturities not found in spot curve")

    data = df.iloc[hdr_idx + 1 :].copy()
    data["_date"] = pd.to_datetime(data.iloc[:, 0], errors="coerce")
    data = data.dropna(subset=["_date"])
    if data.empty:
        raise ValueError("no dated rows in current-month spot curve")
    last = data.sort_values("_date").iloc[-1]
    as_of = last["_date"].date().isoformat()
    return {
        "y2": FieldValue(to_float(last.iloc[c2]), as_of, f"{_YIELD_SOURCE} 2Y"),
        "y10": FieldValue(to_float(last.iloc[c10]), as_of, f"{_YIELD_SOURCE} 10Y"),
    }


def _fetch_yields() -> dict[str, FieldValue]:
    try:
        return _parse_spot_curve(fetch_bytes(YIELD_CURVE_URL, cache_key="gbp_yield_curve"))
    except Exception as exc:  # noqa: BLE001
        err = f"{type(exc).__name__}: {exc}"
        return {"y2": FieldValue(error=err), "y10": FieldValue(error=err)}


def fetch() -> NormalizedRate:
    yields = _fetch_yields()
    return NormalizedRate(
        currency=CURRENCY,
        central_bank=CENTRAL_BANK,
        policy=_fetch_bank_rate(),
        y2=yields["y2"],
        y10=yields["y10"],
    )
