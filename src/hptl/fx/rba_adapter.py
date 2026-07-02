"""AUD currency-rate adapter — Reserve Bank of Australia (first-party).

Fields
------
* Policy rate -> Cash Rate Target (``FIRMMCRTD``) from RBA statistical table
  F1 (daily, ``f01d.xlsx``).
* 2Y / 10Y -> Australian Government bond yields (``FCMYGBAG2D`` /
  ``FCMYGBAG10D``) from RBA table F2 (daily, ``f02d.xlsx``).

RBA tables are Excel workbooks with a metadata block; the ``Series ID`` row
maps columns to series and data rows follow. Each series' latest non-empty
observation (and its date) is taken.
"""

from __future__ import annotations

import io

import pandas as pd

from hptl.fx.rate_adapter_base import (
    FieldValue,
    NormalizedRate,
    fetch_bytes,
    to_float,
)

CURRENCY = "AUD"
CENTRAL_BANK = "Reserve Bank of Australia"

F1_URL = "https://www.rba.gov.au/statistics/tables/xls/f01d.xlsx"
F2_URL = "https://www.rba.gov.au/statistics/tables/xls/f02d.xlsx"

CASH_RATE_SERIES = "FIRMMCRTD"
Y2_SERIES = "FCMYGBAG2D"
Y10_SERIES = "FCMYGBAG10D"


def _latest_for_series(content: bytes, series_id: str) -> tuple[float | None, str | None]:
    """Return (latest non-null value, ISO date) for an RBA series in an xlsx."""
    xl = pd.ExcelFile(io.BytesIO(content))
    df = xl.parse(xl.sheet_names[0], header=None)
    # Locate the "Series ID" metadata row.
    id_row = None
    for i in range(min(20, len(df))):
        if str(df.iloc[i, 0]).strip() == "Series ID":
            id_row = i
            break
    if id_row is None:
        raise ValueError("no 'Series ID' row in RBA workbook")
    ids = [str(x).strip() for x in df.iloc[id_row].tolist()]
    if series_id not in ids:
        raise ValueError(f"series {series_id} not in workbook")
    col = ids.index(series_id)

    data = df.iloc[id_row + 1 :, [0, col]].copy()
    data.columns = ["date", "value"]
    data["value"] = pd.to_numeric(data["value"], errors="coerce")
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data = data.dropna(subset=["date", "value"])
    if data.empty:
        raise ValueError(f"no values for {series_id}")
    last = data.iloc[-1]
    return float(last["value"]), last["date"].date().isoformat()


def _field(url: str, cache_key: str, series: str, source: str) -> FieldValue:
    try:
        value, as_of = _latest_for_series(fetch_bytes(url, cache_key=cache_key), series)
        return FieldValue(value=to_float(value), as_of=as_of, source=source)
    except Exception as exc:  # noqa: BLE001
        return FieldValue(error=f"{type(exc).__name__}: {exc}")


def fetch() -> NormalizedRate:
    return NormalizedRate(
        currency=CURRENCY,
        central_bank=CENTRAL_BANK,
        policy=_field(F1_URL, "aud_f1", CASH_RATE_SERIES, "RBA F1 (Cash Rate Target)"),
        y2=_field(F2_URL, "aud_f2", Y2_SERIES, "RBA F2 (AGB 2Y)"),
        y10=_field(F2_URL, "aud_f2", Y10_SERIES, "RBA F2 (AGB 10Y)"),
    )
