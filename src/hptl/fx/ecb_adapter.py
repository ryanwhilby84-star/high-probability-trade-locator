"""EUR currency-rate adapter — ECB Data Portal (first-party).

Fields
------
* Policy rate -> ECB Deposit Facility Rate (``FM.B.U2.EUR.4F.KR.DFR.LEV``).
  This is a *step* series: the published ``as_of`` is the date of the last
  rate change and may be months old while remaining the current rate.
* 2Y / 10Y -> euro-area AAA government bond spot yield curve
  (``YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y`` / ``SR_10Y``).

Live pulls use ``lastNObservations=1`` into ``*_live`` cache keys so deep
history caches used by regression are not overwritten.
"""

from __future__ import annotations

import csv
import io

from hptl.fx.rate_adapter_base import (
    FieldValue,
    NormalizedRate,
    fetch_text,
    to_float,
)

CURRENCY = "EUR"
CENTRAL_BANK = "ECB"

_BASE = "https://data-api.ecb.europa.eu/service/data"
DFR_URL = f"{_BASE}/FM/B.U2.EUR.4F.KR.DFR.LEV?format=csvdata&lastNObservations=1"
Y2_URL = f"{_BASE}/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y?format=csvdata&lastNObservations=1"
Y10_URL = f"{_BASE}/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y?format=csvdata&lastNObservations=1"


def _last_obs(raw: str) -> tuple[float | None, str | None]:
    reader = csv.DictReader(io.StringIO(raw))
    rows = list(reader)
    if not rows:
        raise ValueError("empty ECB csvdata response")
    last = rows[-1]
    return to_float(last.get("OBS_VALUE")), (str(last.get("TIME_PERIOD"))[:10] or None)


def _fetch_field(url: str, cache_key: str, source: str) -> FieldValue:
    try:
        raw = fetch_text(url, cache_key=cache_key)
        value, as_of = _last_obs(raw)
        return FieldValue(value=value, as_of=as_of, source=source)
    except Exception as exc:  # noqa: BLE001
        return FieldValue(error=f"{type(exc).__name__}: {exc}")


def fetch() -> NormalizedRate:
    return NormalizedRate(
        currency=CURRENCY,
        central_bank=CENTRAL_BANK,
        policy=_fetch_field(DFR_URL, "eur_dfr_live", "ECB (Deposit Facility Rate)"),
        y2=_fetch_field(Y2_URL, "eur_2y_live", "ECB (euro-area AAA gov curve, 2Y)"),
        y10=_fetch_field(Y10_URL, "eur_10y_live", "ECB (euro-area AAA gov curve, 10Y)"),
    )

