"""CAD currency-rate adapter — Bank of Canada Valet API (first-party).

Fields
------
* Policy rate -> ``V39079`` (Target for the overnight rate).
* 2Y / 10Y -> ``BD.CDN.2YR.DQ.YLD`` / ``BD.CDN.10YR.DQ.YLD`` (Government of
  Canada benchmark bond yields).

A single Valet ``observations`` call returns all three with one timestamp.
"""

from __future__ import annotations

import json

from hptl.fx.rate_adapter_base import (
    FieldValue,
    NormalizedRate,
    fetch_text,
    to_float,
)

CURRENCY = "CAD"
CENTRAL_BANK = "Bank of Canada"

POLICY_SERIES = "V39079"
Y2_SERIES = "BD.CDN.2YR.DQ.YLD"
Y10_SERIES = "BD.CDN.10YR.DQ.YLD"
URL = (
    "https://www.bankofcanada.ca/valet/observations/"
    f"{Y2_SERIES},{Y10_SERIES},{POLICY_SERIES}/json?recent=1"
)
SOURCE = "Bank of Canada (Valet)"


def fetch() -> NormalizedRate:
    rate = NormalizedRate(currency=CURRENCY, central_bank=CENTRAL_BANK)
    try:
        data = json.loads(fetch_text(URL, cache_key="cad_valet"))
        obs = (data.get("observations") or [])
        if not obs:
            raise ValueError("no observations returned")
        latest = obs[-1]
        as_of = str(latest.get("d"))[:10] or None

        def cell(series: str) -> float | None:
            return to_float((latest.get(series) or {}).get("v"))

        rate.policy = FieldValue(cell(POLICY_SERIES), as_of, f"{SOURCE} {POLICY_SERIES}")
        rate.y2 = FieldValue(cell(Y2_SERIES), as_of, f"{SOURCE} 2Y benchmark")
        rate.y10 = FieldValue(cell(Y10_SERIES), as_of, f"{SOURCE} 10Y benchmark")
    except Exception as exc:  # noqa: BLE001
        err = f"{type(exc).__name__}: {exc}"
        rate.policy = FieldValue(error=err)
        rate.y2 = FieldValue(error=err)
        rate.y10 = FieldValue(error=err)
    return rate
