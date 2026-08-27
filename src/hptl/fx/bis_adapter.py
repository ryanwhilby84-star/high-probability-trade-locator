"""Central-bank policy rates — BIS WS_CBPOL dataset (official, daily).

The Bank for International Settlements publishes a daily "Central bank policy
rates" dataset (``WS_CBPOL``) compiled **directly from each central bank** (the
records carry ``SOURCE_REF`` = the issuing central bank, e.g. "Reserve Bank of
New Zealand"). BIS is the central banks' own institution, not a commercial
aggregator, so it is an acceptable first-party-grade source for policy rates
that have no clean native machine endpoint (BoJ, SNB, RBNZ).

This adapter is shared: currency adapters call :func:`policy_field` for the
policy leg when their own central bank does not expose a usable feed.
"""

from __future__ import annotations

import csv
import io

from hptl.fx.rate_adapter_base import FieldValue, fetch_text, to_float

# Currency -> BIS REF_AREA code (euro area = XM).
BIS_REF_AREA: dict[str, str] = {
    "USD": "US",
    "EUR": "XM",
    "GBP": "GB",
    "JPY": "JP",
    "AUD": "AU",
    "NZD": "NZ",
    "CAD": "CA",
    "CHF": "CH",
}

_URL = "https://stats.bis.org/api/v1/data/WS_CBPOL/D.{ref}/all?lastNObservations=1&format=csv"


def _parse(raw: str) -> tuple[float | None, str | None, str | None]:
    reader = csv.DictReader(io.StringIO(raw))
    rows = list(reader)
    if not rows:
        raise ValueError("empty BIS WS_CBPOL response")
    last = rows[-1]
    value = to_float(last.get("OBS_VALUE"))
    as_of = (str(last.get("TIME_PERIOD"))[:10] or None)
    source_ref = (last.get("SOURCE_REF") or "").strip() or None
    return value, as_of, source_ref


def policy_field(currency: str) -> FieldValue:
    """Return the policy-rate :class:`FieldValue` for ``currency`` from BIS."""
    ref = BIS_REF_AREA.get(currency.upper())
    if not ref:
        return FieldValue(error=f"no BIS REF_AREA mapping for {currency}")
    try:
        value, as_of, source_ref = _parse(fetch_text(_URL.format(ref=ref), cache_key=f"bis_cbpol_{ref.lower()}"))
        label = f"BIS WS_CBPOL ({source_ref})" if source_ref else "BIS WS_CBPOL (central bank policy rate)"
        return FieldValue(value=value, as_of=as_of, source=label)
    except Exception as exc:  # noqa: BLE001
        return FieldValue(error=f"{type(exc).__name__}: {exc}")
