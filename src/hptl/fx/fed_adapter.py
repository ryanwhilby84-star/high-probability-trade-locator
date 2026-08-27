"""USD currency-rate adapter — Federal Reserve / US Treasury (first-party).

Fields
------
* Policy rate -> effective Fed Funds Rate (EFFR) from the New York Fed markets
  API. This is the realised policy rate, published every business day by the
  Fed itself (no key, no aggregator).
* 2Y / 10Y -> US Treasury daily par yield curve CSV (Treasury's own data
  centre). The CSV exposes constant-maturity yields including ``2 Yr`` /
  ``10 Yr``.

FRED (``DGS2`` / ``DGS10`` / ``DFF``) is the documented fallback but is slower
and frequently rate-limited; the two endpoints above are the primary feed.
"""

from __future__ import annotations

import csv
import io
import json
from datetime import date

from hptl.fx.rate_adapter_base import (
    FieldValue,
    NormalizedRate,
    fetch_text,
    to_float,
)

CURRENCY = "USD"
CENTRAL_BANK = "Federal Reserve"

EFFR_URL = "https://markets.newyorkfed.org/api/rates/unsecured/effr/last/1.json"
TREASURY_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
    "daily-treasury-rates.csv/{year}/all"
    "?type=daily_treasury_yield_curve&field_tdr_date_value={year}&page&_format=csv"
)


def _fetch_policy() -> FieldValue:
    try:
        raw = fetch_text(EFFR_URL, cache_key="usd_effr")
        data = json.loads(raw)
        ref = (data.get("refRates") or [])[0]
        return FieldValue(
            value=to_float(ref.get("percentRate")),
            as_of=str(ref.get("effectiveDate"))[:10] or None,
            source="Federal Reserve (NY Fed EFFR)",
        )
    except Exception as exc:  # noqa: BLE001
        return FieldValue(error=f"{type(exc).__name__}: {exc}")


def _iso_date(mdy: str | None) -> str | None:
    if not mdy:
        return None
    try:
        m, d, y = mdy.split("/")
        return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    except ValueError:
        return None


def _parse_treasury(raw: str) -> dict[str, FieldValue]:
    reader = csv.DictReader(io.StringIO(raw))
    rows = list(reader)
    if not rows:
        raise ValueError("empty Treasury CSV")
    # The Treasury CSV is ordered newest-first; pick the row with the max date.
    rows = [r for r in rows if _iso_date(r.get("Date"))]
    last = max(rows, key=lambda r: _iso_date(r.get("Date")) or "")
    iso = _iso_date(last.get("Date"))
    src = "US Treasury (daily par yield curve)"
    return {
        "y2": FieldValue(value=to_float(last.get("2 Yr")), as_of=iso, source=src),
        "y10": FieldValue(value=to_float(last.get("10 Yr")), as_of=iso, source=src),
    }


def _fetch_yields() -> dict[str, FieldValue]:
    year = date.today().year
    try:
        raw = fetch_text(TREASURY_URL.format(year=year), cache_key="usd_treasury")
        parsed = _parse_treasury(raw)
        if parsed["y2"].value is None and parsed["y10"].value is None:
            raise ValueError("no current-year Treasury rows")
        return parsed
    except Exception as exc:  # noqa: BLE001
        err = f"{type(exc).__name__}: {exc}"
        return {"y2": FieldValue(error=err), "y10": FieldValue(error=err)}


def fetch() -> NormalizedRate:
    yields = _fetch_yields()
    return NormalizedRate(
        currency=CURRENCY,
        central_bank=CENTRAL_BANK,
        policy=_fetch_policy(),
        y2=yields["y2"],
        y10=yields["y10"],
    )
