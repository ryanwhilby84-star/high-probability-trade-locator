"""JPY currency-rate adapter — Japan Ministry of Finance / Bank of Japan.

Fields
------
* 2Y / 10Y -> JGB constant-maturity interest rates from the MoF daily CSV
  (``jgbcme.csv``), which lists 1Y..40Y yields per business day.
* Policy rate -> BoJ policy rate via the BIS ``WS_CBPOL`` dataset (sourced
  directly from the Bank of Japan). The BoJ does not expose its policy rate as
  a clean first-party CSV/JSON endpoint (it is announced in Monetary Policy
  Meeting statements), so the official BIS compilation is used. See
  :mod:`hptl.fx.bis_adapter`.
"""

from __future__ import annotations

import csv
import io

from hptl.fx import bis_adapter
from hptl.fx.rate_adapter_base import (
    FieldValue,
    NormalizedRate,
    fetch_text,
    to_float,
)

CURRENCY = "JPY"
CENTRAL_BANK = "Bank of Japan"

JGB_URL = "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/jgbcme.csv"
SOURCE = "Japan MoF (JGB constant-maturity yields)"


def _parse_jgb(raw: str) -> dict[str, FieldValue]:
    reader = csv.reader(io.StringIO(raw))
    header: list[str] | None = None
    rows: list[list[str]] = []
    for row in reader:
        if not row:
            continue
        first = (row[0] or "").strip()
        if header is None:
            if first == "Date":
                header = [c.strip() for c in row]
            continue
        if first and first[0].isdigit():
            rows.append(row)
    if header is None or not rows:
        raise ValueError("could not locate JGB data table")

    idx = {name: i for i, name in enumerate(header)}
    last = rows[-1]
    y, m, d = last[0].split("/")
    as_of = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"

    def col(label: str) -> float | None:
        i = idx.get(label)
        return to_float(last[i]) if i is not None and i < len(last) else None

    return {
        "y2": FieldValue(col("2Y"), as_of, f"{SOURCE} 2Y"),
        "y10": FieldValue(col("10Y"), as_of, f"{SOURCE} 10Y"),
    }


def _fetch_yields() -> dict[str, FieldValue]:
    try:
        return _parse_jgb(fetch_text(JGB_URL, cache_key="jpy_jgb"))
    except Exception as exc:  # noqa: BLE001
        err = f"{type(exc).__name__}: {exc}"
        return {"y2": FieldValue(error=err), "y10": FieldValue(error=err)}


def fetch() -> NormalizedRate:
    yields = _fetch_yields()
    return NormalizedRate(
        currency=CURRENCY,
        central_bank=CENTRAL_BANK,
        policy=bis_adapter.policy_field(CURRENCY),
        y2=yields["y2"],
        y10=yields["y10"],
        notes=[
            "Policy rate via BIS WS_CBPOL (sourced from the Bank of Japan); BoJ "
            "has no clean first-party machine endpoint for the policy rate.",
        ],
    )
