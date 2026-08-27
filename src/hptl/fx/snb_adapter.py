"""Swiss National Bank currency-rate adapter — SNB data portal (first-party)."""

from __future__ import annotations

from datetime import date, datetime

from hptl.fx import bis_adapter
from hptl.fx.rate_adapter_base import (
    FieldValue,
    NormalizedRate,
    fetch_bytes,
    to_float,
)

CURRENCY = "CHF"
CENTRAL_BANK = "Swiss National Bank"
YIELDS_URL = "https://data.snb.ch/api/cube/rendoblid/data/csv/en"
SOURCE = "SNB (Confederation bond yields, rendoblid)"
MAX_STALE_DAYS = 10

_MAT_2Y = {"2J"}
_MAT_10Y = {"10J0", "10J"}


def _is_stale(as_of: str | None) -> bool:
    if not as_of:
        return True
    try:
        d = datetime.strptime(str(as_of)[:10], "%Y-%m-%d").date()
    except ValueError:
        return True
    return (date.today() - d).days > MAX_STALE_DAYS


def _parse_yields(content: bytes) -> dict[str, FieldValue]:
    text = content.decode("utf-8-sig", errors="replace")
    latest: dict[str, tuple[str, float]] = {}
    for line in text.splitlines():
        parts = line.split(";")
        if len(parts) != 3:
            continue
        d = parts[0].strip('"').strip()
        mat = parts[1].strip('"').strip()
        raw_val = parts[2].strip('"').strip()
        if not raw_val or not d[:4].isdigit():
            continue
        val = to_float(raw_val)
        if val is None:
            continue
        key = "y2" if mat in _MAT_2Y else "y10" if mat in _MAT_10Y else None
        if key is None:
            continue
        prev = latest.get(key)
        if prev is None or d > prev[0]:
            latest[key] = (d, val)

    out: dict[str, FieldValue] = {}
    for key, label in (("y2", "2Y"), ("y10", "10Y")):
        if key in latest:
            d, val = latest[key]
            out[key] = FieldValue(value=val, as_of=d, source=f"{SOURCE} {label}")
        else:
            out[key] = FieldValue(error=f"no {label} observation in rendoblid cube")
    return out


def _fetch_yields() -> dict[str, FieldValue]:
    try:
        return _parse_yields(fetch_bytes(YIELDS_URL, cache_key="chf_rendoblid"))
    except Exception as exc:  # noqa: BLE001
        err = f"{type(exc).__name__}: {exc}"
        return {"y2": FieldValue(error=err), "y10": FieldValue(error=err)}


def fetch() -> NormalizedRate:
    yields = _fetch_yields()
    y2 = yields["y2"]
    y10 = yields["y10"]
    notes = [
        "Policy rate via BIS WS_CBPOL (sourced from the SNB).",
        "2Y/10Y from SNB rendoblid daily cube only — OECD monthly fallbacks are not used for live valuation.",
    ]
    if y2.as_of and _is_stale(y2.as_of):
        notes.append(f"SNB rendoblid last observation {y2.as_of}; CHF yields marked stale.")
    return NormalizedRate(
        currency=CURRENCY,
        central_bank=CENTRAL_BANK,
        policy=bis_adapter.policy_field(CURRENCY),
        y2=y2,
        y10=y10,
        notes=notes,
    )

