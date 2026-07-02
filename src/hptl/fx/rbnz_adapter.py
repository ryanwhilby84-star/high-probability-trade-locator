"""NZD currency-rate adapter — Reserve Bank of New Zealand (first-party).

Fields
------
* Policy rate -> Official Cash Rate. Preferred from RBNZ table B2; falls back
  to the BIS ``WS_CBPOL`` dataset (sourced directly from the RBNZ) when B2 is
  unreachable, so the policy leg is always live.
* 2Y / 10Y -> New Zealand government bond closing yields from RBNZ table **B2
  (Wholesale interest rates, daily)** — columns "2 year" / "10 year".

Access reality
--------------
RBNZ fronts its statistics downloads with Cloudflare/Akamai, which returns
HTTP 403 to datacenter IPs. The established NZ tooling (CRAN ``RBNZ`` package)
works around this only via **IP whitelisting** or a **manual file download**.
This adapter therefore:

1. Attempts the live B2 download (works from residential / whitelisted IPs).
2. Falls back to a manually-placed workbook at
   ``data/manual/rbnz_b2_daily.xlsx`` (download once from the RBNZ B2 page and
   drop it there; it is then parsed exactly like the live file).
3. Falls back to BIS for the policy rate only.

If B2 is unavailable by both routes, the 2Y/10Y stay missing (audit -> FAIL on
yields) — there is no alternative free first-party daily NZ yield feed.
"""

from __future__ import annotations

import io
import re

import pandas as pd

from hptl.config import DATA_DIR
from hptl.fx import bis_adapter
from hptl.fx.rate_adapter_base import (
    BROWSER_HEADERS,
    FieldValue,
    NormalizedRate,
    fetch_bytes,
    offline_mode,
    to_float,
)

CURRENCY = "NZD"
CENTRAL_BANK = "Reserve Bank of New Zealand"

B2_URL = "https://www.rbnz.govt.nz/-/media/project/sites/rbnz/files/statistics/series/b/b2/b2-daily.xlsx"
MANUAL_PATH = DATA_DIR / "manual" / "rbnz_b2_daily.xlsx"
SOURCE = "RBNZ B2 (wholesale interest rates)"

# Column header label -> normalized field.
_OCR_PAT = re.compile(r"official cash rate|\bocr\b", re.I)
_2Y_PAT = re.compile(r"^\s*2\s*year", re.I)
_10Y_PAT = re.compile(r"^\s*10\s*year", re.I)


def _b2_bytes() -> tuple[bytes, str]:
    """Return (xlsx bytes, source-detail). Live first, then manual file."""
    if not offline_mode():
        try:
            content = fetch_bytes(B2_URL, cache_key="nzd_b2", headers=BROWSER_HEADERS)
            if content[:2] == b"PK":
                return content, "live RBNZ download"
        except Exception:  # noqa: BLE001 - fall through to manual
            pass
    if MANUAL_PATH.exists():
        return MANUAL_PATH.read_bytes(), f"manual file {MANUAL_PATH.name}"
    raise FileNotFoundError(
        "RBNZ B2 unavailable: live download blocked (Cloudflare) and no manual "
        f"file at {MANUAL_PATH}"
    )


def _parse_b2_series(content: bytes) -> dict[str, dict[str, float]]:
    """Parse full B2 daily 2Y/10Y history from workbook bytes."""
    xl = pd.ExcelFile(io.BytesIO(content))
    for sheet in xl.sheet_names:
        df = xl.parse(sheet, header=None)
        hdr_idx = None
        for i in range(min(25, len(df))):
            cells = [str(x) for x in df.iloc[i].tolist()]
            if any(_2Y_PAT.search(c) for c in cells) and any(_10Y_PAT.search(c) for c in cells):
                hdr_idx = i
                break
        if hdr_idx is None:
            continue
        cells = [str(x) for x in df.iloc[hdr_idx].tolist()]

        def find(pat: re.Pattern[str]) -> int | None:
            for j, c in enumerate(cells):
                if pat.search(c):
                    return j
            return None

        c2, c10 = find(_2Y_PAT), find(_10Y_PAT)
        if c2 is None or c10 is None:
            continue
        data = df.iloc[hdr_idx + 1 :].copy()
        data["_date"] = pd.to_datetime(data.iloc[:, 0], errors="coerce", dayfirst=True)
        data = data.dropna(subset=["_date"]).sort_values("_date")
        y2: dict[str, float] = {}
        y10: dict[str, float] = {}
        for _, row in data.iterrows():
            iso = row["_date"].date().isoformat()
            v2 = to_float(row.iloc[c2])
            v10 = to_float(row.iloc[c10])
            if v2 is not None:
                y2[iso] = float(v2)
            if v10 is not None:
                y10[iso] = float(v10)
        if y2 or y10:
            return {"y2": y2, "y10": y10}
    return {"y2": {}, "y10": {}}


def _parse_b2(content: bytes) -> dict[str, FieldValue]:
    xl = pd.ExcelFile(io.BytesIO(content))
    for sheet in xl.sheet_names:
        df = xl.parse(sheet, header=None)
        # Find a header row that contains the 2-year and 10-year yield labels.
        hdr_idx = None
        for i in range(min(25, len(df))):
            cells = [str(x) for x in df.iloc[i].tolist()]
            if any(_2Y_PAT.search(c) for c in cells) and any(_10Y_PAT.search(c) for c in cells):
                hdr_idx = i
                break
        if hdr_idx is None:
            continue

        cells = [str(x) for x in df.iloc[hdr_idx].tolist()]

        def find(pat: re.Pattern[str]) -> int | None:
            for j, c in enumerate(cells):
                if pat.search(c):
                    return j
            return None

        c2, c10, cocr = find(_2Y_PAT), find(_10Y_PAT), find(_OCR_PAT)
        data = df.iloc[hdr_idx + 1 :].copy()
        data["_date"] = pd.to_datetime(data.iloc[:, 0], errors="coerce", dayfirst=True)
        data = data.dropna(subset=["_date"]).sort_values("_date")
        if data.empty:
            continue
        last = data.iloc[-1]
        as_of = last["_date"].date().isoformat()

        out: dict[str, FieldValue] = {
            "y2": FieldValue(to_float(last.iloc[c2]) if c2 is not None else None, as_of, f"{SOURCE} 2Y"),
            "y10": FieldValue(to_float(last.iloc[c10]) if c10 is not None else None, as_of, f"{SOURCE} 10Y"),
        }
        if cocr is not None:
            ocr = to_float(last.iloc[cocr])
            if ocr is not None:
                out["policy"] = FieldValue(ocr, as_of, f"{SOURCE} OCR")
        return out
    raise ValueError("no B2 sheet with 2-year/10-year yield columns")


def fetch() -> NormalizedRate:
    notes: list[str] = []
    parsed: dict[str, FieldValue] = {}
    try:
        content, detail = _b2_bytes()
        parsed = _parse_b2(content)
        notes.append(f"B2 obtained via {detail}")
    except Exception as exc:  # noqa: BLE001
        notes.append(f"RBNZ B2 unavailable: {type(exc).__name__}: {exc}")

    # Policy: prefer B2 OCR; otherwise BIS (sourced from RBNZ).
    policy = parsed.get("policy")
    if policy is None or policy.value is None:
        policy = bis_adapter.policy_field(CURRENCY)
        notes.append("Policy rate via BIS WS_CBPOL (sourced from the RBNZ).")

    y2 = parsed.get("y2") or FieldValue(error="DATA_STALE — RBNZ B2 daily source missing")
    y10 = parsed.get("y10") or FieldValue(error="DATA_STALE — RBNZ B2 daily source missing")

    return NormalizedRate(
        currency=CURRENCY,
        central_bank=CENTRAL_BANK,
        policy=policy,
        y2=y2,
        y10=y10,
        notes=notes,
    )
