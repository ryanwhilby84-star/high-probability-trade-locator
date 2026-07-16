"""Minimal EIA Open Data API v2 client.

Requires EIA_API_KEY. Never hardcodes credentials.
Retains caller responsibility for cache retention on failure.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

EIA_BASE = "https://api.eia.gov/v2"
DEFAULT_TIMEOUT = 45


class EiaApiKeyMissing(RuntimeError):
    """Raised when EIA_API_KEY is not configured."""


def get_eia_api_key() -> str:
    from hptl.data_sources.env_loader import load_project_dotenv

    load_project_dotenv(keys=("EIA_API_KEY",))
    key = (os.environ.get("EIA_API_KEY") or "").strip()
    if not key:
        raise EiaApiKeyMissing(
            "EIA_API_KEY is required for EIA Open Data. "
            "Set EIA_API_KEY in the project .env file."
        )
    return key


def fetch_seriesid(series_id: str, *, length: int = 5000, timeout: int = DEFAULT_TIMEOUT) -> dict[str, Any]:
    """Fetch a legacy series via /v2/seriesid/{id} with pagination."""
    api_key = get_eia_api_key()
    offset = 0
    all_rows: list[dict[str, Any]] = []
    meta: dict[str, Any] = {}

    while True:
        params = urllib.parse.urlencode(
            {
                "api_key": api_key,
                "out": "json",
                "length": str(min(5000, length - offset) if length else 5000),
                "offset": str(offset),
            }
        )
        url = f"{EIA_BASE}/seriesid/{urllib.parse.quote(series_id, safe='')}?{params}"
        req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "HPTL/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")[:400]
            raise RuntimeError(f"EIA HTTP {exc.code} for {series_id}: {body}") from exc

        response = payload.get("response") or payload
        if not meta:
            meta = {
                "series_id": series_id,
                "description": response.get("description") or response.get("name"),
                "units": response.get("units") or response.get("unit"),
                "frequency": response.get("frequency"),
            }
        rows = response.get("data") or []
        if not isinstance(rows, list) or not rows:
            break
        all_rows.extend(rows)
        if len(rows) < 5000 or (length and len(all_rows) >= length):
            break
        offset += len(rows)

    return {"meta": meta, "data": all_rows, "raw_keys": list((all_rows[0] or {}).keys()) if all_rows else []}


def observations_from_seriesid(series_id: str, *, value_field: str | None = None) -> list[dict[str, Any]]:
    """Normalise EIA seriesid rows to [{date, value}, ...]."""
    doc = fetch_seriesid(series_id)
    rows = doc["data"]
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        period = row.get("period") or row.get("date") or row.get("periodStart")
        if period is None:
            continue
        date = str(period)[:10]
        # Monthly periods arrive as YYYY-MM
        if len(date) == 7 and date[4] == "-":
            date = f"{date}-01"
        val = None
        if value_field and value_field in row:
            val = row.get(value_field)
        else:
            for key in ("value", "value-data", "value_data"):
                if key in row and row[key] not in (None, "", "null"):
                    val = row[key]
                    break
            if val is None:
                # pick first numeric non-period field
                for k, v in row.items():
                    if k in {"period", "date", "periodStart", "duoarea", "product", "process", "series"}:
                        continue
                    try:
                        val = float(v)
                        break
                    except (TypeError, ValueError):
                        continue
        try:
            f = float(val)
        except (TypeError, ValueError):
            continue
        if f != f:  # NaN
            continue
        out.append({"date": date, "value": f})
    out.sort(key=lambda r: r["date"])
    return out
