"""NOAA Climate Data Online (CDO) API v2 client.

Requires NOAA_API_TOKEN in the environment / project .env.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from hptl.data_sources.env_loader import load_project_dotenv

NOAA_BASE = "https://www.ncei.noaa.gov/cdo-web/api/v2"
DEFAULT_TIMEOUT = 60


class NoaaApiTokenMissing(RuntimeError):
    """Raised when NOAA_API_TOKEN is not configured."""


def get_noaa_api_token() -> str:
    load_project_dotenv(keys=("NOAA_API_TOKEN",))
    token = (os.environ.get("NOAA_API_TOKEN") or "").strip()
    if not token:
        raise NoaaApiTokenMissing(
            "NOAA_API_TOKEN is required for NOAA CDO degree-day ingest. "
            "Set NOAA_API_TOKEN in the project .env file."
        )
    return token


def _get(path: str, params: dict[str, Any], *, timeout: int = DEFAULT_TIMEOUT) -> dict[str, Any]:
    token = get_noaa_api_token()
    qs = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    url = f"{NOAA_BASE}{path}?{qs}"
    req = urllib.request.Request(
        url,
        headers={
            "token": token,
            "Accept": "application/json",
            "User-Agent": "HPTL/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:400]
        raise RuntimeError(f"NOAA HTTP {exc.code}: {body}") from exc


def fetch_degree_days(
    datatype_id: str,
    *,
    start: str = "2000-01-01",
    end: str | None = None,
    dataset_id: str = "NCLIMDIV",
    location_id: str = "CONTUS",
) -> list[dict[str, Any]]:
    """Fetch national Contiguous US HDD/CDD from NOAA NCLIMDIV.

    ``datatype_id`` is ``HDD`` or ``CDD``.
    Returns [{date, value}, ...] sorted ascending.
    """
    from datetime import date as date_cls

    if end is None:
        end = date_cls.today().isoformat()

    out: list[dict[str, Any]] = []
    offset = 1
    limit = 1000
    while True:
        payload = _get(
            "/data",
            {
                "datasetid": dataset_id,
                "datatypeid": datatype_id,
                "locationid": location_id,
                "startdate": start,
                "enddate": end,
                "units": "standard",
                "limit": limit,
                "offset": offset,
            },
        )
        rows = payload.get("results") or []
        if not rows:
            break
        for row in rows:
            d = str(row.get("date") or "")[:10]
            try:
                v = float(row.get("value"))
            except (TypeError, ValueError):
                continue
            if d and v == v:
                out.append({"date": d, "value": v})
        meta = payload.get("metadata") or {}
        resultset = meta.get("resultset") or {}
        count = int(resultset.get("count") or 0)
        if offset + limit > count or len(rows) < limit:
            break
        offset += limit

    # Aggregate duplicate dates (multiple stations/divisions) by mean
    by_date: dict[str, list[float]] = {}
    for row in out:
        by_date.setdefault(row["date"], []).append(row["value"])
    merged = [
        {"date": d, "value": sum(vs) / len(vs)}
        for d, vs in sorted(by_date.items())
    ]
    return merged
