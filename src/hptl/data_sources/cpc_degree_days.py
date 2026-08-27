"""Official NOAA/CPC population-weighted degree-day daily files (no API token).

Source:
  https://ftp.cpc.ncep.noaa.gov/htdocs/degree_days/weighted/daily_data/{year}/Population.Heating.txt
  https://ftp.cpc.ncep.noaa.gov/htdocs/degree_days/weighted/daily_data/{year}/Population.Cooling.txt

File layout (pipe-delimited):
  Region|YYYYMMDD|YYYYMMDD|...
  CONUS|v1|v2|...
"""

from __future__ import annotations

import urllib.request
from datetime import datetime
from typing import Any

UA = {"User-Agent": "Mozilla/5.0 HPTL/1.0"}
BASE = "https://ftp.cpc.ncep.noaa.gov/htdocs/degree_days/weighted/daily_data"


def _fetch_text(url: str) -> str:
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=90) as resp:
        return resp.read().decode("utf-8", "replace")


def _parse_population_file(text: str) -> list[dict[str, Any]]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    header = None
    for ln in lines:
        if ln.startswith("Region|") or (ln.startswith("Region") and "|" in ln):
            header = ln.split("|")
            break
    if not header or len(header) < 3:
        return []

    dates: list[str] = []
    for cell in header[1:]:
        cell = cell.strip()
        if len(cell) == 8 and cell.isdigit():
            dates.append(f"{cell[0:4]}-{cell[4:6]}-{cell[6:8]}")
        else:
            dates.append("")

    conus = None
    for ln in lines:
        if ln.startswith("CONUS|") or ln.startswith("Contiguous"):
            conus = ln.split("|")
            break
    if not conus or len(conus) < 2:
        return []

    out: list[dict[str, Any]] = []
    for i, date in enumerate(dates):
        if not date:
            continue
        idx = i + 1
        if idx >= len(conus):
            break
        try:
            value = float(conus[idx])
        except ValueError:
            continue
        if value < -900:
            continue
        out.append({"date": date, "value": value})
    out.sort(key=lambda r: r["date"])
    return out


def _weekly_from_daily(daily: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, float] = {}
    for row in daily:
        try:
            dt = datetime.strptime(row["date"], "%Y-%m-%d")
        except ValueError:
            continue
        iso = dt.isocalendar()
        week_date = datetime.fromisocalendar(iso[0], iso[1], 4).strftime("%Y-%m-%d")
        buckets[week_date] = buckets.get(week_date, 0.0) + float(row["value"])
    return [{"date": d, "value": v} for d, v in sorted(buckets.items())]


def fetch_cpc_degree_days(
    kind: str, *, start_year: int = 2000, end_year: int | None = None
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if end_year is None:
        end_year = datetime.utcnow().year
    fname = "Population.Heating.txt" if kind == "hdd" else "Population.Cooling.txt"
    daily: list[dict[str, Any]] = []
    years_ok: list[int] = []
    errors: list[str] = []
    for year in range(start_year, end_year + 1):
        url = f"{BASE}/{year}/{fname}"
        try:
            text = _fetch_text(url)
            rows = _parse_population_file(text)
            if rows:
                daily.extend(rows)
                years_ok.append(year)
            else:
                errors.append(f"{year}:parse_empty")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{year}:{exc}")

    by_date = {r["date"]: r["value"] for r in daily}
    daily_u = [{"date": d, "value": v} for d, v in sorted(by_date.items())]
    weekly = _weekly_from_daily(daily_u)
    meta = {
        "official_source": "NOAA/CPC population-weighted degree days (public FTP)",
        "source_url": f"{BASE}/{{year}}/{fname}",
        "series_identifier": f"CPC/Population/{'Heating' if kind == 'hdd' else 'Cooling'}/CONUS",
        "years_loaded": years_ok,
        "parse_errors": errors[-5:],
        "n_daily": len(daily_u),
    }
    return weekly, meta
