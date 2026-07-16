"""Natural Gas institutional driver ingestion -> data/cache/energy_drivers/.

Sources:
  - EIA Open Data API v2 (EIA_API_KEY): storage, dry production, LNG exports
  - NOAA CDO API v2 (NOAA_API_TOKEN): HDD / CDD

Never fabricates observations. On failure, retains the last verified cache.
"""

from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.data_sources.cpc_degree_days import fetch_cpc_degree_days
from hptl.data_sources.eia_client import EiaApiKeyMissing, observations_from_seriesid
from hptl.data_sources.eia_public_xls import EIA_XLS, fetch_eia_xls_observations
from hptl.data_sources.env_loader import load_project_dotenv
from hptl.data_sources.noaa_client import NoaaApiTokenMissing, fetch_degree_days

CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "energy_drivers"

EIA_WORKING_GAS_L48 = "NW2_EPG0_SWO_R48_BCF_W"
EIA_DRY_GAS_PROD = "N9070US2"  # Dry Natural Gas Production (not gross withdrawals)
EIA_LNG_EXPORTS = "N9133US2"  # Liquefied U.S. Natural Gas Exports

EIA_DRIVERS: dict[str, dict[str, Any]] = {
    "working_gas_storage": {
        "cache_file": "eia_working_gas_storage.json",
        "driver_name": "Working Gas Storage (Lower 48)",
        "eia_series_id": EIA_WORKING_GAS_L48,
        "units": "Bcf",
        "frequency": "weekly",
        "concept": "Total working gas in underground storage, Lower 48",
        "unit_scale": 1.0,
        "api_key": "EIA_API_KEY",
    },
    "dry_gas_production": {
        "cache_file": "eia_dry_gas_production.json",
        "driver_name": "US Dry Natural Gas Production",
        "eia_series_id": EIA_DRY_GAS_PROD,
        "units": "Bcf/d",
        "frequency": "monthly",
        "concept": "U.S. dry natural gas production (not oil&gas IP proxy)",
        "unit_scale": 1.0 / 1000.0 / 30.437,
        "api_key": "EIA_API_KEY",
    },
    "lng_exports": {
        "cache_file": "eia_lng_exports.json",
        "driver_name": "US LNG Exports",
        "eia_series_id": EIA_LNG_EXPORTS,
        "units": "Bcf/d",
        "frequency": "monthly",
        "concept": "U.S. liquefied natural gas exports (not feedgas)",
        "unit_scale": 1.0 / 1000.0 / 30.437,
        "api_key": "EIA_API_KEY",
    },
}

WEATHER_DRIVERS: dict[str, dict[str, Any]] = {
    "hdd": {
        "cache_file": "noaa_hdd.json",
        "driver_name": "Heating Degree Days (Contiguous US)",
        "datatype_id": "HDD",
        "units": "degree-days",
        "frequency": "monthly",
        "concept": "Contiguous US heating degree days (actuals, NCLIMDIV)",
        "api_key": "NOAA_API_TOKEN",
    },
    "cdd": {
        "cache_file": "noaa_cdd.json",
        "driver_name": "Cooling Degree Days (Contiguous US)",
        "datatype_id": "CDD",
        "units": "degree-days",
        "frequency": "monthly",
        "concept": "Contiguous US cooling degree days (actuals, NCLIMDIV)",
        "api_key": "NOAA_API_TOKEN",
    },
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _num(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _validate_obs(rows: list[dict[str, Any]], *, min_points: int = 12) -> tuple[list[dict[str, Any]], list[str]]:
    flags: list[str] = []
    clean: list[dict[str, Any]] = []
    prev: float | None = None
    for row in rows:
        d = str(row.get("date") or "")[:10]
        v = _num(row.get("value"))
        if len(d) < 10 or v is None:
            continue
        if v < 0:
            flags.append(f"reject_negative:{d}")
            continue
        # Soft spike flag only — do not drop (LNG/production can step-change).
        if prev is not None and prev > 0 and abs(v - prev) / prev > 8.0:
            flags.append(f"spike_flag:{d}")
        clean.append({"date": d, "value": v})
        prev = v
    if len(clean) < min_points:
        flags.append(f"insufficient_points:{len(clean)}")
    return clean, flags


def _load_existing(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_cache(path: Path, doc: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2), encoding="utf-8")


def _retain_or_unavailable(
    *,
    path: Path,
    previous: dict[str, Any] | None,
    spec: dict[str, Any],
    errors: list[str],
    flags: list[str],
    series_id: str | None,
    source_url: str | None,
    result: dict[str, Any],
) -> dict[str, Any]:
    err_text = "; ".join([e for e in errors + flags if e]) or "no observations"

    if previous and (previous.get("series") or previous.get("observations")):
        previous = dict(previous)
        previous["status"] = "STALE"
        previous["last_attempt"] = _now_iso()
        previous["last_error"] = err_text
        _write_cache(path, previous)
        result.update(
            {
                "status": "STALE",
                "retained_previous_cache": True,
                "error": err_text,
                "n_observations": len(previous.get("series") or previous.get("observations") or []),
                "latest_date": previous.get("latest_observation_date"),
                "latest_value": previous.get("latest_value"),
                "source": previous.get("official_source"),
                "series_id": previous.get("series_identifier"),
            }
        )
        return result

    doc = {
        "driver_name": spec["driver_name"],
        "official_source": None,
        "source_url": source_url,
        "series_identifier": series_id,
        "concept": spec.get("concept"),
        "units": spec.get("units"),
        "frequency": spec.get("frequency"),
        "observations": [],
        "series": [],
        "latest_observation_date": None,
        "latest_value": None,
        "last_successful_refresh": None,
        "last_attempt": _now_iso(),
        "status": "UNAVAILABLE",
        "validation": {"flags": flags, "errors": errors},
        "api_key_required": spec.get("api_key"),
        "last_error": err_text,
    }
    _write_cache(path, doc)
    result.update(
        {
            "status": "UNAVAILABLE",
            "error": err_text,
            "n_observations": 0,
            "retained_previous_cache": False,
        }
    )
    return result


def _write_live(
    *,
    path: Path,
    spec: dict[str, Any],
    clean: list[dict[str, Any]],
    source_name: str,
    series_id: str,
    source_url: str,
    flags: list[str],
    result: dict[str, Any],
) -> dict[str, Any]:
    latest = clean[-1]
    doc = {
        "driver_name": spec["driver_name"],
        "official_source": source_name,
        "source_url": source_url,
        "dataset_identifier": series_id,
        "series_identifier": series_id,
        "concept": spec.get("concept"),
        "units": spec.get("units"),
        "frequency": spec.get("frequency"),
        "observations": clean,
        "series": clean,
        "latest_observation_date": latest["date"],
        "latest_value": latest["value"],
        "last_successful_refresh": _now_iso(),
        "last_attempt": _now_iso(),
        "status": "LIVE",
        "validation": {
            "n": len(clean),
            "flags": flags,
            "min_date": clean[0]["date"],
            "max_date": latest["date"],
        },
    }
    _write_cache(path, doc)
    result.update(
        {
            "status": "LIVE",
            "source": source_name,
            "series_id": series_id,
            "n_observations": len(clean),
            "latest_date": latest["date"],
            "latest_value": latest["value"],
            "error": None,
            "retained_previous_cache": False,
        }
    )
    return result


def ingest_eia_driver(driver_key: str) -> dict[str, Any]:
    spec = EIA_DRIVERS[driver_key]
    path = CACHE_DIR / spec["cache_file"]
    previous = _load_existing(path)
    result: dict[str, Any] = {
        "driver": driver_key,
        "driver_name": spec["driver_name"],
        "cache_path": str(path.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "status": "ERROR",
        "source": None,
        "series_id": None,
        "n_observations": 0,
        "latest_date": None,
        "latest_value": None,
        "error": None,
        "retained_previous_cache": False,
    }

    errors: list[str] = []
    rows: list[dict[str, Any]] = []
    series_id = spec["eia_series_id"]
    source_url = f"https://api.eia.gov/v2/seriesid/{series_id}"
    source_name = "EIA Open Data API v2"

    # 1) API when key present
    try:
        raw = observations_from_seriesid(series_id)
        scale = float(spec["unit_scale"])
        for r in raw:
            v = _num(r["value"])
            if v is None:
                continue
            rows.append({"date": r["date"], "value": v * scale})
    except EiaApiKeyMissing:
        pass  # fall through to official public hist_xls
    except Exception as exc:  # noqa: BLE001
        errors.append(f"EIA API fetch failed: {exc}")

    # 2) Official public EIA hist_xls (no key)
    if len(rows) < 12 and driver_key in EIA_XLS:
        try:
            xrows, xmeta = fetch_eia_xls_observations(driver_key)
            if len(xrows) >= 12:
                rows = xrows
                source_name = xmeta["official_source"]
                series_id = xmeta["series_identifier"]
                source_url = xmeta["source_url"]
        except Exception as exc:  # noqa: BLE001
            errors.append(f"EIA hist_xls fetch failed: {exc}")

    clean, flags = _validate_obs(rows, min_points=12) if rows else ([], ["no_rows"])
    if len(clean) < 12:
        return _retain_or_unavailable(
            path=path,
            previous=previous,
            spec=spec,
            errors=errors or ["EIA data unavailable"],
            flags=flags,
            series_id=series_id,
            source_url=source_url,
            result=result,
        )

    return _write_live(
        path=path,
        spec=spec,
        clean=clean,
        source_name=source_name,
        series_id=series_id,
        source_url=source_url,
        flags=flags,
        result=result,
    )


def ingest_weather_driver(driver_key: str) -> dict[str, Any]:
    spec = WEATHER_DRIVERS[driver_key]
    path = CACHE_DIR / spec["cache_file"]
    previous = _load_existing(path)
    result: dict[str, Any] = {
        "driver": driver_key,
        "driver_name": spec["driver_name"],
        "cache_path": str(path.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "status": "ERROR",
        "source": None,
        "series_id": None,
        "n_observations": 0,
        "latest_date": None,
        "latest_value": None,
        "error": None,
        "retained_previous_cache": False,
    }

    errors: list[str] = []
    rows: list[dict[str, Any]] = []
    series_id = f"NCLIMDIV/{spec['datatype_id']}/CONTUS"
    source_url = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
    source_name = "NOAA CDO NCLIMDIV"

    try:
        rows = fetch_degree_days(spec["datatype_id"])
    except NoaaApiTokenMissing:
        pass  # fall through to CPC public FTP
    except Exception as exc:  # noqa: BLE001
        errors.append(f"NOAA CDO fetch failed: {exc}")

    # Official public CPC population-weighted degree days (no token)
    if len(rows) < 12:
        try:
            wrows, wmeta = fetch_cpc_degree_days(driver_key, start_year=2000)
            if len(wrows) >= 12:
                rows = wrows
                source_name = wmeta["official_source"]
                series_id = wmeta["series_identifier"]
                source_url = wmeta["source_url"]
        except Exception as exc:  # noqa: BLE001
            errors.append(f"CPC degree-day fetch failed: {exc}")

    clean, flags = _validate_obs(rows, min_points=12) if rows else ([], ["no_rows"])
    if len(clean) < 12:
        return _retain_or_unavailable(
            path=path,
            previous=previous,
            spec=spec,
            errors=errors or ["weather data unavailable"],
            flags=flags,
            series_id=series_id,
            source_url=source_url,
            result=result,
        )

    return _write_live(
        path=path,
        spec=spec,
        clean=clean,
        source_name=source_name,
        series_id=series_id,
        source_url=source_url,
        flags=flags,
        result=result,
    )


def ingest_all_ng_drivers() -> dict[str, Any]:
    present = load_project_dotenv(keys=("EIA_API_KEY", "NOAA_API_TOKEN", "FRED_API_KEY"))

    results: dict[str, Any] = {}
    for key in EIA_DRIVERS:
        results[key] = ingest_eia_driver(key)
    for key in WEATHER_DRIVERS:
        results[key] = ingest_weather_driver(key)

    # Keys optional when official public EIA hist_xls / CPC FTP succeed.
    required_keys: list[str] = []
    for key, row in results.items():
        if row.get("status") not in ("LIVE", "STALE"):
            if key in EIA_DRIVERS and not present.get("EIA_API_KEY"):
                if "EIA_API_KEY" not in required_keys:
                    required_keys.append("EIA_API_KEY")
            if key in WEATHER_DRIVERS and not present.get("NOAA_API_TOKEN"):
                if "NOAA_API_TOKEN" not in required_keys:
                    required_keys.append("NOAA_API_TOKEN")

    return {
        "generated_at": _now_iso(),
        "drivers": results,
        "live": sum(1 for r in results.values() if r["status"] == "LIVE"),
        "stale": sum(1 for r in results.values() if r["status"] == "STALE"),
        "unavailable": sum(1 for r in results.values() if r["status"] == "UNAVAILABLE"),
        "error": sum(1 for r in results.values() if r["status"] == "ERROR"),
        "required_keys": required_keys,
        "keys_present": present,
    }


def main() -> int:
    summary = ingest_all_ng_drivers()
    out = PROJECT_ROOT / "data" / "audits" / "energy_ng_driver_ingest_audit.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "live": summary["live"],
                "stale": summary["stale"],
                "unavailable": summary["unavailable"],
                "error": summary["error"],
                "required_keys": summary["required_keys"],
            },
            indent=2,
        )
    )
    for key, row in summary["drivers"].items():
        print(
            f"  {key}: status={row['status']} n={row['n_observations']} "
            f"latest={row['latest_date']} val={row['latest_value']} src={row['source']}"
        )
        if row.get("error"):
            print(f"    error: {row['error']}")
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
