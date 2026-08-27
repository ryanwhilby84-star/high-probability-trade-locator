# Energy driver caches (Natural Gas)

Produced by:

```bash
python scripts/refresh_natural_gas_drivers.py
```

## Files

| File | Driver | Preferred source | Key |
|------|--------|------------------|-----|
| `eia_working_gas_storage.json` | Working gas L48 (Bcf, weekly) | EIA `NW2_EPG0_SWO_R48_BCF_W` | `EIA_API_KEY` |
| `eia_dry_gas_production.json` | Dry gas production (Bcf/d) | EIA `N9010US2` / FRED `NGPRODUSM` | `EIA_API_KEY` or `FRED_API_KEY` |
| `eia_lng_exports.json` | US LNG exports (Bcf/d) | EIA `NG_MOVE_EXP_NUS_MMCFM` | `EIA_API_KEY` (FRED fallback if series exists) |
| `noaa_hdd.json` | Heating degree days | NOAA | `NOAA_API_TOKEN` |
| `noaa_cdd.json` | Cooling degree days | NOAA | `NOAA_API_TOKEN` |

## Schema

```json
{
  "driver_name": "...",
  "official_source": "EIA Open Data API v2",
  "source_url": "https://api.eia.gov/v2/seriesid/...",
  "series_identifier": "NW2_EPG0_SWO_R48_BCF_W",
  "units": "Bcf",
  "frequency": "weekly",
  "series": [{ "date": "2024-01-05", "value": 3120.5 }],
  "observations": [{ "date": "2024-01-05", "value": 3120.5 }],
  "latest_observation_date": "2024-01-05",
  "latest_value": 3120.5,
  "last_successful_refresh": "...",
  "status": "LIVE",
  "validation": {}
}
```

On temporary failure the last verified cache is retained and marked `STALE`.
