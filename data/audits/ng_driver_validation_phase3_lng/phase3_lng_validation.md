# Natural Gas Valuation — Driver Validation Phase 3 (LNG Exports)

Generated: `2026-08-03T08:00:45+00:00`

## Task 1 — LNG dataset quality

- **Source:** EIA dnav hist_xls (official public download) (`N9133US2`)
- **URL:** https://www.eia.gov/dnav/ng/hist_xls/N9133US2m.xls
- **Frequency:** monthly · Units: Bcf/d
- **Release cadence:** Monthly EIA Natural Gas Monthly / dnav hist_xls republish. Typical publish lag ~1–3 months after the reference month.
- **History:** n=353 from 1997-01-15 to 2026-05-15
- **Current observation date:** 2026-05-15 (value=16.519762131616123)
- **Missing periods:** 0
- **Point-in-time safety:** YoY/seasonal transforms computed on native monthly dates or past-only weekly as-of levels; as-of forward-fill onto weekly price dates. Full-sample z-score is leaky and used only as contrast.
- **Revisions policy:** EIA monthly series can be revised in subsequent Natural Gas Monthly releases. HPTL stores the latest downloaded hist_xls snapshot; it does not retain a vintage/point-in-time revision archive. Validation treats the current cache as the working series (standard for this pillar).
- **Expected sign:** positive — Stronger LNG export volumes tighten the domestic balance and are typically supportive for Henry Hub / NG prices (positive coefficient).

## Task 3 — Model comparison

### Storage-only baseline

- OOS RMSE=0.359504 MAE=0.264547 R²=0.1962 in-sample R²=0.239
- Coefs: `{"storage_surplus_bcf": -0.000544}`

### v2 Storage + Production YoY

- OOS RMSE=0.34586 MAE=0.255877 R²=0.256 in-sample R²=0.3075
- Coefs: `{"production_yoy_pct": -0.021977, "storage_surplus_bcf": -0.000799}`

### Storage + Production YoY + LNG (one transform at a time)

| Transform | OOS RMSE | OOS MAE | OOS R² | ΔRMSE% vs v2 | LNG coef | Sign OK | Sign flip | DM p | Regime OK | Leaky | Decision |
|---|---:|---:|---:|---:|---:|---|---|---:|---|---|---|
| raw_level | 0.335031 | 0.259258 | 0.3019 | 3.13 | 0.022445 | True | True | 0.2596 | False | False | Keep Experimental |
| yoy_pct | 0.363425 | 0.266188 | 0.1612 | -0.29 | -2.8e-05 | False | True | 0.8437 | False | False | Reject |
| seasonal_deviation | 0.331664 | 0.264364 | 0.3414 | 7.97 | 0.054816 | True | True | 0.04 | False | False | Keep Experimental |
| trailing_zscore_156 | 0.371467 | 0.281721 | 0.1289 | -1.79 | -0.034141 | False | True | 0.8329 | False | False | Reject |
| chg_4w | 0.349286 | 0.261298 | 0.2457 | -0.52 | 0.015935 | True | True | 0.7488 | False | False | Keep Experimental |
| chg_12w | 0.355765 | 0.268757 | 0.2235 | -2.1 | 0.00493 | True | True | 0.9907 | False | False | Keep Experimental |
| v1_fullsample_zscore | 0.335031 | 0.259258 | 0.3019 | 3.13 | 0.112356 | True | True | 0.2596 | False | True | Keep Experimental |

## Recommendation

**Reject** (research status: Keep Experimental)

Reject LNG for promotion into the published fair-value model. LNG remains Experimental. No transform clears all promotion gates versus v2 (Storage + Production YoY). Keep published model ng_storage_production_v2.

Published model remains **`ng_storage_production_v2`** (unchanged=True).

## Safety

- Weekly COT / HPTL_SKIP_VALUATION untouched
- No Weather / USD / Inflation / Bonds / Seasonality testing

