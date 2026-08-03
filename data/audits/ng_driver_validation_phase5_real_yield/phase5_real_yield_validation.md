# Natural Gas Valuation — Macro Validation Phase 5 (US 10Y Real Yield)

Generated: `2026-08-03T08:28:14+00:00`

## Task 1 — Real Yield dataset quality

- **Series:** Market Yield on U.S. Treasury Securities at 10-Year Constant Maturity, Quoted on an Investment Basis, Inflation-Indexed (`DFII10`)
- **Provider:** FRED (U.S. Treasury / Board of Governors via FRED)
- **Source:** FRED API / resilient macro_cache (load_fred_daily_map)
- **URL:** https://fred.stlouisfed.org/series/DFII10
- **Frequency:** Daily (business days)
- **Release cadence:** Daily Treasury constant-maturity TIPS yield; typically available same day or next business day via FRED.
- **History:** n=2644 from 2016-01-04 to 2026-07-30
- **Current observation date:** 2026-07-30 (value=2.41)
- **Missing periods (>5 calendar days):** 0
- **Point-in-time safety:** Daily real-yield levels as-of joined to Friday NG weeks (last observation on or before week date). Transforms use only past weekly as-of levels — no full-sample z-score. YoY uses absolute pp change (safe when yields cross zero).
- **Revisions policy:** Treasury constant-maturity yields via FRED are market quotes; material delayed revision risk is low. Research treats downloaded daily values as point-in-time as-of.
- **NG alignment:** Daily DFII10 covers the post-2016 NG weekly valuation panel. Safe to align with storage + production YoY history via as-of join.
- **Expected sign:** negative — Higher 10Y real yields raise the opportunity cost of holding commodities and often coincide with tighter financial conditions, so the coefficient on log(NG price) should be negative.

## Task 2 — Transformations tested

- `raw_yield` — raw 10Y real yield level (%, as-of weekly) (expect negative)
- `yoy_chg` — year-over-year change in real yield (percentage points) (expect negative)
- `chg_4w` — 4-week change in real yield (pp) (expect negative)
- `chg_12w` — 12-week change in real yield (pp) (expect negative)
- `rolling_zscore_156` — trailing 156-week z-score (past-only mean/sd) (expect negative)
- `trend_deviation_260` — deviation from trailing 260-week (~5y) linear trend (past-only) (expect negative)

## Task 3 — Model comparison (identical walk-forward)

### Storage-only baseline

- OOS RMSE=0.359504 MAE=0.264547 R²=0.1962 in-sample R²=0.239
- Coefs: `{"storage_surplus_bcf": -0.000544}`

### Current published v2 — Storage + Production YoY

- OOS RMSE=0.34586 MAE=0.255877 R²=0.256 in-sample R²=0.3075
- Coefs: `{"production_yoy_pct": -0.021977, "storage_surplus_bcf": -0.000799}`

### Candidate — Storage + Production YoY + Real Yield

| Transform | OOS RMSE | OOS MAE | OOS R² | ΔRMSE% vs v2 | RY coef | Sign OK | Sign flip | DM p | Regime OK | Decision |
|---|---:|---:|---:|---:|---:|---|---|---:|---|---|
| raw_yield | 0.360958 | 0.258107 | 0.1896 | -4.37 | 0.041273 | False | True | 0.8634 | False | Reject |
| yoy_chg | 0.329513 | 0.263277 | 0.3104 | 9.06 | 0.188104 | False | False | 0.1682 | False | Reject |
| chg_4w | 0.342911 | 0.255953 | 0.273 | 1.32 | 0.308082 | False | False | 0.3118 | False | Reject |
| chg_12w | 0.311691 | 0.242335 | 0.404 | 10.55 | 0.417568 | False | False | 0.0432 | False | Reject |
| rolling_zscore_156 | 0.230869 | 0.18462 | 0.6546 | 18.75 | 0.096807 | False | False | 0.0249 | False | Reject |
| trend_deviation_260 | 0.221367 | 0.174284 | -0.1909 | -2.64 | 0.063753 | False | False | 0.6193 | False | Reject |

**Best-performing transformation (by ΔRMSE% vs v2):** `rolling_zscore_156`

## Coefficient interpretation

Economically, higher 10Y real yields should pressure NG prices (negative β). Best candidate coefficients: `{"production_yoy_pct": -0.010923, "real_yield__rolling_zscore_156": 0.096807, "storage_surplus_bcf": -0.001296}`.

## Regime stability

```json
{
  "ok": false,
  "coefficient_halves": {
    "early_mean": 0.147737,
    "late_mean": 0.100252,
    "same_sign": true,
    "tip_sign_positive": true
  },
  "both_halves_improve": false,
  "not_single_regime": false,
  "reason": "improvement_or_sign_concentrated_in_one_regime",
  "halves": {
    "early": {
      "n": 109,
      "date_start": "2022-05-29",
      "date_end": "2024-06-23",
      "v2_oos_rmse": 0.353953,
      "candidate_oos_rmse": 0.259933,
      "improvement_pct": 26.56,
      "improves": true
    },
    "late": {
      "n": 110,
      "date_start": "2024-06-30",
      "date_end": "2026-07-30",
      "v2_oos_rmse": 0.19132,
      "candidate_oos_rmse": 0.197902,
      "improvement_pct": -3.44,
      "improves": false
    }
  }
}
```

## Recommendation

**Reject** (research status: Reject)

Reject Real Yield for fair value. No transform improves Storage+Production YoY with a stable, economically sensible coefficient under walk-forward. Keep published model ng_storage_production_v2.

Published model remains **`ng_storage_production_v2`** (unchanged=True).

## Safety

- Weekly COT / HPTL_SKIP_VALUATION / Stage 4 / Scanner / Inspector / Seasonality untouched
- No nominal yields / inflation expectations / liquidity tested in this phase

