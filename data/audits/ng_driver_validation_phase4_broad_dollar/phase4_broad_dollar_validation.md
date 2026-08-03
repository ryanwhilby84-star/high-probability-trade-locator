# Natural Gas Valuation — Macro Validation Phase 4 (Broad US Dollar)

Generated: `2026-08-03T08:17:18+00:00`

## Task 1 — Broad Dollar dataset quality

- **Series:** Nominal Broad U.S. Dollar Index (`DTWEXBGS`)
- **Provider:** FRED (Federal Reserve Board H.10 via FRED)
- **Source:** FRED API / resilient macro_cache (load_fred_daily_map)
- **URL:** https://fred.stlouisfed.org/series/DTWEXBGS
- **Frequency:** Daily (business days)
- **Release cadence:** Daily; FRB trade-weighted indexes typically available with about one business-day lag.
- **History:** n=2633 from 2016-01-04 to 2026-07-24
- **Current observation date:** 2026-07-24 (value=120.7105)
- **Missing periods (>5 calendar days):** 1
- **Point-in-time safety:** Daily Broad USD levels as-of joined to Friday NG weeks (last observation on or before week date). Transforms use only past weekly as-of levels — no full-sample z-score.
- **Revisions policy:** FRB/FRED index levels; occasional restatements possible but material delayed-revision risk is low versus EIA physical series. Research treats the downloaded daily values as point-in-time as-of.
- **NG alignment:** Daily Broad USD covers the post-2016 NG weekly valuation panel. Safe to align with storage + production YoY history via as-of join.
- **Expected sign:** negative — A stronger Broad USD typically pressures commodity prices, so the coefficient on log(NG price) should be negative.

## Task 2 — Transformations tested

- `raw_index` — raw Broad USD index level (as-of weekly) (expect negative)
- `yoy_pct` — year-over-year Broad USD % change (expect negative)
- `chg_4w` — 4-week change in Broad USD (expect negative)
- `chg_12w` — 12-week change in Broad USD (expect negative)
- `rolling_zscore_156` — trailing 156-week z-score (past-only mean/sd) (expect negative)
- `trend_deviation_104` — deviation from trailing 104-week linear trend (past-only) (expect negative)

## Task 3 — Model comparison (identical walk-forward)

### Storage-only baseline

- OOS RMSE=0.359504 MAE=0.264547 R²=0.1962 in-sample R²=0.239
- Coefs: `{"storage_surplus_bcf": -0.000544}`

### Current published v2 — Storage + Production YoY

- OOS RMSE=0.34586 MAE=0.255877 R²=0.256 in-sample R²=0.3075
- Coefs: `{"production_yoy_pct": -0.021977, "storage_surplus_bcf": -0.000799}`

### Candidate — Storage + Production YoY + Broad Dollar

| Transform | OOS RMSE | OOS MAE | OOS R² | ΔRMSE% vs v2 | USD coef | Sign OK | Sign flip | DM p | Regime OK | Decision |
|---|---:|---:|---:|---:|---:|---|---|---:|---|---|
| raw_index | 0.343103 | 0.255487 | 0.2678 | 0.8 | 0.020598 | False | True | 0.4035 | False | Reject |
| yoy_pct | 0.367672 | 0.274023 | 0.1415 | -1.47 | 0.013983 | False | True | 0.6949 | False | Reject |
| chg_4w | 0.349073 | 0.257411 | 0.2467 | -0.46 | 0.001986 | False | True | 0.7251 | False | Reject |
| chg_12w | 0.351881 | 0.263443 | 0.2403 | -0.99 | 0.009691 | False | True | 0.7567 | False | Reject |
| rolling_zscore_156 | 0.303193 | 0.239863 | 0.4042 | -6.7 | 0.019883 | False | True | 0.9664 | False | Reject |
| trend_deviation_104 | 0.393062 | 0.318796 | 0.0251 | -1.88 | 0.018226 | False | True | 0.6652 | False | Reject |

**Best-performing transformation (by ΔRMSE% vs v2):** `raw_index`

## Coefficient interpretation

Economically, a stronger Broad USD should pressure NG prices (negative β). Best candidate coefficients: `{"broad_usd__raw_index": 0.020598, "production_yoy_pct": -0.021534, "storage_surplus_bcf": -0.000887}`.

## Regime stability

```json
{
  "ok": false,
  "coefficient_halves": {
    "early_mean": -0.012499,
    "late_mean": 0.023639,
    "same_sign": false,
    "tip_sign_positive": true
  },
  "both_halves_improve": false,
  "not_single_regime": false,
  "reason": "improvement_or_sign_concentrated_in_one_regime",
  "halves": {
    "early": {
      "n": 187,
      "date_start": "2019-06-02",
      "date_end": "2022-12-22",
      "v2_oos_rmse": 0.45258,
      "candidate_oos_rmse": 0.445362,
      "improvement_pct": 1.59,
      "improves": true
    },
    "late": {
      "n": 188,
      "date_start": "2022-12-29",
      "date_end": "2026-07-30",
      "v2_oos_rmse": 0.186715,
      "candidate_oos_rmse": 0.193701,
      "improvement_pct": -3.74,
      "improves": false
    }
  }
}
```

## Recommendation

**Reject** (research status: Reject)

Reject Broad Dollar for fair value. No transform improves Storage+Production YoY with a stable, economically sensible coefficient under walk-forward. Keep published model ng_storage_production_v2.

Published model remains **`ng_storage_production_v2`** (unchanged=True).

## Safety

- Weekly COT / HPTL_SKIP_VALUATION / Stage 4 / Scanner / Inspector / Seasonality untouched
- No bond yields / inflation / liquidity tested in this phase

