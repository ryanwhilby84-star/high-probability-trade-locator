# Natural Gas Valuation — Driver Validation Phase 2 (Production)

Generated: `2026-08-03T06:58:02+00:00`

## Task 1 — Current valuation mathematics

- **Engine:** `energy_natural_gas_v1`
- **Fair-value equation:** `log(P) = 1.131379 + (-0.000544) * storage_surplus_bcf; fair = exp(log(P))`
- **Identity:** `log(fair) = intercept + Σ (βᵢ · xᵢ); fair = exp(log(fair))`
- **Validated drivers:** storage_surplus_bcf
- **Experimental drivers:** dry_gas_production, lng_exports, log_dxy, hdd_anomaly, cdd_anomaly
- **Informational drivers:** working_gas_storage_level, seasonality_factor
- **Confidence rules:**
  - `None`: n < 52 or n_features < 1
  - `Low`: n_features ≥ 1 otherwise
  - `Medium`: oos_r2 ≥ 0.15 AND n_features ≥ 1 AND in-sample R² ≥ 0.12
  - `High`: oos_r2 ≥ 0.22 AND n ≥ 156 AND n_features ≥ 2 AND extreme_fv_rate ≤ 0.35 AND in-sample R² ≥ 0.2
  - Current: **Medium** (R²=0.239, OOS R²=0.1962)
- **How storage affects fair value:**
  - Feature: `storage_surplus_bcf`
  - Definition: Working-gas level (Bcf) minus trailing same-ISO-week 5-year average using strictly prior years (≥3 peers). As-of forward-filled onto weekly price dates. Entered in regression as raw Bcf (not z-scored).
  - Latest β: `-0.000544`
  - Latest surplus: `172.6` Bcf → log contribution `-0.093891`
  - Higher surplus lowers fair value when β < 0.

## Task 2 — Production research

### Storage-only baseline

- Spec `A_storage`: OOS RMSE=0.359504, OOS MAE=0.264547, OOS R²=0.1962, in-sample R²=0.239
- Coefficients: `{"storage_surplus_bcf": -0.000544}`
- Sample: 2016-06-05 → 2026-07-30 (n=531)

### Storage + Production transforms (one at a time)

| Transform | OOS RMSE | OOS MAE | OOS R² | ΔRMSE% vs storage | Prod coef | Sign OK | Sign flip | DM p | Leaky | Decision |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| raw_level | 0.360297 | 0.26666 | 0.1926 | -0.22 | 0.005676 | False | True | 0.5272 | False | Reject |
| yoy_pct | 0.363061 | 0.266307 | 0.1629 | 3.14 | -0.023204 | True | False | 0.0697 | False | Promote |
| seasonal_deviation | 0.375337 | 0.272036 | 0.1565 | -1.37 | 0.007859 | False | True | 0.8473 | False | Reject |
| trailing_zscore_156 | 0.37479 | 0.271222 | 0.1132 | -0.11 | -0.03125 | True | True | 0.5602 | False | Keep Experimental |
| chg_4w | 0.36125 | 0.265891 | 0.1932 | -0.18 | 0.000682 | False | True | 0.9686 | False | Reject |
| chg_12w | 0.36227 | 0.266995 | 0.1948 | -0.18 | 0.005656 | False | True | 0.6145 | False | Reject |
| v1_fullsample_zscore | 0.360297 | 0.26666 | 0.1926 | -0.22 | 0.067209 | False | True | 0.5272 | True | Reject |

### Production recommendation

**Promote**

Promote Production into fair value only as `yoy_pct` (year-over-year production % change). Raw production level and most other transforms fail economics and/or OOS gates and must stay out of the model. This phase does not change the published export; a separate wiring step is required to adopt the YoY (or other promoted) form.

- Best non-leaky candidate: `yoy_pct` → Promote
- Candidate OOS: RMSE=0.363061, MAE=0.266307, R²=0.1629; coefs=`{"production__yoy_pct": -0.023204, "storage_surplus_bcf": -0.000845}`

Published fair-value model was **not** changed in this research phase. If recommendation is Promote, a separate wiring step must adopt the specific winning transform (not raw production) before it enters fair value.

### Plain-English verdict

Storage alone remains a solid baseline: higher inventory surplus versus the same-week 5-year average lowers estimated fair value. Adding **raw** dry-gas production (the form already tested inside V1 spec B) does **not** help — the coefficient points the wrong way and walk-forward fit does not improve. Among the transforms tested one-by-one, **year-over-year production growth** is the only point-in-time-safe form that clears the promotion gates on this sample: correct negative sign, no coefficient sign flips, >2% OOS RMSE improvement versus storage-only on the same aligned weeks, and a one-sided Diebold-Mariano p-value below 10%. That is evidence to promote YoY production in a follow-on wiring step — not to promote production levels or the current leaky full-sample z-score.

## Safety

- Weekly COT workflow unchanged
- `HPTL_SKIP_VALUATION` untouched
- No LNG / Weather / USD / Seasonality testing in this phase

