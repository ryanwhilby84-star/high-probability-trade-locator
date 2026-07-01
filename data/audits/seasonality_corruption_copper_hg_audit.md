# Seasonality Data Corruption — Root Cause Audit

- **Generated:** 2026-06-18  
- **Trigger:** Seasonality projection chart showing indexed values ~100,000 (expected ~100)  
- **Mode:** Audit only — no fixes applied

---

## 1. Affected instrument

**Copper / HG** — the only radar instrument with indexed values near **99,392** in the live export (matches the ~100,000 screenshot spike).

| Field | Value |
|---|---|
| **Instrument** | Copper / HG |
| **Symbol** | `Copper / HG` (Alpha Vantage commodity mapping) |
| **Data source** | `alpha_vantage` → canonical timeline → `derived_iso_week_end_from_canonical_daily` |
| **Earliest date** | 2016-06-01 (daily canonical) |
| **Latest date** | 2026-06-16 (price); timeline projection to 2026-12-22 |
| **Trust grade** | **A** (misleading — grading does not detect unit breaks) |
| **Export file** | `web-dashboard/public/data/seasonality_price_latest.json` (generated 2026-06-18T10:21:32Z) |

### Secondary affected instrument

**Corn** — same pipeline, indexed max **4,901** (same Alpha Vantage unit-mix pattern, smaller relative spike).

---

## 2. Expected vs observed

| Series | Expected | Observed (Copper / HG) |
|---|---|---|
| Historical seasonal avg (10Y/5Y/3Y) | ~50–200 | up to **36,979** (3Y w35) |
| Current-year path | ~50–200 | up to **99,392** (actual w5) |
| Forward projection | extends from ~107 anchor | up to **24,517** (10Y proj w39) |
| Raw weekly close | ~$4–6/lb | **5,874.63** at 2026-02-01 ISO week |

---

## 3. Pipeline stage audit

### Stage A — Raw canonical daily prices ❌ **CORRUPTION STARTS HERE**

Source: `load_canonical_timeline("Copper / HG")` → Alpha Vantage daily.

| Metric | Value |
|---|---|
| Daily bars | 2,628 |
| Correct scale ($/lb, close < 50) | 2,508 bars (96%) — range **2.02–6.62** |
| Wrong scale (close ≥ 50) | 120 bars (4%) — range **2,106–6,116** |
| Day-over-day jumps > 10× | **239 events** |

**Pattern:** Wrong-scale prints cluster on **month-start dates** (e.g. 2016-07-01 → 2,206; next day back to 2.22). Looks like monthly/index-series points merged into daily $/lb series without unit conversion.

Example transitions:

```
2016-06-01  2105.56  ->  2016-06-09  2.02
2016-06-30  2.22     ->  2016-07-01  2206.68  (995×)
2016-07-01  2206.68  ->  2016-07-02  2.22
```

### Stage B — Weekly ISO aggregation ❌ **SPIKES PRESERVED**

`weekly_closes_for_instrument()` → `derived_iso_week_end_from_canonical_daily`

18 weekly bars with close **> 1,000**, including:

| ISO week ending | Close |
|---|---|
| 2026-02-01 | 5,874.63 |
| 2026-03-01 | 5,682.93 |
| 2025-06-01 | 4,461.11 |
| 2024-09-01 | 4,199.87 |

Weekly resampling picks up the contaminated month-start daily close for that ISO week.

### Stage C — Rebase-to-100 (`normalized_year_path`) ❌ **AMPLIFICATION**

Logic (`seasonality_engine.py`):

```python
base = week_closes.get(1)  # or earliest week
return {w: (c / base) * 100.0 for w, c in week_closes.items()}
```

**2026 example:**

| Week | Raw close | Indexed |
|---:|---:|---:|
| 1 | 5.91 | 100.0 |
| 5 | **5,874.63** | **99,392.13** |
| 9 | 5,682.93 | 96,148.71 |
| 22 | 6.51 | 110.12 |

Formula: `(5874.63 / 5.91056) × 100 = 99,392` — matches export exactly.

### Stage D — Historical seasonal averages ❌ **CORRUPTION PROPAGATES**

`avg_path()` averages per-year normalized paths. Years with month-start spikes (2024 w35, 2025 w22, etc.) produce indexed values **> 100,000** in individual years, pulling 3Y/5Y/10Y averages to **21,000–37,000**.

### Stage E — Forward projection ❌ **CORRUPTION PROPAGATES**

`project_forward()` scales seasonal shape from anchor_index × (avg[w]/avg[anchor_week]). With corrupted averages, projections reach **24,000+** even though anchor_index is sane (**107.05** at week 25).

### Stage F — Export payload ❌ **CORRUPTION PRESENT**

`seasonality_price_latest.json` → `markets["Copper / HG"].chart_series`:

| Field | Min | Max | Median |
|---|---:|---:|---:|
| All indexed fields | 91.26 | **99,392.13** | 107.05 |
| Raw `close` in same rows | 5.43 | **5,874.63** | 5.94 |

Top indexed values in export:

| Value | Field | Week |
|---:|---|---:|
| 99,392.13 | actual | 5 |
| 96,148.71 | actual | 9 |
| 36,978.94 | seasonal_3y | 35 |
| 36,097.40 | proj_3y | 35 |

### Stage G — UI chart series ✅ **NO ADDITIONAL CORRUPTION**

Simulated `buildProjectionChartRows()` on export:

```
UI max: currentYearPath, week 5, 99392.13
```

`SeasonalityProjectionPanel` plots `currentYearPath`, `seasonal_*`, `forwardSeasonalPath` — all pass through export values unchanged. **Chart rendering is not the bug.**

---

## 4. Diagnostics summary (Copper / HG)

| Metric | Indexed series | Raw close (chart_series.close) |
|---|---:|---:|
| **Min** | 91.26 | 5.43 |
| **Max** | 99,392.13 | 5,874.63 |
| **Median** | 107.05 | 5.94 |

Trust grade **A** is assigned because the engine sees 10+ years and 525 weekly bars — it does **not** validate price scale consistency.

---

## 5. Root cause classification

| Layer | Verdict |
|---|---|
| Source data | **ROOT CAUSE** — Alpha Vantage canonical daily mixes $/lb (~2–6) with ~1000× scaled month-start prints |
| Weekly aggregation | **Contributing** — ISO week-end picks spike days |
| Rebase-to-100 | **Amplifying** — correct math on bad inputs |
| Seasonal averaging / projection | **Downstream propagation** |
| Export | **Faithful copy of corrupted compute** |
| Chart rendering | **Not at fault** |

**First divergence from expected indexed values:** raw canonical daily price series, before any seasonality math.

---

## 6. Proposed smallest fix (not applied)

### Option A — Seasonality ingest filter (smallest, localized)

Apply the same median-band outlier filter already used in `metals_valuation_v1._filter_price_outliers()` to weekly bars at the start of `compute_seasonality_price_block()` (or in `weekly_closes_for_instrument()`):

- Keep prices within `[0.2 × median, 4.0 × median]`  
- For Copper median ~$4/lb, drops 2,100–5,800 spikes while keeping legitimate moves

**Pros:** One function, fixes seasonality + hist paths + projection.  
**Cons:** Does not fix canonical timeline for other consumers (valuation, location).

### Option B — Canonical timeline repair (broader, correct)

Normalize Alpha Vantage Copper daily at ingest: detect month-start index-scale rows and divide by ~1000 (or drop rows with >10× day-over-day jump).

**Pros:** Fixes all downstream pillars.  
**Cons:** Slightly wider scope; needs unit test on AV Copper series.

### Option C — Trust grade gate (display safety net)

Downgrade to **C** when `max(close)/median(close) > 4` on weekly bars; suppress directional labels.

**Pros:** Prevents misleading Grade A.  
**Cons:** Does not fix chart spikes — user still sees bad chart.

**Recommendation:** **Option A + B** — filter in seasonality immediately (smallest user-visible fix), then repair canonical Copper ingest so valuation/location stay consistent. Re-export `seasonality_price_latest.json` after fix.

---

## 7. Verification commands (post-fix)

```powershell
python scripts/audit_seasonality_live_spikes.py
python scripts/audit_seasonality_spikes.py
# Copper indexed max should be < 200
```

---

## Artifacts

- `data/audits/seasonality_live_spike_audit.json` — live recompute scan  
- `scripts/audit_seasonality_spikes.py` — export scan  
- `scripts/audit_seasonality_live_spikes.py` — live pipeline scan
