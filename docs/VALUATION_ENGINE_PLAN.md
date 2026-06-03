# Valuation engine — implementation plan (pre-code)

**Status:** Approved direction; implementation not started.  
**Prerequisite:** COT weekly integrity gate operational (`docs/COT_FROZEN.md`).  
**Target UI example (per instrument):**

| Pillar | Example |
|--------|---------|
| Valuation | Undervalued |
| Institutions | Bullish |
| Retail | Bearish |
| Seasonality | Bullish |
| Location | Demand |
| Alignment | 5/5 |

---

## 1. Objective

For **every tradeable instrument** in the HTPL registry, compute a discrete valuation state:

- **Undervalued**
- **Fair Value**
- **Overvalued**

and expose it as a first-class pillar in confluence rows, thesis snapshots, scanner cards, and alignment scoring — using **real price and macro data only** (no fabricated scores).

---

## 2. Existing HTPL methodology to reuse (do not reinvent)

| Layer | Already in repo | Role for valuation |
|-------|-----------------|-------------------|
| **Institutions** | Legacy COT NC/NR via `legacy_cot_loader` → `cot_bias` / `cot_score` | Pillar: Institutions (frozen; gate-protected) |
| **Retail proxy** | Legacy non-reportable group (`cot_groups_integrity`, `retail_proxy`) | Pillar: Retail — derive bias from NR net / 4w change |
| **Location** | L5 `tactical_readiness.zone_focus` (Demand / Supply / …) | Pillar: Location — map from structural + tactical layers |
| **Macro fair-value context** | `macro_relationship_maps` (FRED price vs driver rolling correlation) | Core input: “price rich/cheap vs macro anchor” |
| **Structural anchor** | L1 `structural_regime` + `structural_score` | Regime filter (don't call undervalued in euphoric exhaustion) |
| **Alignment** | Thesis `decision.readiness` + `MISSING_CONFIRMATIONS` pattern | Extend to 5-pillar checklist → `alignment_score` (0–5) |

**Not yet wired (explicit placeholders today):** `valuation_score`, `seasonality_score`, `retail_positioning_score` on thesis snapshots.

---

## 3. Proposed valuation methodology

### 3.1 Price series (per instrument)

| Asset class | Primary series | Fallback |
|-------------|----------------|----------|
| Commodities / metals / energy | Continuous futures or spot proxy via OANDA (`hptl.oanda`) | Macro map price series if OANDA missing |
| Equity indices | Index future / cash proxy | ES/NQ macro map |
| FX | OANDA mid price | — |

Persist weekly snapshots aligned to **COT report date** (Tuesday close / last business day), same cadence as confluence rows.

### 3.2 Fair-value anchor (per instrument)

Use the **existing macro relationship map** when `available: true`:

1. Take `latest_rolling_corr_20` (or 60) sign + magnitude vs configured driver (rates, DXY, oil, etc.).
2. Compare **current price percentile** (52-week or 3-year window) to **driver-implied direction**:
   - Driver supportive + price in bottom tercile → **Undervalued**
   - Driver headwind + price in top tercile → **Overvalued**
   - Otherwise → **Fair Value**
3. If map `data_status != live` or `available: false` → `valuation_state: unavailable` (never guess).

Optional commodity supplement (phase 2): term structure / inventory proxies only where series already exist in config — no new feeds in phase 1.

### 3.3 Confidence / gating

- Require minimum observations (e.g. ≥ 120 weekly points) for percentile bands.
- Downgrade to `Fair Value` when `|rolling_corr| < 0.25` (no reliable macro link).
- Apply **exhaustion downgrade** from L4 when institutions crowded against valuation call.

Output fields on confluence row:

```json
{
  "valuation_state": "undervalued | fair_value | overvalued | unavailable",
  "valuation_score": 0-100,
  "valuation_confidence": 0-1,
  "valuation_drivers": ["macro_relationship_map", "price_percentile_52w"],
  "valuation_as_of": "2026-05-26"
}
```

---

## 4. Seasonality pillar (phase 1b — parallel track)

Not part of valuation math but required for **5/5 alignment**:

- Calendar month / week seasonal return bands from historical OANDA weekly returns (5y window).
- States: `bullish | neutral | bearish | unavailable`.
- Field: `seasonality_state`, `seasonality_score`.

---

## 5. Five-pillar alignment model

| # | Pillar | Source field | “Aligned” rule (long thesis example) |
|---|--------|--------------|--------------------------------------|
| 1 | Valuation | `valuation_state` | Undervalued |
| 2 | Institutions | `cot_bias` | Bullish |
| 3 | Retail | `retail_bias` (NR proxy) | Bearish (contrarian) |
| 4 | Seasonality | `seasonality_state` | Bullish |
| 5 | Location | `zone_focus` / tactical | Demand |

`alignment_score` = count of pillars aligned with thesis direction (0–5).  
`alignment_label` = `"5/5"` … `"0/5"`.

Wire into:

- `build_decision_table` record builder  
- `thesis_tracker.snapshot` copy fields  
- `thesis_tracker.conviction` (re-enable weight once wired)  
- Dashboard scanner card (existing pillar strip pattern in thesis UI)

---

## 6. Module layout (new code)

```
src/hptl/valuation/
  __init__.py
  price_history.py      # OANDA weekly close cache
  fair_value.py         # percentile + macro map fusion
  seasonality.py        # calendar bias (phase 1b)
  retail_bias.py        # NR proxy labels from COT groups
  alignment.py          # 5-pillar counter
  run_valuation_update.py
```

Integration point: **`build_decision_table.run()`** after institutional context, before UI pack:

```text
cot row → institutional_context → valuation_layer → seasonality → alignment → ui_pack
```

Weekly job: optional `python -m hptl.valuation.run_valuation_update` before confluence build, or inline in confluence build (phase 1: inline for simplicity).

---

## 7. Implementation phases

| Phase | Deliverable | Exit criteria |
|-------|-------------|---------------|
| **V1** | `valuation_state` + score on all COT-mapped instruments with live macro maps | Dashboard shows Undervalued/Fair/Overvalued when map available |
| **V2** | Retail + seasonality pillars | Alignment can reach 5/5 on demo instrument |
| **V3** | Thesis conviction weights rebalanced to include valuation | Composite score uses ≥4 wired pillars |
| **V4** | Expand to non-COT FX via proxy macro legs | Document proxy limitations in `data_status` |

---

## 8. Testing

- Unit: percentile bands, macro-driver direction matrix, alignment counter edge cases.
- Integration: one commodity (CL), one index (NQ), one FX (6E) — known week snapshot golden files.
- Regression: COT integrity gate still 23/23 PASS after valuation fields added (no scoring drift on COT numbers).

---

## 9. Out of scope (this phase)

- New audit/report pages  
- Intraday valuation or options-implied fair value  
- ML price forecasting  
- Changes to COT import, lineage gate, or legacy loader  

---

## 10. First coding task (when approved)

1. Add `src/hptl/valuation/price_history.py` + disk cache under `data/processed/valuation_prices/`.  
2. Implement `fair_value.py` consuming `macro_relationship_maps_latest.json` + price cache.  
3. Attach `valuation_*` fields in `build_decision_table` for `TARGET_MARKETS` only.  
4. Extend thesis snapshot schema + seed copy.  
5. Update dashboard scanner pillar strip (no new routes).

Estimated effort: **V1 ~2–3 sessions**, full 5/5 alignment **+2 sessions**.
