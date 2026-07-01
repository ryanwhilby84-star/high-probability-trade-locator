# Seasonality Engine Inventory

Generated: 2026-06-13 (Priority 1 foundation rebuild — documentation only, no merge)

## Summary

| Engine | Role | Wired to trades? | Recommendation |
|--------|------|------------------|----------------|
| **V1 pillar** (`engine.py`) | Calendar-month weekly-return bias | Yes (confluence, grade A gate) | **Merge** into canonical ISO engine |
| **Chart engine** (`seasonality_engine.py`) | ISO-week paths, forward reads, trust | Display + trust gating | **Keep** as canonical target |
| **V2 audit** (`seasonality_v2.py`) | Statistical win-rate / z-score audit | No (`live_wired: false`) | **Retire** or promote after V1 merge |

---

## 1. V1 Seasonality Pillar

**Module:** `src/hptl/seasonality/engine.py`  
**Export:** `src/hptl/seasonality/export.py`  
**Output files:**
- `data/seasonality_latest.json`
- `web-dashboard/public/data/seasonality_latest.json`

### Inputs
- Chronological weekly closes from `weekly_closes_from_record()` (price store / canonical daily ISO resample)
- Current calendar month derived from `as_of_week` date

### Logic
- Tag each adjacent weekly return to the **calendar month** of the bar
- Average all historical weekly returns for the current month
- Bias: Bullish ≥ +0.15%, Bearish ≤ −0.15%, else Neutral
- Score: `min(10, |avg_ret| / 0.35)`

### Outputs (per instrument)
- `bias`, `score`, `wired`, `month`, `sample_weeks`, `avg_weekly_return_pct`

### Dependencies
- `hptl.prices.price_store.load_price_store`
- `hptl.seasonality.seasonality_price_bars.weekly_closes_from_record`
- `hptl.confluence.build_decision_table.TARGET_MARKETS`

### Current consumers
- `hptl.pillars.confluence_attach.pillar_fields_for_market_week()` — seasonality pillar on confluence rows
- `hptl.thesis.*` — thesis eligibility checks referencing seasonality bias
- Dashboard: legacy seasonality pillar displays (where still referenced)

### Recommendation: **Merge**
- Different methodology (calendar month vs ISO week) causes contradictions with chart engine
- After foundation rebuild, map confluence to chart-engine forward reads + trust grade
- Do not delete until confluence attach is migrated

---

## 2. Chart Seasonality Engine (Production Charts)

**Module:** `src/hptl/seasonality/seasonality_engine.py`  
**Trust:** `src/hptl/seasonality/seasonality_trust.py`  
**Bars:** `src/hptl/seasonality/seasonality_price_bars.py`  
**Export:** `src/hptl/seasonality/seasonality_price_export.py`  
**Output files:**
- `data/processed/seasonality_price_latest.json` (if written)
- `web-dashboard/public/data/seasonality_price_latest.json`

### Inputs
- Canonical daily timeline → ISO weekly closes (`weekly_closes_for_instrument`)
- Historical years `< current_year`

### Logic
- Rebase each year to 100 at ISO week 1
- Arithmetic mean across 3Y / 5Y / 10Y windows
- Forward reads: 4W / 8W / 12W average indexed return
- Direction: Bullish ≥ +1%, Bearish ≤ −1% (gated: n ≥ 5 years after foundation rebuild)
- Confidence: 3Y/5Y/10Y window agreement on 8W horizon
- Trust grade A/B/C via `seasonality_trust.classify_trust()`

### Outputs (per market)
- `chart_series`, `forward_read`, `confidence`, `trust_grade`, `trust_score`, `timeline_series`, etc.

### Dependencies
- `hptl.prices.canonical_timeline.build_canonical_timeline`
- `hptl.prices.price_store`
- `hptl.markets.instrument_registry`

### Current consumers
- Dashboard: `SeasonalityPriceChart`, `SeasonalityTimelineChart`, `SeasonalityDecisionPanel`
- `web-dashboard/src/seasonality/seasonalityDecision.js` — trust-aware decision copy
- `hptl.pillars.confluence_attach` — reads `trust_grade` to gate confluence eligibility (grade A only)
- `scripts/run_seasonality_foundation_audit.py`, foundation audits

### Recommendation: **Keep**
- This should become the **single canonical seasonality engine** after V1 merge
- Foundation rebuild improves inputs; engine logic unchanged except sample-safety gating (n ≥ 5)

---

## 3. V2 Seasonality Audit Engine

**Module:** `src/hptl/seasonality/seasonality_v2.py`  
**Audit runner:** `src/hptl/seasonality/seasonality_v2_audit.py`  
**Detail:** `src/hptl/seasonality/seasonality_v2_detail.py`  
**Staging export:** `src/hptl/seasonality/seasonality_v2_staging_chart_export.py`

### Output files
- `data/audits/seasonality_v2_audit.json` / `.md`
- `data/audits/seasonality_v2_detail.json` / `.md`
- `data/audits/seasonality_v2_fx_staging_audit.json`
- `web-dashboard/public/data/seasonality_v2_staging_latest.json`

### Inputs
- Daily bars from price store, staging, or FMP fallback
- 10Y rolling ISO week lookback

### Logic
- Per ISO week: collect prior-year weekly returns (max 10Y)
- Bias: win_rate ≥ 65% AND avg > +0.5% → Bullish; mirror for Bearish
- Confidence: High/Medium/Low from sample size, |z|, years, price staleness

### Status
- `live_wired: false` in audit export
- Last audit: 0 PASS assets, all Low confidence

### Current consumers
- Dashboard: `SeasonalityV2Panel.jsx` (audit/staging display only)
- Audit scripts only — not confluence, scanner, or thesis

### Recommendation: **Retire** (after chart engine absorbs statistical confidence)
- Valuable research prototype but duplicates chart engine with different thresholds
- Either fold z-score/win-rate into chart engine confidence or remove V2 panel entirely
- Do not wire to trades without foundation-grade daily data on all markets

---

## Dependency Graph

```
canonical_price_timeline (daily)
        │
        ▼
seasonality_price_bars (ISO weekly)
        │
        ├──────────────────┬─────────────────────┐
        ▼                  ▼                     ▼
  engine.py (V1)   seasonality_engine.py    seasonality_v2.py
  month bias       chart + forward reads     audit stats
        │                  │                     │
        ▼                  ▼                     ▼
seasonality_latest   seasonality_price_latest   seasonality_v2_audit
        │                  │
        ▼                  ▼
 confluence_attach    dashboard charts
 (pillar + trust)     (audit only label)
```

---

## Merge Plan (Future — Not Implemented)

1. **Keep** chart engine + trust grading as canonical
2. **Merge** V1 confluence attach to use chart `forward_read.next_8w` + `trust_grade` instead of calendar-month bias
3. **Retire** V2 after optional statistical layer ported to chart confidence
4. **Single export:** `seasonality_price_latest.json` becomes sole seasonality artifact
