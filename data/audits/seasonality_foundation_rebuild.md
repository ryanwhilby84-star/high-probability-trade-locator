# Seasonality Foundation Rebuild Audit

Generated: 2026-06-18

## Executive Summary

Priority 1 foundation rebuild completed for Silver, Copper, Corn, Cotton, and Coffee.

| Market | Trust Before | Trust After | Outcome |
|--------|:---:|:---:|---------|
| **Silver** | C | **A** | Recovered — 10Y dense OANDA daily |
| **Copper / HG** | B | **A** | Recovered — OANDA XCU_USD + sparse prefix removed |
| **Corn** | B | **A** | Recovered — OANDA CORN_USD + sparse prefix removed |
| **Cotton** | B | B | FRED monthly only — blocked on data source |
| **Coffee** | B | B | FRED monthly only — blocked on data source |

**3 of 5 markets** now meet trust grade **A** (production-ready for confluence).

---

## Before vs After

| Market | Earliest | Latest | Hist Yrs | Weekly Obs | Avg Wk/Yr | 3Y Curve Wks | 8W Sample | Trust |
|--------|----------|--------|----------|------------|-----------|--------------|-----------|-------|
| Silver (before) | 2025-06-01 | 2026-05-28 | 1 | 53 | 31.0 | 31 | n=1 | C |
| Silver (after) | 2016-06-12 | 2026-06-16 | 10 | 524 | 49.9 | 52 | n=10 | **A** |
| Copper (before) | 2004-10-01 | 2026-05-01 | 22* | 260 | 11.6 | 17 | n=3 | B |
| Copper (after) | 2016-06-01 | 2026-06-16 | 10 | 525 | 50.0 | 52 | n=10 | **A** |
| Corn (before) | 2004-10-01 | 2026-05-01 | 22* | 260 | 11.6 | 17 | n=3 | B |
| Corn (after) | 2016-06-01 | 2026-06-16 | 10 | 525 | 50.0 | 52 | n=10 | **A** |
| Cotton (before) | 1992-01-01 | 2026-05-01 | 34 | 413 | 12.0 | 17 | n=3 | B |
| Cotton (after) | 1992-01-01 | 2026-05-01 | 34 | 413 | 12.0 | 17 | n=3† | B |
| Coffee (before) | 1992-01-01 | 2026-05-01 | 34 | 413 | 12.0 | 17 | n=3 | B |
| Coffee (after) | 1992-01-01 | 2026-05-01 | 34 | 413 | 12.0 | 17 | n=3† | B |

\*Long calendar span but **monthly** Alpha Vantage / FRED — only ~12 ISO weeks/year  
†Forward direction now labeled **"Low sample reliability"** (sample safety gate)

---

## Phase 2 — Silver Recovery

**Root cause:** The live OANDA price loader fetches `count=260` (~1 trading year) with no historical merge. Silver was never backfilled despite `backfill_metal_daily.py` existing for Gold.

**Fix:** `python -m hptl.prices.backfill_metal_daily --instrument Silver --years 10 --execute --promote`

**Result:** 2,592 daily bars (2016-06-09 .. 2026-06-16) → 524 ISO weekly bars, trust **A**, forward 8W based on **n=10** (was n=1).

---

## Phase 3 — Sample Safety Rules

Implemented in `seasonality_engine.py` (export layer only — **no UI changes**):

- Forward reads require **≥5 historical years** before Bullish/Bearish labels
- Below threshold: `"Low sample reliability"` or `"Insufficient history"`
- Confidence windows with <5 years excluded from Strong/Medium agreement
- Cotton/Coffee now export `"Low sample reliability"` instead of Bearish + Strong

---

## Phase 4 — Engine Inventory

See [seasonality_engine_inventory.md](./seasonality_engine_inventory.md)

| Engine | Recommendation |
|--------|----------------|
| V1 pillar (`engine.py`) | **Merge** into chart engine |
| Chart engine (`seasonality_engine.py`) | **Keep** as canonical |
| V2 audit (`seasonality_v2.py`) | **Retire** after stats layer ported |

---

## Remaining Weaknesses

1. **Cotton / Coffee:** No free daily source (OANDA 400, FMP 403 premium). FRED IMF monthly → max ~12 ISO weeks/year → trust B ceiling.
2. **Copper/Corn source label:** `select_price_source` still reports `alpha_vantage` though data is OANDA-dense post-rebuild.
3. **Three engines** still produce conflicting answers for confluence vs charts (documented, not merged).
4. **V2 audit** still unwired with 0 PASS assets.

---

## Artifacts

- `data/audits/seasonality_foundation_rebuild.json`
- `data/audits/seasonality_engine_inventory.md`
- `src/hptl/seasonality/seasonality_foundation_rebuild.py` (repeatable rebuild runner)
- `web-dashboard/public/data/seasonality_price_latest.json` (re-exported)

---

## Recommended Next Steps (Phase 2 of Seasonality Upgrade — Not Started)

1. Upgrade FMP tier or add ICE continuous futures feed for Cotton/Coffee daily
2. Merge V1 pillar into chart engine for single confluence truth
3. Wire unified seasonality to confluence with explicit support/fight semantics
