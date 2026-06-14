# Valuation Phase 2 — FX Architecture Cleanup

Generated: 2026-06-13  
Scope: FX only (no non-FX valuation changes, no formula changes)

## Goal

Make **`fx_carry_real_yield_v3`** the single source of truth for FX valuation on the dashboard pillar path.

## Canonical FX valuation path

```
Raw: price_store, fx currency_rates, macro caches (DXY, Treasury)
  → Transform: fx_macro_history, foundation audit, fx_v3_audit gates
  → Engine: fx_carry_real_yield_v3
  → Export: fx_valuation_v3_latest.json + valuation_latest.json
  → Dashboard: ValuationCell, FxValuationV3Panel, chart workstation (V3 overlay)
```

## Engine roles (after cleanup)

| Engine | Module | Role | Dashboard use |
|--------|--------|------|----------------|
| **V3 (canonical)** | `src/hptl/valuation/fx_carry_real_yield_v3.py` | Pillar fair-value regression | Scanner ValuationCell, V3 panel, thesis V3 snap |
| **V2 (secondary)** | `src/hptl/fx/fx_institutional_valuation.py` | Parallel currency scoring | `fx_valuation_latest.json`, FxSetupRankingPanel, FxValuationPanel |
| **V1 (legacy)** | `src/hptl/fx/fx_valuation.py` | Yield-diff confluence attach | Confluence rows, history chart only |
| **Stub (disabled)** | `src/hptl/fx/fx_fair_value.py` | Reserved regression shell | Never wired |

All V1/V2 modules now carry module-level warnings: **do not use for pillar export or main valuation column**.

## Code changes

### Pillar wiring

- **`FX_V3_CANONICAL_MARKET_BY_PAIR`** — one registry instrument per live-wired pair
- **`FX_V3_PILLAR_ALIAS_OF`** — spot-format duplicates defer to COT majors
- **`apply_pillar_canonical_gate()`** — dedupes pillar `wired` flags in `valuation_latest.json`
- **`export.py`** — uses V3 markets + pairs cache; applies canonical gate; sets `fx_pillar_engine`
- **`fx_v3_audit.py`** — populates `markets` for live cross pairs (incl. EUR/GBP)

### Registry / dashboard wiring fixes

| Issue | Resolution |
|-------|------------|
| **EUR/GBP** V3 live-wired but missing from registry | Added `EUR/GBP` to `FX_CROSSES` in `instrument_registry.py` (138 instruments) |
| **AUD/USD** duplicate pillar wiring with `Australian Dollar / 6A` | Pillar wires **6A only**; `AUD/USD` gets `valuation_pillar_role: alias` + pointer to canonical market |

### Dashboard labelling

- **`ValuationCell.jsx`** — V3-only for FX; removed unused V2 import; alias tooltip hint
- **`fxValuationV3Display.js`** — documented as canonical FX path
- **`fxInstitutionalValuation.js`**, **`useFxValuation.js`**, **`FxValuationPanel.jsx`** — marked secondary/V2

## Live-wired pillar instruments (expected)

After next `write_pillar_exports()` run:

| Instrument | Pair | Role |
|------------|------|------|
| Euro FX / 6E | EUR/USD | canonical |
| Australian Dollar / 6A | AUD/USD | canonical |
| Canadian Dollar / 6C | USD/CAD | canonical |
| Swiss Franc / 6S | USD/CHF | canonical (foundation-gated) |
| EUR/GBP | EUR/GBP | canonical (cross-only) |
| EUR/AUD | EUR/AUD | canonical (cross-only) |

**Alias (not pillar-wired):** `AUD/USD`, `NZD/USD`, `EUR/USD`, `GBP/USD`, `USD/JPY`, `USD/CAD`, `USD/CHF` — display may still resolve V3 pair data via `fx_valuation_v3_latest.json`, but `valuation_latest.json` sets `wired: false` with `valuation_canonical_market`.

## Not changed (by design)

- V3 regression formulas, gates, R² thresholds
- Failed pairs (GBP/USD, USD/JPY, etc.) — still not promoted
- Non-FX asset classes — still UNAVAILABLE on pillar
- V2 export pipeline — still runs for setup ranking (secondary)

## Files touched

**Python:** `fx_carry_real_yield_v3.py`, `export.py`, `fx_v3_audit.py`, `engine.py`, `fx_valuation.py`, `fx_institutional_valuation.py`, `fx_valuation_export.py`, `fx_valuation_attach.py`, `fx_fair_value.py`, `instrument_registry.py`

**Dashboard:** `ValuationCell.jsx`, `fxValuationV3Display.js`, `fxInstitutionalValuation.js`, `useFxValuation.js`, `FxValuationPanel.jsx`

**Tests:** `tests/test_fx_carry_real_yield_v3.py` — `test_pillar_canonical_gate_aliases`

## Verification

```powershell
$env:PYTHONPATH="src"
python -m pytest tests/test_fx_carry_real_yield_v3.py -q
python -c "from hptl.markets.instrument_registry import export_registry_json; export_registry_json()"
python -m hptl.valuation.run_valuation_update   # regenerates valuation_latest.json
```

Check `valuation_latest.json`:
- `summary.wired_count` should reflect **one row per pair** (no duplicate AUD/USD + 6A)
- `EUR/GBP` present with V3 block when audit PASS
- `fx_pillar_engine` = `fx_carry_real_yield_v3`

## Success criteria

| Criterion | Status |
|-----------|--------|
| Single canonical pillar path for FX | Done |
| V1 marked legacy, V2 marked secondary | Done |
| ValuationCell uses V3 for FX column | Done (unchanged behaviour, clarified) |
| EUR/GBP in registry | Done |
| AUD/USD duplication resolved in pillar export | Done |
| No formula changes | Done |
| Failed instruments not promoted | Done |
