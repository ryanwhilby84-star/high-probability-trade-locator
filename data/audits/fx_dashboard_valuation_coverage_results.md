# FX Dashboard Valuation Coverage — Results

- Generated: 2026-06-14 (offline regeneration)
- Commands:
  - `python scripts/audit_fx_v3_parser_repair.py --offline --pairs USD/JPY NZD/USD USD/CHF GBP/USD`
  - Foundation + V3 audit + `valuation_latest` export (offline, `HPTL_SKIP_LIVE_FEEDS=1`)

## Rows recovered (dashboard should show valuation %)

| Display name | Pair | Deviation | Foundation | V3 audit | Export wired |
|---|---|---:|---|---|---|
| Japanese Yen / 6J | USD/JPY | **-5.44%** | PASS | PASS | Yes |
| NZ Dollar / 6N | NZD/USD | **+4.50%** | PASS | PASS | Yes |
| Swiss Franc / 6S | USD/CHF | **-28.62%** | PASS | PASS | Yes |

## Rows still unavailable

| Display name | Pair | Reason | Category |
|---|---|---|---|
| British Pound / 6B | GBP/USD | R²=0.0248 < gate 0.08; foundation FAIL | **MODEL_GATE_FAIL** |
| US Dollar Index / DX | — | No V3 pair mapping / engine | **NOT_SUPPORTED** |

## Full path trace

| Field | 6J | 6N | 6S | 6B | DX |
|---|---|---|---|---|---|
| instrument_id | Japanese Yen / 6J | NZ Dollar / 6N | Swiss Franc / 6S | British Pound / 6B | US Dollar Index / DX |
| expected_v3_pair | USD/JPY | NZD/USD | USD/CHF | GBP/USD | None |
| compute_result_status | PASS | PASS | PASS | FAIL | NOT_SUPPORTED |
| compute_result_value | -5.44 | +4.50 | -28.62 | null | null |
| build_all_fx_v3_pairs_key_found | Yes | Yes | Yes | Yes | — |
| audit_status (gated) | PASS | PASS | PASS | FAIL | — |
| valuation_latest_key_found | Yes | Yes | Yes | Yes | Yes |
| valuation_latest wired | Yes | Yes | Yes | No | No |
| dashboard_lookup_key | USD/JPY | NZD/USD | USD/CHF | GBP/USD | null |
| repair_category applied | DASHBOARD_KEY_MISMATCH + EXPORT_MISSING | same | same | none (gate) | none |

## Fixes applied (no formula / gate changes)

1. **DASHBOARD_KEY_MISMATCH** — Added `USD/JPY`, `NZD/USD`, `GBP/USD`, `USD/CHF` to `FX_V3_LIVE_PAIRS` (Python + `fxValuationV3Display.js`); added canonical `markets` keys in `FX_V3_CANONICAL_MARKET_BY_PAIR`.
2. **EXPORT_MISSING** — Regenerated `fx_valuation_data_foundation_audit.json`, `fx_valuation_v3_latest.json`, `fx_valuation_v3_audit.json`, `valuation_latest.json` to `data/` and `web-dashboard/public/data/`.
3. **PARSER_DATA** — Stale foundation audit (2026-06-12) referenced shallow BIS caches; deep `*_history.txt` caches on disk were already loaded by `load_bis_policy_history()` — **re-ran foundation audit** with current loaders (no gate change).

## Dashboard checklist (hard-refresh browser)

- [ ] **6J** Valuation cell: ~**-5.44%** Undervalued (not “Unavailable”)
- [ ] **6N** Valuation cell: ~**+4.50%** Overvalued
- [ ] **6S** Valuation cell: ~**-28.62%** Undervalued
- [ ] **6B** remains **Unavailable** (R² gate)
- [ ] **DX** remains **Unavailable** / no V3 panel (NOT_SUPPORTED)

## Parser repair audit (offline)

| Pair | V3 | R² | Class |
|---|---:|---:|---|
| USD/JPY | PASS | 0.637 | pass |
| NZD/USD | PASS | 0.339 | pass |
| USD/CHF | PASS | 0.291 | pass |
| GBP/USD | FAIL | 0.025 | model_weakness |

Shallow `bis_cbpol_jp.txt` / `bis_cbpol_nz.txt` remain empty (1 row); engine uses `*_history.txt` (2611 / 3787 rows).
