# FX Dashboard Valuation Coverage — Priority Rows

Generated during Phase 3 recovery trace.

## Priority row map

| Display name | Instrument ID | V3 pair | DX |
|---|---|---|---|
| Japanese Yen / 6J | Japanese Yen / 6J | USD/JPY | — |
| NZ Dollar / 6N | NZ Dollar / 6N | NZD/USD | — |
| Swiss Franc / 6S | Swiss Franc / 6S | USD/CHF | — |
| British Pound / 6B | British Pound / 6B | GBP/USD | — |
| US Dollar Index / DX | US Dollar Index / DX | *(none)* | NOT_SUPPORTED |

## Valuation path

1. **Engine:** `compute_fx_pair_v3()` → `FxV3PairResult.as_dict()`
2. **Batch:** `build_all_fx_v3_pairs()` → `pairs[PAIR_ID]`
3. **Audit gate:** `run_fx_v3_audit()` → foundation + live-scope wiring → `markets[COT_MARKET]`
4. **Pillar export:** `build_valuation_latest()` → `instruments[COT_MARKET]`
5. **Dashboard:** `resolveFxPairId(market)` → `fxValuationV3Display()` reads `fx_valuation_v3_latest.json` + foundation audit

## Repair categories

- **PARSER_DATA** — shallow/missing macro or spot history (BIS `*_history.txt`, COT spot preference)
- **EXPORT_MISSING** — `fx_valuation_v3_latest.json` / foundation audit absent from `public/data`
- **DASHBOARD_KEY_MISMATCH** — pair not in `FX_V3_LIVE_PAIRS` (Python + JS) or `markets` block missing canonical COT key
- **MODEL_GATE_FAIL** — R² < 0.08 or foundation FAIL (do not weaken)
- **NOT_SUPPORTED** — DX has no V3 pair engine
