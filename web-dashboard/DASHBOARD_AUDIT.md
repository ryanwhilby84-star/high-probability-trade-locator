# Dashboard wiring audit (read-only)

## Data source

- **Primary JSON:** `public/data/confluence_history_latest.json` (from `hptl.confluence.build_decision_table.run()`).
- **Secondary:** `public/data/news_catalysts.json` (optional catalyst scan).

## Real / wired (per instrument row)

| Area | Source | Notes |
|------|--------|--------|
| COT levels, net, 1W/4W changes | Row fields from COT pipeline | Unchanged parsing |
| Positioning state, COT bias/score, narratives | `build_decision_table` | Text generation only where edited |
| Macro regime / macro score / rates_macro | Row + macro merge | |
| `ui_pack` | Built server-side; dashboard falls back if missing | `buildFallbackUiPack` |
| Week backdrop | `global_market_regime_latest_week` on payload | Human summary + technical fields in regime object |
| Macro Relationship Map | `macro_relationship_maps[market]` | FRED-backed where `available: true` |
| Intermarket | `intermarket_impulse_context` | Real when present in JSON |
| Instrument intel block | `instrument_intel_context` | Real when present |
| News catalyst table | `news_catalysts.json` + row sensitivity | Empty states are honest |

## Placeholder / limited (not fake numbers)

| Area | Behaviour |
|------|-----------|
| USD / liquidity / inflation regime | Often `"source unavailable"` until wired |
| Macro map secondary drivers (VIX, EIA series, etc.) | Marked **Context** / not on live chart |
| GDELT / calendar | “Not configured” or empty query messages |

## Graphs

| Chart | Wired when |
|-------|----------------|
| Macro Relationship overlay | `macro_relationship_maps[market].available === true` and series merge succeeds |
| COT score bar (table view) | Row has `cot_score` |
| **Pending:** dedicated price/COT time-series charts beyond macro overlay | Not in this bundle |

## UI policy

- Anything not wired is labelled **Data source pending** or **source unavailable**, not fabricated intelligence.
