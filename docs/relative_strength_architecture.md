# FX + Intermarket Relative Strength Architecture

## Purpose

Directional **institutional context only** — not signals, not entries/exits, not predictions.

Answers: *Where is institutional pressure strongest and weakest right now?*

## Two-step model

### Step 1 — Currency leg scores (−100 … +100)

Each major currency with **direct CFTC COT** is scored from the latest calendar week confluence row:

| Component | Source (existing) | Weight (of leg score) |
|-----------|-------------------|------------------------|
| COT / positioning | `cot_score`, structural regime | ~30% |
| Flow momentum | L2 `flow_momentum`, `weekly_change` | ~25% |
| Macro alignment | L3 `macro_alignment_score`, rates backdrop | ~20% |
| Regime persistence | `weeks_in_regime`, pending flip | ~10% |
| Anomaly / transition | attention alerts, flow extremes | ~15% |

Penalties (subtracted):

- Crowding / exhaustion (L4)
- Macro contradiction (L3 `strong_contradiction`, `risk_off_pressure`)
- Missing / generic macro (`generic_rates_only`)
- Low data confidence (proxy-only, macro-only legs)

**USD** is **synthesized** from the seven G10 COT legs (inverse where the futures quote is USD/XXX). This is an approximation — documented in audit JSON as `synthetic_usd: true`, confidence capped.

**EM currencies** (TRY, ZAR, PLN, …) have **no direct COT** in this pipeline. They are **not** ranked on the main currency leaderboard until a reliable leg proxy exists. Pair rows that use only macro-only legs are flagged `low_confidence_cross`.

### Step 2 — Pair opportunities (differential)

**Display ranking uses `|raw_differential_score|`** (base leg − quote leg), not a conviction-filtered subset.

All **G10 × G10** crosses are audited (e.g. `GBP/JPY` even when not in the OANDA registry). Registry-only EM pairs are appended separately.

Each pair row in `pair_audit_all` includes: `raw_differential_score`, `adjusted_opportunity_score`, `downgrade_penalties`, `confidence_score`, `final_rank`, `display_exclusion_reason`, `included_in_display`.

For each cross `BASE/QUOTE`:

```
differential = score(BASE) − score(QUOTE)
```

- **Bullish pair bias** → base currency relatively stronger (institutional pressure)
- **Bearish** → quote currency relatively stronger
- **Conviction** from |differential| and `min(confidence_base, confidence_quote)`

| Conviction | |differential| | Min confidence |
|------------|----------------|----------------|
| HIGH | ≥ 35 | ≥ 0.65 |
| MEDIUM | ≥ 22 | ≥ 0.50 |
| LOW | ≥ 12 | ≥ 0.35 |
| WATCHLIST | ≥ 6 | any |

Crowding warning if either leg has exhaustion `euphoric_longs` / `crowded_shorts` aligned against the pair bias.

Momentum: **expanding** if weekly flow on both legs supports the differential direction; **fading** if flow conflicts.

## What we explicitly do NOT do

- MA crossovers, RSI, retail “strength meters”
- Trade levels, TP/SL, auto execution
- Fake precision on EM legs without COT

## Integration (extend, not replace)

| System | Role |
|--------|------|
| `institutional_context` | L1–L5 inputs per COT market |
| `regime_store` | Structural persistence |
| `macro_transmission` | Macro leg + generic-macro penalty |
| `priority_board` | Commodity/theme heatmap ranks (parallel) |
| `build_decision_table` | Writes `relative_strength_latest.json` |

## Outputs

- `data/relative_strength_latest.json`
- `web-dashboard/public/data/relative_strength_latest.json`

Sections: `currency_leaderboard`, `pair_opportunities`, `heatmap`, `commodity_ranks`, `audit`, `limitations`

## Migration from priority board

| Priority board | Relative strength |
|----------------|-------------------|
| Top 6 **markets** (any asset) | Top **FX pairs** by differential |
| Single-instrument attention | **Leg** scores then **spread** |
| Commodity COT names on board | Commodities in **heatmap** only |
| `priority_debug_latest.json` | Kept; RS is additive |

Scanner UI: **Relative Strength** panel above the narrative priority board.

## Reliability limits (honest)

1. **USD** is synthetic from G10 — not a standalone COT contract in this build.
2. **Crosses without both legs scored** (e.g. TRY/JPY) may be omitted or marked watchlist-only.
3. **JPY/CHF/CAD** signs are inverted from USD/XXX futures quotes — documented per leg in audit.
4. **Macro-only weeks** (rates only) compress macro_component and cap conviction.
