# Macro Intelligence Engine — Architecture (Phase 5)

## Purpose

Deterministic **Macro Intelligence** summarises whether the macro backdrop
**supports**, **opposes**, or is **neutral** toward a selected instrument.

It does **not** predict prices and does **not** emit buy/sell signals.

Phase 5 delivers the **framework only**. Contributor modules return
`Unavailable` placeholders until later phases connect live data.

## Engine structure

```
src/hptl/macro_intelligence/
  models.py          # bias labels, MacroContributorResult, payload shape
  contributor.py     # MacroContributor protocol
  contributors/      # independent placeholder modules
  engine.py          # orchestration + overall bias aggregation
  service.py         # API / CLI payload builder
```

CLI: `scripts/build_macro_intelligence_payload.py`  
HTTP: `GET /api/macro-intelligence?instrument_id=Gold`  
UI: `#/macro-intelligence`

## Contributor interface

Each contributor implements:

| Field | Meaning |
|---|---|
| `name` | Display name |
| `status` | Bullish / Bearish / Neutral / Unavailable (etc.) |
| `strength` | Optional numeric strength (unused in Phase 5) |
| `summary` | Deterministic explanation string |
| `last_updated` | ISO timestamp or null |
| `weight` | Reserved for future aggregation (0.0 in Phase 5) |
| `contributor_id` | Stable machine id |

Protocol: `MacroContributor.evaluate(instrument_id) -> MacroContributorResult`

**Independence rule:** no contributor may depend on another contributor’s output.

## Initial contributors (placeholders)

1. Interest Rates  
2. Inflation  
3. Economic Growth  
4. Commodity Exposure  
5. Risk Sentiment  
6. Central Banks  
7. Government Bonds  
8. Dollar Environment  

Each currently returns:

- `status = Unavailable`
- deterministic summary stating the module is not yet connected
- `last_updated = null`

## Overall Macro Bias

Closed set (no probabilities):

- Strongly Bullish  
- Moderately Bullish  
- Neutral  
- Moderately Bearish  
- Strongly Bearish  

**Phase 5 aggregation rule:** if every contributor is `Unavailable` (or none
exist), overall bias is **Neutral**. Placeholders never invent lean.

## Data flow

```
UI instrument select
  → GET /api/macro-intelligence?instrument_id=…
  → build_macro_intelligence_payload.py
  → MacroIntelligenceEngine.analyse
       → for each contributor: evaluate(instrument_id) independently
       → aggregate_overall_bias(results)
  → JSON { overall_macro_bias, contributors[], notes[] }
  → Macro Intelligence page
```

## Future extension points

1. Replace a placeholder class with a live evaluator (same interface).  
2. Register additional contributors in `default_contributors()`.  
3. Extend `aggregate_overall_bias` to use `weight` and non-Unavailable statuses.  
4. Persist `last_updated` from source timestamps.  
5. Keep Macro Hub / DXY Macro Bias / FRED helpers as optional *data inputs*,
   not as this engine’s UI home.

## Out of scope (Phase 5)

Live rates / CPI / GDP / bonds analysis, COT / seasonality / valuation
integration, AI summaries, trade recommendations.

## Pillar position

Independent pillar alongside COT, Seasonality, Correlation, Location, and
future Valuation — not merged into Macro Hub.
