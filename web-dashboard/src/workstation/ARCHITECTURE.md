# Instrument Research Workstation — Architecture

UI-only layer for institutional instrument analysis. **No valuation, COT, or seasonality logic lives here.**

## Layer separation

| Layer | Location | Responsibility |
|-------|----------|----------------|
| **Data retrieval** | `hooks/` | Fetch static JSON (`prices_latest`, `instrument_valuation_history_latest`) |
| **Normalization** | `data/` | Weekly OHLC alignment, valuation join, ISO week derivation |
| **Timeline state** | `context/WeeklyTimelineContext.jsx` | Shared crosshair week for panel sync |
| **Rendering** | `charts/` | lightweight-charts candlesticks + fair-value line |
| **Layout** | `InstrumentWorkstationLayout.jsx` | Composes hooks → context → panels |

## Model calculation (external)

Point-in-time fair values are **precomputed** by:

```bash
python scripts/export_instrument_valuation_history_viz.py --market Gold
```

This calls `compute_valuation(market, as_of_week=date)` with no look-ahead. The UI only plots the exported series.

## Adding a new valuation model (Gold v2, FX v2, …)

1. Implement model in `src/hptl/valuation/` (unchanged process).
2. Ensure `compute_valuation(..., as_of_week=)` supports point-in-time truncation.
3. Re-run visualization export — fair-value points appear on the chart automatically.
4. No workstation chart rewrite required.

## Future panel sync

Panels read `useWeeklyTimeline()` or `useWeeklyTimelineOptional()`:

- `activeWeekDate` — ISO date under crosshair
- `activeRow` — OHLC + fair value + deviation for that week
- `setCrosshairTime` — programmatic focus (future)

Planned: commercial / non-commercial / retail positioning, seasonality, supply & demand.
