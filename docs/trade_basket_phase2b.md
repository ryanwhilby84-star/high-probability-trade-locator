# Trade Basket Workstation — Phase 2B

Presentation layer for the Phase 2A trade-basket engine.

No correlation mathematics in the UI. No portfolio scoring or Phase 3 features.

## UI structure

Route: `#/trade-basket`

Three sections:

1. **Basket Builder** — up to five trade cards (instrument, LONG/SHORT, risk %, remove) + Add Trade
2. **Basket Controls** — Daily/Weekly, lookback 20/60/120/252, Calculate, Reset + summary (trades entered, pair count, frequency, lookback)
3. **Pairwise Relationship Table** — unique pairs from the engine; raw + direction-adjusted correlations (2 d.p.); sortable by absolute magnitude

Validation errors from the engine are shown in an alert panel.

## Components

| File | Role |
|---|---|
| `web-dashboard/src/pages/TradeBasketWorkstationPage.jsx` | Page shell / nav |
| `web-dashboard/src/trade_basket/TradeBasketWorkstation.jsx` | Workstation UI |
| `web-dashboard/src/trade_basket/tradeBasketWorkstation.css` | HPTL-aligned styles |

Legacy Phase 2A verify route `#/trade-basket-verify` redirects to the same workstation.

## API calls

```
POST /api/trade-basket
Content-Type: application/json

{
  "frequency": "daily",
  "lookback": 60,
  "trades": [
    { "instrument_id": "Gold", "direction": "LONG", "risk_percent": 1.0 },
    { "instrument_id": "Silver", "direction": "SHORT", "risk_percent": 1.0 }
  ]
}
```

React never computes Pearson or direction adjustment — it only renders the JSON response.

## Data flow

```
User edits card / controls
  → React state (trades, frequency, lookback)
  → debounced POST /api/trade-basket
  → Phase 2A service → Phase 1 correlation map
  → JSON pairs + summary
  → table + summary panel
```

## User interaction flow

1. Add up to five trades via **Add Trade**.
2. Choose instrument and LONG/SHORT (risk % stored only).
3. Changing trade, direction, frequency, or lookback auto-refreshes the pair table.
4. **Calculate** forces an immediate refresh.
5. **Reset Basket** clears all trades and restores default controls.
6. Click column headers to sort by raw or adjusted correlation (highest absolute first by default).

## Out of scope (later phases)

Diversification / portfolio scores, risk weighting, FX decomposition, COT / seasonality / valuation, save/load, AI explanations.
