# Trade Basket Mathematics — Phase 2A

Logic-only foundation for planned-trade direction-adjusted correlations.

No portfolio scoring, exposure analysis, polished UI, or written intelligence.

## Modules

`src/hptl/trade_basket/`

| Module | Responsibility |
|---|---|
| `models.py` | `TradeEntry`, `TradeBasketInput`, `TradePairResult`, `TradeBasketResult` |
| `validator.py` | `TradeBasketValidator` |
| `pairs.py` | `TradePairGenerator` |
| `calculator.py` | `DirectionAdjustedCorrelationCalculator` |
| `engine.py` | Orchestration |
| `service.py` | API / CLI payload |

## Trade input schema

```json
{
  "frequency": "daily",
  "lookback": 60,
  "trades": [
    {
      "instrument_id": "Gold",
      "direction": "LONG",
      "risk_percent": 1.0
    },
    {
      "instrument_id": "Silver",
      "direction": "SHORT",
      "risk_percent": 1.0
    }
  ]
}
```

- Maximum **5** populated trades.
- Empty slots (`null`, `{}`, missing `instrument_id`) are ignored.
- `risk_percent` defaults to `1.0`, is stored, and **does not** affect Phase 2A maths.
- Instruments must be in `LEGACY_COT_MARKETS`.

## Direction encoding

| Direction | Sign |
|---|---|
| LONG | +1 |
| SHORT | −1 |

## Pair-generation logic

Unique unordered pairs with `i < j`:

| Populated trades | Pairs |
|---|---|
| 2 | 1 |
| 3 | 3 |
| 4 | 6 |
| 5 | 10 |

No reversed duplicates. No self-pairs.

## Direction-adjustment formula

```
direction_adjusted_correlation = raw_correlation × direction_a × direction_b
```

Examples:

- Raw +, LONG/LONG → adjusted +
- Raw +, LONG/SHORT → adjusted −
- Raw −, LONG/LONG → adjusted −
- Raw −, LONG/SHORT → adjusted +
- Raw +, SHORT/SHORT → adjusted +

## Validation behaviour

Rejected / reported (no silent substitution):

- Unknown instrument IDs
- Direction not in `{LONG, SHORT}`
- More than five trades
- Same instrument twice (same or opposite directions)
- Missing / non-finite Phase 1 correlation
- Insufficient overlapping return history

## How Phase 1 is reused

Basket code calls:

`hptl.correlation_matrix.service.get_instrument_correlation_map`

then reads cells via `lookup_raw_correlation`.

It does **not**:

- Load price files
- Calculate returns
- Align dates independently
- Recalculate Pearson

Architectural test `test_8_phase1_reuse_architecture` asserts this.

## API

```
POST /api/trade-basket
Content-Type: application/json
```

CLI:

```bash
python scripts/build_trade_basket_payload.py request.json
```

## Minimal verification surface

Developer-only route: `#/trade-basket-verify`

Plain inputs + result table. Not Phase 2B UI.

## Limitations

- No risk weighting
- No diversification / portfolio scores
- No relationship labels or recommendations
- No COT / seasonality / valuation integration
- Opposite-direction same instrument rejected (Phase 2A policy)

## Tests

```bash
python -m pytest tests/test_trade_basket_phase2a.py -q
```

Covers Long/Long, Long/Short, Short/Short Gold–Silver, five-trade pair count,
empty slots, invalid direction, duplicates, Phase 1 reuse, live Phase 1 integration.
