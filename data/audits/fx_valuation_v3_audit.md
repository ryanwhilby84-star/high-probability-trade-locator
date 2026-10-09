# FX Valuation V3.0 Audit — fx_carry_real_yield_v3

- Generated: 2026-10-07T07:35:33.787729+00:00
- Live wired: 0 / 9 live-scope pairs
- Model pass (all pairs): 8 / 13

## Live scope audit (dashboard + thesis)

| Pair | Spot | Fair Value | Deviation % | Confidence | Audit Status | PASS/FAIL |
|---|---:|---:|---:|---|---|---|
| EUR/USD | 1.12147 | — | — | None | PASS | **FAIL** |
| GBP/USD | 1.32499 | — | — | None | PASS | **FAIL** |
| AUD/USD | 0.71236 | — | — | None | PASS | **FAIL** |
| NZD/USD | 0.58085 | — | — | None | FAIL | **FAIL** |
| USD/JPY | 0.00635 | — | — | None | FAIL | **FAIL** |
| USD/CHF | 1.21055 | — | — | None | PASS | **FAIL** |
| USD/CAD | 0.70540 | — | — | None | PASS | **FAIL** |
| EUR/GBP | 0.85816 | — | — | None | PASS | **FAIL** |
| EUR/AUD | 1.62401 | — | — | None | PASS | **FAIL** |

## All pairs (diagnostic)

| Pair | Spot obs | Yield obs | Policy obs | Aligned obs | R² | Fair Value | Deviation % | State | Confidence | PASS/FAIL |
|---|---:|---:|---:|---:|---:|---:|---:|---|---|---|
| EUR/USD | 5029 | 2608 | 2738 | 2790 | 0.0318 | — | — | Unavailable | None | **FAIL** |
| GBP/USD | 5029 | 2551 | 2792 | 2791 | 0.0495 | — | — | Unavailable | None | **FAIL** |
| AUD/USD | 5029 | 2548 | 2790 | 2789 | 0.2886 | — | — | Unavailable | None | **FAIL** |
| NZD/USD | 5029 | 0 | 77 | 0 | — | — | — | Unavailable | None | **FAIL** |
| USD/JPY | 0 | 101 | 0 | 0 | — | — | — | Unavailable | None | **FAIL** |
| USD/CHF | 6488 | 2331 | 75 | 2707 | 0.2008 | — | — | Unavailable | None | **FAIL** |
| USD/CAD | 6510 | 2590 | 2706 | 2706 | 0.5059 | — | — | Unavailable | None | **FAIL** |
| EUR/JPY | 0 | 94 | 0 | 0 | — | — | — | Unavailable | None | **FAIL** |
| AUD/JPY | 5004 | 79 | 50 | 2765 | 0.6649 | — | — | Unavailable | None | **FAIL** |
| NZD/JPY | 5004 | 0 | 50 | 0 | — | — | — | Unavailable | None | **FAIL** |
| EUR/GBP | 5004 | 2614 | 2715 | 2767 | 0.0238 | — | — | Unavailable | None | **FAIL** |
| EUR/AUD | 5004 | 2620 | 2713 | 2765 | 0.2107 | — | — | Unavailable | None | **FAIL** |
| GBP/JPY | 0 | 79 | 0 | 0 | — | — | — | Unavailable | None | **FAIL** |

## Dashboard-eligible COT markets

None — no pairs passed live wiring gate.