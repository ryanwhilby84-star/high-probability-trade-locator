# FX Valuation V3.0 Audit — fx_carry_real_yield_v3

- Generated: 2026-07-11T14:18:48.124777+00:00
- Live wired: 0 / 9 live-scope pairs
- Model pass (all pairs): 11 / 13

## Live scope audit (dashboard + thesis)

| Pair | Spot | Fair Value | Deviation % | Confidence | Audit Status | PASS/FAIL |
|---|---:|---:|---:|---|---|---|
| EUR/USD | 1.14149 | — | — | None | PASS | **FAIL** |
| GBP/USD | 1.34021 | — | — | None | PASS | **FAIL** |
| AUD/USD | 0.69524 | — | — | None | PASS | **FAIL** |
| NZD/USD | 0.57630 | — | — | None | FAIL | **FAIL** |
| USD/JPY | 161.70500 | — | — | None | PASS | **FAIL** |
| USD/CHF | 0.80841 | — | — | None | PASS | **FAIL** |
| USD/CAD | 1.41574 | — | — | None | PASS | **FAIL** |
| EUR/GBP | 0.85173 | — | — | None | PASS | **FAIL** |
| EUR/AUD | 1.64175 | — | — | None | PASS | **FAIL** |

## All pairs (diagnostic)

| Pair | Spot obs | Yield obs | Policy obs | Aligned obs | R² | Fair Value | Deviation % | State | Confidence | PASS/FAIL |
|---|---:|---:|---:|---:|---:|---:|---:|---|---|---|
| EUR/USD | 2623 | 2588 | 2623 | 2623 | 0.0326 | — | — | Unavailable | None | **FAIL** |
| GBP/USD | 2624 | 2551 | 2624 | 2624 | 0.0814 | — | — | Unavailable | None | **FAIL** |
| AUD/USD | 2622 | 2548 | 2622 | 2622 | 0.3174 | — | — | Unavailable | None | **FAIL** |
| NZD/USD | 2632 | 0 | 15 | 0 | — | — | — | Unavailable | None | **FAIL** |
| USD/JPY | 2633 | 81 | 13 | 2633 | 0.6288 | — | — | Unavailable | None | **FAIL** |
| USD/CHF | 2628 | 2331 | 13 | 2628 | 0.1451 | — | — | Unavailable | None | **FAIL** |
| USD/CAD | 2634 | 2570 | 2634 | 2634 | 0.4977 | — | — | Unavailable | None | **FAIL** |
| EUR/JPY | 2623 | 76 | 13 | 2623 | 0.6087 | — | — | Unavailable | None | **FAIL** |
| AUD/JPY | 289 | 78 | 13 | 289 | 0.8803 | — | — | Unavailable | None | **FAIL** |
| NZD/JPY | 289 | 0 | 13 | 0 | — | — | — | Unavailable | None | **FAIL** |
| EUR/GBP | 2622 | 2614 | 2622 | 2622 | 0.1411 | — | — | Unavailable | None | **FAIL** |
| EUR/AUD | 289 | 2620 | 289 | 289 | 0.9032 | — | — | Unavailable | None | **FAIL** |
| GBP/JPY | 2624 | 78 | 13 | 2624 | 0.6948 | — | — | Unavailable | None | **FAIL** |

## Dashboard-eligible COT markets

None — no pairs passed live wiring gate.