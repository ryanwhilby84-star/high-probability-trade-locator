# FX Valuation V3.0 Audit — fx_carry_real_yield_v3

- Generated: 2026-08-15T07:26:13.974754+00:00
- Live wired: 0 / 9 live-scope pairs
- Model pass (all pairs): 8 / 13

## Live scope audit (dashboard + thesis)

| Pair | Spot | Fair Value | Deviation % | Confidence | Audit Status | PASS/FAIL |
|---|---:|---:|---:|---|---|---|
| EUR/USD | 1.15705 | — | — | None | PASS | **FAIL** |
| GBP/USD | 1.35336 | — | — | None | PASS | **FAIL** |
| AUD/USD | 0.69524 | — | — | None | PASS | **FAIL** |
| NZD/USD | 0.57630 | — | — | None | FAIL | **FAIL** |
| USD/JPY | 0.00628 | — | — | None | FAIL | **FAIL** |
| USD/CHF | 0.81341 | — | — | None | PASS | **FAIL** |
| USD/CAD | 1.38745 | — | — | None | PASS | **FAIL** |
| EUR/GBP | 0.85494 | — | — | None | PASS | **FAIL** |
| EUR/AUD | 1.64175 | — | — | None | PASS | **FAIL** |

## All pairs (diagnostic)

| Pair | Spot obs | Yield obs | Policy obs | Aligned obs | R² | Fair Value | Deviation % | State | Confidence | PASS/FAIL |
|---|---:|---:|---:|---:|---:|---:|---:|---|---|---|
| EUR/USD | 2648 | 2608 | 2648 | 2648 | 0.0333 | — | — | Unavailable | None | **FAIL** |
| GBP/USD | 2649 | 2551 | 2649 | 2649 | 0.0817 | — | — | Unavailable | None | **FAIL** |
| AUD/USD | 2647 | 2548 | 2647 | 2647 | 0.3098 | — | — | Unavailable | None | **FAIL** |
| NZD/USD | 2657 | 0 | 40 | 0 | — | — | — | Unavailable | None | **FAIL** |
| USD/JPY | 0 | 101 | 0 | 0 | — | — | — | Unavailable | None | **FAIL** |
| USD/CHF | 2653 | 2331 | 38 | 2653 | 0.1574 | — | — | Unavailable | None | **FAIL** |
| USD/CAD | 2659 | 2590 | 2659 | 2659 | 0.5077 | — | — | Unavailable | None | **FAIL** |
| EUR/JPY | 0 | 94 | 0 | 0 | — | — | — | Unavailable | None | **FAIL** |
| AUD/JPY | 289 | 79 | 13 | 289 | 0.8762 | — | — | Unavailable | None | **FAIL** |
| NZD/JPY | 289 | 0 | 13 | 0 | — | — | — | Unavailable | None | **FAIL** |
| EUR/GBP | 2647 | 2614 | 2647 | 2647 | 0.1410 | — | — | Unavailable | None | **FAIL** |
| EUR/AUD | 289 | 2620 | 289 | 289 | 0.9008 | — | — | Unavailable | None | **FAIL** |
| GBP/JPY | 0 | 79 | 0 | 0 | — | — | — | Unavailable | None | **FAIL** |

## Dashboard-eligible COT markets

None — no pairs passed live wiring gate.