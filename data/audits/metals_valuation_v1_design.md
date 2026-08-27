# Metals Valuation V1 — Model Design

- Generated: `2026-07-06T18:42:45+00:00`
- Engine: **`metals_real_yield_v1`**
- Phase: V3.1 Metals

## Objective

Institutional macro fair value for precious and industrial metals using existing
FRED macro_cache infrastructure — no location percentile substitution.

## Input series

| Series | FRED ID | Role |
| --- | --- | --- |
| 10Y real yield | DFII10 | Primary discount-rate / opportunity-cost driver |
| Broad USD index | DTWEXBGS | Dollar overlay (fallback: DX canonical timeline) |
| Metal spot | canonical_price_timeline | Dependent variable |
| China manufacturing PMI | CHINAMANUFPMIMEI | Copper placeholder (V1.1 — not in regression until audit pass) |

## Regression

```
log(price) = β0 + β1·real_yield + β2·log(DXY) [+ β3·china_pmi when wired]
fair_value = exp(predicted log price at current macro)
deviation_pct = (spot − fair) / fair × 100
```

## Output labels

| Deviation | Label |
| --- | --- |
| ≤ −5% | Undervalued |
| −5% to +5% | Fair Value |
| ≥ +5% | Overvalued |

## Tier behaviour

| Tier | Markets | Extras |
| --- | --- | --- |
| Premium | Gold, Silver | Composite score from price/fair ratio percentile |
| Industrial | Copper / HG | China PMI architecture block (placeholder) |
| PGM | Platinum, Palladium | Residual percentile + macro regression |

## Trust grades

| Grade | Criteria |
| --- | --- |
| A | n ≥ 156 weeks, R² ≥ 0.15, macro inputs fresh |
| B | n ≥ 52 weeks, R² ≥ 0.08 |
| C | Below B thresholds — display with caution |

## Gates

- Minimum aligned weekly observations: **52**
- Minimum R²: **0.08**
- Does not modify confluence scoring, COT, seasonality, or dashboard layout.

## Wired markets

- **Gold**: dev 71.46% · fair 2404.9702 · spot 4123.605 · trust **A** · Overvalued
- **Silver**: dev 105.83% · fair 29.6223 · spot 60.973 · trust **A** · Overvalued
- **Copper / HG**: unavailable — Metals valuation unavailable — Model R² 0.002 below gate 0.08.
- **Platinum**: dev 45.35% · fair 1108.4254 · spot 1611.095 · trust **B** · Overvalued
- **Palladium**: dev 27.76% · fair 976.0355 · spot 1246.986 · trust **A** · Overvalued

## Backtest diagnostics (deviation vs forward return)

- **Gold**: R²=0.3666 · n=527 · trust A · 4W corr=-0.1097 · MAD=25.39%
- **Silver**: R²=0.2096 · n=526 · trust A · 4W corr=-0.0663 · MAD=33.23%
- **Copper / HG**: Model R² 0.002 below gate 0.08.
- **Platinum**: R²=0.0928 · n=527 · trust B · 4W corr=-0.1044 · MAD=17.25%
- **Palladium**: R²=0.4121 · n=527 · trust A · 4W corr=-0.2684 · MAD=20.97%
