# Trade Basket — Phase 3 Portfolio Intelligence

Deterministic portfolio mathematics on top of Phase 1 correlations and Phase 2
direction-adjusted pairs. No trade recommendations. No COT / seasonality /
valuation / FX-leg / AI features.

## Inputs

From Phase 2A basket result only:

- Populated trades: `instrument_id`, `direction`, `risk_percent`
- Pairs: `direction_adjusted_correlation` (and raw for display)

Phase 1/2 calculations are **not** recomputed here.

## Constants

| Constant | Default | Meaning |
|---|---|---|
| `EXPOSURE_CLUSTER_ABS_THRESHOLD` | `0.60` | Link trades into an **exposure cluster** when `\|ρ_adj\| ≥ threshold` |

Defined in `src/hptl/portfolio_intelligence/config.py`. Grouping must consume this
constant (not a hardcoded literal in call sites).

## Formulas

### Risk weights
\[
w_i = \frac{r_i}{\sum_k r_k}
\]
If \(\sum r = 0\), use equal weights \(w_i = 1/n\).

### Correlation matrix \(C\)
- \(C_{ii} = 1\)
- \(C_{ij} = \rho^{\mathrm{adj}}_{ij}\) (Phase 2 direction-adjusted correlation)

### Effective Independent Trades
\[
Q = \mathbf{w}^{\mathsf{T}} C \mathbf{w}
\]
\[
N_{\mathrm{eff}}^{\mathrm{raw}} = \frac{1}{\max(Q,\varepsilon)}\quad(\varepsilon=10^{-12})
\]
\[
N_{\mathrm{eff}} = \mathrm{clamp}(N_{\mathrm{eff}}^{\mathrm{raw}},\,1.0,\,n)
\]
where \(n\) = number of populated trades.

Reported to **1 decimal place**.

### Diversification Score (0–100)
\[
D =
\begin{cases}
0 & n \le 1 \\
100\cdot\mathrm{clamp}\!\left(\dfrac{N_{\mathrm{eff}}-1}{n-1},0,1\right) & n\ge 2
\end{cases}
\]

### Duplication Score (0–100)
\[
U = 100 - D
\]

### Risk concentration
- **Total planned risk** = \(\sum r_i\)
- **Exposure clusters** = connected components with \(|\rho_{ij}| \ge\) `EXPOSURE_CLUSTER_ABS_THRESHOLD`
- **Largest Exposure Cluster** = cluster with greatest risk sum (then size)
- **Largest risk concentration** = (cluster risk sum) / (total planned risk)

### Pair classification
Bands on \(|\rho_{\mathrm{adj}}|\):

| |ρ| | Strength |
|---|---|
| 0.80–1.00 | Very High |
| 0.60–0.79 | High |
| 0.40–0.59 | Moderate |
| 0.20–0.39 | Low |
| 0.00–0.19 | Minimal |

Negative ρ: same strength + negative relationship.

### Portfolio diagnostics
- **Highest correlated pair** — max signed \(\rho_{\mathrm{adj}}\)
- **Lowest correlated pair** — min signed \(\rho_{\mathrm{adj}}\)

Informational only.

---

## Worked examples

### Example A — Two perfectly correlated trades

Trades:

| Trade | Direction | Risk % |
|---|---|---|
| A | LONG | 1.0 |
| B | LONG | 1.0 |

Raw / adjusted correlation: \(\rho = \rho_{\mathrm{adj}} = 1.0\)

Weights: \(w = (0.5, 0.5)\)

\[
Q = 0.5\cdot0.5\cdot1 + 0.5\cdot0.5\cdot1 + 0.5\cdot0.5\cdot1 + 0.5\cdot0.5\cdot1 = 1.0
\]
\[
N_{\mathrm{eff}} = \mathrm{clamp}(1/1, 1, 2) = 1.0
\]
\[
D = 100\cdot\frac{1-1}{2-1} = 0,\quad U = 100
\]

Largest Exposure Cluster: {A LONG, B LONG} (linked at 1.0 ≥ 0.60), 100% of risk.

Highest = Lowest pair: A LONG × B LONG, adjusted +1.00

---

### Example B — Three uncorrelated trades

Trades A,B,C LONG, risk 1.0 each. All \(\rho_{\mathrm{adj}} = 0\).

Weights: \(w_i = 1/3\)

\[
Q = \sum_i w_i^2 = 3\cdot\frac{1}{9} = \frac{1}{3}
\]
\[
N_{\mathrm{eff}} = \mathrm{clamp}(3, 1, 3) = 3.0
\]
\[
D = 100,\quad U = 0
\]

Largest Exposure Cluster: single-trade clusters only (no edge ≥ 0.60).

---

### Example C — Five mixed trades

Trades T0…T4 LONG, risk 1.0 each.

Adjusted correlations (symmetric):

| Pair | ρ_adj |
|---|---|
| T0–T1 | 0.90 |
| T0–T2 | 0.85 |
| T1–T2 | 0.80 |
| T0–T3 | 0.10 |
| T0–T4 | 0.05 |
| T1–T3 | 0.12 |
| T1–T4 | 0.08 |
| T2–T3 | 0.10 |
| T2–T4 | 0.05 |
| T3–T4 | 0.20 |

Equal weights \(w_i = 0.2\).

Manual expansion of \(Q = \sum_i\sum_j w_i w_j\rho_{ij}\):

- Diagonal contribution: \(5 \times 0.04 \times 1 = 0.20\)
- Off-diagonal (each unique pair counted twice):  
  \(2\times0.04\times(0.90+0.85+0.80+0.10+0.05+0.12+0.08+0.10+0.05+0.20)\)  
  \(= 0.08 \times 3.25 = 0.26\)
- \(Q = 0.20 + 0.26 = 0.46\)

\[
N_{\mathrm{eff}}^{\mathrm{raw}} = 1/0.46 \approx 2.173913
\]
\[
N_{\mathrm{eff}} = \mathrm{clamp}(2.173913, 1, 5) \approx 2.173913
\]
Scores are computed from the unrounded \(N_{\mathrm{eff}}\), then rounded for display:

\[
D = 100\cdot\frac{2.173913-1}{5-1} \approx 29.3478 \rightarrow 29.3
\]
\[
U = 100 - 29.3478 \approx 70.6522 \rightarrow 70.7
\]
\[
N_{\mathrm{eff}}\ \text{(reported)} = 2.2
\]

Largest Exposure Cluster: {T0, T1, T2} (size 3; all pairwise \(\ge 0.60\)),  
risk sum \(3.0\), concentration \(3/5 = 0.60\).

Highest correlated pair: T0 LONG × T1 LONG, adjusted \(+0.90\) (Very High).  
Lowest correlated pair: T0 LONG × T4 LONG (or T2–T4), adjusted \(+0.05\) (Minimal).

Reproduce:

```python
from hptl.portfolio_intelligence.metrics import compute_portfolio_intelligence

trades = [
    {"instrument_id": f"T{i}", "direction": "LONG", "risk_percent": 1.0}
    for i in range(5)
]
rhos = {
    frozenset(("T0", "T1")): 0.90,
    frozenset(("T0", "T2")): 0.85,
    frozenset(("T1", "T2")): 0.80,
    frozenset(("T0", "T3")): 0.10,
    frozenset(("T0", "T4")): 0.05,
    frozenset(("T1", "T3")): 0.12,
    frozenset(("T1", "T4")): 0.08,
    frozenset(("T2", "T3")): 0.10,
    frozenset(("T2", "T4")): 0.05,
    frozenset(("T3", "T4")): 0.20,
}
pairs = []
ids = [f"T{i}" for i in range(5)]
for i in range(5):
    for j in range(i + 1, 5):
        rho = rhos[frozenset((ids[i], ids[j]))]
        pairs.append({
            "trade_a_instrument_id": ids[i],
            "trade_a_direction": "LONG",
            "trade_b_instrument_id": ids[j],
            "trade_b_direction": "LONG",
            "raw_correlation": rho,
            "direction_adjusted_correlation": rho,
        })
intel = compute_portfolio_intelligence(trades=trades, pairs=pairs)
assert intel["diagnostics"]["q"] == 0.46
assert intel["effective_independent_trades"] == 2.2
assert intel["diversification_score"] == 29.3
assert intel["duplication_score"] == 70.7
assert intel["largest_exposure_cluster"]["size"] == 3
```

---

## API / payload

Workstation CLI / `POST /api/trade-basket` returns Phase 2A fields unchanged, plus
(Example C shape):

```json
"portfolio_intelligence": {
  "engine": "portfolio_intelligence_v3",
  "status": "ok",
  "trades_entered": 5,
  "effective_independent_trades": 2.2,
  "diversification_score": 29.3,
  "duplication_score": 70.7,
  "largest_exposure_cluster": {
    "size": 3,
    "risk_percent_sum": 3.0,
    "members": ["T0 LONG", "T1 LONG", "T2 LONG"]
  },
  "highest_correlated_pair": {
    "trade_a_instrument_id": "T0",
    "trade_b_instrument_id": "T1",
    "direction_adjusted_correlation": 0.9
  },
  "lowest_correlated_pair": {
    "trade_a_instrument_id": "T0",
    "trade_b_instrument_id": "T4",
    "direction_adjusted_correlation": 0.05
  },
  "total_planned_risk": 5.0,
  "largest_risk_concentration": 0.6,
  "pair_classifications": [],
  "explanations": []
}
```

## Modules

`src/hptl/portfolio_intelligence/`

- `config.py` — thresholds / version
- `metrics.py` — mathematics
- `explanations.py` — deterministic text
- `service.py` — enrichment wrapper

## Out of scope

COT, seasonality, valuation, supply/demand, currency-leg decomposition,
macro themes, AI recommendations, trade ranking.
