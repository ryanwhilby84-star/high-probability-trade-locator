# Correlation Matrix — Phase 1 Developer Documentation

Statistical correlation engine only. No portfolio analysis, COT, seasonality,
valuation, scoring, or trade recommendations.

## Package

`src/hptl/correlation_matrix/`

| Module | Role |
|---|---|
| `prices.py` | Load canonical daily closes (DX → ICE DX futures) |
| `returns.py` | Percentage returns; daily / weekly |
| `alignment.py` | Pairwise date inner-join; no forward fill |
| `methods.py` | Pearson (Spearman/Kendall registerable later) |
| `engine.py` | Full square matrix builder |
| `service.py` | UI / API payload shaping |

UI must not recompute correlations — it consumes `build_correlation_matrix_payload`.

## Data source

- Universe: `LEGACY_COT_MARKETS` from `hptl.markets.instrument_registry` (26 IDs).
- Prices: canonical timeline via `build_canonical_timeline(..., apply_supplements=False).daily_closes()`, same path as Seasonality Workstation.
- Identity: existing registry / `canonical_identity` — no duplicate market maps.
- `US Dollar Index / DX` resolves to ICE DX futures prices (never FRED broad USD proxy).

## Return calculation

1. Select frequency closes:
   - **Daily:** cleaned positive closes, sorted by date.
   - **Weekly:** last trading-day close of each ISO week (no forward fill).
2. Percentage returns: \( r_t = P_t / P_{t-1} - 1 \).
3. Non-finite / non-positive closes break the chain (no fabrication).

Correlations are **never** computed on price levels.

## Alignment logic

For each instrument pair:

1. Inner-join return series on shared dates only.
2. Drop NaN / non-finite returns.
3. Keep the most recent `lookback` overlapping observations.
4. If overlap `< lookback`, cell is `null` and a data-quality warning is logged.

No forward filling. No invented dates or values.

## Correlation calculation

- Method (Phase 1): **Pearson** sample correlation.
- Extension: `register_method(...)` in `methods.py` for Spearman / Kendall later.
- Diagonal: `1.0` when the instrument has a return series; otherwise `null`.
- Off-diagonal values clamped numerically into `[-1, 1]`.
- Matrix is forced symmetrical (`m[i][j] == m[j][i]`).

## Lookbacks

UI presets: `20`, `60`, `120`, `252`.

The engine accepts **any** positive integer lookback without code changes — presets are UI convenience only.

## API

```
GET /api/correlation-matrix?frequency=daily&lookback=60
```

CLI:

```bash
python scripts/build_correlation_matrix_payload.py daily 60
```

## EI route

```
#/correlation-matrix
```

## Limitations (Phase 1)

- Pearson only (no Spearman / Kendall yet).
- Equal-weighted overlapping windows; no exponential weighting.
- Pairwise deletion (not listwise across the whole matrix).
- Different pairs may use slightly different date windows.
- No significance tests, p-values, or rolling windows.
- No portfolio / diversification / COT / seasonality / valuation layers.
- Sparse history → `null` cells rather than short-window fallbacks.

## Validation

```bash
python -m pytest tests/test_correlation_matrix_engine.py -q
```

Checks: symmetry, diagonal = 1, bounds `[-1, 1]`, missing-data handling,
date alignment, run-to-run stability.
