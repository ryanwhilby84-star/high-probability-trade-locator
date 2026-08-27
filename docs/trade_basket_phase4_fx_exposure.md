# Trade Basket — Phase 4 FX Trade Decomposition & Exposure Intelligence

## Purpose

Allow the Trade Basket Workstation to accept **complete FX pair trades**
(e.g. `AUD/NZD LONG`) instead of requiring users to enter currency-futures legs
manually. Shared currency exposure across distinct pairs is valid and must not
trigger duplicate-trade rejection.

## FX decomposition rules

For `BASE/QUOTE`:

| Trade | Base leg | Quote leg |
|---|---|---|
| LONG | +1 | −1 |
| SHORT | −1 | +1 |

Examples:

- `AUD/NZD LONG` → AUD +1, NZD −1
- `AUD/CHF LONG` → AUD +1, CHF −1
- `GBP/AUD LONG` → GBP +1, AUD −1
- `AUD/NZD SHORT` → AUD −1, NZD +1

Non-FX instruments (Gold, Corn, indices, crypto, …) are **not** decomposed.

## Exposure allocation formula

For FX trade with risk percent \(r\):

\[
w_{\mathrm{leg}} = \frac{r}{2}
\]

Signed allocation:

\[
e_{\mathrm{leg}} = \mathrm{sign}_{\mathrm{leg}} \times w_{\mathrm{leg}}
\]

## Net exposure formula

\[
\mathrm{net}_c = \sum_{\mathrm{trades}} e_{c,\mathrm{trade}}
\]

Display direction:

| Condition | Label |
|---|---|
| \(\mathrm{net}_c > 0\) | Long |
| \(\mathrm{net}_c < 0\) | Short |
| \(\lvert\mathrm{net}_c\rvert < 10^{-12}\) | Neutral |

## Gross exposure and dominant exposure

\[
\mathrm{gross} = \sum_c \lvert\mathrm{net}_c\rvert
\]

\[
\mathrm{share}_{\mathrm{dom}} = \frac{\lvert\mathrm{net}_{\mathrm{dom}}\rvert}{\mathrm{gross}}
\]

### Tie-breaking

Currencies are sorted by:

1. descending \(\lvert\mathrm{net}\rvert\)
2. ascending alphabetical currency code

The first row is the dominant currency. Ties at equal absolute net are therefore
broken alphabetically (e.g. AUD before CHF when both equal \(0.5\)).

Dominant Exposure is omitted when `has_fx_trades` is false, or when gross is zero
(fully offset basket).

## Pair-return correlation method

Trade-to-trade correlation uses the **FX pair return series** via Phase 1
(`get_instrument_correlation_map` / Pearson on aligned percentage returns).

Direction adjustment (unchanged Phase 2A):

```
adjusted = raw_pair_correlation × direction_A × direction_B
LONG = +1, SHORT = −1
```

Labels in Portfolio Intelligence use complete trade IDs
(`AUD/NZD LONG`, `AUD/CHF LONG`), never decomposed futures legs.

## Duplicate validation rules

| Situation | Behaviour |
|---|---|
| Same pair + same direction | **Rejected** (`duplicate_instrument_direction`) |
| Same pair + opposite directions | **Accepted**, warning `offsetting_same_instrument` |
| Distinct pairs sharing a currency leg | **Accepted** |

## Opposite-direction behaviour (Case D)

`AUD/NZD LONG` + `AUD/NZD SHORT`:

- Accepted as two offsetting trades
- Warning: `offsetting_same_instrument='AUD/NZD' (LONG vs SHORT)`
- Pair correlation: raw ≈ +1, adjusted ≈ −1
- Net AUD / NZD ≈ 0 → Neutral; no dominant exposure
- Diagnostic: opposing pair trades + fully offset currencies

## Mixed-asset handling

Example: `AUD/NZD LONG`, `Gold LONG`, `Corn SHORT`

- Portfolio intelligence includes all three instruments
- Currency Exposure lists only AUD and NZD
- Gold / Corn are not FX-decomposed

## Shared-exposure diagnostics (deterministic)

| Condition | Statement pattern |
|---|---|
| ≥2 trades share long currency \(c\) | `N trades share long {label} exposure.` |
| ≥2 trades share short currency \(c\) | `N trades share short {label} exposure.` |
| Opposing signs, \(\lvert\mathrm{net}\rvert\approx 0\) | `{label} exposure is fully offset.` |
| Opposing signs, residual net | `{label} exposure is partially offset.` |
| Same pair LONG+SHORT | `{PAIR} appears as both LONG and SHORT — opposing pair trades.` |
| Dominant share ≥ 0.45 | concentration in strength / weakness |
| FX present but no share/offset/oppose | `No meaningful shared currency exposure detected across trades.` |

No trade advice. No macroeconomic inference.

## Worked examples

### 1. Shared base — AUD/NZD LONG + AUD/CHF LONG (1% each)

| Currency | Net | Direction | Contributing trades |
|---|---|---|---|
| AUD | +1.00 | Long | AUD/NZD LONG, AUD/CHF LONG |
| CHF | −0.50 | Short | AUD/CHF LONG |
| NZD | −0.50 | Short | AUD/NZD LONG |

(CHF before NZD when \(|\mathrm{net}|\) ties — alphabetical tie-break.)

Dominant: **AUD LONG** — 50% of gross.

### 2. Fully offset AUD — AUD/NZD LONG + GBP/AUD LONG (1% each)

| Currency | Net | Direction |
|---|---|---|
| AUD | 0 | Neutral |
| GBP | +0.50 | Long |
| NZD | −0.50 | Short |

Diagnostic includes fully offset Australian-dollar exposure.

### 3. Opposite pair — AUD/NZD LONG + AUD/NZD SHORT (1% each)

Net AUD = 0, NZD = 0. Accepted with offsetting warning. No dominant exposure.

### 4. Mixed FX / commodity

`AUD/NZD LONG`, `Gold LONG`, `Corn SHORT`

Currency panel: AUD, NZD only. Phase 3 metrics use all three trades.

### 5. Unequal risk — AUD/NZD LONG 2% + AUD/CHF LONG 1%

| Currency | Net |
|---|---|
| AUD | +1.50 |
| NZD | −1.00 |
| CHF | −0.50 |

### 6. Tied absolute nets

`AUD/NZD LONG` 1% + `EUR/CHF LONG` 1% → all \(\lvert\mathrm{net}\rvert=0.5\).
Dominant = **AUD** by alphabetical tie-break.

## API fields

`POST /api/trade-basket` accepts `instrument_pair` or `instrument_id`.

```json
"currency_exposure": {
  "engine": "currency_exposure_v4",
  "has_fx_trades": true,
  "currencies": [...],
  "dominant_currency_exposure": {...},
  "diagnostics": ["..."],
  "trade_decompositions": [...],
  "method": {
    "dominant_tie_break": "max_abs_net_then_alphabetical_currency"
  }
}
```

`workstation_phase` becomes `"4"`.

## Modules

| Module | Role |
|---|---|
| `trade_basket/fx_decomposition.py` | Pair → signed currency legs |
| `trade_basket/currency_exposure.py` | Net exposure, dominant, diagnostics |
| `trade_basket/validator.py` | FX IDs + exact-duplicate rule |
| `trade_basket/pairs.py` | Allows same-instrument opposite directions |

## UI behaviour

- Basket Builder selects complete FX pairs or non-FX instruments
- LONG/SHORT + risk %; max five trades
- Calculate / Reset / loading / empty / validation errors / offsetting warnings
- Currency Exposure table sorted by absolute net
- Dominant block hidden when no FX trades or gross = 0
- Portfolio Intelligence labels use pair IDs

## Known limitations

- FX cross history may be short (~289 daily bars). Lookback 252 can fail overlap checks.
- Exposure units are risk-weighted half-legs, not beta- or dollar-neutral sizes.
- Diagnostics are mechanical only.
- Correlation matrix page still shows the standalone LEGACY_COT universe.

## Out of scope

COT validation, seasonality, valuation, central-bank / CPI / rate differentials,
macro recommendations, opportunity ranking, trade journalling, Supabase, auth.
