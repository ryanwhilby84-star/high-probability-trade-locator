# Data Sources

## 1. CFTC Commitments of Traders data

Primary source for Phase 1.

Official CFTC pages:

- CFTC Commitments of Traders overview.
- CFTC Historical Compressed COT reports.
- CFTC COT Public Reporting Environment.

Current implementation uses the CFTC historical compressed ZIP files where possible. These are useful because they can be downloaded, stored, audited, and reprocessed.

Default report type:

- Disaggregated Futures Only.

Future supported report types:

- Legacy Futures Only.
- Traders in Financial Futures.
- Futures-and-options combined variants.

## 2. Interest rates placeholder

Potential future sources:

- Central bank public datasets.
- Government bond yield APIs.
- Treasury yield curve datasets.

Purpose:

- Add macro regime context.
- Track rate changes that may influence indices, currencies, metals, and bonds.

## 3. Economic calendar placeholder

Potential future sources:

- Public economic calendar providers.
- Paid API providers if licensing permits automated use.

Purpose:

- Avoid major scheduled event risk.
- Add context around CPI, rates, payrolls, GDP, PMI and other major releases.

## 4. Price data API placeholder

Potential future sources:

- Futures/FX/indices/commodities data APIs.
- Exchange-approved market data providers.
- Broker-independent data feeds where possible.

Purpose:

- Build OHLCV history.
- Support swing high / swing low detection.
- Support scanner backtests.

## 5. Volume profile data placeholder

Potential future sources:

- Market data vendors with volume-at-price support.
- Platform exports if licensing allows.
- Futures exchange data products.

Purpose:

- Detect high-volume nodes.
- Combine volume location with supply/demand and swing levels.


## CFTC Traders in Financial Futures; Futures Only historical compressed data

Source URL pattern:

- `https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip`

For 2026, the updater downloads:

- `https://www.cftc.gov/files/dea/history/fut_fin_txt_2026.zip`

Used for NASDAQ and S&P 500 historical backfill:

| Dashboard Name | CFTC Contract | Code |
|---|---|---|
| NASDAQ | E-MINI NASDAQ 100 - CHICAGO MERCANTILE EXCHANGE | 209742 |
| NASDAQ | NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE | 209742 |
| S&P 500 | E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE | 13874A |

This source provides all available weekly rows for the configured year. The updater warns if fewer than 5 rows are found for either NASDAQ or S&P 500. Four-week change is calculated in the exporter from the historical rows once each market has enough weeks available.

The parser is strict: rows must match the approved CME equity-index contract names and/or the approved CFTC contract codes with Chicago Mercantile Exchange context. This prevents unrelated ICE markets such as CAISO SP-15 or California Carbon from being treated as NASDAQ/S&P history.
