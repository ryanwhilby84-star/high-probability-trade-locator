# Project Plan

## Project purpose

`high-probability-trade-locator` is a staged trading research automation system. The long-term goal is to combine market structure, macro context, volume/location analysis, and alerting into a clean scanner for high-probability swing trade areas.

The project starts with dependable data ingestion and spreadsheet outputs before any signal logic is added.

## Phase 1: COT spreadsheet updater

Build the first reliable automation module:

- Download official CFTC Commitments of Traders data.
- Save raw files for auditability.
- Parse and clean the data.
- Export formatted Excel workbooks.
- Generate a simple markdown summary.
- Add basic tests.

## Phase 2: Add more macro/fundamental data sources

Potential additions:

- Interest rates.
- Central-bank calendar data.
- Inflation, employment, GDP and PMI data.
- Economic calendar events.

Focus remains on clean ingestion and traceable outputs.

## Phase 3: Add price data collector

Add OHLCV market data collection for futures, FX, indices, commodities, and other target markets.

Requirements:

- Configurable symbol list.
- Repeatable historical downloads.
- Raw and processed storage.
- Clear source attribution.

## Phase 4: Add swing high / swing low scanner

Build a market-structure scanner that detects important swing highs and swing lows.

Initial scope:

- Daily and weekly timeframe support.
- Configurable lookback windows.
- No live execution.
- Scanner output as spreadsheet/CSV first.

## Phase 5: Add supply/demand and high volume node detection

Add location-based market analysis:

- Supply zones.
- Demand zones.
- Prior swing areas.
- High-volume nodes where volume profile data is available.
- Confluence scoring later, once raw calculations are validated.

## Phase 6: Add alerting through Telegram or another notification system

Add notifications only after the scanner logic is stable.

Potential channels:

- Telegram bot.
- Email.
- OneSignal or another push service.

Alerts should be auditable, deduplicated, and rate-limited.

## Phase 7: Add optional AI summary layer

Add AI-generated summaries only after deterministic data pipelines are working.

Potential uses:

- Weekly market summaries.
- Macro context summaries.
- COT positioning summaries.
- Human-readable explanation of scanner outputs.

The AI layer should never place trades and should not be required for the core scanner to work.
