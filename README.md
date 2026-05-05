# High Probability Trade Locator

Foundation repository for a trading data automation and high-probability swing trade scanner.

The first working module is a **CFTC Commitments of Traders (COT) spreadsheet updater**. It downloads official CFTC historical compressed data, parses and cleans it with pandas, then exports a formatted Excel workbook.

This project does **not** include live trading execution, broker integrations, AI agent logic, or complex trading signals yet.

## Tech stack

- Python 3.11+
- pandas
- openpyxl
- requests
- python-dotenv
- pytest

## Setup

```bash
python -m venv .venv
```

Activate the environment:

Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

macOS/Linux:

```bash
source .venv/bin/activate
```

Install requirements:

```bash
pip install -r requirements.txt
```

Optional editable install:

```bash
pip install -e .
```

## Environment configuration

Copy the example file:

```bash
cp .env.example .env
```

The updater runs without editing `.env`. You can override the COT report type and year if needed.

## Run the COT updater

From the repository root:

```bash
python -m hptl.cot.run_update
```

Alternative after editable install:

```bash
hptl-cot-update
```

## Outputs

- Raw CFTC downloads: `data/raw/`
- Processed data: `data/processed/`
- Excel exports: `data/exports/`

The Excel workbook filename includes the current date, for example:

```text
data/exports/cot_update_2026-05-02.xlsx
```

## What the first module does

1. Downloads the latest configured CFTC COT historical compressed file.
2. Saves the raw ZIP to `data/raw/`.
3. Reads the contained CSV/TXT data into pandas.
4. Cleans column names into snake_case.
5. Adds helper columns for future market mapping.
6. Exports a formatted Excel workbook with:
   - bold headers
   - frozen top row
   - autofilter
   - sensible column widths
   - summary tab
   - cleaned COT data tab
7. Prints and saves a markdown update summary.

## Run tests

```bash
pytest
```

## Future roadmap

See `docs/PROJECT_PLAN.md` for the full phased roadmap.

Immediate next build steps:

- Add more macro/fundamental data sources.
- Add price data collection.
- Add swing high / swing low scanner.
- Add supply/demand and high-volume-node detection.
- Add Telegram alerting.
- Add optional AI summary layer later.

## Safety boundary

This repository is for data automation and trade research only. It intentionally avoids execution systems, brokerage APIs, and automated order placement.


## Latest dashboard update

The COT updater now also downloads the official CFTC historical compressed **Traders in Financial Futures; Futures Only** file for the configured year. This backfills NASDAQ and S&P 500 history from January 2026 through the latest available report week when `COT_YEAR=2026`:

- `https://www.cftc.gov/files/dea/history/fut_fin_txt_2026.zip`

The live CME Futures Only page remains documented as the current report reference, but index history comes from the compressed annual Financial Futures dataset. These mapped CME equity index futures are added to the Excel workbook:

| Dashboard Name | CFTC Contract | Code |
|---|---|---|
| NASDAQ | E-MINI NASDAQ 100 - CHICAGO MERCANTILE EXCHANGE | 209742 |
| NASDAQ | NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE | 209742 |
| S&P 500 | E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE | 13874A |

The workbook keeps these tabs. `Trader_Report` is the master calculated table; `Market_Blocks` is only a formatted view of `Trader_Report`, and `Data_Checks` reports row coverage/calculation status for the required markets:

- Dashboard
- Trader_Report
- Market_Blocks
- Raw_Data_Slim
- Source_Notes
- Data_Checks

Run it with:

```bash
python -m hptl.cot.run_update
```

## Layer 2: Macro / Interest Rates Context

Run the macro/rates layer with:

```bash
python -m hptl.macro.run_macro_update
```

The macro layer is a regime/context filter only. It does not create standalone buy/sell signals. Technicals locate the trade; macro/rates context filters, weights, or qualifies trade quality.

Outputs are saved as timestamped Excel files in:

```text
data/exports/macro_output_YYYYMMDD_HHMMSS.xlsx
```

Sheets:

- `Rates_History` — full recent daily FRED rates history from 2025-01-01 onward, with valid/invalid scoring rows marked.
- `Macro_Dashboard` — latest usable complete macro snapshot, not a misleading incomplete current row.
- `Macro_Score` — macro/rates context history with signal, score, strength, and trade-context fields.
- `Macro_Source_Notes` — source notes, fail-closed rules, FRED/H.15 explanation, and future ratio/news placeholders.

Required core series for scoring:

- `DGS2`
- `DGS10`
- `DGS30`

Supporting series:

- `DFF` / `fed_funds` — historical effective federal funds rate only, not real-time policy expectations.
- `T10Y2Y` — 10Y minus 2Y spread where available.

The layer fails closed: no `macro_score` is produced unless all required yield and directional-change fields are available.
