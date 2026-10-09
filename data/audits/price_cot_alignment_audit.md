# Price ↔ COT Alignment Audit

Generated: `2026-10-07T09:55:51.061044+00:00`
Max alignment gap: **5 calendar days**

## Summary

- Markets total: **26**
- PASS: **26**
- FAIL: **0**
- Gate open: **True**

## Frontend cache

- WeeklyOHLCStore cache bust: **PASS**
- `cache: 'no-store'`: True
- query bust `Date.now()`: True

## Per instrument

### NASDAQ / NQ — **PASS**

- Provider: `oanda`
- Symbol: `NAS100_USD`
- Raw daily date: `2026-10-05`
- Store weekly date: `2026-09-25`
- Weekly aggregation date: `2026-10-05`
- Workstation weekly date: `2026-10-05`
- COT date: `2026-09-29`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=30890.4 H=31400.8 L=30735.8 C=31288.0 (2026-10-05)

### S&P 500 / ES — **PASS**

- Provider: `oanda`
- Symbol: `SPX500_USD`
- Raw daily date: `2026-10-05`
- Store weekly date: `2026-09-25`
- Weekly aggregation date: `2026-10-05`
- Workstation weekly date: `2026-10-05`
- COT date: `2026-09-29`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=7737.4 H=7852.6 L=7713.6 C=7836.2 (2026-10-05)

### Dow / YM — **PASS**

- Provider: `oanda`
- Symbol: `US30_USD`
- Raw daily date: `2026-10-05`
- Store weekly date: `2026-09-25`
- Weekly aggregation date: `2026-10-05`
- Workstation weekly date: `2026-10-05`
- COT date: `2026-09-29`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=51269.5 H=51718.4 L=50889.0 C=51605.0 (2026-10-05)

### Euro FX / 6E — **PASS**

- Provider: `oanda`
- Symbol: `EUR_USD`
- Raw daily date: `2026-10-05`
- Store weekly date: `2026-09-25`
- Weekly aggregation date: `2026-10-05`
- Workstation weekly date: `2026-10-05`
- COT date: `2026-09-29`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=1.12596 H=1.12768 L=1.11612 C=1.126 (2026-10-05)

### British Pound / 6B — **PASS**

- Provider: `oanda`
- Symbol: `GBP_USD`
- Raw daily date: `2026-10-05`
- Store weekly date: `2026-09-25`
- Weekly aggregation date: `2026-10-05`
- Workstation weekly date: `2026-10-05`
- COT date: `2026-09-29`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=1.32427 H=1.32857 L=1.319 C=1.32752 (2026-10-05)

### Japanese Yen / 6J — **PASS**

- Provider: `yahoo_futures`
- Symbol: `6J=F`
- Raw daily date: `2026-10-07`
- Store weekly date: `2026-10-07`
- Weekly aggregation date: `2026-10-07`
- Workstation weekly date: `2026-10-07`
- COT date: `2026-09-29`
- Gap days: `8`
- Gap weeks: `1.14`
- Latest OHLC: O=0.006351000163704157 H=0.006366000045090914 L=0.006324999965727329 C=0.006347999908030033 (2026-10-07)

### Swiss Franc / 6S — **PASS**

- Provider: `yahoo_futures`
- Symbol: `6S=F`
- Raw daily date: `2026-10-07`
- Store weekly date: `2026-10-07`
- Weekly aggregation date: `2026-10-07`
- Workstation weekly date: `2026-10-07`
- COT date: `2026-09-29`
- Gap days: `8`
- Gap weeks: `1.14`
- Latest OHLC: O=1.2164499759674072 H=1.2178499698638916 L=1.2091000080108643 C=1.2105499505996704 (2026-10-07)

### Australian Dollar / 6A — **PASS**

- Provider: `oanda`
- Symbol: `AUD_USD`
- Raw daily date: `2026-10-05`
- Store weekly date: `2026-09-25`
- Weekly aggregation date: `2026-10-05`
- Workstation weekly date: `2026-10-05`
- COT date: `2026-09-29`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=0.69479 H=0.699 L=0.69325 C=0.69838 (2026-10-05)

### Canadian Dollar / 6C — **PASS**

- Provider: `yahoo_futures`
- Symbol: `6C=F`
- Raw daily date: `2026-10-07`
- Store weekly date: `2026-10-07`
- Weekly aggregation date: `2026-10-07`
- Workstation weekly date: `2026-10-07`
- COT date: `2026-09-29`
- Gap days: `8`
- Gap weeks: `1.14`
- Latest OHLC: O=0.7006999850273132 H=0.7063000202178955 L=0.7006999850273132 C=0.7053999900817871 (2026-10-07)

### NZ Dollar / 6N — **PASS**

- Provider: `oanda`
- Symbol: `NZD_USD`
- Raw daily date: `2026-10-05`
- Store weekly date: `2026-09-25`
- Weekly aggregation date: `2026-10-05`
- Workstation weekly date: `2026-10-05`
- COT date: `2026-09-29`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=0.56146 H=0.56284 L=0.55803 C=0.56238 (2026-10-05)

### Gold — **PASS**

- Provider: `oanda`
- Symbol: `XAU_USD`
- Raw daily date: `2026-10-05`
- Store weekly date: `2026-09-25`
- Weekly aggregation date: `2026-10-05`
- Workstation weekly date: `2026-10-05`
- COT date: `2026-09-29`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=4141.605 H=4184.385 L=4103.515 C=4163.855 (2026-10-05)

### Silver — **PASS**

- Provider: `oanda`
- Symbol: `XAG_USD`
- Raw daily date: `2026-10-05`
- Store weekly date: `2026-09-25`
- Weekly aggregation date: `2026-10-05`
- Workstation weekly date: `2026-10-05`
- COT date: `2026-09-29`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=60.7485 H=62.0385 L=60.2775 C=61.3495 (2026-10-05)

### Copper / HG — **PASS**

- Provider: `yahoo_futures`
- Symbol: `HG=F`
- Raw daily date: `2026-10-07`
- Store weekly date: `2026-10-07`
- Weekly aggregation date: `2026-10-07`
- Workstation weekly date: `2026-10-07`
- COT date: `2026-09-29`
- Gap days: `8`
- Gap weeks: `1.14`
- Latest OHLC: O=6.539999961853027 H=6.666999816894531 L=6.539999961853027 C=6.620999813079834 (2026-10-07)

### Crude Oil / CL — **PASS**

- Provider: `oanda`
- Symbol: `WTICO_USD`
- Raw daily date: `2026-10-05`
- Store weekly date: `2026-09-25`
- Weekly aggregation date: `2026-10-05`
- Workstation weekly date: `2026-10-05`
- COT date: `2026-09-29`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=93.644 H=93.972 L=88.657 C=91.718 (2026-10-05)

### Natural Gas / NG — **PASS**

- Provider: `oanda`
- Symbol: `NATGAS_USD`
- Raw daily date: `2026-10-05`
- Store weekly date: `2026-09-25`
- Weekly aggregation date: `2026-10-05`
- Workstation weekly date: `2026-10-05`
- COT date: `2026-09-29`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=2.914 H=3.032 L=2.899 C=3.02 (2026-10-05)

### Coffee — **PASS**

- Provider: `yahoo_futures`
- Symbol: `KC=F`
- Raw daily date: `2026-10-07`
- Store weekly date: `2026-10-07`
- Weekly aggregation date: `2026-10-07`
- Workstation weekly date: `2026-10-07`
- COT date: `2026-09-29`
- Gap days: `8`
- Gap weeks: `1.14`
- Latest OHLC: O=293.1499938964844 H=308.3500061035156 L=291.29998779296875 C=307.20001220703125 (2026-10-07)

### Cocoa — **PASS**

- Provider: `yahoo_futures`
- Symbol: `CC=F`
- Raw daily date: `2026-10-06`
- Store weekly date: `2026-10-06`
- Weekly aggregation date: `2026-10-06`
- Workstation weekly date: `2026-10-06`
- COT date: `2026-09-29`
- Gap days: `7`
- Gap weeks: `1.0`
- Latest OHLC: O=5658.0 H=5908.0 L=5567.0 C=5700.0 (2026-10-06)

### Cotton — **PASS**

- Provider: `yahoo_futures`
- Symbol: `CT=F`
- Raw daily date: `2026-10-07`
- Store weekly date: `2026-10-07`
- Weekly aggregation date: `2026-10-07`
- Workstation weekly date: `2026-10-07`
- COT date: `2026-09-29`
- Gap days: `8`
- Gap weeks: `1.14`
- Latest OHLC: O=77.12999725341797 H=78.93000030517578 L=77.12999725341797 C=80.75 (2026-10-07)

### Corn — **PASS**

- Provider: `yahoo_futures`
- Symbol: `ZC=F`
- Raw daily date: `2026-10-07`
- Store weekly date: `None`
- Weekly aggregation date: `2026-10-07`
- Workstation weekly date: `2026-10-07`
- COT date: `2026-09-29`
- Gap days: `8`
- Gap weeks: `1.14`
- Latest OHLC: O=4.975 H=5.0975 L=4.95 C=5.055 (2026-10-07)

### Wheat — **PASS**

- Provider: `oanda`
- Symbol: `WHEAT_USD`
- Raw daily date: `2026-10-05`
- Store weekly date: `2026-09-25`
- Weekly aggregation date: `2026-10-05`
- Workstation weekly date: `2026-10-05`
- COT date: `2026-09-29`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=6.698 H=6.936 L=6.688 C=6.933 (2026-10-05)

### Soybeans — **PASS**

- Provider: `oanda`
- Symbol: `SOYBN_USD`
- Raw daily date: `2026-10-05`
- Store weekly date: `2026-09-25`
- Weekly aggregation date: `2026-10-05`
- Workstation weekly date: `2026-10-05`
- COT date: `2026-09-29`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=12.696 H=12.973 L=12.664 C=12.97 (2026-10-05)

### Sugar — **PASS**

- Provider: `oanda`
- Symbol: `SUGAR_USD`
- Raw daily date: `2026-10-05`
- Store weekly date: `2026-09-25`
- Weekly aggregation date: `2026-10-05`
- Workstation weekly date: `2026-10-05`
- COT date: `2026-09-29`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=0.18956 H=0.19974 L=0.18932 C=0.1989 (2026-10-05)

### Platinum — **PASS**

- Provider: `oanda`
- Symbol: `XPT_USD`
- Raw daily date: `2026-10-05`
- Store weekly date: `2026-09-25`
- Weekly aggregation date: `2026-10-05`
- Workstation weekly date: `2026-10-05`
- COT date: `2026-09-29`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=1686.288 H=1729.148 L=1666.82 C=1693.943 (2026-10-05)

### Palladium — **PASS**

- Provider: `oanda`
- Symbol: `XPD_USD`
- Raw daily date: `2026-10-05`
- Store weekly date: `2026-09-25`
- Weekly aggregation date: `2026-10-05`
- Workstation weekly date: `2026-10-05`
- COT date: `2026-09-29`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=1163.058 H=1178.948 L=1145.256 C=1158.9 (2026-10-05)

### Bitcoin — **PASS**

- Provider: `oanda`
- Symbol: `BTC_USD`
- Raw daily date: `2026-10-05`
- Store weekly date: `2026-09-25`
- Weekly aggregation date: `2026-10-05`
- Workstation weekly date: `2026-10-05`
- COT date: `2026-09-29`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=85835.6 H=86999.0 L=84961.0 C=85602.0 (2026-10-05)

### US Dollar Index / DX — **PASS**

- Provider: `yahoo_futures`
- Symbol: `None`
- Raw daily date: `2026-10-07`
- Store weekly date: `2026-10-07`
- Weekly aggregation date: `2026-10-07`
- Workstation weekly date: `2026-10-07`
- COT date: `2026-09-29`
- Gap days: `8`
- Gap weeks: `1.14`
- Latest OHLC: O=101.91999816894531 H=102.54000091552734 L=101.75 C=102.14600372314453 (2026-10-07)

## OVERALL STATUS

PASS
