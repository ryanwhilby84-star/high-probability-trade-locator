# Price ↔ COT Alignment Audit

Generated: `2026-08-15T07:30:26.279520+00:00`
Max alignment gap: **5 calendar days**

## Summary

- Markets total: **26**
- PASS: **0**
- FAIL: **26**
- Gate open: **False**

## Frontend cache

- WeeklyOHLCStore cache bust: **PASS**
- `cache: 'no-store'`: True
- query bust `Date.now()`: True

## Per instrument

### NASDAQ / NQ — **FAIL**

- Provider: `oanda`
- Symbol: `NAS100_USD`
- Raw daily date: `2026-07-30`
- Store weekly date: `2026-07-24`
- Weekly aggregation date: `2026-07-30`
- Workstation weekly date: `2026-07-24`
- COT date: `2026-08-11`
- Gap days: `18`
- Gap weeks: `2.57`
- Latest OHLC: O=28511.2 H=28627.7 L=27093.2 C=28191.8 (2026-07-24)
- Pipeline break: `provider_series_cross_check`
- FAIL: price behind COT by 18d exceeds max 5d (weekly=2026-07-24 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-24 trails provider tip 2026-08-07 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### S&P 500 / ES — **FAIL**

- Provider: `oanda`
- Symbol: `SPX500_USD`
- Raw daily date: `2026-07-30`
- Store weekly date: `2026-07-24`
- Weekly aggregation date: `2026-07-30`
- Workstation weekly date: `2026-07-24`
- COT date: `2026-08-11`
- Gap days: `18`
- Gap weeks: `2.57`
- Latest OHLC: O=7474.2 H=7518.2 L=7299.6 C=7483.6 (2026-07-24)
- Pipeline break: `provider_series_cross_check`
- FAIL: price behind COT by 18d exceeds max 5d (weekly=2026-07-24 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-24 trails provider tip 2026-08-07 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Dow / YM — **FAIL**

- Provider: `oanda`
- Symbol: `US30_USD`
- Raw daily date: `2026-07-30`
- Store weekly date: `2026-07-24`
- Weekly aggregation date: `2026-07-30`
- Workstation weekly date: `2026-07-24`
- COT date: `2026-08-11`
- Gap days: `18`
- Gap weeks: `2.57`
- Latest OHLC: O=52278.8 H=52960.6 L=51498.5 C=52420.3 (2026-07-24)
- Pipeline break: `provider_series_cross_check`
- FAIL: price behind COT by 18d exceeds max 5d (weekly=2026-07-24 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-24 trails provider tip 2026-08-07 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Euro FX / 6E — **FAIL**

- Provider: `oanda`
- Symbol: `EUR_USD`
- Raw daily date: `2026-08-13`
- Store weekly date: `2026-08-07`
- Weekly aggregation date: `2026-08-13`
- Workstation weekly date: `2026-07-24`
- COT date: `2026-08-11`
- Gap days: `18`
- Gap weeks: `2.57`
- Latest OHLC: O=1.13958 H=1.15476 L=1.13532 C=1.15306 (2026-07-24)
- Pipeline break: `provider_series_cross_check`
- FAIL: workstation weekly tip matches neither store native weekly nor derived weekly (ws=2026-07-24 store_weekly=2026-08-07 derived=2026-08-13)
- FAIL: price behind COT by 18d exceeds max 5d (weekly=2026-07-24 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-24 trails provider tip 2026-08-07 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### British Pound / 6B — **FAIL**

- Provider: `oanda`
- Symbol: `GBP_USD`
- Raw daily date: `2026-08-13`
- Store weekly date: `2026-08-07`
- Weekly aggregation date: `2026-08-13`
- Workstation weekly date: `2026-07-24`
- COT date: `2026-08-11`
- Gap days: `18`
- Gap weeks: `2.57`
- Latest OHLC: O=1.33478 H=1.34954 L=1.32735 C=1.34836 (2026-07-24)
- Pipeline break: `provider_series_cross_check`
- FAIL: workstation weekly tip matches neither store native weekly nor derived weekly (ws=2026-07-24 store_weekly=2026-08-07 derived=2026-08-13)
- FAIL: price behind COT by 18d exceeds max 5d (weekly=2026-07-24 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-24 trails provider tip 2026-08-07 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Japanese Yen / 6J — **FAIL**

- Provider: `yahoo_futures`
- Symbol: `6J=F`
- Raw daily date: `2026-08-14`
- Store weekly date: `2026-08-14`
- Weekly aggregation date: `2026-08-14`
- Workstation weekly date: `2026-07-30`
- COT date: `2026-08-11`
- Gap days: `12`
- Gap weeks: `1.71`
- Latest OHLC: O=0.006120999809354544 H=0.006337999831885099 L=0.00610999995842576 C=0.006300999782979488 (2026-07-30)
- Pipeline break: `provider_series_cross_check`
- FAIL: workstation weekly tip matches neither store native weekly nor derived weekly (ws=2026-07-30 store_weekly=2026-08-14 derived=2026-08-14)
- FAIL: price behind COT by 12d exceeds max 5d (weekly=2026-07-30 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-30 trails provider tip 2026-08-07 by 8d
- FAIL: OHLC mismatch near 2026-07-31 (high): provider=0.006360999774187803 workstation=0.006337999831885099 (ws_date=2026-07-30)
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Swiss Franc / 6S — **FAIL**

- Provider: `oanda`
- Symbol: `USD_CHF`
- Raw daily date: `2026-08-13`
- Store weekly date: `2026-08-07`
- Weekly aggregation date: `2026-08-13`
- Workstation weekly date: `2026-07-24`
- COT date: `2026-08-11`
- Gap days: `18`
- Gap weeks: `2.57`
- Latest OHLC: O=0.816 H=0.8207 L=0.80391 C=0.80705 (2026-07-24)
- Pipeline break: `provider_series_cross_check`
- FAIL: workstation weekly tip matches neither store native weekly nor derived weekly (ws=2026-07-24 store_weekly=2026-08-07 derived=2026-08-13)
- FAIL: price behind COT by 18d exceeds max 5d (weekly=2026-07-24 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-24 trails provider tip 2026-08-07 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Australian Dollar / 6A — **FAIL**

- Provider: `oanda`
- Symbol: `AUD_USD`
- Raw daily date: `2026-08-13`
- Store weekly date: `2026-08-07`
- Weekly aggregation date: `2026-08-13`
- Workstation weekly date: `2026-07-24`
- COT date: `2026-08-11`
- Gap days: `18`
- Gap weeks: `2.57`
- Latest OHLC: O=0.70016 H=0.70447 L=0.69222 C=0.70302 (2026-07-24)
- Pipeline break: `provider_series_cross_check`
- FAIL: workstation weekly tip matches neither store native weekly nor derived weekly (ws=2026-07-24 store_weekly=2026-08-07 derived=2026-08-13)
- FAIL: price behind COT by 18d exceeds max 5d (weekly=2026-07-24 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-24 trails provider tip 2026-08-07 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Canadian Dollar / 6C — **FAIL**

- Provider: `oanda`
- Symbol: `USD_CAD`
- Raw daily date: `2026-08-13`
- Store weekly date: `2026-08-07`
- Weekly aggregation date: `2026-08-13`
- Workstation weekly date: `2026-07-24`
- COT date: `2026-08-11`
- Gap days: `18`
- Gap weeks: `2.57`
- Latest OHLC: O=1.40884 H=1.41292 L=1.3991 C=1.40136 (2026-07-24)
- Pipeline break: `provider_series_cross_check`
- FAIL: workstation weekly tip matches neither store native weekly nor derived weekly (ws=2026-07-24 store_weekly=2026-08-07 derived=2026-08-13)
- FAIL: price behind COT by 18d exceeds max 5d (weekly=2026-07-24 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-24 trails provider tip 2026-08-07 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### NZ Dollar / 6N — **FAIL**

- Provider: `oanda`
- Symbol: `NZD_USD`
- Raw daily date: `2026-08-13`
- Store weekly date: `2026-08-07`
- Weekly aggregation date: `2026-08-13`
- Workstation weekly date: `2026-07-24`
- COT date: `2026-08-11`
- Gap days: `18`
- Gap weeks: `2.57`
- Latest OHLC: O=0.58035 H=0.58958 L=0.5762 C=0.58892 (2026-07-24)
- Pipeline break: `provider_series_cross_check`
- FAIL: workstation weekly tip matches neither store native weekly nor derived weekly (ws=2026-07-24 store_weekly=2026-08-07 derived=2026-08-13)
- FAIL: price behind COT by 18d exceeds max 5d (weekly=2026-07-24 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-24 trails provider tip 2026-08-07 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Gold — **FAIL**

- Provider: `oanda`
- Symbol: `XAU_USD`
- Raw daily date: `2026-07-30`
- Store weekly date: `2026-07-24`
- Weekly aggregation date: `2026-07-30`
- Workstation weekly date: `2026-07-24`
- COT date: `2026-08-11`
- Gap days: `18`
- Gap weeks: `2.57`
- Latest OHLC: O=4090.16 H=4120.495 L=3996.055 C=4045.165 (2026-07-24)
- Pipeline break: `provider_series_cross_check`
- FAIL: price behind COT by 18d exceeds max 5d (weekly=2026-07-24 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-24 trails provider tip 2026-08-07 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Silver — **FAIL**

- Provider: `oanda`
- Symbol: `XAG_USD`
- Raw daily date: `2026-07-30`
- Store weekly date: `2026-07-24`
- Weekly aggregation date: `2026-07-30`
- Workstation weekly date: `2026-07-24`
- COT date: `2026-08-11`
- Gap days: `18`
- Gap weeks: `2.57`
- Latest OHLC: O=59.44875 H=60.0955 L=56.6405 C=57.6325 (2026-07-24)
- Pipeline break: `provider_series_cross_check`
- FAIL: price behind COT by 18d exceeds max 5d (weekly=2026-07-24 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-24 trails provider tip 2026-08-07 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Copper / HG — **FAIL**

- Provider: `yahoo_futures`
- Symbol: `HG=F`
- Raw daily date: `2026-07-31`
- Store weekly date: `2026-07-31`
- Weekly aggregation date: `2026-07-31`
- Workstation weekly date: `2026-07-31`
- COT date: `2026-08-11`
- Gap days: `11`
- Gap weeks: `1.57`
- Latest OHLC: O=6.340000152587891 H=6.515500068664551 L=6.253499984741211 C=6.435999870300293 (2026-07-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: price behind COT by 11d exceeds max 5d (weekly=2026-07-31 cot=2026-08-11)
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Crude Oil / CL — **FAIL**

- Provider: `oanda`
- Symbol: `WTICO_USD`
- Raw daily date: `2026-07-30`
- Store weekly date: `2026-07-24`
- Weekly aggregation date: `2026-07-30`
- Workstation weekly date: `2026-07-24`
- COT date: `2026-08-11`
- Gap days: `18`
- Gap weeks: `2.57`
- Latest OHLC: O=84.55 H=87.364 L=78.355 C=86.921 (2026-07-24)
- Pipeline break: `provider_series_cross_check`
- FAIL: price behind COT by 18d exceeds max 5d (weekly=2026-07-24 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-24 trails provider tip 2026-08-07 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Natural Gas / NG — **FAIL**

- Provider: `oanda`
- Symbol: `NATGAS_USD`
- Raw daily date: `2026-07-30`
- Store weekly date: `2026-07-24`
- Weekly aggregation date: `2026-07-30`
- Workstation weekly date: `2026-07-24`
- COT date: `2026-08-11`
- Gap days: `18`
- Gap weeks: `2.57`
- Latest OHLC: O=2.87 H=2.87 L=2.676 C=2.799 (2026-07-24)
- Pipeline break: `provider_series_cross_check`
- FAIL: price behind COT by 18d exceeds max 5d (weekly=2026-07-24 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-24 trails provider tip 2026-08-07 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Coffee — **FAIL**

- Provider: `yahoo_futures`
- Symbol: `KC=F`
- Raw daily date: `2026-07-31`
- Store weekly date: `2026-07-31`
- Weekly aggregation date: `2026-07-31`
- Workstation weekly date: `2026-07-31`
- COT date: `2026-08-11`
- Gap days: `11`
- Gap weeks: `1.57`
- Latest OHLC: O=314.0 H=346.6499938964844 L=313.8999938964844 C=332.1000061035156 (2026-07-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: price behind COT by 11d exceeds max 5d (weekly=2026-07-31 cot=2026-08-11)
- FAIL: missing workstation weekly candle for provider week 2026-06-25
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Cocoa — **FAIL**

- Provider: `yahoo_futures`
- Symbol: `CC=F`
- Raw daily date: `2026-07-31`
- Store weekly date: `2026-07-31`
- Weekly aggregation date: `2026-07-31`
- Workstation weekly date: `2026-07-31`
- COT date: `2026-08-11`
- Gap days: `11`
- Gap weeks: `1.57`
- Latest OHLC: O=5332.0 H=5561.0 L=4992.0 C=5397.0 (2026-07-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: price behind COT by 11d exceeds max 5d (weekly=2026-07-31 cot=2026-08-11)
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Cotton — **FAIL**

- Provider: `yahoo_futures`
- Symbol: `CT=F`
- Raw daily date: `2026-07-31`
- Store weekly date: `2026-07-31`
- Weekly aggregation date: `2026-07-31`
- Workstation weekly date: `2026-07-31`
- COT date: `2026-08-11`
- Gap days: `11`
- Gap weeks: `1.57`
- Latest OHLC: O=78.62999725341797 H=80.48999786376953 L=77.86000061035156 C=80.5 (2026-07-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: price behind COT by 11d exceeds max 5d (weekly=2026-07-31 cot=2026-08-11)
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Corn — **FAIL**

- Provider: `alpha_vantage`
- Symbol: `ZC=F`
- Raw daily date: `2026-07-31`
- Store weekly date: `None`
- Weekly aggregation date: `2026-07-31`
- Workstation weekly date: `2026-07-31`
- COT date: `2026-08-11`
- Gap days: `11`
- Gap weeks: `1.57`
- Latest OHLC: O=4.5925 H=4.6225 L=4.38 C=4.4075 (2026-07-31)
- Pipeline break: `alignment`
- FAIL: price behind COT by 11d exceeds max 5d (weekly=2026-07-31 cot=2026-08-11)

### Wheat — **FAIL**

- Provider: `oanda`
- Symbol: `WHEAT_USD`
- Raw daily date: `2026-07-30`
- Store weekly date: `2026-07-24`
- Weekly aggregation date: `2026-07-30`
- Workstation weekly date: `2026-07-24`
- COT date: `2026-08-11`
- Gap days: `18`
- Gap weeks: `2.57`
- Latest OHLC: O=6.673 H=6.794 L=6.284 C=6.319 (2026-07-24)
- Pipeline break: `provider_series_cross_check`
- FAIL: price behind COT by 18d exceeds max 5d (weekly=2026-07-24 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-24 trails provider tip 2026-08-07 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Soybeans — **FAIL**

- Provider: `oanda`
- Symbol: `SOYBN_USD`
- Raw daily date: `2026-07-30`
- Store weekly date: `2026-07-24`
- Weekly aggregation date: `2026-07-30`
- Workstation weekly date: `2026-07-24`
- COT date: `2026-08-11`
- Gap days: `18`
- Gap weeks: `2.57`
- Latest OHLC: O=12.225 H=12.294 L=11.622 C=11.691 (2026-07-24)
- Pipeline break: `provider_series_cross_check`
- FAIL: price behind COT by 18d exceeds max 5d (weekly=2026-07-24 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-24 trails provider tip 2026-08-07 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Sugar — **FAIL**

- Provider: `oanda`
- Symbol: `SUGAR_USD`
- Raw daily date: `2026-07-30`
- Store weekly date: `2026-07-24`
- Weekly aggregation date: `2026-07-30`
- Workstation weekly date: `2026-07-24`
- COT date: `2026-08-11`
- Gap days: `18`
- Gap weeks: `2.57`
- Latest OHLC: O=0.14366 H=0.14456 L=0.1406 C=0.1433 (2026-07-24)
- Pipeline break: `provider_series_cross_check`
- FAIL: price behind COT by 18d exceeds max 5d (weekly=2026-07-24 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-24 trails provider tip 2026-08-07 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Platinum — **FAIL**

- Provider: `oanda`
- Symbol: `XPT_USD`
- Raw daily date: `2026-07-30`
- Store weekly date: `2026-07-24`
- Weekly aggregation date: `2026-07-30`
- Workstation weekly date: `2026-07-24`
- COT date: `2026-08-11`
- Gap days: `18`
- Gap weeks: `2.57`
- Latest OHLC: O=1612.423 H=1657.322 L=1568.545 C=1639.938 (2026-07-24)
- Pipeline break: `provider_series_cross_check`
- FAIL: price behind COT by 18d exceeds max 5d (weekly=2026-07-24 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-24 trails provider tip 2026-08-07 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Palladium — **FAIL**

- Provider: `oanda`
- Symbol: `XPD_USD`
- Raw daily date: `2026-07-30`
- Store weekly date: `2026-07-24`
- Weekly aggregation date: `2026-07-30`
- Workstation weekly date: `2026-07-24`
- COT date: `2026-08-11`
- Gap days: `18`
- Gap weeks: `2.57`
- Latest OHLC: O=1248.383 H=1306.954 L=1226.557 C=1264.91 (2026-07-24)
- Pipeline break: `provider_series_cross_check`
- FAIL: price behind COT by 18d exceeds max 5d (weekly=2026-07-24 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-24 trails provider tip 2026-08-07 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### Bitcoin — **FAIL**

- Provider: `oanda`
- Symbol: `BTC_USD`
- Raw daily date: `2026-07-30`
- Store weekly date: `2026-07-24`
- Weekly aggregation date: `2026-07-30`
- Workstation weekly date: `2026-07-24`
- COT date: `2026-08-11`
- Gap days: `18`
- Gap weeks: `2.57`
- Latest OHLC: O=64098.0 H=65683.5 L=62361.5 C=62886.5 (2026-07-24)
- Pipeline break: `provider_series_cross_check`
- FAIL: price behind COT by 18d exceeds max 5d (weekly=2026-07-24 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-24 trails provider tip 2026-08-07 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07

### US Dollar Index / DX — **FAIL**

- Provider: `yahoo_futures`
- Symbol: `None`
- Raw daily date: `2026-08-14`
- Store weekly date: `2026-08-14`
- Weekly aggregation date: `2026-08-14`
- Workstation weekly date: `2026-07-31`
- COT date: `2026-08-11`
- Gap days: `11`
- Gap weeks: `1.57`
- Latest OHLC: O=101.31999969482422 H=101.63999938964844 L=99.69000244140625 C=99.80000305175781 (2026-07-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: workstation weekly tip matches neither store native weekly nor derived weekly (ws=2026-07-31 store_weekly=2026-08-14 derived=2026-08-14)
- FAIL: price behind COT by 11d exceeds max 5d (weekly=2026-07-31 cot=2026-08-11)
- FAIL: workstation weekly tip 2026-07-31 trails provider tip 2026-08-14 by 14d
- FAIL: missing workstation weekly candle for provider week 2026-08-07
- FAIL: missing workstation weekly candle for provider week 2026-08-14

## Failing instruments

- NASDAQ / NQ
- S&P 500 / ES
- Dow / YM
- Euro FX / 6E
- British Pound / 6B
- Japanese Yen / 6J
- Swiss Franc / 6S
- Australian Dollar / 6A
- Canadian Dollar / 6C
- NZ Dollar / 6N
- Gold
- Silver
- Copper / HG
- Crude Oil / CL
- Natural Gas / NG
- Coffee
- Cocoa
- Cotton
- Corn
- Wheat
- Soybeans
- Sugar
- Platinum
- Palladium
- Bitcoin
- US Dollar Index / DX

## OVERALL STATUS

FAIL
