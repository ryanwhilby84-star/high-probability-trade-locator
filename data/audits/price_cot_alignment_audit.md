# Price ↔ COT Alignment Audit

Generated: `2026-09-02T11:14:14.572015+00:00`
Max alignment gap: **5 calendar days**

## Summary

- Markets total: **26**
- PASS: **4**
- FAIL: **22**
- Gate open: **False**

## Frontend cache

- WeeklyOHLCStore cache bust: **PASS**
- `cache: 'no-store'`: True
- query bust `Date.now()`: True

## Per instrument

### NASDAQ / NQ — **FAIL**

- Provider: `oanda`
- Symbol: `NAS100_USD`
- Raw daily date: `2026-08-31`
- Store weekly date: `2026-08-21`
- Weekly aggregation date: `2026-08-31`
- Workstation weekly date: `2026-08-31`
- COT date: `2026-08-25`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=29471.2 H=29531.4 L=28965.8 C=29099.2 (2026-08-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-19
- FAIL: missing workstation weekly candle for provider week 2026-06-26
- FAIL: missing workstation weekly candle for provider week 2026-07-03
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: missing workstation weekly candle for provider week 2026-07-17
- FAIL: missing workstation weekly candle for provider week 2026-07-24
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07
- FAIL: missing workstation weekly candle for provider week 2026-08-14
- FAIL: missing workstation weekly candle for provider week 2026-08-21

### S&P 500 / ES — **FAIL**

- Provider: `oanda`
- Symbol: `SPX500_USD`
- Raw daily date: `2026-08-31`
- Store weekly date: `2026-08-21`
- Weekly aggregation date: `2026-08-31`
- Workstation weekly date: `2026-08-31`
- COT date: `2026-08-25`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=7693.6 H=7700.4 L=7614.6 C=7639.6 (2026-08-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-19
- FAIL: missing workstation weekly candle for provider week 2026-06-26
- FAIL: missing workstation weekly candle for provider week 2026-07-03
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: missing workstation weekly candle for provider week 2026-07-17
- FAIL: missing workstation weekly candle for provider week 2026-07-24
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07
- FAIL: missing workstation weekly candle for provider week 2026-08-14
- FAIL: missing workstation weekly candle for provider week 2026-08-21

### Dow / YM — **FAIL**

- Provider: `oanda`
- Symbol: `US30_USD`
- Raw daily date: `2026-08-31`
- Store weekly date: `2026-08-21`
- Weekly aggregation date: `2026-08-31`
- Workstation weekly date: `2026-08-31`
- COT date: `2026-08-25`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=53192.8 H=53277.8 L=52686.8 C=52781.7 (2026-08-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-19
- FAIL: missing workstation weekly candle for provider week 2026-06-26
- FAIL: missing workstation weekly candle for provider week 2026-07-03
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: OHLC mismatch near 2026-07-17 (open): provider=52130.9 workstation=52554.0 (ws_date=2026-07-19)
- FAIL: missing workstation weekly candle for provider week 2026-07-24
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07
- FAIL: missing workstation weekly candle for provider week 2026-08-14
- FAIL: missing workstation weekly candle for provider week 2026-08-21

### Euro FX / 6E — **FAIL**

- Provider: `oanda`
- Symbol: `EUR_USD`
- Raw daily date: `2026-08-31`
- Store weekly date: `2026-08-21`
- Weekly aggregation date: `2026-08-31`
- Workstation weekly date: `2026-08-31`
- COT date: `2026-08-25`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=1.16178 H=1.16246 L=1.15848 C=1.15923 (2026-08-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-19
- FAIL: missing workstation weekly candle for provider week 2026-06-26
- FAIL: missing workstation weekly candle for provider week 2026-07-03
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: missing workstation weekly candle for provider week 2026-07-17
- FAIL: missing workstation weekly candle for provider week 2026-07-24
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07
- FAIL: missing workstation weekly candle for provider week 2026-08-14
- FAIL: missing workstation weekly candle for provider week 2026-08-21

### British Pound / 6B — **FAIL**

- Provider: `oanda`
- Symbol: `GBP_USD`
- Raw daily date: `2026-08-31`
- Store weekly date: `2026-08-21`
- Weekly aggregation date: `2026-08-31`
- Workstation weekly date: `2026-08-31`
- COT date: `2026-08-25`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=1.35486 H=1.35598 L=1.35061 C=1.3516 (2026-08-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-19
- FAIL: missing workstation weekly candle for provider week 2026-06-26
- FAIL: missing workstation weekly candle for provider week 2026-07-03
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: missing workstation weekly candle for provider week 2026-07-17
- FAIL: missing workstation weekly candle for provider week 2026-07-24
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07
- FAIL: missing workstation weekly candle for provider week 2026-08-14
- FAIL: missing workstation weekly candle for provider week 2026-08-21

### Japanese Yen / 6J — **PASS**

- Provider: `yahoo_futures`
- Symbol: `6J=F`
- Raw daily date: `2026-09-02`
- Store weekly date: `2026-09-02`
- Weekly aggregation date: `2026-09-02`
- Workstation weekly date: `2026-09-02`
- COT date: `2026-08-25`
- Gap days: `8`
- Gap weeks: `1.14`
- Latest OHLC: O=0.0062489998526871204 H=0.0062779998406767845 L=0.006240500137209892 C=0.006260499823838472 (2026-09-02)

### Swiss Franc / 6S — **FAIL**

- Provider: `oanda`
- Symbol: `USD_CHF`
- Raw daily date: `2026-08-31`
- Store weekly date: `2026-08-21`
- Weekly aggregation date: `2026-08-31`
- Workstation weekly date: `2026-08-31`
- COT date: `2026-08-25`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=0.80837 H=0.81248 L=0.8079 C=0.81164 (2026-08-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-19
- FAIL: missing workstation weekly candle for provider week 2026-06-26
- FAIL: missing workstation weekly candle for provider week 2026-07-03
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: missing workstation weekly candle for provider week 2026-07-17
- FAIL: missing workstation weekly candle for provider week 2026-07-24
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07
- FAIL: missing workstation weekly candle for provider week 2026-08-14
- FAIL: missing workstation weekly candle for provider week 2026-08-21

### Australian Dollar / 6A — **FAIL**

- Provider: `oanda`
- Symbol: `AUD_USD`
- Raw daily date: `2026-08-31`
- Store weekly date: `2026-08-21`
- Weekly aggregation date: `2026-08-31`
- Workstation weekly date: `2026-08-31`
- COT date: `2026-08-25`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=0.71658 H=0.71809 L=0.71389 C=0.71446 (2026-08-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-19
- FAIL: missing workstation weekly candle for provider week 2026-06-26
- FAIL: missing workstation weekly candle for provider week 2026-07-03
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: missing workstation weekly candle for provider week 2026-07-17
- FAIL: missing workstation weekly candle for provider week 2026-07-24
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07
- FAIL: missing workstation weekly candle for provider week 2026-08-14
- FAIL: missing workstation weekly candle for provider week 2026-08-21

### Canadian Dollar / 6C — **FAIL**

- Provider: `oanda`
- Symbol: `USD_CAD`
- Raw daily date: `2026-08-31`
- Store weekly date: `2026-08-21`
- Weekly aggregation date: `2026-08-31`
- Workstation weekly date: `2026-08-31`
- COT date: `2026-08-25`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=1.38568 H=1.39068 L=1.38453 C=1.38964 (2026-08-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-19
- FAIL: missing workstation weekly candle for provider week 2026-06-26
- FAIL: missing workstation weekly candle for provider week 2026-07-03
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: missing workstation weekly candle for provider week 2026-07-17
- FAIL: missing workstation weekly candle for provider week 2026-07-24
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07
- FAIL: missing workstation weekly candle for provider week 2026-08-14
- FAIL: missing workstation weekly candle for provider week 2026-08-21

### NZ Dollar / 6N — **FAIL**

- Provider: `oanda`
- Symbol: `NZD_USD`
- Raw daily date: `2026-08-31`
- Store weekly date: `2026-08-21`
- Weekly aggregation date: `2026-08-31`
- Workstation weekly date: `2026-08-31`
- COT date: `2026-08-25`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=0.59148 H=0.59291 L=0.58874 C=0.58913 (2026-08-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-19
- FAIL: missing workstation weekly candle for provider week 2026-06-26
- FAIL: missing workstation weekly candle for provider week 2026-07-03
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: missing workstation weekly candle for provider week 2026-07-17
- FAIL: missing workstation weekly candle for provider week 2026-07-24
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07
- FAIL: missing workstation weekly candle for provider week 2026-08-14
- FAIL: missing workstation weekly candle for provider week 2026-08-21

### Gold — **FAIL**

- Provider: `oanda`
- Symbol: `XAU_USD`
- Raw daily date: `2026-08-31`
- Store weekly date: `2026-08-21`
- Weekly aggregation date: `2026-08-31`
- Workstation weekly date: `2026-08-31`
- COT date: `2026-08-25`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=4454.255 H=4461.7 L=4322.745 C=4328.405 (2026-08-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-19
- FAIL: missing workstation weekly candle for provider week 2026-06-26
- FAIL: missing workstation weekly candle for provider week 2026-07-03
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: missing workstation weekly candle for provider week 2026-07-17
- FAIL: missing workstation weekly candle for provider week 2026-07-24
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07
- FAIL: missing workstation weekly candle for provider week 2026-08-14
- FAIL: missing workstation weekly candle for provider week 2026-08-21

### Silver — **FAIL**

- Provider: `oanda`
- Symbol: `XAG_USD`
- Raw daily date: `2026-08-31`
- Store weekly date: `2026-08-21`
- Weekly aggregation date: `2026-08-31`
- Workstation weekly date: `2026-08-31`
- COT date: `2026-08-25`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=66.6081 H=67.078 L=64.022 C=64.0785 (2026-08-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-19
- FAIL: missing workstation weekly candle for provider week 2026-06-26
- FAIL: missing workstation weekly candle for provider week 2026-07-03
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: missing workstation weekly candle for provider week 2026-07-17
- FAIL: missing workstation weekly candle for provider week 2026-07-24
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07
- FAIL: missing workstation weekly candle for provider week 2026-08-14
- FAIL: missing workstation weekly candle for provider week 2026-08-21

### Copper / HG — **PASS**

- Provider: `yahoo_futures`
- Symbol: `HG=F`
- Raw daily date: `2026-09-02`
- Store weekly date: `2026-09-02`
- Weekly aggregation date: `2026-09-02`
- Workstation weekly date: `2026-09-02`
- COT date: `2026-08-25`
- Gap days: `8`
- Gap weeks: `1.14`
- Latest OHLC: O=6.518499851226807 H=6.639500141143799 L=6.428999900817871 C=6.540999889373779 (2026-09-02)

### Crude Oil / CL — **FAIL**

- Provider: `oanda`
- Symbol: `WTICO_USD`
- Raw daily date: `2026-08-31`
- Store weekly date: `2026-08-21`
- Weekly aggregation date: `2026-08-31`
- Workstation weekly date: `2026-08-31`
- COT date: `2026-08-25`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=86.854 H=91.586 L=86.779 C=91.283 (2026-08-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-19
- FAIL: missing workstation weekly candle for provider week 2026-06-26
- FAIL: missing workstation weekly candle for provider week 2026-07-03
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: missing workstation weekly candle for provider week 2026-07-17
- FAIL: missing workstation weekly candle for provider week 2026-07-24
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: OHLC mismatch near 2026-08-07 (open): provider=78.827 workstation=80.469 (ws_date=2026-08-09)
- FAIL: missing workstation weekly candle for provider week 2026-08-14
- FAIL: missing workstation weekly candle for provider week 2026-08-21

### Natural Gas / NG — **FAIL**

- Provider: `oanda`
- Symbol: `NATGAS_USD`
- Raw daily date: `2026-08-31`
- Store weekly date: `2026-08-21`
- Weekly aggregation date: `2026-08-31`
- Workstation weekly date: `2026-08-31`
- COT date: `2026-08-25`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=2.922 H=2.946 L=2.845 C=2.942 (2026-08-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-19
- FAIL: missing workstation weekly candle for provider week 2026-06-26
- FAIL: missing workstation weekly candle for provider week 2026-07-03
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: missing workstation weekly candle for provider week 2026-07-17
- FAIL: missing workstation weekly candle for provider week 2026-07-24
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07
- FAIL: missing workstation weekly candle for provider week 2026-08-14
- FAIL: missing workstation weekly candle for provider week 2026-08-21

### Coffee — **FAIL**

- Provider: `yahoo_futures`
- Symbol: `KC=F`
- Raw daily date: `2026-09-02`
- Store weekly date: `2026-09-02`
- Weekly aggregation date: `2026-09-02`
- Workstation weekly date: `2026-09-02`
- COT date: `2026-08-25`
- Gap days: `8`
- Gap weeks: `1.14`
- Latest OHLC: O=343.6000061035156 H=345.0 L=301.8999938964844 C=304.25 (2026-09-02)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-25
- FAIL: missing workstation weekly candle for provider week 2026-08-27

### Cocoa — **PASS**

- Provider: `yahoo_futures`
- Symbol: `CC=F`
- Raw daily date: `2026-09-02`
- Store weekly date: `2026-09-02`
- Weekly aggregation date: `2026-09-02`
- Workstation weekly date: `2026-09-02`
- COT date: `2026-08-25`
- Gap days: `8`
- Gap weeks: `1.14`
- Latest OHLC: O=6600.0 H=6603.0 L=6414.0 C=6520.0 (2026-09-02)

### Cotton — **FAIL**

- Provider: `yahoo_futures`
- Symbol: `CT=F`
- Raw daily date: `2026-09-01`
- Store weekly date: `2026-08-28`
- Weekly aggregation date: `2026-09-01`
- Workstation weekly date: `2026-08-28`
- COT date: `2026-08-25`
- Gap days: `3`
- Gap weeks: `0.43`
- Latest OHLC: O=87.05999755859375 H=91.37999725341797 L=86.05000305175781 C=89.91999816894531 (2026-08-28)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: missing workstation weekly candle for provider week 2026-07-31

### Corn — **FAIL**

- Provider: `alpha_vantage`
- Symbol: `ZC=F`
- Raw daily date: `2026-07-31`
- Store weekly date: `2026-07-01`
- Weekly aggregation date: `2026-07-31`
- Workstation weekly date: `2026-07-31`
- COT date: `2026-08-25`
- Gap days: `25`
- Gap weeks: `3.57`
- Latest OHLC: O=4.5925 H=4.6225 L=4.38 C=4.4075 (2026-07-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: price behind COT by 25d exceeds max 5d (weekly=2026-07-31 cot=2026-08-25)
- FAIL: missing workstation weekly candle for provider week 2025-10-01
- FAIL: OHLC mismatch near 2025-11-01 (open): provider=201.6578882352942 workstation=4.27 (ws_date=2025-11-01)
- FAIL: missing workstation weekly candle for provider week 2025-12-01
- FAIL: missing workstation weekly candle for provider week 2026-01-01
- FAIL: OHLC mismatch near 2026-02-01 (open): provider=210.6354865131578 workstation=4.3 (ws_date=2026-02-01)
- FAIL: OHLC mismatch near 2026-03-01 (open): provider=213.3041428571428 workstation=4.2725 (ws_date=2026-03-01)
- FAIL: missing workstation weekly candle for provider week 2026-04-01
- FAIL: OHLC mismatch near 2026-05-01 (open): provider=215.6206469812925 workstation=4.5425 (ws_date=2026-05-01)
- FAIL: missing workstation weekly candle for provider week 2026-06-01
- FAIL: missing workstation weekly candle for provider week 2026-07-01

### Wheat — **FAIL**

- Provider: `oanda`
- Symbol: `WHEAT_USD`
- Raw daily date: `2026-08-31`
- Store weekly date: `2026-08-21`
- Weekly aggregation date: `2026-08-31`
- Workstation weekly date: `2026-08-31`
- COT date: `2026-08-25`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=7.561 H=7.728 L=7.486 C=7.633 (2026-08-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-19
- FAIL: missing workstation weekly candle for provider week 2026-06-26
- FAIL: missing workstation weekly candle for provider week 2026-07-03
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: missing workstation weekly candle for provider week 2026-07-17
- FAIL: missing workstation weekly candle for provider week 2026-07-24
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07
- FAIL: missing workstation weekly candle for provider week 2026-08-14
- FAIL: missing workstation weekly candle for provider week 2026-08-21

### Soybeans — **FAIL**

- Provider: `oanda`
- Symbol: `SOYBN_USD`
- Raw daily date: `2026-08-31`
- Store weekly date: `2026-08-21`
- Weekly aggregation date: `2026-08-31`
- Workstation weekly date: `2026-08-31`
- COT date: `2026-08-25`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=12.824 H=13.06 L=12.766 C=13.038 (2026-08-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-19
- FAIL: missing workstation weekly candle for provider week 2026-06-26
- FAIL: missing workstation weekly candle for provider week 2026-07-03
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: missing workstation weekly candle for provider week 2026-07-17
- FAIL: missing workstation weekly candle for provider week 2026-07-24
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07
- FAIL: missing workstation weekly candle for provider week 2026-08-14
- FAIL: missing workstation weekly candle for provider week 2026-08-21

### Sugar — **FAIL**

- Provider: `oanda`
- Symbol: `SUGAR_USD`
- Raw daily date: `2026-08-31`
- Store weekly date: `2026-08-21`
- Weekly aggregation date: `2026-08-31`
- Workstation weekly date: `2026-08-31`
- COT date: `2026-08-25`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=0.1757 H=0.18216 L=0.17506 C=0.182 (2026-08-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-19
- FAIL: missing workstation weekly candle for provider week 2026-06-26
- FAIL: missing workstation weekly candle for provider week 2026-07-03
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: missing workstation weekly candle for provider week 2026-07-17
- FAIL: missing workstation weekly candle for provider week 2026-07-24
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07
- FAIL: missing workstation weekly candle for provider week 2026-08-14
- FAIL: missing workstation weekly candle for provider week 2026-08-21

### Platinum — **FAIL**

- Provider: `oanda`
- Symbol: `XPT_USD`
- Raw daily date: `2026-08-31`
- Store weekly date: `2026-08-21`
- Weekly aggregation date: `2026-08-31`
- Workstation weekly date: `2026-08-31`
- COT date: `2026-08-25`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=1787.22 H=1808.612 L=1736.022 C=1738.314 (2026-08-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-19
- FAIL: missing workstation weekly candle for provider week 2026-06-26
- FAIL: missing workstation weekly candle for provider week 2026-07-03
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: OHLC mismatch near 2026-07-17 (open): provider=1579.886 workstation=1596.54 (ws_date=2026-07-19)
- FAIL: missing workstation weekly candle for provider week 2026-07-24
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07
- FAIL: missing workstation weekly candle for provider week 2026-08-14
- FAIL: missing workstation weekly candle for provider week 2026-08-21

### Palladium — **FAIL**

- Provider: `oanda`
- Symbol: `XPD_USD`
- Raw daily date: `2026-08-31`
- Store weekly date: `2026-08-21`
- Weekly aggregation date: `2026-08-31`
- Workstation weekly date: `2026-08-31`
- COT date: `2026-08-25`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=1362.301 H=1373.894 L=1296.472 C=1299.688 (2026-08-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-19
- FAIL: missing workstation weekly candle for provider week 2026-06-26
- FAIL: missing workstation weekly candle for provider week 2026-07-03
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: missing workstation weekly candle for provider week 2026-07-17
- FAIL: missing workstation weekly candle for provider week 2026-07-24
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07
- FAIL: missing workstation weekly candle for provider week 2026-08-14
- FAIL: missing workstation weekly candle for provider week 2026-08-21

### Bitcoin — **FAIL**

- Provider: `oanda`
- Symbol: `BTC_USD`
- Raw daily date: `2026-08-31`
- Store weekly date: `2026-08-21`
- Weekly aggregation date: `2026-08-31`
- Workstation weekly date: `2026-08-31`
- COT date: `2026-08-25`
- Gap days: `6`
- Gap weeks: `0.86`
- Latest OHLC: O=78808.8 H=79189.0 L=76386.0 C=77400.0 (2026-08-31)
- Pipeline break: `provider_series_cross_check`
- FAIL: missing workstation weekly candle for provider week 2026-06-19
- FAIL: missing workstation weekly candle for provider week 2026-06-26
- FAIL: OHLC mismatch near 2026-07-03 (open): provider=62620.6 workstation=60193.5 (ws_date=2026-07-05)
- FAIL: missing workstation weekly candle for provider week 2026-07-10
- FAIL: missing workstation weekly candle for provider week 2026-07-17
- FAIL: missing workstation weekly candle for provider week 2026-07-24
- FAIL: missing workstation weekly candle for provider week 2026-07-31
- FAIL: missing workstation weekly candle for provider week 2026-08-07
- FAIL: missing workstation weekly candle for provider week 2026-08-14
- FAIL: missing workstation weekly candle for provider week 2026-08-21

### US Dollar Index / DX — **PASS**

- Provider: `yahoo_futures`
- Symbol: `None`
- Raw daily date: `2026-09-02`
- Store weekly date: `2026-09-02`
- Weekly aggregation date: `2026-09-02`
- Workstation weekly date: `2026-09-02`
- COT date: `2026-08-25`
- Gap days: `8`
- Gap weeks: `1.14`
- Latest OHLC: O=99.69999694824219 H=99.86299896240234 L=99.3499984741211 C=99.8550033569336 (2026-09-02)

## Failing instruments

- NASDAQ / NQ
- S&P 500 / ES
- Dow / YM
- Euro FX / 6E
- British Pound / 6B
- Swiss Franc / 6S
- Australian Dollar / 6A
- Canadian Dollar / 6C
- NZ Dollar / 6N
- Gold
- Silver
- Crude Oil / CL
- Natural Gas / NG
- Coffee
- Cotton
- Corn
- Wheat
- Soybeans
- Sugar
- Platinum
- Palladium
- Bitcoin

## OVERALL STATUS

FAIL
