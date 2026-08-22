# Derived COT Integrity Audit

Generated: `2026-07-26T06:08:20.240063+00:00`
Lookback: latest + prior 12 weeks (13 total)

## Summary

- Total instruments: **26**
- PASS: **26**
- FAIL: **0**
- Gate open: **True**

## Crude Oil trace note

- complete for latest 13 weeks when present
- Frontend failure mode: Price timeline dates (OANDA Friday weeks) did not exact-match inspector COT dates (Tuesday). Without as-of join, percentiles rendered as Unavailable.
- Fix: resolveInspectorWeekForDate + integrity banner in WeeklyInspector

## Per instrument

### NASDAQ / NQ — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### S&P 500 / ES — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Dow / YM — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `233`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Euro FX / 6E — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### British Pound / 6B — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Japanese Yen / 6J — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Swiss Franc / 6S — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Australian Dollar / 6A — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Canadian Dollar / 6C — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### NZ Dollar / 6N — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Gold — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Silver — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Copper / HG — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Crude Oil / CL — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `233`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Natural Gas / NG — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Coffee — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Cocoa — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Cotton — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Corn — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Wheat — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Soybeans — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Sugar — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Platinum — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Palladium — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### Bitcoin — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `433`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

### US Dollar Index / DX — **PASS**

- Latest COT week: `2026-07-21`
- Raw COT row present: `True`
- Historical window length: `499`
- Lookback weeks audited: `13`
- Commercial completeness: `PASS`
- Non-commercial completeness: `PASS`
- Non-reportable completeness: `PASS`
- Cross-group completeness: `PASS`
- Inspector completeness: `PASS`
- First failing stage: `None`

## OVERALL STATUS

PASS: 26
FAIL: 0
OVERALL STATUS: PASS
