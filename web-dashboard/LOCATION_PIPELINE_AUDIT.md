# Location pipeline audit

**Generated:** 2026-06-12 · Re-run: `python scripts/audit_location_pipeline.py`

## Why score showed but chart was empty

| Layer | Source | What it had |
|-------|--------|-------------|
| **Subtitle (score / bias / pct)** | `location_latest.json` | Single snapshot per instrument (106 wired). No time series. |
| **History chart (broken)** | Confluence history rows | Only **13 weeks** passed to workstation; fields were `valuation_score` (legacy), **zero** `location_score`. After valuation reset many rows had `valuation_wired: false` → **0 chart points**. |
| **Price panel** | `cot_3y_series_latest.json` | Full COT-aligned canonical price (e.g. Gold **492** weeks, 2017→2026). |

The chart never read `location_latest.json` and never computed from the price line already on the workstation.

## Fix applied

Location history is now computed **client-side** from the same `price` column as the Price panel (`computeLocationSeriesFromPrices` → rolling 52-week percentile, 0–100). No confluence join. No alternate series.

Subtitle on the chart uses the **last computed point** from that series (same source as the line).

---

| Market | Snapshot source | Snapshot pct/score | Price weeks (COT) | Location history pts | First hist date | Last hist date | Confluence hist scores | Chart renders |
|--------|-----------------|--------------------|--------------------|----------------------|-----------------|----------------|------------------------|---------------|
| Australian Dollar / 6A | location_latest.json | 73.1/4.6 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 13 | YES |
| Bitcoin | location_latest.json | 1.9/9.6 | 426 (2018-04→2026-06) | 415 | 2018-06-26 | 2026-06-02 | 13 | YES |
| British Pound / 6B | location_latest.json | 25.0/5.0 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 13 | YES |
| Canadian Dollar / 6C | location_latest.json | 82.7/6.5 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 13 | YES |
| Cocoa | location_latest.json | 50.0/0.5 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 0 | YES |
| Coffee | location_latest.json | 69.2/4.3 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 13 | YES |
| Copper / HG | location_latest.json | 100.0/9.5 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 13 | YES |
| Corn | location_latest.json | 59.6/1.9 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 13 | YES |
| Crude Oil / CL | location_latest.json | 80.8/6.7 | 226 (2022-02→2026-06) | 215 | 2022-04-26 | 2026-06-02 | 0 | YES |
| Dow / YM | location_latest.json | 100.0/10.0 | 226 (2022-02→2026-06) | 215 | 2022-04-26 | 2026-06-02 | 13 | YES |
| Euro FX / 6E | location_latest.json | 9.6/8.1 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 13 | YES |
| Gold | location_latest.json | 38.5/1.8 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 13 | YES |
| Japanese Yen / 6J | location_latest.json | 100.0/10.0 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 13 | YES |
| NASDAQ / NQ | location_latest.json | 100.0/10.0 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 13 | YES |
| NZ Dollar / 6N | location_latest.json | 32.7/3.5 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 13 | YES |
| Natural Gas / NG | location_latest.json | 46.2/0.3 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 13 | YES |
| Palladium | location_latest.json | 19.2/6.2 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 0 | YES |
| Platinum | location_latest.json | 46.2/0.8 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 0 | YES |
| S&P 500 / ES | location_latest.json | 100.0/10.0 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 0 | YES |
| Silver | location_latest.json | 67.3/4.0 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 13 | YES |
| Soybeans | location_latest.json | 67.3/3.0 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 0 | YES |
| Sugar | location_latest.json | 9.6/8.1 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 0 | YES |
| Swiss Franc / 6S | location_latest.json | 57.7/1.5 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 13 | YES |
| US Dollar Index / DX | location_latest.json | 46.2/0.8 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 0 | YES |
| Wheat | location_latest.json | 76.9/5.4 | 492 (2017-01→2026-06) | 481 | 2017-03-21 | 2026-06-02 | 0 | YES |

**Notes**

- **Location history pts** = COT weeks with ≥12 prior price observations (rolling 52w percentile computable).
- First ~11 COT weeks with price have no location line (insufficient window) — same rule as Python `hptl.location.engine`.
- **Crude Oil / indices with 2022+ price only** — history starts when canonical price backfill starts (matches Price panel).
- Scanner column still reads `location_latest.json` snapshot; instrument chart subtitle now prefers computed last point from workstation price.
