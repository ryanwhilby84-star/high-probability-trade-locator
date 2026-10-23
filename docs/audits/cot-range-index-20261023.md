# COT range-index audit — 23 October 2026

Scope: the live Weekly Inspector export generated 18 October 2026, report week 13 October 2026; all 26 exported markets, all available report dates, all three participant groups, and 3Y/5Y/10Y/15Y/ALL windows. ALL means the retained available history, at most 780 reports, not the entire CFTC archive.

## Independent checks

- 56,625 raw net positions matched contract-code/date records in the official CFTC Legacy Futures Only annual text archives for 2011–2026. No missing or duplicate matched source weeks.
- 509,625 chart-net, index and 3Y index-movement checks passed. The audit computes `(net - min) / (max - min) * 100` directly without importing the production calculation function.
- Raw archive and dataset SHA-256 digests are recorded in the adjacent JSON report.
- Calculation audit: `python scripts/audit_cot_range_indices.py` (use `--inspector` and `--source` to audit specific snapshots).
- Source audit: `python scripts/audit_cot_cftc_archives.py` (downloads public CFTC annual ZIPs to `/tmp`, six concurrent requests, then matches chart history by contract code and date).

## Palladium reproduction

Non-commercial net on 13 October 2026: **−8,078**, from 8,785 longs minus 16,863 shorts, CFTC contract 075651.

| Baseline | First report | Observations | Minimum net | Maximum net | COT range index |
|---|---|---:|---:|---:|---:|
| 3Y | 2023-10-24 | 156 | −13,914 | +1,225 | 38.55 |
| ALL | 2011-11-08 | 780 | −13,914 | +30,090 | 13.26 |

Both figures match the existing min–max formula. Counting observations below the current value instead gives approximately 39.74% (3Y) and 9.87% (ALL); these are different statistical quantities. A visual count of lower weeks cannot validate a min–max range index.

## Confirmed display defects and changes

The inspector used ordinal “13th/39th percentile” presentation for a range index. It also placed selected-window values beside fixed-3Y movements, states and narratives without sufficiently clear baseline labels. The new display identifies the range index out of 100, exposes historical bounds and sample dates, labels the fixed-3Y analysis, and suppresses a selected index if the independent chart-history calculation disagrees with its export. Calculations and thresholds are unchanged.

Tests: 13 engine/basis tests, exact Palladium server-rendered inspector tests (both windows and mismatch guard), production build, independent audit, and existing browser controls/mobile checks passed in CI for PR 67.

## Limits

This audit does not validate price-provider history, spreads, execution, every unrelated trading metric, or causation of any trading loss. Partial historical windows and missing observations are counted as actually available by the existing exporter; displayed calculation evidence shows the actual sample. The historical research module separately requires complete rolling baselines before emitting research samples. Earlier than retained 780-week history is outside this export audit.
