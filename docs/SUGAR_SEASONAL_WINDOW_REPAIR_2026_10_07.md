# Sugar archive seasonal-window repair — 7 October 2026

## Findings

The supplied compact return archive has 10,400 unique, increasing return dates. The previous-session chain reconciles across all stored intervals. Coverage is 1961-01-04 to 2002-09-30; analysis uses 41 calendar years, 1961–2001, excluding incomplete 2002. It is historical archive coverage, not 41 years ending today. There are 552 quality-flagged intervals. JSON SHA-256: fdac239ac017d8a832136bbb3c0ff48a43265b2db191b485c4af7589f1a3f857.

The original uploaded SB ZIP was recovered and independently parsed: 208 contracts, 71,557 raw rows, 10,401 source sessions. All 10,400 stored returns reconcile directly to same-contract close ratios; all 207 switches and all 552 quality flags pass. Contract maturities follow the existing forward-only calendar roll policy, with delivery months excluded. The two uploaded SB ZIP copies have identical SHA-256 ed6543753e994b0a53e3dee3bc268b08ddd9efee88ea353fc653c84d8a34a63e, matching the prior processed archive audit. The checker is scripts/audit_sugar_archive_source.py. This proves reconciliation to the supplied files, not independent verification of the vendor’s historical prices. This repair does not modify the raw series or its calendar roll policy.

The old scanner tried 29 starts × 84 durations = 2,436 windows and selected on full-archive results. Its duplicate key was direction plus exact start offset. Consequently nearby entry-date variants appeared independent, and the same optimized observations were labelled as a Pay Validation pass. Bullish shorter windows and bearish longer windows can both be valid because endpoints differ; quality filtering also gives each window a different sample of years.

The archive evaluator also used the first session on or after the requested exit, importing a later Monday's return into weekend-ending windows. It now uses the last session on or before the end, consistent with the recent-data scanner. Entry remains the first session on or after the start. Missing interval chains exclude the whole year-window. Coverage years and eligibility flags are validated on ingestion.

## Selection and evidence

Select direction, dates and ranking using 1961–1987 only. At least 12 usable discovery cases, over 60% directional frequency and agreeing mean/median are required. Similar same-direction intervals are grouped when intersection/union is at least 0.6, or at least 80% of the shorter interval overlaps and durations differ by no more than threefold. The top eight distinct representatives are selected without reading later-year performance. Group counts describe represented variants, not additional independent observations.

Fixed windows are then checked on 1988–2001. A pass needs at least six usable cases, over 60% directional success, and mean and median agreeing with the selected direction. Failed checks remain in research and validation details; they do not cause dates or direction to be reoptimized. The main list shows those passing this later-year check. Opposing overlaps remain explicit and are compared on common usable years. Full-archive counts are context, not the selection or validation sample. All-observation results accompany the filtered results so exclusions cannot silently inflate the read. A historical split is not a forecast probability or evidence of current profitability; the old archive remains distinct from recent 5Y/10Y/15Y data.

## Reproduced result with quality filter enabled

| Window | Discovery direction | Full usable years | Later-year hits | Later-year check | Grouped variants |
| --- | --- | ---: | ---: | --- | ---: |
| 08 Oct → 30 Oct | Bullish | 29 | 6/11 | Unconfirmed | 3 |
| 13 Oct → 24 Oct | Bearish | 32 | 4/11 | Unconfirmed | 2 |
| 13 Oct → 20 Nov | Bearish | 24 | 6/11 | Unconfirmed | 94 |
| 16 Oct → 03 Nov | Bullish | 29 | 8/11 | Pass | 99 |
| 27 Oct → 24 Nov | Bullish | 26 | 8/12 | Pass | 147 |
| 27 Oct → 24 Jan | Bullish | 24 | 3/11 | Unconfirmed | 56 |
| 29 Oct → 01 Dec | Bearish | 26 | 4/12 | Unconfirmed | 4 |
| 03 Nov → 12 Nov | Bearish | 30 | 7/12 | Unconfirmed | 19 |

The direction here is fixed from discovery. A failed pattern may have opposite full-archive mean/median; it stays unconfirmed rather than being relabelled after validation.

## Verification

14 Node regression tests and 3 standard-library Python source-audit tests pass, including same-contract compounding, entry-day exclusion, weekend exit bounds, missing intervals, duplicate metadata rejection, year wrap/partial-2002 exclusion, grouped variants, explicit opposite overlaps and later-year mutation not affecting selection. Vite production build passes. Existing raw archive data and price/COT repair files are unchanged. The original-ZIP audit passes with no failures. A browser interaction test was not performed in this checkout.

The generic interval selector and directional validation helpers are reusable for further archives. Each other asset still needs an independently verified importer, correct contract-roll policy, coverage and quality flags before reuse.

## Windows update

From the repository root on the local seasonal feature branch:

```powershell
git pull --no-edit origin feature/cross-market-seasonal-confirmation
```

If the prompt is already inside web-dashboard, use the same git command there. Vite will reload source changes; refresh the Sugar page. No full data refresh is required. Do not discard local modifications if Git reports conflicts.
