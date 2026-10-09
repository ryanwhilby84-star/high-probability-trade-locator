# Sugar seasonality research dataset

Source: user's supplied `sb (3)(1).zip`. Archive SHA256 is recorded in audit.json. Provider provenance and licensing were not supplied or independently verified.

## What to load

`sugar_daily_returns.csv` is the primary input. `daily_return` is a decimal (0.01 means 1%). `return_index` starts from 100 and compounds those returns. It is a synthetic return index, not an actual sugar price or total-return investment series. `raw_close` changes contract at rolls and must NOT be used directly for cross-contract percentage returns.

Source dates run from 1961-01-04 to 2002-09-30. There are 41 full calendar years represented, 1961–2001, and partial 2002. The first source session cannot have a prior-session return. Later-year windows must exclude partial 2002. Some early January windows also lack a complete starting interval in 1961. Calendar coverage is not proof of exchange-complete history.

## Roll method

Use the nearest listed contract with a delivery month strictly later than the observation month, with closes available on both the observation date and preceding archive session. Thus roll away from a delivery contract at the start of its delivery month. Historical listings differ, and the method uses the actual contracts present in the archive. Rolls move forward only. This is a reproducible research convention; it is not a claim about exact historical exchange notice dates or a broker's continuous-chart convention. If the next contract lacks a paired observation, choose the next available forward contract.

Daily return = selected contract's close today / SAME selected contract's close on the preceding archive session - 1. On a switch, never divide the incoming close by the outgoing close. This excludes artificial inter-contract price gaps without back-adjusting prices. No source price is winsorized, interpolated, forward-filled, or deleted. The archive contains mixed MM/DD/YYYY and YYMMDD dates; YY >= 50 maps to the 1900s, otherwise the 2000s.

## Quality flags and seasonal windows

Some historical records have zero reported volume. That does not prove their settlement price is invalid, but those returns need care. Daily moves above 10% are flagged for source review, not asserted to be errors. No independent reference source was available to validate these large moves. `strict_window_eligible` is a conservative sensitivity filter requiring positive volume on both endpoints and an absolute daily move no greater than 10%. This threshold is a screening choice, not proof of correctness; it can exclude genuine volatile-market moves.

For a seasonal window, compound daily_return for intervals AFTER the entry close and THROUGH the exit close. Map non-session boundaries consistently (for example first session on or after each requested calendar date). Ensure every archive interval inside the window is present. Do not merely skip flagged rows and compound the rest: reject that year's entire window for a strict sample. Report total complete windows, accepted windows, and exclusions. Compare unfiltered and strict results and compare early and late subsamples. A larger old sample does not automatically describe current market behaviour better.

The chained index remains research input pending independent price verification. The sugar archive panel on the instrument and Seasonality Workstation pages uses this archive independently of current price/COT history. It ranks 63 candidate windows by profitable-year frequency, then directional median return, requiring at least 15 usable years. Rankings are in-sample and overlapping; they are not a walk-forward validation.

The panel defaults to the browser's current month/day, a 28-calendar-day window, long direction and quality-filtered observations. Entry/exit boundaries use the first archive session on or after the selected date, allowing up to seven calendar days. Returns run strictly after the entry close through the exit close. Wrapped windows exclude endpoints in partial 2002. Each outcome retains its actual dates and quality exclusions.

The processor also exports `sugar_archive_daily.json`; copy this to `web-dashboard/public/data/` to regenerate the panel's source. There is no network dependency for archive calculations and no blend with recent prices.

## Files

- sugar_daily_returns.csv: daily return index, source closes, selected contract, quality flags.
- sugar_contracts_normalized.csv: all 71,557 contract observations in consistent dates/columns.
- sugar_roll_audit.csv: every switch and excluded price gap, plus the included same-contract return.
- sugar_year_coverage.csv: session coverage and zero-volume counts by year.
- audit.json: source checksum, validation results, unavailable sessions and large-return records.
- process_sugar.py: reproducible processor, standard-library Python only.

Rebuild: `python process_sugar.py "INPUT.zip" OUTPUT_DIRECTORY`

Validation checks: valid finite positive prices; nonnegative volume/open interest; conflicting duplicate detection; unique output dates; exact same-contract return reconciliation; forward-only maturity transitions. No missing daily intervals beyond the first session. These checks verify construction, not the truth of source prices.
