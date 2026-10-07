# Workstation data repair — 7 October 2026

The trading view compared expanding-history net percentiles in the Weekly Inspector with 156-report percentiles in the detail sheet. Both could be mathematically correct but their different horizons made the trading view misleading. Inspector readings now use the rolling 156-report convention, including percentile movements, participant states and cross-group spreads. Historical research events retain their separate long-history definitions.

## Changes

- The inspector normalises older exports from their dated raw participant nets, without look-ahead. New backend exports use the same three-year horizon. The detail sheet derives every participant from raw rows capped at the selected report date.
- Null or missing positions and prices are excluded rather than converted to zero. Percentile display inputs use consistent precision to avoid double-rounding differences.
- The derived integrity gate checks numerical values and their 156-report horizon against source positions, beyond merely checking for finite values.
- Raw archive fetch failures propagate to the UI, do not poison the cache, and can be retried. Missing rows or disagreement with the selected snapshot block the raw sheet.
- Quote freshness is recalculated every second. LIVE expires after 60 seconds even with an open WebSocket; missing/invalid/future timestamps cannot certify a LIVE quote. Reconnect snapshots cannot replace a newer quote with an older one.
- Instrument payloads are cleared and bound to the selected market. The workstation blocks inspector readings if source dates or participant nets disagree. Outdated narrative source weeks are suppressed.
- Current-price display uses the canonical quote selection and shows a prominent non-live warning, timestamp and reconnect action. Saved prices are labelled historical reference. Inspector closes are explicitly labelled selected-week closes.
- The quick COT updater stops on failed stages, rebuilds its inspector and runs the price/COT, derived and universe gates before final dist sync. Earlier confluence/chart stages write their own outputs; this is not a fully transactional publish. Frontend source checks protect against partial-stage disagreement.

## Verification

- 36 focused Python tests passed, including derived integrity, inspector flow, positioning research, current-price infrastructure and price/COT alignment logic.
- Five Node regression tests passed, covering silent feeds, store rerenders as quotes age, reconnect ordering, missing values, raw-fetch retry, 156-report parity and no look-ahead.
- Reconciliation against the repository snapshot: 1,014 participant-week comparisons (26 markets × 13 reports × 3 groups) match Python and JavaScript; 78 latest raw-sheet comparisons match exactly; cross-spread percentile ranks agree within 0.01 point of language rounding precision.
- 21 existing frontend workstation model/binding tests pass. Production build passes.

## Data/runtime limit

This audit used GitHub branch recover-before-emergency at b58b590. Its raw/chart COT snapshot ends 18 August 2026; the original inspector ended 28 July and the price history mostly ended 30–31 July. It is older than the user's 29 September screenshots. Universe gate initially failed 18/26; stricter price/COT alignment and original inspector gates failed 26/26. Rebuilding the inspector from the saved source repairs the inspector gate, but does not make those historical prices current.

No generated August dataset is included in the source commit. Do not overwrite the user's newer local datasets with this audit snapshot. Broker credentials and the user's running local price service were not accessible here. The code fixes are verified; actual current-price availability and the newer local data must be verified on that machine.

## Apply on the trading computer

From the correct Institutional Edge Dashboard repository:

```powershell
git pull origin recover-before-emergency
python scripts/weekly_dashboard_refresh.py --force-cot
```

Run the existing current-price service in its own terminal, using the existing configured broker environment:

```powershell
python scripts/run_current_price_service.py
```

Restart the dashboard and verify the workstation quote's provider/symbol, timestamp, bid/ask and status against the broker. The weekly refresh must end PASS. If it ends FAIL, use its named audit report to resolve the failing instruments; do not call the runtime verified until those gates pass.
# Weekly alignment follow-up

The local October audit exposed native OANDA weekly candle start dates being
compared with workstation end dates. OANDA daily candles also carry session
start dates. Weekly aggregation now uses the provider's Friday-to-Friday
boundary and the audit normalizes native weekly start labels to week ends.
Cached OANDA/Yahoo weekly series no longer overwrite daily-derived candles.
Corn's provider cross-check now uses ZC=F futures with cents converted to
USD/bushel, matching the existing Corn foundation builder.

From the Windows repository, after pulling `recover-before-emergency`:

```powershell
python scripts/repair_workstation_alignment.py
```

This refreshes Corn, Cocoa and Cotton via their existing foundation builders,
rebuilds processed/public/dist workstation OHLC and runs the full price/COT
alignment gate. It does not repeat COT downloading or valuation processing.
After successful downloads, `--rebuild-only` avoids downloading them again.
Any remaining gate failures must be investigated; the command exits nonzero.
The user's newer datasets and live provider access remain locally verified on
their machine, not verified by the source regression tests.
