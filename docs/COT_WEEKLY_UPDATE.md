# Weekly COT automation

HPTL checks CFTC for a new Commitments of Traders report each week. If a newer report exists, it updates the Excel workbook (`Trader_Report`, `Market_Blocks`), rebuilds the tracked master CSV, and regenerates the dashboard JSON — with backups and validation.

## What is automated

| Step | Automated |
|------|-----------|
| Compare latest local vs CFTC report date | Yes |
| Skip when no new report (`No new COT report available.`) | Yes |
| Download + parse CFTC when newer | Yes |
| Rebuild `Trader_Report` / `Market_Blocks` (via `run_update`) | Yes |
| Rebuild `cot_tracked_master_normalized.csv` | Yes |
| Rebuild `web-dashboard/public/data/confluence_history_latest.json` | Yes |
| Weekly integrity gate (source truth + lineage + quarantine) | Yes |
| Backup / restore confluence JSON on failure | Yes |
| Append run logs under `data/exports/` | Yes |

**Production automation (your PC):** Windows Task Scheduler — one-time registration, then fully automatic.

**CI automation (GitHub):** Scheduled workflow runs the same command for health checks; it does **not** persist your local `data/` history unless you add separate artifact/commit steps.

## One-time setup (Windows — recommended)

From PowerShell in the repo root:

```powershell
# Saturday 06:00 local time (UK morning)
.\scripts\register_weekly_cot_task.ps1 -Schedule SaturdayUK

# OR Friday 20:00 local time (US evening)
.\scripts\register_weekly_cot_task.ps1 -Schedule FridayUS
```

Test immediately:

```powershell
Start-ScheduledTask -TaskName "HPTL-Weekly-COT-Update"
```

Verify:

```powershell
.\scripts\verify_cot_dashboard_week.ps1
Get-Content data\exports\weekly_cot_update_latest.json
```

Remove the task:

```powershell
Unregister-ScheduledTask -TaskName "HPTL-Weekly-COT-Update" -Confirm:$false
```

### Manual wrapper (same as the scheduled task)

```powershell
.\scripts\run_weekly_cot_update.ps1
```

Or:

```bat
scripts\run_weekly_cot_update.bat
```

## Command (direct)

**Primary entry** (download → master → confluence JSON → dashboard export):

```powershell
cd c:\Users\ryanw\Documents\ClawWork\high-probability-trade-locator
$env:PYTHONPATH = "src"
python -m hptl.cot.run_update
```

Options:

| Flag | Effect |
|------|--------|
| `--force` | Re-download and rebuild even when local week matches CFTC |
| `--with-live-feeds` | After confluence rebuild, run Finnhub/calendar/weather feed update |
| `--skip-confluence` | Stop after tracked master (no JSON rebuild) |
| `--probe-only` | Print latest CFTC week only |

Force refresh even when dates match:

```powershell
python -m hptl.cot.run_update --force
```

Include live environment feeds (requires `.env` keys):

```powershell
python -m hptl.cot.run_update --with-live-feeds
```

Alias (same pipeline): `python -m hptl.cot.run_weekly_update`

Console entry (after `pip install -e .`): `hptl-weekly-cot-update`

### Weekly integrity gate (automatic)

After confluence rebuild, `run_update` runs the integrity gate:

1. Source truth validation (official CFTC Legacy vs dashboard NC/NR)  
2. Thesis snapshot refresh (all 23 COT markets)  
3. Lineage validation (Source Truth → Dashboard → Scanner → Thesis → Scoring)  
4. Quarantine any FAIL instrument and rebuild confluence without it  

Console summary example:

```text
23 instruments checked
23 passed
0 failed
```

Standalone (no CFTC download — uses cached official zip):

```powershell
python -m hptl.cot.run_weekly_integrity_gate
```

Machine-readable status: `data/cot_weekly_integrity_gate_latest.json`, `data/cot_quarantine_latest.json`.

COT layer policy: `docs/COT_FROZEN.md`.

## Schedule

| Region | Suggested time | Task Scheduler |
|--------|----------------|----------------|
| UK | Saturday 06:00 | `register_weekly_cot_task.ps1 -Schedule SaturdayUK` |
| US | Friday 20:00 (local) | `register_weekly_cot_task.ps1 -Schedule FridayUS` |

GitHub Actions (`.github/workflows/cot-weekly-update.yml`):

- Saturday **06:00 UTC**
- Saturday **01:00 UTC** (~Friday 8 PM US Eastern)

## Log files

| File | Purpose |
|------|---------|
| `data/exports/weekly_cot_update.log` | Human-readable append log |
| `data/exports/weekly_cot_update_latest.json` | Last run status (machine-readable) |
| `data/exports/weekly_cot_update_history.jsonl` | All runs (one JSON object per line) |

Each run records:

- `run_timestamp_utc`
- `latest_local_report_date` / `latest_cftc_report_date`
- `update_needed` / `update_performed`
- `rows_fetched`, `rows_added`, `rows_skipped_duplicates`
- `markets_updated`, `markets_missing`
- `export_workbook_path`, `export_confluence_path`, `export_latest_cot_week`
- `error`, `exit_code`

## Confirm newest COT week in the dashboard

1. `.\scripts\verify_cot_dashboard_week.ps1`
2. Open `web-dashboard/public/data/confluence_history_latest.json` → `latest_cot_report_date`
3. In the UI, select that report week on any instrument row
4. If export lags CFTC, the scanner shows a **COT stale** badge — run `python -m hptl.cot.run_update` (or `--force` if master is current but JSON is old)

`confluence_history_latest.json` includes `cot_feed_status` (`latest_export_cot_week`, `latest_cftc_report_date`, `is_stale`).

After a code change to the dashboard UI, run `cd web-dashboard && npm run build` once (not part of the weekly COT job).

## Safety

- **Fail closed** if CFTC download/parse fails (exit code `1`, no confluence replace).
- **No new report** exits `0` without touching workbook/export.
- **Confluence JSON** backed up to `confluence_history_latest.json.bak` before rebuild; restored if empty or date regresses.
- **Deduplication** on `(market, cot_report_date)` when merging history.

## Environment

`.env`: `COT_REPORT_TYPE`, `COT_YEAR`, `REQUEST_TIMEOUT_SECONDS`. No API key for public CFTC ZIPs.

## Do you still need to run anything manually?

| Task | Manual? |
|------|---------|
| Weekly COT fetch + dashboard data | **No** — after Task Scheduler registration |
| First-time Task Scheduler setup | **Yes** — once: `register_weekly_cot_task.ps1` |
| Dashboard UI rebuild (`npm run build`) | **Only** when you change frontend code |
| Macro rates refresh | **Yes** — separate `run_macro_update` (not in weekly COT job) |
