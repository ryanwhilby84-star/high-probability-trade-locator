# HPTL weekly COT update — use from Task Scheduler or run manually.
# Logs: data/exports/weekly_cot_update.log + weekly_cot_update_latest.json
$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot
$env:PYTHONPATH = "src"
$env:HPTL_SKIP_LIVE_FEEDS = "1"
# Fast COT/dashboard export: skip per-row FX V3 valuation in Stage 4 (engine.py gate).
# FX histories are still process-cached when valuation is explicitly enabled.
$env:HPTL_SKIP_VALUATION = "1"

$Python = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $Python) {
    Write-Error "python not found on PATH. Install Python 3.11+ and retry."
}

Write-Host "HPTL weekly COT update starting at $(Get-Date -Format o)"
Write-Host "Repo: $RepoRoot"

& $Python -m hptl.cot.run_update @args
$exitCode = $LASTEXITCODE

Write-Host "Finished with exit code $exitCode"
Write-Host "Log: $RepoRoot\data\exports\weekly_cot_update.log"
Write-Host "Status JSON: $RepoRoot\data\exports\weekly_cot_update_latest.json"

exit $exitCode
