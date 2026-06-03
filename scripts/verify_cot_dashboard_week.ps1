# Quick check that confluence export latest_cot_report_date matches expectation.
param(
    [string]$ExportPath = ""
)
$RepoRoot = Split-Path -Parent $PSScriptRoot
if (-not $ExportPath) {
    $ExportPath = Join-Path $RepoRoot "web-dashboard\public\data\confluence_history_latest.json"
}
if (-not (Test-Path $ExportPath)) {
    Write-Error "Export not found: $ExportPath"
}
$json = Get-Content $ExportPath -Raw | ConvertFrom-Json
$latest = $json.latest_cot_report_date
$count = @($json.records).Count
Write-Host "confluence export: $ExportPath"
Write-Host "latest_cot_report_date: $latest"
Write-Host "record rows: $count"
$statusPath = Join-Path $RepoRoot "data\exports\weekly_cot_update_latest.json"
if (Test-Path $statusPath) {
    $st = Get-Content $statusPath -Raw | ConvertFrom-Json
    Write-Host "last weekly run: $($st.run_timestamp_utc)"
    Write-Host "  update_performed: $($st.update_performed)"
    Write-Host "  dashboard week: $($st.export_latest_cot_week)"
    Write-Host "  exit_code: $($st.exit_code)"
}
