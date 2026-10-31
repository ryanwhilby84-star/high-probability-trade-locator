# Trigger the existing cloud production release and verify the published COT week.
# Requires GitHub CLI (gh) authenticated with Actions write access.
$ErrorActionPreference = 'Stop'
$Repo = 'ryanwhilby84-star/high-probability-trade-locator'
$Workflow = 'production-data-release.yml'
$Root = Split-Path -Parent $PSScriptRoot
$StatusFile = Join-Path $Root 'data\exports\weekly_cot_update_latest.json'
try {
  if (-not (Get-Command gh -ErrorAction SilentlyContinue)) { throw 'GitHub CLI (gh) is required; install and run gh auth login.' }
  if (-not (Test-Path $StatusFile)) { throw "Missing local status: $StatusFile" }
  $status = Get-Content $StatusFile -Raw | ConvertFrom-Json
  if ($status.cot_data_stale -ne $false -or @($status.markets_missing).Count -gt 0) { throw 'Local COT integrity status is not healthy; refusing publication.' }
  if ([int]$status.integrity_gate_checked -lt 26) { throw 'Local integrity gate did not check all 26 markets.' }
  $today = (Get-Date).Date
  $offset = ([int]$today.DayOfWeek + 5) % 7
  $expected = $today.AddDays(-$offset)
  if ($today.DayOfWeek -in @('Tuesday','Wednesday','Thursday')) { $expected = $expected.AddDays(-7) }
  $expectedText = $expected.ToString('yyyy-MM-dd')
  $localWeek = [string]$status.latest_cot_week
  if (-not $localWeek) { throw 'Local status does not contain latest_cot_week.' }
  if ($localWeek -ne $expectedText) { throw "Local COT week $localWeek does not match expected $expectedText" }
  $start = (Get-Date).ToUniversalTime()
  & gh workflow run $Workflow --repo $Repo --ref main
  if ($LASTEXITCODE -ne 0) { throw 'Could not dispatch production release.' }
  Start-Sleep -Seconds 15
  $run = $null
  for ($i=0; $i -lt 12; $i++) {
    $runs = & gh run list --repo $Repo --workflow $Workflow --event workflow_dispatch --limit 10 --json databaseId,createdAt,status,conclusion | ConvertFrom-Json
    $run = $runs | Where-Object { ([datetime]$_.createdAt).ToUniversalTime() -ge $start.AddSeconds(-5) } | Sort-Object createdAt -Descending | Select-Object -First 1
    if ($run) { break }
    Start-Sleep -Seconds 10
  }
  if (-not $run) { throw 'Dispatched release run was not found.' }
  & gh run watch $run.databaseId --repo $Repo --exit-status
  if ($LASTEXITCODE -ne 0) { throw "Production workflow failed: $($run.databaseId)" }
  $base = 'https://institutional.ztack.co.uk/data/'
  $inspector = Invoke-RestMethod ($base + 'cot_weekly_inspector_latest.json?verify=' + [guid]::NewGuid().ToString('N'))
  $attention = Invoke-RestMethod ($base + 'cot_attention_80_20_latest.json?verify=' + [guid]::NewGuid().ToString('N'))
  $dates = @($inspector.markets.PSObject.Properties | ForEach-Object { if ($_.Value.available -and $_.Value.rows.Count) { $_.Value.rows[-1][0] } })
  $latest = ($dates | Sort-Object -Descending | Select-Object -First 1)
  if ($latest -ne $expectedText -or $attention.latestDate -ne $expectedText) { throw "Production date mismatch: expected=$expectedText inspector=$latest attention=$($attention.latestDate)" }
  Write-Host "PASS: production Inspector and Attention Board verified at $expectedText"
  exit 0
} catch {
  Write-Error $_
  exit 21
}
