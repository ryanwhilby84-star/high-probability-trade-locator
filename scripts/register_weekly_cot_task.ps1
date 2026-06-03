# Register Windows Task Scheduler job for HPTL weekly COT update (current user).
# Run once in PowerShell:  .\scripts\register_weekly_cot_task.ps1
# Requires: Python on PATH, repo checked out at this path.
param(
    [ValidateSet("SaturdayUK", "FridayUS")]
    [string]$Schedule = "SaturdayUK"
)

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Runner = Join-Path $RepoRoot "scripts\run_weekly_cot_update.ps1"
$TaskName = "HPTL-Weekly-COT-Update"

if (-not (Test-Path $Runner)) {
    Write-Error "Missing runner script: $Runner"
}

$Action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$Runner`"" `
    -WorkingDirectory $RepoRoot

if ($Schedule -eq "SaturdayUK") {
    # Saturday 06:00 local — UK morning after US Friday CFTC release
    $Trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Saturday -At "06:00"
    $Description = "HPTL: fetch new CFTC COT, rebuild workbook + dashboard (Saturday 06:00 local)."
} else {
    # Friday 20:00 local — US evening after typical CFTC publish window
    $Trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Friday -At "20:00"
    $Description = "HPTL: fetch new CFTC COT, rebuild workbook + dashboard (Friday 20:00 local)."
}

$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2)

$Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Principal $Principal `
    -Description $Description `
    -Force | Out-Null

Write-Host "Registered scheduled task: $TaskName"
Write-Host "  Schedule: $Schedule"
Write-Host "  Runner:   $Runner"
Write-Host "  Logs:     $RepoRoot\data\exports\weekly_cot_update.log"
Write-Host ""
Write-Host "Test now:  Start-ScheduledTask -TaskName '$TaskName'"
Write-Host "Remove:    Unregister-ScheduledTask -TaskName '$TaskName' -Confirm:`$false"
