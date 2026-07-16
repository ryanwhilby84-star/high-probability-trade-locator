# Shared helpers for HPTL local-dev start/stop/status scripts.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-HptlRepoRoot {
    # scripts/ -> repo root
    return (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
}

function Get-ListenerOnPort {
    param(
        [Parameter(Mandatory = $true)][int]$Port
    )
    $conns = @(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)
    if (-not $conns -or $conns.Count -eq 0) { return $null }
    return $conns[0]
}

function Get-ProcessInfo {
    param(
        [Parameter(Mandatory = $true)][int]$ProcessId
    )
    try {
        return Get-CimInstance Win32_Process -Filter "ProcessId=$ProcessId" -ErrorAction Stop
    } catch {
        return $null
    }
}

function Test-IsHptlBackendProcess {
    param(
        [Parameter(Mandatory = $true)]$Process
    )
    if (-not $Process) { return $false }
    $cmd = [string]$Process.CommandLine
    if ([string]::IsNullOrWhiteSpace($cmd)) { return $false }
    return (
        $cmd -match 'run_current_price_service\.py' -or
        ($cmd -match 'uvicorn' -and $cmd -match 'current_price') -or
        ($cmd -match 'hptl\.prices\.current_price_api')
    )
}

function Test-IsHptlFrontendProcess {
    param(
        [Parameter(Mandatory = $true)]$Process
    )
    if (-not $Process) { return $false }
    $cmd = [string]$Process.CommandLine
    if ([string]::IsNullOrWhiteSpace($cmd)) { return $false }
    return (
        ($cmd -match 'vite' -and ($cmd -match 'web-dashboard' -or $cmd -match '5173')) -or
        ($cmd -match 'npm.*(run )?dev' -and $cmd -match 'web-dashboard')
    )
}

function Stop-HptlProcessTree {
    param(
        [Parameter(Mandatory = $true)][int]$ProcessId,
        [string]$Reason = ''
    )
    $info = Get-ProcessInfo -ProcessId $ProcessId
    $label = if ($info) { $info.CommandLine } else { "(pid $ProcessId)" }
    try {
        # /T kills child tree (python wrapper -> uvicorn worker, npm -> node vite)
        & taskkill.exe /PID $ProcessId /T /F 2>$null | Out-Null
        return [pscustomobject]@{
            Pid     = $ProcessId
            Stopped = $true
            Reason  = $Reason
            Command = $label
        }
    } catch {
        return [pscustomobject]@{
            Pid     = $ProcessId
            Stopped = $false
            Reason  = "$Reason :: $($_.Exception.Message)"
            Command = $label
        }
    }
}

function Wait-HttpOk {
    param(
        [Parameter(Mandatory = $true)][string]$Url,
        [int]$TimeoutSec = 60,
        [int]$RetryMs = 500
    )
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSec)
    $lastErr = $null
    while ([DateTime]::UtcNow -lt $deadline) {
        try {
            $resp = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5
            if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 300) {
                return $resp
            }
            $lastErr = "HTTP $($resp.StatusCode)"
        } catch {
            $lastErr = $_.Exception.Message
        }
        Start-Sleep -Milliseconds $RetryMs
    }
    throw "Timed out waiting for $Url ($lastErr)"
}

function Get-JsonUrl {
    param(
        [Parameter(Mandatory = $true)][string]$Url,
        [int]$TimeoutSec = 15
    )
    $resp = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec $TimeoutSec
    return ($resp.Content | ConvertFrom-Json)
}

function Count-PriceInstruments {
    param($PricesDoc)
    if (-not $PricesDoc -or -not $PricesDoc.prices) { return 0 }
    return @($PricesDoc.prices.PSObject.Properties).Count
}
