# Start HPTL local development stack in persistent external PowerShell windows.
# Child processes survive after this script exits (and after Cursor agent terminals close).
#
# Usage (from repo root or anywhere):
#   powershell -ExecutionPolicy Bypass -File scripts\start_hptl_dev.ps1

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot 'lib\hptl_dev_common.ps1')

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$BackendPort = 8787
$FrontendPort = 5173
$HealthUrl = "http://127.0.0.1:$BackendPort/health"
$PricesUrl = "http://127.0.0.1:$BackendPort/api/prices"
$DashUrl = "http://localhost:$FrontendPort/"
$ProxyPricesUrl = "http://localhost:$FrontendPort/api/prices"

Write-Host "HPTL dev launcher"
Write-Host ("  repo: {0}" -f $RepoRoot)

# --- Backend ---
$backendListener = Get-ListenerOnPort -Port $BackendPort
if ($backendListener) {
    $bp = Get-ProcessInfo -ProcessId ([int]$backendListener.OwningProcess)
    if (Test-IsHptlBackendProcess -Process $bp) {
        Write-Host ("  backend: already listening on {0} (PID {1}) - reuse" -f $BackendPort, $backendListener.OwningProcess)
    } else {
        throw ("Port {0} is in use by a non-HPTL process (PID {1}). Stop it manually, then retry." -f $BackendPort, $backendListener.OwningProcess)
    }
} else {
    Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
        Where-Object { Test-IsHptlBackendProcess -Process $_ } |
        ForEach-Object {
            Write-Host ("  backend: stopping stale PID {0}" -f $_.ProcessId)
            Stop-HptlProcessTree -ProcessId ([int]$_.ProcessId) -Reason 'stale-backend' | Out-Null
        }

    $backendCmd = @"
Set-Location '$RepoRoot'
`$Host.UI.RawUI.WindowTitle = 'HPTL Current Price Service :$BackendPort'
Write-Host 'HPTL Current Price Service - port $BackendPort' -ForegroundColor Cyan
Write-Host 'Repo: $RepoRoot'
python scripts/run_current_price_service.py --port $BackendPort
Write-Host 'Backend exited. Press Enter to close.' -ForegroundColor Yellow
Read-Host
"@
    Start-Process -FilePath 'powershell.exe' -WorkingDirectory $RepoRoot -ArgumentList @(
        '-NoExit',
        '-ExecutionPolicy', 'Bypass',
        '-Command', $backendCmd
    ) | Out-Null
    Write-Host "  backend: started in new PowerShell window - waiting for health..."
}

$healthResp = Wait-HttpOk -Url $HealthUrl -TimeoutSec 90
$health = $healthResp.Content | ConvertFrom-Json
$prices = Get-JsonUrl -Url $PricesUrl -TimeoutSec 20
$instrumentCount = Count-PriceInstruments -PricesDoc $prices
Write-Host ("  backend: health={0} status={1} subscribed={2} connected={3} instruments={4}" -f `
    $healthResp.StatusCode, $health.status, $health.subscribed_instruments, $health.any_connected, $instrumentCount)

$backendListener = Get-ListenerOnPort -Port $BackendPort
$backendPid = if ($backendListener) { [int]$backendListener.OwningProcess } else { $null }

# --- Frontend ---
$feListener = Get-ListenerOnPort -Port $FrontendPort
if ($feListener) {
    $fp = Get-ProcessInfo -ProcessId ([int]$feListener.OwningProcess)
    if (Test-IsHptlFrontendProcess -Process $fp) {
        Write-Host ("  frontend: already listening on {0} (PID {1}) - reuse" -f $FrontendPort, $feListener.OwningProcess)
    } else {
        throw ("Port {0} is in use by a non-HPTL process (PID {1}). Stop it with scripts\stop_hptl_dev.ps1 or manually." -f $FrontendPort, $feListener.OwningProcess)
    }
} else {
    Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object { Test-IsHptlFrontendProcess -Process $_ } |
        ForEach-Object {
            Write-Host ("  frontend: stopping stale PID {0}" -f $_.ProcessId)
            Stop-HptlProcessTree -ProcessId ([int]$_.ProcessId) -Reason 'stale-frontend' | Out-Null
        }

    $frontendCmd = @"
Set-Location '$RepoRoot'
`$Host.UI.RawUI.WindowTitle = 'HPTL Vite Dashboard :$FrontendPort'
Write-Host 'HPTL Vite Dashboard - port $FrontendPort' -ForegroundColor Cyan
Write-Host 'Repo: $RepoRoot'
npm run dev --prefix web-dashboard -- --host localhost --port $FrontendPort --strictPort
Write-Host 'Frontend exited. Press Enter to close.' -ForegroundColor Yellow
Read-Host
"@
    Start-Process -FilePath 'powershell.exe' -WorkingDirectory $RepoRoot -ArgumentList @(
        '-NoExit',
        '-ExecutionPolicy', 'Bypass',
        '-Command', $frontendCmd
    ) | Out-Null
    Write-Host "  frontend: started in new PowerShell window - waiting for HTTP..."
}

$null = Wait-HttpOk -Url $DashUrl -TimeoutSec 90
$proxy = Wait-HttpOk -Url $ProxyPricesUrl -TimeoutSec 30
$proxyDoc = $proxy.Content | ConvertFrom-Json
$proxyCount = Count-PriceInstruments -PricesDoc $proxyDoc

$feListener = Get-ListenerOnPort -Port $FrontendPort
$frontendPid = if ($feListener) { [int]$feListener.OwningProcess } else { $null }

$wsOk = $false
$wsErr = $null
try {
    $ws = [System.Net.WebSockets.ClientWebSocket]::new()
    $cts = [System.Threading.CancellationTokenSource]::new()
    $cts.CancelAfter(8000)
    $uri = [Uri]"ws://localhost:$FrontendPort/ws/prices"
    $ws.ConnectAsync($uri, $cts.Token).GetAwaiter().GetResult()
    $wsOk = ($ws.State -eq [System.Net.WebSockets.WebSocketState]::Open)
    if ($wsOk) {
        $buf = [byte[]]::new(4096)
        $seg = [ArraySegment[byte]]::new($buf)
        $recv = $ws.ReceiveAsync($seg, $cts.Token).GetAwaiter().GetResult()
        $wsOk = $recv.Count -gt 0
    }
    $ws.Dispose()
} catch {
    $wsErr = $_.Exception.Message
}

Write-Host ""
Write-Host "READY - child consoles stay open after this launcher exits." -ForegroundColor Green
Write-Host ("  Dashboard : {0}" -f $DashUrl)
Write-Host ("  Backend   : {0}" -f $HealthUrl)
Write-Host ("  Backend PID / port : {0} / {1}" -f $backendPid, $BackendPort)
Write-Host ("  Frontend PID / port: {0} / {1}" -f $frontendPid, $FrontendPort)
Write-Host ("  Proxy /api/prices  : HTTP {0}, instruments={1}" -f $proxy.StatusCode, $proxyCount)
if ($wsOk) {
    Write-Host "  WebSocket /ws/prices: OK (frame received)"
} else {
    Write-Host ("  WebSocket /ws/prices: FAIL ({0})" -f $wsErr)
}
Write-Host ""
Write-Host "Stop with:  powershell -ExecutionPolicy Bypass -File scripts\stop_hptl_dev.ps1"
Write-Host "Status:     powershell -ExecutionPolicy Bypass -File scripts\status_hptl_dev.ps1"
exit 0
