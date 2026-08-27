# Report HPTL local-dev service status (ports, health, PIDs, command lines).
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File scripts\status_hptl_dev.ps1

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Continue'

. (Join-Path $PSScriptRoot 'lib\hptl_dev_common.ps1')

$BackendPort = 8787
$FrontendPort = 5173

function Show-PortStatus {
    param([int]$Port, [string]$Label)
    $listener = Get-ListenerOnPort -Port $Port
    if (-not $listener) {
        Write-Host ("{0} port {1} : NOT listening" -f $Label, $Port)
        return $null
    }
    $procId = [int]$listener.OwningProcess
    $proc = Get-ProcessInfo -ProcessId $procId
    $cmd = if ($proc) { $proc.CommandLine } else { '(process exited)' }
    Write-Host ("{0} port {1} : LISTENING  PID={2}" -f $Label, $Port, $procId)
    Write-Host ("  cmdline: {0}" -f $cmd)
    return $procId
}

Write-Host "HPTL dev status"
Write-Host ("=" * 60)

$fePid = Show-PortStatus -Port $FrontendPort -Label 'Frontend'
$bePid = Show-PortStatus -Port $BackendPort -Label 'Backend'

Write-Host ""
Write-Host "Backend /health"
try {
    $h = Invoke-WebRequest -Uri ("http://127.0.0.1:{0}/health" -f $BackendPort) -UseBasicParsing -TimeoutSec 5
    $hj = $h.Content | ConvertFrom-Json
    Write-Host ("  HTTP {0}  status={1}  subscribed={2}  any_connected={3}  cached={4}" -f `
        $h.StatusCode, $hj.status, $hj.subscribed_instruments, $hj.any_connected, $hj.cached_quotes)
} catch {
    Write-Host ("  FAIL: {0}" -f $_.Exception.Message)
}

Write-Host ""
Write-Host "Backend /api/prices"
try {
    $p = Invoke-WebRequest -Uri ("http://127.0.0.1:{0}/api/prices" -f $BackendPort) -UseBasicParsing -TimeoutSec 15
    $pj = $p.Content | ConvertFrom-Json
    $n = Count-PriceInstruments -PricesDoc $pj
    Write-Host ("  HTTP {0}  instruments={1}" -f $p.StatusCode, $n)
} catch {
    Write-Host ("  FAIL: {0}" -f $_.Exception.Message)
}

Write-Host ""
Write-Host "Frontend HTTP"
try {
    $f = Invoke-WebRequest -Uri ("http://localhost:{0}/" -f $FrontendPort) -UseBasicParsing -TimeoutSec 8
    Write-Host ("  http://localhost:{0}/  HTTP {1}  bytes={2}" -f $FrontendPort, $f.StatusCode, $f.RawContentLength)
} catch {
    Write-Host ("  FAIL: {0}" -f $_.Exception.Message)
}

Write-Host ""
Write-Host "Vite proxy /api/prices"
try {
    $px = Invoke-WebRequest -Uri ("http://localhost:{0}/api/prices" -f $FrontendPort) -UseBasicParsing -TimeoutSec 15
    $pxj = $px.Content | ConvertFrom-Json
    $pn = Count-PriceInstruments -PricesDoc $pxj
    Write-Host ("  HTTP {0}  instruments={1}" -f $px.StatusCode, $pn)
} catch {
    Write-Host ("  FAIL: {0}" -f $_.Exception.Message)
}

Write-Host ""
Write-Host ("WebSocket ws://localhost:{0}/ws/prices" -f $FrontendPort)
try {
    $ws = [System.Net.WebSockets.ClientWebSocket]::new()
    $cts = [System.Threading.CancellationTokenSource]::new()
    $cts.CancelAfter(8000)
    $ws.ConnectAsync([Uri]("ws://localhost:{0}/ws/prices" -f $FrontendPort), $cts.Token).GetAwaiter().GetResult()
    $buf = [byte[]]::new(8192)
    $seg = [ArraySegment[byte]]::new($buf)
    $recv = $ws.ReceiveAsync($seg, $cts.Token).GetAwaiter().GetResult()
    Write-Host ("  connected={0}  bytes={1}" -f $ws.State, $recv.Count)
    $ws.Dispose()
} catch {
    Write-Host ("  FAIL: {0}" -f $_.Exception.Message)
}

Write-Host ("=" * 60)
Write-Host ("frontend_pid={0}  backend_pid={1}" -f $fePid, $bePid)
exit 0
