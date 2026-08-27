# Stop only this project's HPTL Vite (5173) and Current Price Service (8787).
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File scripts\stop_hptl_dev.ps1

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Continue'

. (Join-Path $PSScriptRoot 'lib\hptl_dev_common.ps1')

$BackendPort = 8787
$FrontendPort = 5173
$stopped = @()
$skipped = @()

function Stop-PortIfHptl {
    param(
        [int]$Port,
        [ValidateSet('backend', 'frontend')]$Kind
    )
    $listener = Get-ListenerOnPort -Port $Port
    if (-not $listener) {
        Write-Host ("  port {0}: not listening" -f $Port)
        return
    }
    $procId = [int]$listener.OwningProcess
    $proc = Get-ProcessInfo -ProcessId $procId
    $isOurs = if ($Kind -eq 'backend') {
        Test-IsHptlBackendProcess -Process $proc
    } else {
        Test-IsHptlFrontendProcess -Process $proc
    }

    if (-not $isOurs -and $proc) {
        $cmd = [string]$proc.CommandLine
        if ($Kind -eq 'backend' -and $cmd -match 'python' -and $cmd -match '8787|current_price') {
            $isOurs = $true
        }
        if ($Kind -eq 'frontend' -and $cmd -match 'node' -and ($cmd -match 'vite' -or $cmd -match '5173')) {
            $isOurs = $true
        }
    }

    if (-not $isOurs) {
        $msg = ("port {0} PID {1} is not an HPTL {2} process - left running" -f $Port, $procId, $Kind)
        Write-Host ("  SKIP: {0}" -f $msg)
        $script:skipped += $msg
        return
    }

    Write-Host ("  stopping {0} on port {1} (PID {2})" -f $Kind, $Port, $procId)
    $result = Stop-HptlProcessTree -ProcessId $procId -Reason $Kind
    $script:stopped += $result
}

Write-Host "HPTL dev stop"
Stop-PortIfHptl -Port $FrontendPort -Kind frontend
Stop-PortIfHptl -Port $BackendPort -Kind backend

Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
    Where-Object {
        (Test-IsHptlBackendProcess -Process $_) -or (Test-IsHptlFrontendProcess -Process $_)
    } |
    ForEach-Object {
        Write-Host ("  stopping orphan PID {0}" -f $_.ProcessId)
        $script:stopped += (Stop-HptlProcessTree -ProcessId ([int]$_.ProcessId) -Reason 'orphan')
    }

Start-Sleep -Milliseconds 400

$fe = Get-ListenerOnPort -Port $FrontendPort
$be = Get-ListenerOnPort -Port $BackendPort

Write-Host ""
Write-Host ("Stopped {0} process tree(s)." -f $stopped.Count)
foreach ($s in $stopped) {
    Write-Host ("  PID {0} stopped={1} [{2}]" -f $s.Pid, $s.Stopped, $s.Reason)
}
if ($skipped.Count) {
    Write-Host "Skipped:"
    $skipped | ForEach-Object { Write-Host ("  {0}" -f $_) }
}
Write-Host ("  port 5173 listening: {0}" -f [bool]$fe)
Write-Host ("  port 8787 listening: {0}" -f [bool]$be)
exit 0
