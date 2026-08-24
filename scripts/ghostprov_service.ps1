param(
  [Parameter(Mandatory = $true, Position = 0)]
  [ValidateSet("install", "uninstall", "start", "stop", "restart", "status")]
  [string]$Action
)

$ErrorActionPreference = "Stop"
$TaskName = "GhostProv"
$Workspace = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$Bun = (Get-Command bun -ErrorAction Stop).Source
$StatusFile = Join-Path $Workspace ".ghostprov\supervisor.json"

function Request-GracefulStop {
  Push-Location $Workspace
  try {
    & $Bun "scripts/request_shutdown.ts"
    if ($LASTEXITCODE -ne 0) { throw "GhostProv shutdown request failed" }
  } finally {
    Pop-Location
  }
}

function Wait-TaskReady([int]$TimeoutSeconds = 360) {
  $Deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
  while ([DateTime]::UtcNow -lt $Deadline) {
    $Task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if (-not $Task -or $Task.State -ne "Running") { return $true }
    Start-Sleep -Milliseconds 500
  }
  return $false
}

switch ($Action) {
  "install" {
    $TaskAction = New-ScheduledTaskAction `
      -Execute $Bun `
      -Argument "run start:supervised" `
      -WorkingDirectory $Workspace
    $Trigger = New-ScheduledTaskTrigger -AtLogOn -User "$env:USERDOMAIN\$env:USERNAME"
    $Principal = New-ScheduledTaskPrincipal `
      -UserId "$env:USERDOMAIN\$env:USERNAME" `
      -LogonType Interactive `
      -RunLevel Limited
    $Settings = New-ScheduledTaskSettingsSet `
      -RestartCount 999 `
      -RestartInterval (New-TimeSpan -Minutes 1) `
      -ExecutionTimeLimit ([TimeSpan]::Zero) `
      -MultipleInstances IgnoreNew `
      -AllowStartIfOnBatteries `
      -DontStopIfGoingOnBatteries
    Register-ScheduledTask `
      -TaskName $TaskName `
      -Action $TaskAction `
      -Trigger $Trigger `
      -Principal $Principal `
      -Settings $Settings `
      -Description "GhostProv resilient host supervisor" `
      -Force | Out-Null
    Start-ScheduledTask -TaskName $TaskName
    Write-Host "GhostProv supervisor installed and started."
  }
  "uninstall" {
    if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
      Request-GracefulStop
      if (-not (Wait-TaskReady)) {
        Write-Warning "Graceful stop timed out; stopping only the owned GhostProv task."
        Stop-ScheduledTask -TaskName $TaskName
      }
      Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    }
    Write-Host "GhostProv supervisor uninstalled."
  }
  "start" {
    Start-ScheduledTask -TaskName $TaskName
    Write-Host "GhostProv supervisor start requested."
  }
  "stop" {
    Request-GracefulStop
    if (-not (Wait-TaskReady)) {
      throw "GhostProv did not stop within the graceful timeout"
    }
    Write-Host "GhostProv supervisor stopped gracefully."
  }
  "restart" {
    Push-Location $Workspace
    try {
      & $Bun "scripts/request_upgrade.ts"
      if ($LASTEXITCODE -ne 0) { throw "GhostProv upgrade request failed" }
    } finally {
      Pop-Location
    }
    Write-Host "GhostProv rolling worker restart requested; the public listener remains online."
  }
  "status" {
    $Task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($Task) { $Task | Select-Object TaskName, State }
    else { Write-Host "GhostProv scheduled task is not installed." }
    if (Test-Path -LiteralPath $StatusFile) {
      Get-Content -LiteralPath $StatusFile -Raw
    }
  }
}
