param(
  [Parameter(Mandatory = $true, Position = 0)]
  [ValidateSet("install", "uninstall", "start", "stop", "restart", "config-restart", "status")]
  [string]$Action
)

$ErrorActionPreference = "Stop"
$TaskName = "GhostProv"
$Workspace = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$Bun = (Get-Command bun -ErrorAction Stop).Source
$ControlDescriptorFile = Join-Path $Workspace ".ghostprov\supervisor-control.json"
$DefaultStatusFile = Join-Path $Workspace ".ghostprov\supervisor.json"

function Read-ControlDescriptor {
  if (-not (Test-Path -LiteralPath $ControlDescriptorFile)) { return $null }
  try {
    $Value = Get-Content -LiteralPath $ControlDescriptorFile -Raw | ConvertFrom-Json
    if (
      $Value.schema -ne 2 -or -not $Value.statusFile -or -not $Value.supervisorPid -or
      -not $Value.startedAt -or -not $Value.instanceNonce
    ) {
      return $null
    }
    return $Value
  } catch {
    return $null
  }
}

function Get-TimeoutSeconds($Milliseconds, [int]$FallbackSeconds = 360) {
  try {
    $Value = [long]$Milliseconds
    if ($Value -gt 0) {
      return [int][Math]::Min(
        [int]::MaxValue,
        [Math]::Ceiling($Value / 1000.0) + 30
      )
    }
  } catch {}
  return $FallbackSeconds
}

function Request-GracefulStop {
  Push-Location $Workspace
  try {
    & $Bun "scripts/request_shutdown.ts"
    if ($LASTEXITCODE -ne 0) { throw "GhostProv shutdown request failed" }
  } finally {
    Pop-Location
  }
}

function Wait-TaskStopped([int]$TimeoutSeconds = 360) {
  $Deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
  while ([DateTime]::UtcNow -lt $Deadline) {
    $Task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if (-not $Task -or $Task.State -ne "Running") { return $true }
    Start-Sleep -Milliseconds 500
  }
  return $false
}

function Read-SupervisorStatus([string]$StatusFile) {
  if (-not $StatusFile -or -not (Test-Path -LiteralPath $StatusFile)) { return $null }
  try {
    return Get-Content -LiteralPath $StatusFile -Raw | ConvertFrom-Json
  } catch {
    return $null
  }
}

function Wait-FreshControlDescriptor(
  [DateTime]$NotBefore,
  [string]$PreviousInstanceNonce,
  [int]$TimeoutSeconds = 120
) {
  $Deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
  while ([DateTime]::UtcNow -lt $Deadline) {
    $Task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    $Descriptor = Read-ControlDescriptor
    if ($Task -and $Task.State -eq "Running" -and $Descriptor) {
      try {
        $StartedAt = [DateTime]::Parse([string]$Descriptor.startedAt).ToUniversalTime()
        if (
          $StartedAt -ge $NotBefore.ToUniversalTime() -and
          [string]$Descriptor.instanceNonce -ne $PreviousInstanceNonce
        ) { return $Descriptor }
      } catch {}
    }
    Start-Sleep -Milliseconds 500
  }
  return $null
}

function Wait-SupervisorReady($Descriptor, [DateTime]$NotBefore, [int]$TimeoutSeconds) {
  $Deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
  while ([DateTime]::UtcNow -lt $Deadline) {
    $Task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    $LatestDescriptor = Read-ControlDescriptor
    if ($LatestDescriptor) {
      try {
        $LatestStartedAt = [DateTime]::Parse([string]$LatestDescriptor.startedAt).ToUniversalTime()
        $CurrentStartedAt = [DateTime]::Parse([string]$Descriptor.startedAt).ToUniversalTime()
        if (
          $LatestStartedAt -ge $NotBefore.ToUniversalTime() -and
          (
            $LatestStartedAt -gt $CurrentStartedAt -or
            [string]$LatestDescriptor.instanceNonce -ne [string]$Descriptor.instanceNonce
          )
        ) { $Descriptor = $LatestDescriptor }
      } catch {}
    }
    $Status = Read-SupervisorStatus -StatusFile ([string]$Descriptor.statusFile)
    if (
      $Task -and $Task.State -eq "Running" -and $Status -and
      $Status.schema -eq 1 -and
      $Status.state -eq "ready" -and
      $Status.workerPid -and
      [long]$Status.supervisorPid -eq [long]$Descriptor.supervisorPid -and
      [string]$Status.startedAt -eq [string]$Descriptor.startedAt -and
      [string]$Status.instanceNonce -eq [string]$Descriptor.instanceNonce
    ) {
      try {
        $LastReadyAt = [DateTime]::Parse([string]$Status.lastReadyAt).ToUniversalTime()
        $DescriptorStartedAt = [DateTime]::Parse([string]$Descriptor.startedAt).ToUniversalTime()
        if ($LastReadyAt -ge $DescriptorStartedAt) { return $true }
      } catch {}
    }
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
    $Task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($Task) {
      if ($Task.State -eq "Running") {
        $Descriptor = Read-ControlDescriptor
        Request-GracefulStop
        $StopTimeout = Get-TimeoutSeconds $Descriptor.gracefulShutdownMs
        if (-not (Wait-TaskStopped -TimeoutSeconds $StopTimeout)) {
          Write-Warning "Graceful stop timed out; stopping only the owned GhostProv task."
          Stop-ScheduledTask -TaskName $TaskName
        }
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
    $Task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($Task -and $Task.State -eq "Running") {
      $Descriptor = Read-ControlDescriptor
      Request-GracefulStop
      $StopTimeout = Get-TimeoutSeconds $Descriptor.gracefulShutdownMs
      if (-not (Wait-TaskStopped -TimeoutSeconds $StopTimeout)) {
        throw "GhostProv did not stop within the graceful timeout"
      }
    }
    Write-Host "GhostProv supervisor stopped gracefully."
  }
  "restart" {
    $Task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    $Descriptor = Read-ControlDescriptor
    $Status = if ($Descriptor) {
      Read-SupervisorStatus -StatusFile ([string]$Descriptor.statusFile)
    } else { $null }
    if (
      -not $Task -or $Task.State -ne "Running" -or -not $Descriptor -or -not $Status -or
      $Status.state -ne "ready" -or
      [long]$Status.supervisorPid -ne [long]$Descriptor.supervisorPid
    ) {
      throw "GhostProv is not running and ready; no rolling restart was requested"
    }
    Push-Location $Workspace
    try {
      & $Bun "scripts/request_upgrade.ts"
      if ($LASTEXITCODE -ne 0) { throw "GhostProv upgrade request failed" }
    } finally {
      Pop-Location
    }
    Write-Host "GhostProv rolling worker restart requested; the public listener remains online."
  }
  "config-restart" {
    # Unlike the rolling restart above, this stops the supervisor itself.  The
    # next Task Scheduler launch is a fresh process, so dotenv reads .env again
    # and the worker/fleet receives a new immutable startup environment.
    $RestartRequestedAt = [DateTime]::UtcNow
    $OldDescriptor = Read-ControlDescriptor
    $OldTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($OldTask -and $OldTask.State -eq "Running") {
      Request-GracefulStop
      $StopTimeout = Get-TimeoutSeconds $OldDescriptor.gracefulShutdownMs
      if (-not (Wait-TaskStopped -TimeoutSeconds $StopTimeout)) {
        throw "GhostProv did not stop within the graceful timeout; configuration was not restarted"
      }
    }
    $OldStatusFile = if ($OldDescriptor) { [string]$OldDescriptor.statusFile } else { $DefaultStatusFile }
    $OldStatus = Read-SupervisorStatus -StatusFile $OldStatusFile
    $CleanDrain = $OldStatus -and $OldStatus.state -eq "stopped" -and $OldStatus.shutdownClean -eq $true
    Start-ScheduledTask -TaskName $TaskName
    $OldInstanceNonce = if ($OldDescriptor) { [string]$OldDescriptor.instanceNonce } else { "" }
    $NewDescriptor = Wait-FreshControlDescriptor `
      -NotBefore $RestartRequestedAt `
      -PreviousInstanceNonce $OldInstanceNonce
    if (-not $NewDescriptor) {
      throw "A fresh GhostProv task started but did not publish its control descriptor"
    }
    $ReadyTimeout = Get-TimeoutSeconds $NewDescriptor.readyTimeoutMs
    if (-not (Wait-SupervisorReady -Descriptor $NewDescriptor -NotBefore $RestartRequestedAt -TimeoutSeconds $ReadyTimeout)) {
      throw "A fresh GhostProv supervisor was started but did not become ready before the timeout"
    }
    if ($CleanDrain) {
      Write-Host "GhostProv full configuration restart completed after a proven clean drain; .env was reloaded."
    } else {
      Write-Warning "GhostProv full configuration restart completed, but the old fleet did not prove a clean drain (or predates drain-outcome reporting)."
    }
  }
  "status" {
    $Task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($Task) { $Task | Select-Object TaskName, State }
    else { Write-Host "GhostProv scheduled task is not installed." }
    Push-Location $Workspace
    try {
      & $Bun "scripts/show_supervisor_status.ts"
      if ($LASTEXITCODE -ne 0) { throw "No live descriptor-bound GhostProv status is available" }
    } finally {
      Pop-Location
    }
  }
}
