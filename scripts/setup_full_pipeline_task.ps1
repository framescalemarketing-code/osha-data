param(
    [string]$TaskName = "OSHA Full Pipeline Daily",
    [string]$StartTime = "09:10",
    [string]$ProjectId = "osha-data-live-20260303",
    [string]$Dataset = "osha_raw",
    [string]$SinceDate = "",
    [int]$ApiLimit = 5000,
    [int]$ApiMaxPages = 2,
    [switch]$DisableLegacyTasks = $true
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $PSScriptRoot "run_daily_full_pipeline.ps1"

if (-not (Test-Path $runner)) {
    throw "Runner script not found: $runner"
}

$argParts = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", "`"$runner`"",
    "-ProjectId", "`"$ProjectId`"",
    "-Dataset", "`"$Dataset`"",
    "-ApiLimit", "$ApiLimit",
    "-ApiMaxPages", "$ApiMaxPages"
)
if (-not [string]::IsNullOrWhiteSpace($SinceDate)) {
    $argParts += @("-SinceDate", "`"$SinceDate`"")
}
$taskCommand = "powershell.exe " + ($argParts -join " ")

Write-Host "Creating/updating scheduled task '$TaskName' at $StartTime ..." -ForegroundColor Cyan
schtasks /Create /SC DAILY /TN "$TaskName" /TR "$taskCommand" /ST $StartTime /F | Out-Null

if ($DisableLegacyTasks) {
    $legacyTasks = @(
        "OSHA Ingest to BigQuery",
        "OSHA Ingest BayArea to BigQuery",
        "OSHA Enrichment Daily"
    )
    foreach ($legacy in $legacyTasks) {
        try {
            schtasks /Query /TN "$legacy" | Out-Null
            schtasks /Change /TN "$legacy" /DISABLE | Out-Null
            Write-Host "Disabled legacy task: $legacy" -ForegroundColor Yellow
        }
        catch {
            # Task may not exist; ignore.
        }
    }
}

Write-Host "Scheduled task is ready." -ForegroundColor Green
Write-Host "Task name: $TaskName" -ForegroundColor Green
Write-Host "Runs daily at: $StartTime" -ForegroundColor Green
