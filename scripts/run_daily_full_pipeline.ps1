param(
    [string]$ProjectId = "osha-data-live-20260303",
    [string]$Dataset = "osha_raw",
    [string]$SinceDate = "",
    [int]$ApiLimit = 5000,
    [int]$ApiMaxPages = 2
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

function Invoke-Step {
    param(
        [string]$Name,
        [string]$ScriptPath,
        [hashtable]$Arguments = @{}
    )

    Write-Host ("-" * 70) -ForegroundColor DarkGray
    Write-Host "Starting: $Name" -ForegroundColor Cyan
    $sw = [System.Diagnostics.Stopwatch]::StartNew()

    try {
        & $ScriptPath @Arguments
        $sw.Stop()
        Write-Host "Completed: $Name in $([Math]::Round($sw.Elapsed.TotalSeconds, 1))s" -ForegroundColor Green
    }
    catch {
        $sw.Stop()
        throw "Step failed [$Name] after $([Math]::Round($sw.Elapsed.TotalSeconds, 1))s. $($_.Exception.Message)"
    }
}

Write-Host "OSHA full pipeline started at $(Get-Date -Format o)" -ForegroundColor Cyan

Invoke-Step -Name "Ingest SoCal Inspection" -ScriptPath ".\scripts\run_daily_ingest.ps1" -Arguments @{
    ProjectId = $ProjectId
    Dataset = $Dataset
}

Invoke-Step -Name "Ingest Bay Area Inspection" -ScriptPath ".\scripts\run_daily_ingest_bayarea.ps1" -Arguments @{
    ProjectId = $ProjectId
    Dataset = $Dataset
}

$enrichmentArgs = @{
    ProjectId = $ProjectId
    Dataset = $Dataset
    ApiLimit = $ApiLimit
    ApiMaxPages = $ApiMaxPages
}
if (-not [string]::IsNullOrWhiteSpace($SinceDate)) {
    $enrichmentArgs["SinceDate"] = $SinceDate
}

Invoke-Step -Name "Ingest Enrichment + Refresh Sales Tables" -ScriptPath ".\scripts\run_daily_enrichment_ingest.ps1" -Arguments $enrichmentArgs

Write-Host ("-" * 70) -ForegroundColor DarkGray
Write-Host "OSHA full pipeline finished at $(Get-Date -Format o)" -ForegroundColor Green
