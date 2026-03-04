param(
    [string]$ProjectId = "osha-data-live-20260303",
    [string]$Dataset = "osha_raw",
    [string]$Table = "inspection_bayarea_incremental",
    [string]$CsvPath = "C:\Users\jonat\osha-data\data\inspection_bayarea_incremental.csv"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

Write-Host "Step 1: Pull incremental OSHA Bay Area data..." -ForegroundColor Cyan
try {
    & ".\scripts\query-osha-inspection.ps1" -GeoProfile bay_area -Sort desc -MaxPages 1 -OutCsv ".\data\inspection_bayarea_incremental.csv" -StatePath ".\data\inspection_bayarea_checkpoint.json"
}
catch {
    Write-Warning "Incremental Bay Area pull failed ($($_.Exception.Message)). Will use last available CSV snapshot."
    if (-not (Test-Path $CsvPath)) {
        throw "No CSV available at $CsvPath to load after pull failure."
    }
}

Write-Host "Step 2: Load CSV to BigQuery..." -ForegroundColor Cyan
bq load `
  --replace `
  --autodetect `
  --source_format=CSV `
  --skip_leading_rows=1 `
  --column_name_character_map=V2 `
  "$ProjectId`:$Dataset.$Table" `
  "$CsvPath"

Write-Host "Done." -ForegroundColor Green
