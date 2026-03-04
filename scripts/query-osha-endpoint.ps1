param(
    [Parameter(Mandatory = $true)]
    [string]$Endpoint,
    [int]$Limit = 200,
    [string]$Sort = "desc",
    [string]$SortBy = "load_dt",
    [string]$FilterObjectJson = "",
    [string]$ApiKey = "",
    [string]$DotenvPath = ".env",
    [string]$OutCsv = "",
    [int]$MaxPages = 0,
    [int]$MaxRetries = 5,
    [double]$BaseWaitSeconds = 1.0,
    [double]$RequestDelaySeconds = 0,
    [int]$TimeoutSeconds = 120,
    [switch]$Append
)

$ErrorActionPreference = "Stop"
. "$PSScriptRoot\api_common.ps1"

function Get-ApiKeyFromDotenv {
    param([string]$Path)
    if (-not (Test-Path $Path)) { return "" }
    $line = Get-Content $Path | Where-Object { $_ -match "^\s*DOL_API_KEY\s*=" } | Select-Object -First 1
    if (-not $line) { return "" }
    return (($line -split "=", 2)[1].Trim().Trim('"').Trim("'"))
}

function UrlEncode {
    param([string]$Value)
    return [System.Uri]::EscapeDataString($Value)
}

if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    $ApiKey = $env:DOL_API_KEY
}
if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    $ApiKey = Get-ApiKeyFromDotenv -Path $DotenvPath
}
if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    throw "Missing API key. Set DOL_API_KEY in env or .env, or pass -ApiKey."
}

if ([string]::IsNullOrWhiteSpace($OutCsv)) {
    $OutCsv = "data/$Endpoint.csv"
}

if ($Limit -lt 1 -or $Limit -gt 10000) {
    throw "Limit must be between 1 and 10000."
}

$outDir = Split-Path -Parent $OutCsv
if (-not [string]::IsNullOrWhiteSpace($outDir)) {
    New-Item -ItemType Directory -Path $outDir -Force | Out-Null
}

$metadataUrl = "https://apiprod.dol.gov/v4/get/OSHA/$Endpoint/json/metadata?X-API-KEY=$(UrlEncode $ApiKey)"
$metadata = safe_request -Uri $metadataUrl -TimeoutSeconds $TimeoutSeconds -MaxRetries $MaxRetries -BaseBackoffSeconds $BaseWaitSeconds -Label "$Endpoint metadata"
$columns = @($metadata | ForEach-Object { $_.short_name } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique)
if ($columns.Count -eq 0) {
    throw "[$Endpoint] No columns found in metadata."
}

if (-not $Append -and (Test-Path $OutCsv)) {
    Remove-Item -Path $OutCsv -Force
}

$base = "https://apiprod.dol.gov/v4/get/OSHA/$Endpoint/json"
$offset = 0
$totalRows = 0
$pageCount = 0
$wroteAnyRows = $false

Write-Host "[$Endpoint] start limit=$Limit sort=$Sort sort_by=$SortBy append=$Append" -ForegroundColor Cyan

while ($true) {
    $params = @(
        "limit=$Limit",
        "offset=$offset",
        "sort=$(UrlEncode $Sort)",
        "sort_by=$(UrlEncode $SortBy)",
        "X-API-KEY=$(UrlEncode $ApiKey)"
    )

    if (-not [string]::IsNullOrWhiteSpace($FilterObjectJson)) {
        $params += "filter_object=$(UrlEncode $FilterObjectJson)"
    }

    $uri = "${base}?" + ($params -join "&")
    $response = safe_request -Uri $uri -TimeoutSeconds $TimeoutSeconds -MaxRetries $MaxRetries -BaseBackoffSeconds $BaseWaitSeconds -Label "$Endpoint data"

    $rows = @()
    if ($response -and $response.data) {
        $rows = @($response.data)
    }

    $count = $rows.Count
    if ($count -eq 0) { break }

    $selected = $rows | Select-Object -Property $columns
    if ($wroteAnyRows -or (Test-Path $OutCsv)) {
        $selected | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8 -Append
    }
    else {
        $selected | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
    }
    $wroteAnyRows = $true

    $totalRows += $count
    $pageCount += 1
    Write-Host "[$Endpoint] page=$pageCount offset=$offset rows=$count total=$totalRows" -ForegroundColor Green

    if ($count -lt $Limit) { break }
    if ($MaxPages -gt 0 -and $pageCount -ge $MaxPages) { break }
    $offset += $Limit
    if ($RequestDelaySeconds -gt 0) {
        Start-Sleep -Seconds $RequestDelaySeconds
    }
}

if (-not (Test-Path $OutCsv)) {
    ($columns -join ",") | Set-Content -Path $OutCsv -Encoding UTF8
}

if ($totalRows -eq 0) {
    Write-Host "[$Endpoint] no rows returned (header-only file created/retained)." -ForegroundColor Yellow
}
else {
    Write-Host "[$Endpoint] done, wrote $totalRows rows to $OutCsv" -ForegroundColor Cyan
}
