param(
    [int]$Limit = 50,
    [string]$Sort = "asc",
    [string]$SortBy = "load_dt",
    [ValidateSet("socal", "bay_area")]
    [string]$GeoProfile = "socal",
    [string]$ApiKey = "",
    [string]$DotenvPath = ".env",
    [string]$OutCsv = "data/inspection_socal_incremental.csv",
    [string]$StatePath = "data/inspection_checkpoint.json",
    [string]$SinceDate = "",
    [int]$MaxPages = 0,
    [int]$MaxRetries = 5,
    [double]$BaseWaitSeconds = 1.0,
    [double]$RequestDelaySeconds = 0,
    [int]$TimeoutSeconds = 120,
    [switch]$ResetCheckpoint
)

$ErrorActionPreference = "Stop"
. "$PSScriptRoot\api_common.ps1"

$Columns = @(
    "activity_nr",
    "reporting_id",
    "state_flag",
    "estab_name",
    "site_address",
    "site_city",
    "site_state",
    "site_zip",
    "owner_type",
    "owner_code",
    "adv_notice",
    "safety_hlth",
    "sic_code",
    "naics_code",
    "insp_type",
    "insp_scope",
    "why_no_insp",
    "union_status",
    "safety_manuf",
    "safety_const",
    "safety_marit",
    "health_manuf",
    "health_const",
    "health_marit",
    "migrant",
    "mail_street",
    "mail_city",
    "mail_state",
    "mail_zip",
    "host_est_key",
    "nr_in_estab",
    "open_date",
    "case_mod_date",
    "close_conf_date",
    "close_case_date",
    "load_dt"
)

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

function Read-Checkpoint {
    param([string]$Path)
    if (-not (Test-Path $Path)) { return $null }

    try {
        $obj = Get-Content $Path -Raw | ConvertFrom-Json
        return $obj.last_load_dt
    }
    catch {
        return $null
    }
}

function Write-Checkpoint {
    param(
        [string]$Path,
        [string]$LastLoadDt,
        [string]$CloseCaseSince,
        [int]$LimitValue,
        [string]$SortByValue
    )

    $dir = Split-Path -Parent $Path
    if (-not [string]::IsNullOrWhiteSpace($dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }

    $state = [ordered]@{
        last_success_utc = [DateTime]::UtcNow.ToString("o")
        last_load_dt = $LastLoadDt
        close_case_date_gt = $CloseCaseSince
        limit = $LimitValue
        sort = "asc"
        sort_by = $SortByValue
    }
    $state | ConvertTo-Json | Set-Content -Path $Path -Encoding UTF8
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

if ($Limit -lt 1 -or $Limit -gt 10000) {
    throw "Limit must be between 1 and 10000 (per DOL API guide)."
}

if ([string]::IsNullOrWhiteSpace($SinceDate)) {
    $SinceDate = (Get-Date).AddYears(-1).ToString("yyyy-MM-dd")
}

if ($ResetCheckpoint -and (Test-Path $StatePath)) {
    Remove-Item -Force $StatePath
}

$checkpointLoadDt = Read-Checkpoint -Path $StatePath

$conditions = @()

if ($GeoProfile -eq "socal") {
    $conditions += @{ field = "site_state"; operator = "eq"; value = "CA" }
    $conditions += @{ field = "site_zip"; operator = "gt"; value = "89999" }
    $conditions += @{ field = "site_zip"; operator = "lt"; value = "93600" }
}
else {
    # Bay Area coverage by ZIP bands: 940-941, 943-951, 954.
    $conditions += @{ field = "site_state"; operator = "eq"; value = "CA" }
    $conditions += @{
        or = @(
            @{
                and = @(
                    @{ field = "site_zip"; operator = "gt"; value = "93999" },
                    @{ field = "site_zip"; operator = "lt"; value = "94200" }
                )
            },
            @{
                and = @(
                    @{ field = "site_zip"; operator = "gt"; value = "94299" },
                    @{ field = "site_zip"; operator = "lt"; value = "95200" }
                )
            },
            @{
                and = @(
                    @{ field = "site_zip"; operator = "gt"; value = "95399" },
                    @{ field = "site_zip"; operator = "lt"; value = "95500" }
                )
            }
        )
    }
}

$conditions += @{ field = "close_case_date"; operator = "gt"; value = $SinceDate }
if (-not [string]::IsNullOrWhiteSpace($checkpointLoadDt)) {
    $conditions += @{ field = "load_dt"; operator = "gt"; value = $checkpointLoadDt }
}

$filterObject = @{ and = $conditions } | ConvertTo-Json -Compress -Depth 8
$base = "https://apiprod.dol.gov/v4/get/OSHA/inspection/json"

$outDir = Split-Path -Parent $OutCsv
if (-not [string]::IsNullOrWhiteSpace($outDir)) {
    New-Item -ItemType Directory -Path $outDir -Force | Out-Null
}
$appendMode = Test-Path $OutCsv

$offset = 0
$totalRows = 0
$pageCount = 0
$latestLoadDt = $checkpointLoadDt

Write-Host "Starting incremental pull (guide-based)." -ForegroundColor Cyan
Write-Host "geo_profile=$GeoProfile limit=$Limit sort=$Sort sort_by=$SortBy close_case_date>$SinceDate" -ForegroundColor Cyan
if (-not [string]::IsNullOrWhiteSpace($checkpointLoadDt)) {
    Write-Host "checkpoint load_dt>$checkpointLoadDt" -ForegroundColor Cyan
}

while ($true) {
    $params = @(
        "limit=$Limit",
        "offset=$offset",
        "sort=$(UrlEncode $Sort)",
        "sort_by=$(UrlEncode $SortBy)",
        "filter_object=$(UrlEncode $filterObject)",
        "X-API-KEY=$(UrlEncode $ApiKey)"
    )

    $uri = "${base}?" + ($params -join "&")
    $response = safe_request -Uri $uri -TimeoutSeconds $TimeoutSeconds -MaxRetries $MaxRetries -BaseBackoffSeconds $BaseWaitSeconds -Label "inspection"

    $rows = @()
    if ($response -and $response.data) {
        $rows = @($response.data)
    }

    $count = $rows.Count
    if ($count -eq 0) { break }

    $selected = $rows | Select-Object -Property $Columns
    if ($appendMode) {
        $selected | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8 -Append
    }
    else {
        $selected | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
        $appendMode = $true
    }

    foreach ($row in $rows) {
        $ld = [string]$row.load_dt
        if (-not [string]::IsNullOrWhiteSpace($ld)) {
            if ([string]::IsNullOrWhiteSpace($latestLoadDt) -or $ld -gt $latestLoadDt) {
                $latestLoadDt = $ld
            }
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($latestLoadDt)) {
        Write-Checkpoint -Path $StatePath -LastLoadDt $latestLoadDt -CloseCaseSince $SinceDate -LimitValue $Limit -SortByValue $SortBy
    }

    $totalRows += $count
    $pageCount += 1
    Write-Host "page=$pageCount offset=$offset rows=$count total_new=$totalRows" -ForegroundColor Green

    if ($count -lt $Limit) { break }
    if ($MaxPages -gt 0 -and $pageCount -ge $MaxPages) { break }

    $offset += $Limit
    if ($RequestDelaySeconds -gt 0) {
        Start-Sleep -Seconds $RequestDelaySeconds
    }
}

if ($totalRows -eq 0) {
    Write-Host "No new rows." -ForegroundColor Yellow
}
else {
    Write-Host "Done. Added $totalRows new rows to $OutCsv" -ForegroundColor Cyan
    Write-Host "Checkpoint saved at $StatePath (last_load_dt=$latestLoadDt)" -ForegroundColor Cyan
}

