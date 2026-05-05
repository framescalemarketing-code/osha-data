Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir ".." )).Path
$runtimeDir = Join-Path $scriptDir ".runtime"

function Read-DotEnv {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    $values = @{}
    if (-not (Test-Path -LiteralPath $Path)) {
        return $values
    }

    foreach ($rawLine in Get-Content -LiteralPath $Path) {
        $line = $rawLine.Trim()
        if (-not $line -or $line.StartsWith("#") -or -not $line.Contains("=")) {
            continue
        }

        $idx = $line.IndexOf("=")
        $key = $line.Substring(0, $idx).Trim()
        $value = $line.Substring($idx + 1).Trim().Trim('"').Trim("'")
        if ($key) {
            $values[$key] = $value
        }
    }

    return $values
}

function Apply-DotEnv {
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Values
    )

    foreach ($key in $Values.Keys) {
        if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($key, "Process"))) {
            [Environment]::SetEnvironmentVariable($key, $Values[$key], "Process")
        }
    }
}

function Resolve-KeyPath {
    param(
        [string]$Candidate
    )

    if ([string]::IsNullOrWhiteSpace($Candidate)) {
        return $null
    }

    if ($Candidate.Trim().StartsWith("{")) {
        return $null
    }

    if ([System.IO.Path]::IsPathRooted($Candidate)) {
        if (Test-Path -LiteralPath $Candidate) {
            return $Candidate
        }
        return $null
    }

    $resolved = Join-Path $repoRoot $Candidate
    if (Test-Path -LiteralPath $resolved) {
        return (Resolve-Path -LiteralPath $resolved).Path
    }

    return $null
}

function Get-ServiceAccountPath {
    New-Item -ItemType Directory -Force -Path $runtimeDir | Out-Null

    $pathCandidates = @(
        [Environment]::GetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", "Process"),
        [Environment]::GetEnvironmentVariable("BIGQUERY_SERVICE_ACCOUNT_KEY_PATH", "Process"),
        [Environment]::GetEnvironmentVariable("BIGQUERY_SERVICE_ACCOUNT_KEY_FILE", "Process"),
        [Environment]::GetEnvironmentVariable("BigQuery_Service_Account_Key", "Process")
    )

    foreach ($candidate in $pathCandidates) {
        $resolved = Resolve-KeyPath -Candidate $candidate
        if ($resolved) {
            return $resolved
        }
    }

    $inlineCandidates = @(
        [Environment]::GetEnvironmentVariable("BIGQUERY_SERVICE_ACCOUNT_KEY_JSON", "Process"),
        [Environment]::GetEnvironmentVariable("GOOGLE_SERVICE_ACCOUNT_JSON", "Process"),
        [Environment]::GetEnvironmentVariable("GCP_SERVICE_ACCOUNT_JSON", "Process"),
        [Environment]::GetEnvironmentVariable("BigQuery_Service_Account_Key", "Process")
    )

    foreach ($candidate in $inlineCandidates) {
        if ([string]::IsNullOrWhiteSpace($candidate)) {
            continue
        }

        $trimmed = $candidate.Trim()
        if ($trimmed.StartsWith("{") -and $trimmed.EndsWith("}")) {
            $keyPath = Join-Path $runtimeDir "gcp-service-account.json"
            Set-Content -LiteralPath $keyPath -Value $trimmed -Encoding UTF8
            return $keyPath
        }

        try {
            $decoded = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($trimmed)).Trim()
            if ($decoded.StartsWith("{") -and $decoded.EndsWith("}")) {
                $keyPath = Join-Path $runtimeDir "gcp-service-account.json"
                Set-Content -LiteralPath $keyPath -Value $decoded -Encoding UTF8
                return $keyPath
            }
        } catch {
            # Ignore invalid base64 and continue.
        }
    }

    return $null
}

function Ensure-BqAuthentication {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ProjectId
    )

    if (-not (Get-Command bq -ErrorAction SilentlyContinue)) {
        throw "bq CLI was not found. Install Google Cloud SDK and ensure bq is in PATH."
    }

    & bq "--project_id=$ProjectId" query --nouse_legacy_sql --max_rows=1 "SELECT 1" | Out-Null
    if ($LASTEXITCODE -eq 0) {
        return
    }

    if (-not (Get-Command gcloud -ErrorAction SilentlyContinue)) {
        throw "BigQuery auth failed and gcloud CLI was not found for login flow."
    }

    Write-Host "Google auth is required. Opening application-default login..." -ForegroundColor Yellow
    & gcloud auth application-default login
    if ($LASTEXITCODE -ne 0) {
        throw "gcloud application-default login failed."
    }

    & bq "--project_id=$ProjectId" query --nouse_legacy_sql --max_rows=1 "SELECT 1" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "BigQuery auth check still failed after login."
    }
}

$envLocal = Join-Path $repoRoot ".env.local"
$envFallback = Join-Path $repoRoot ".env"
$dotenvPath = if (Test-Path -LiteralPath $envLocal) { $envLocal } else { $envFallback }
$dotenvValues = Read-DotEnv -Path $dotenvPath
Apply-DotEnv -Values $dotenvValues

$serviceAccountPath = Get-ServiceAccountPath
if ($serviceAccountPath -and [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", "Process"))) {
    [Environment]::SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", $serviceAccountPath, "Process")
}

$projectId = [Environment]::GetEnvironmentVariable("PROJECT_ID", "Process")
if ([string]::IsNullOrWhiteSpace($projectId)) {
    $projectId = "cold-lead-pipeline-dashboard"
}

Write-Host "Skipping startup BigQuery auth preflight for fast launch. Live queries will authenticate on demand." -ForegroundColor DarkGray

Set-Location -LiteralPath $scriptDir
npm run dev
