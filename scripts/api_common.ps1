function Get-RetryAfterSeconds {
    param($Response)

    if (-not $Response) { return $null }
    $value = $Response.Headers["Retry-After"]
    if (-not $value) { return $null }

    [double]$seconds = 0
    if ([double]::TryParse($value, [ref]$seconds)) {
        return [Math]::Max(0.0, $seconds)
    }

    try {
        $target = [DateTimeOffset]::Parse($value)
        $delta = ($target - [DateTimeOffset]::UtcNow).TotalSeconds
        return [Math]::Max(0.0, $delta)
    }
    catch {
        return $null
    }
}

function Enter-RateLimitWindow {
    param(
        [string]$Label = "api",
        [double]$MinIntervalSeconds = 1.0
    )

    if (-not $script:SafeRequestMutex) {
        $createdNew = $false
        $script:SafeRequestMutex = New-Object System.Threading.Mutex($false, "Local\OSHA_DOL_API_RATE_LIMIT", [ref]$createdNew)
    }
    if (-not $script:SafeRequestStateFile) {
        $script:SafeRequestStateFile = Join-Path $env:TEMP "osha_dol_api_last_call_utc.txt"
    }

    $hasHandle = $false
    try {
        $hasHandle = $script:SafeRequestMutex.WaitOne([TimeSpan]::FromSeconds(30))
        if (-not $hasHandle) {
            throw "Could not acquire rate-limit lock within timeout."
        }

        $lastCallUtc = $null
        if (Test-Path $script:SafeRequestStateFile) {
            try {
                $raw = (Get-Content -Path $script:SafeRequestStateFile -ErrorAction Stop | Select-Object -First 1)
                if (-not [string]::IsNullOrWhiteSpace($raw)) {
                    $lastCallUtc = [DateTimeOffset]::Parse($raw)
                }
            }
            catch {
                $lastCallUtc = $null
            }
        }

        $nowUtc = [DateTimeOffset]::UtcNow
        if ($null -ne $lastCallUtc) {
            $elapsed = ($nowUtc - $lastCallUtc).TotalSeconds
            if ($elapsed -lt $MinIntervalSeconds) {
                $waitMs = [int][Math]::Ceiling(($MinIntervalSeconds - $elapsed) * 1000.0)
                Write-Host "[safe_request] [$Label] rate-limit pacing, sleeping ${waitMs}ms before request." -ForegroundColor DarkGray
                Start-Sleep -Milliseconds $waitMs
                $nowUtc = [DateTimeOffset]::UtcNow
            }
        }

        Set-Content -Path $script:SafeRequestStateFile -Value $nowUtc.ToString("o") -Encoding UTF8
        $script:SafeRequestLastCallUtc = $nowUtc
    }
    finally {
        if ($hasHandle) {
            $script:SafeRequestMutex.ReleaseMutex()
        }
    }
}

function safe_request {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Uri,
        [string]$Method = "Get",
        [int]$TimeoutSeconds = 120,
        [int]$MaxRetries = 5,
        [double]$BaseBackoffSeconds = 1.0,
        [string]$Label = "api"
    )

    # MaxRetries means "retry attempts after first try". Total attempts = MaxRetries + 1.
    $totalAttempts = $MaxRetries + 1

    for ($attempt = 1; $attempt -le $totalAttempts; $attempt++) {
        Enter-RateLimitWindow -Label $Label -MinIntervalSeconds 1.0

        try {
            return Invoke-RestMethod -Method $Method -Uri $Uri -TimeoutSec $TimeoutSeconds
        }
        catch {
            $status = $null
            if ($_.Exception.Response) {
                $status = [int]$_.Exception.Response.StatusCode
            }

            $retryable = $status -in @(429, 500, 502, 503, 504)
            if (-not $retryable -or $attempt -ge $totalAttempts) {
                Write-Host "[safe_request] [$Label] request failed (status=$status, attempt=$attempt/$totalAttempts)." -ForegroundColor Red
                throw
            }

            $retryNumber = $attempt
            $retryAfter = Get-RetryAfterSeconds -Response $_.Exception.Response
            if ($null -ne $retryAfter) {
                $waitSeconds = [Math]::Max(1.0, [Math]::Round($retryAfter, 1))
                Write-Host "[safe_request] [$Label] HTTP $status, retry $retryNumber/$MaxRetries, respecting Retry-After=$waitSeconds s." -ForegroundColor Yellow
            }
            else {
                $waitSeconds = [Math]::Round(($BaseBackoffSeconds * [Math]::Pow(2, $attempt - 1)), 1)
                Write-Host "[safe_request] [$Label] HTTP $status, retry $retryNumber/$MaxRetries, exponential backoff=$waitSeconds s." -ForegroundColor Yellow
            }

            Start-Sleep -Seconds $waitSeconds
        }
    }

    throw "[safe_request] [$Label] exhausted retries ($MaxRetries)."
}
