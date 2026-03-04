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

if ([string]::IsNullOrWhiteSpace($SinceDate)) {
    $SinceDate = (Get-Date).AddYears(-1).ToString("yyyy-MM-dd")
}

function Pull-EndpointByLoadDate {
    param(
        [string]$Endpoint,
        [string]$OutCsv
    )

    $filter = @{ field = "load_dt"; operator = "gt"; value = $SinceDate } | ConvertTo-Json -Compress

    & ".\scripts\query-osha-endpoint.ps1" `
        -Endpoint $Endpoint `
        -Limit $ApiLimit `
        -MaxPages $ApiMaxPages `
        -Sort "desc" `
        -SortBy "load_dt" `
        -FilterObjectJson $filter `
        -OutCsv $OutCsv `
        -MaxRetries 5
}

function Try-PullEndpointByLoadDate {
    param(
        [string]$Endpoint,
        [string]$OutCsv
    )
    try {
        Pull-EndpointByLoadDate -Endpoint $Endpoint -OutCsv $OutCsv
        return $true
    }
    catch {
        Write-Warning "[$Endpoint] pull skipped due to API error: $($_.Exception.Message)"
        return $false
    }
}

function Get-EndpointSchema {
    param([string]$TableName)
    $map = @{
        "violation_recent" = "activity_nr:STRING,citation_id:STRING,delete_flag:STRING,standard:STRING,viol_type:STRING,issuance_date:STRING,abate_date:STRING,abate_complete:STRING,current_penalty:STRING,initial_penalty:STRING,contest_date:STRING,final_order_date:STRING,nr_instances:STRING,nr_exposed:STRING,rec:STRING,gravity:STRING,emphasis:STRING,hazcat:STRING,fta_insp_nr:STRING,fta_issuance_date:STRING,fta_penalty:STRING,fta_contest_date:STRING,fta_final_order_date:STRING,hazsub1:STRING,hazsub2:STRING,hazsub3:STRING,hazsub4:STRING,hazsub5:STRING,load_dt:STRING"
        "violation_event_recent" = "activity_nr:STRING,citation_id:STRING,pen_fta:STRING,hist_event:STRING,hist_date:STRING,hist_penalty:STRING,hist_abate_date:STRING,hist_vtype:STRING,hist_insp_nr:STRING,load_dt:STRING"
        "related_activity_recent" = "activity_nr:STRING,rel_type:STRING,rel_act_nr:STRING,rel_safety:STRING,rel_health:STRING,load_dt:STRING"
        "emphasis_codes_recent" = "activity_nr:STRING,prog_type:STRING,prog_value:STRING,load_dt:STRING"
        "accident_injury_recent" = "summary_nr:STRING,rel_insp_nr:STRING,age:STRING,sex:STRING,nature_of_inj:STRING,part_of_body:STRING,src_of_injury:STRING,event_type:STRING,evn_factor:STRING,hum_factor:STRING,occ_code:STRING,degree_of_inj:STRING,task_assigned:STRING,hazsub:STRING,const_op:STRING,const_op_cause:STRING,fat_cause:STRING,fall_distance:STRING,fall_ht:STRING,injury_line_nr:STRING,load_dt:STRING"
        "accident_recent" = "summary_nr:STRING,report_id:STRING,event_date:STRING,event_time:STRING,event_desc:STRING,event_keyword:STRING,const_end_use:STRING,build_stories:STRING,nonbuild_ht:STRING,project_cost:STRING,project_type:STRING,sic_list:STRING,fatality:STRING,state_flag:STRING,abstract_text:STRING,load_dt:STRING"
    }
    if (-not $map.ContainsKey($TableName)) {
        throw "No schema mapping found for $TableName"
    }
    return $map[$TableName]
}

function Get-CsvRowCount {
    param([string]$CsvPath)
    if (-not (Test-Path $CsvPath)) { return 0 }
    return (Import-Csv $CsvPath | Measure-Object).Count
}

function Load-CsvToBigQueryIfNonEmpty {
    param(
        [string]$TableName,
        [string]$CsvPath,
        [string]$Schema
    )

    if (-not (Test-Path $CsvPath)) {
        Write-Warning "[$TableName] CSV not found at $CsvPath. Skipping load."
        return
    }

    $rowCount = Get-CsvRowCount -CsvPath $CsvPath
    if ($rowCount -le 0) {
        Write-Warning "[$TableName] CSV has 0 rows; skipping load to keep existing table."
        return
    }

    Write-Host "Loading $CsvPath ($rowCount rows) -> $ProjectId`:$Dataset.$TableName" -ForegroundColor Cyan
    bq load `
      --replace `
      --source_format=CSV `
      --skip_leading_rows=1 `
      --allow_quoted_newlines `
      --schema="$Schema" `
      "$ProjectId`:$Dataset.$TableName" `
      "$CsvPath"
}

Write-Host "Step 1: Pull OSHA enrichment endpoints by load_dt > $SinceDate ..." -ForegroundColor Cyan
New-Item -ItemType Directory -Path ".\data" -Force | Out-Null

$violationCsv = ".\data\violation_recent.csv"
$violationEventCsv = ".\data\violation_event_recent.csv"
$relatedCsv = ".\data\related_activity_recent.csv"
$emphasisCsv = ".\data\emphasis_codes_recent.csv"
$injuryCsv = ".\data\accident_injury_recent.csv"
$accidentCsv = ".\data\accident_recent.csv"

$pullResults = [ordered]@{}
$pullResults["violation"] = Try-PullEndpointByLoadDate -Endpoint "violation" -OutCsv $violationCsv
$pullResults["violation_event"] = Try-PullEndpointByLoadDate -Endpoint "violation_event" -OutCsv $violationEventCsv
$pullResults["related_activity"] = Try-PullEndpointByLoadDate -Endpoint "related_activity" -OutCsv $relatedCsv
$pullResults["emphasis_codes"] = Try-PullEndpointByLoadDate -Endpoint "emphasis_codes" -OutCsv $emphasisCsv
$pullResults["accident_injury"] = Try-PullEndpointByLoadDate -Endpoint "accident_injury" -OutCsv $injuryCsv
$pullResults["accident"] = Try-PullEndpointByLoadDate -Endpoint "accident" -OutCsv $accidentCsv

Write-Host ("Pull summary: " + (($pullResults.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join ", ")) -ForegroundColor Yellow

Write-Host "Step 2: Load enrichment CSVs to BigQuery (non-empty only)..." -ForegroundColor Cyan
Load-CsvToBigQueryIfNonEmpty -TableName "violation_recent" -CsvPath $violationCsv -Schema (Get-EndpointSchema -TableName "violation_recent")
Load-CsvToBigQueryIfNonEmpty -TableName "violation_event_recent" -CsvPath $violationEventCsv -Schema (Get-EndpointSchema -TableName "violation_event_recent")
Load-CsvToBigQueryIfNonEmpty -TableName "related_activity_recent" -CsvPath $relatedCsv -Schema (Get-EndpointSchema -TableName "related_activity_recent")
Load-CsvToBigQueryIfNonEmpty -TableName "emphasis_codes_recent" -CsvPath $emphasisCsv -Schema (Get-EndpointSchema -TableName "emphasis_codes_recent")
Load-CsvToBigQueryIfNonEmpty -TableName "accident_injury_recent" -CsvPath $injuryCsv -Schema (Get-EndpointSchema -TableName "accident_injury_recent")
Load-CsvToBigQueryIfNonEmpty -TableName "accident_recent" -CsvPath $accidentCsv -Schema (Get-EndpointSchema -TableName "accident_recent")

Write-Host "Step 3: Refresh v2 sales views/tables..." -ForegroundColor Cyan
$refreshSql = Get-Content ".\sql\refresh_sales_followup_v2.sql" -Raw
$null = $refreshSql | bq query --project_id=$ProjectId --use_legacy_sql=false

Write-Host "Done. Enrichment and v2 sales tables refreshed." -ForegroundColor Green
