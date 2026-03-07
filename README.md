# OSHA Sales Pipeline (Python)

This repo pulls OSHA Open Data, loads BigQuery tables, and refreshes sales-focused outputs for:

- San Diego / SoCal target ZIP ranges
- Bay Area target ZIP ranges

The pipeline now runs through Python with:

- Shared API safety controls (global pacing + retries + request budgets)
- Compliance preflight checks before any API pull
- One daily orchestrated run for all stages
- Unit tests in CI before the scheduled run starts
- Pipeline log artifact upload in GitHub Actions for failure review

## Pipeline stages

1. SoCal inspection incremental pull + BigQuery load
2. Bay Area inspection incremental pull + BigQuery load
3. Enrichment endpoint pulls + BigQuery loads + v2 sales SQL refresh
4. Public enrichment pulls (Census CBP, BLS, USAspending) + derived public signals tables
5. FDA enrichment pulls (openFDA registration listing, 510(k), PMA) + eyewear-support scoring tables
6. EPA enrichment pulls (ECHO facility signals) + compliance/environment scoring tables
7. NIH enrichment pulls (RePORTER project signals) + research/lab scoring tables
8. RSS current-awareness pulls + article/alignment watchlist tables
9. Cross-source sales priority outputs for action queues and call review

Primary command:

```powershell
python -m pipeline.cli run-full
```

## Repository layout

- `pipeline/`: Python package with config, compliance, API client, extractors, and workflows
- `scripts/*.py`: task-oriented entrypoints for direct use and scheduler integration
- `sql/refresh_sales_followup_v2.sql`: scoring/view refresh logic
- `sql/refresh_public_signals.sql`: public enrichment summary tables
- `sql/refresh_fda_followup.sql`: FDA facility eyewear-support scoring refresh logic
- `sql/refresh_epa_followup.sql`: EPA facility support scoring refresh logic
- `sql/refresh_nih_followup.sql`: NIH research organization support scoring refresh logic
- `sql/refresh_rss_signals.sql`: RSS article normalization + alignment watchlist refresh logic
- `sql/refresh_sales_priority_outputs.sql`: cross-source match + final sales priority outputs
- `data/`: runtime CSV/checkpoint artifacts (gitignored except `.gitkeep`)

## Compliance and API controls

See `COMPLIANCE.md` for full details. Key controls:

- `DOL_API_TERMS_ACCEPTED=true` required in strict mode
- `DOL_API_POLICY_REVIEWED_ON`, `DOL_API_CONTACT_EMAIL`, `DOL_API_INTENDED_USE` required in strict mode
- Global min request interval: `API_MIN_INTERVAL_SECONDS` (default `1.0`)
- Retry behavior for `429/5xx` honoring `Retry-After`
- Hard request cap per run: `API_MAX_REQUESTS_PER_RUN`

## SQL outputs

`sql/refresh_sales_followup_v2.sql` refreshes:

- `osha_raw.v_sales_followup_sandiego_v2`
- `osha_raw.v_sales_followup_bayarea_v2`
- `osha_raw.sales_followup_sandiego_current`
- `osha_raw.sales_followup_bayarea_current`
- `osha_raw.sales_call_now_current`
- `osha_raw.eyewear_opportunity_current`
- `osha_raw.eyewear_opportunity_actionable_current`

## One-time setup

1. Create `.env` from `.env.example`.
2. Fill required secrets and compliance fields in `.env`.
3. Confirm `bq` and `gcloud` auth are ready in your shell.
4. Confirm Python 3.10+ is available.

## Main commands

Run full daily workflow:

```powershell
python -m pipeline.cli run-full
```

Run individual stages:

```powershell
python -m pipeline.cli ingest-socal
python -m pipeline.cli ingest-bayarea
python -m pipeline.cli ingest-enrichment
python -m pipeline.cli ingest-public-signals
python -m pipeline.cli ingest-fda-signals
python -m pipeline.cli ingest-epa-signals
python -m pipeline.cli ingest-nih-signals
python -m pipeline.cli ingest-ca-sos-signals
python -m pipeline.cli ingest-rss-signals
python -m pipeline.cli ingest-local-osha-downloads
```

Public signals wrapper script:

```powershell
python .\scripts\run_daily_public_signals.py
```

FDA signals wrapper script:

```powershell
python .\scripts\run_daily_fda_signals.py
```

EPA signals wrapper script:

```powershell
python .\scripts\run_daily_epa_signals.py
```

NIH signals wrapper script:

```powershell
python .\scripts\run_daily_nih_signals.py
```

RSS signals wrapper script:

```powershell
python .\scripts\run_daily_rss_signals.py
```

Manual local OSHA download ingest:

```powershell
python -m pipeline.cli ingest-local-osha-downloads
```

This stage reads locally downloaded OSHA files from `data/downloads/osha/`, loads them to BigQuery, refreshes the local OSHA helper tables, and then rebuilds the final sales-priority outputs so the new ITA / severe-injury / health-sample signals flow into `sales_call_now_*`. It is intentionally not part of the GitHub Actions schedule because the GitHub runner cannot see files downloaded on your local machine.

Additional `.env` keys for public signals:

- `PUBLIC_PROJECT_ID` (optional, defaults to `PROJECT_ID`)
- `PUBLIC_BQ_DATASET` (optional, default `public_signals`)
- `CENSUS_API_KEY` (optional but recommended)
- `BLS_API_KEY` (optional but recommended)
- `CENSUS_CBP_YEAR` (optional, default `2022`)

Additional `.env` keys for FDA signals:

- `OPENFDA_API_KEY` (optional but strongly recommended for higher daily quota)
- `FDA_PROJECT_ID` (optional, defaults to `PROJECT_ID`)
- `FDA_BQ_DATASET` (optional, default `fda_raw`)
- `FDA_LOOKBACK_YEARS` (optional, default `10`)

Additional `.env` keys for EPA / NIH / California SOS:

- `EPA_PROJECT_ID` (optional, defaults to `PROJECT_ID`)
- `EPA_BQ_DATASET` (optional, default `epa_raw`)
- `NIH_PROJECT_ID` (optional, defaults to `PROJECT_ID`)
- `NIH_BQ_DATASET` (optional, default `nih_raw`)
- `NIH_LOOKBACK_YEARS` (optional, default `5`)
- `CA_SOS_PROJECT_ID` (optional, defaults to `PROJECT_ID`)
- `CA_SOS_BQ_DATASET` (optional, default `ca_sos_raw`)
- `CA_SOS_SUBSCRIPTION_KEY` (required only when California SOS enrichment is enabled)

Additional `.env` keys for RSS feed grouping:

- `RSS_PROJECT_ID` (optional, defaults to `PROJECT_ID`)
- `RSS_BQ_DATASET` (optional, default `rss_feed`)
- `RSS_FEED_URLS` (optional, semicolon-delimited `name|url` list; defaults are industry publications across biopharma, manufacturing, construction, chemicals, energy, logistics, automotive, and electronics)
- `RSS_LOOKBACK_DAYS` (optional, default `30`)
- `RSS_MAX_ITEMS_PER_FEED` (optional, default `40`)
- `RSS_COMPANY_SEARCH_LIMIT` (optional, default `0`; set above zero only if you want company-specific Google News searches in addition to the fixed industry feeds)

Ad-hoc pulls:

```powershell
python .\scripts\query-osha-inspection.py --geo-profile socal --out-csv .\data\inspection_socal_incremental.csv --checkpoint-path .\data\inspection_checkpoint.json
python .\scripts\query-osha-endpoint.py --endpoint violation --out-csv .\data\violation_recent.csv --filter-object-json '{"field":"load_dt","operator":"gt","value":"2025-03-04"}'
```

## GitHub Actions automation

Workflow file:

- `.github/workflows/daily-osha-pipeline.yml`

It is the primary scheduler for this repo. It supports:

- Daily scheduled run
- Manual run (`Run workflow`) with optional overrides (`since_date`, `api_limit`, `api_max_pages`)
- Unit test verification before the pipeline run
- Uploaded pipeline log artifact on every run
- GitHub-first orchestration with no Windows Task Scheduler dependency

### Required GitHub secrets

- `DOL_API_KEY`
- `GCP_WORKLOAD_IDENTITY_PROVIDER`
- `GCP_SERVICE_ACCOUNT_EMAIL`
- `OPENFDA_API_KEY` (recommended; pipeline will still run with lower openFDA quota if omitted)

### Required GitHub repository variables

- `PROJECT_ID`
- `BQ_DATASET`
- `PUBLIC_PROJECT_ID`
- `PUBLIC_BQ_DATASET`
- `DOL_API_POLICY_URL`
- `DOL_API_POLICY_REVIEWED_ON`
- `DOL_API_CONTACT_EMAIL`
- `DOL_API_INTENDED_USE`
- `DATA_RETENTION_DAYS`
- `API_MIN_INTERVAL_SECONDS`
- `API_TIMEOUT_SECONDS`
- `API_MAX_RETRIES`
- `API_BASE_BACKOFF_SECONDS`
- `API_MAX_REQUESTS_PER_RUN`
- `API_USER_AGENT`

### Optional GitHub repository variables

- `ACCIDENT_API_LIMIT` (default `1000`)
- `ACCIDENT_API_MAX_PAGES` (default `1`)
- `CENSUS_CBP_YEAR` (default `2022`)
- `RSS_PROJECT_ID` (default `PROJECT_ID`)
- `RSS_BQ_DATASET` (default `rss_feed`)
- `FDA_PROJECT_ID` (optional; if set, FDA tables are written to this separate project)
- `FDA_BQ_DATASET` (default `fda_raw`)
- `FDA_LOOKBACK_YEARS` (default `10`)

### Optional GitHub secrets

- `CENSUS_API_KEY` (optional; public mode still works with lower/no-key quota)
- `BLS_API_KEY` (optional; public mode still works with lower/no-key quota)

If required secrets/variables are missing, the workflow fails before running the pull.

## Key outputs

OSHA source outputs:

- `osha_raw.sales_followup_sandiego_current`
- `osha_raw.sales_followup_bayarea_current`
- `osha_raw.sales_followup_all_current`

FDA source outputs:

- `fda_raw.sales_followup_facility_sandiego_current`
- `fda_raw.sales_followup_facility_bayarea_current`
- `fda_raw.sales_followup_facility_current`

EPA source outputs:

- `epa_raw.sales_followup_facility_sandiego_current`
- `epa_raw.sales_followup_facility_bayarea_current`
- `epa_raw.sales_followup_facility_current`

NIH source outputs:

- `nih_raw.sales_followup_org_sandiego_current`
- `nih_raw.sales_followup_org_bayarea_current`
- `nih_raw.sales_followup_org_current`

Cross-source outputs for Google Sheets / call review:

- `osha_raw.sales_followup_cross_source_current`
- `osha_raw.sales_call_now_current`
- `osha_raw.sales_call_now_sandiego_current`
- `osha_raw.sales_call_now_bayarea_current`
- `osha_raw.eyewear_opportunity_current`
- `osha_raw.eyewear_opportunity_actionable_current`
- `osha_raw.eyewear_opportunity_sandiego_current`
- `osha_raw.eyewear_opportunity_bayarea_current`

RSS current-awareness outputs:

- `rss_feed.feed_items_raw`
- `rss_feed.rss_articles_current`
- `rss_feed.rss_company_matches_current`
- `rss_feed.alignment_watchlist_current`
- `rss_feed.rss_watchlist_current`

Local OSHA download helper outputs:

- `osha_raw.ita_300a_summary_ca_current`
- `osha_raw.ita_case_detail_ca_current`
- `osha_raw.severe_injury_ca_current`
- `osha_raw.health_samples_focus_current`
- `osha_raw.osha_downloads_company_signals_current`

`eyewear_opportunity_current` classifies records into:

- `Direct Need`: explicit eye / prescription evidence from OSHA citations or downloadable OSHA injury records
- `Probable Need`: likely eyewear-program need based on supportive OSHA download signals, PPE context, or cross-source support
- `Fit Only`: commercially relevant account without enough need evidence for active outreach

Use `eyewear_opportunity_actionable_current` when you want to work only the `Direct Need` and `Probable Need` records.

## Google Sheets formatting

Use `scripts/google_sheets_format_sales_followup.gs` in the bound spreadsheet Apps Script project.

Main entry points:

- `setupAllOshaViewsSalesFriendly()`
- `installHourlyOshaViewTriggers()`
- `installDailyOshaViewTriggersAt9am()`
- `installOshaViewAssets()`
