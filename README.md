# OSHA Sales Pipeline (Python)

This repo pulls OSHA Open Data, loads BigQuery tables, and refreshes sales-focused outputs for:

- San Diego / SoCal target ZIP ranges
- Bay Area target ZIP ranges

The pipeline now runs through Python with:

- Shared API safety controls (global pacing + retries + request budgets)
- Compliance preflight checks before any API pull
- One daily orchestrated run for all stages

## Pipeline stages

1. SoCal inspection incremental pull + BigQuery load
2. Bay Area inspection incremental pull + BigQuery load
3. Enrichment endpoint pulls + BigQuery loads + v2 sales SQL refresh

Primary command:

```powershell
python -m pipeline.cli run-full
```

## Repository layout

- `pipeline/`: Python package with config, compliance, API client, extractors, and workflows
- `scripts/*.py`: task-oriented entrypoints for direct use and scheduler integration
- `sql/refresh_sales_followup_v2.sql`: scoring/view refresh logic
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
```

Ad-hoc pulls:

```powershell
python .\scripts\query-osha-inspection.py --geo-profile socal --out-csv .\data\inspection_socal_incremental.csv --checkpoint-path .\data\inspection_checkpoint.json
python .\scripts\query-osha-endpoint.py --endpoint violation --out-csv .\data\violation_recent.csv --filter-object-json '{"field":"load_dt","operator":"gt","value":"2025-03-04"}'
```

## GitHub Actions automation (recommended)

Workflow file:

- `.github/workflows/daily-osha-pipeline.yml`

It supports:

- Daily scheduled run
- Manual run (`Run workflow`) with optional overrides (`since_date`, `api_limit`, `api_max_pages`)

### Required GitHub secrets

- `DOL_API_KEY`
- `GCP_WORKLOAD_IDENTITY_PROVIDER`
- `GCP_SERVICE_ACCOUNT_EMAIL`

### Required GitHub repository variables

- `PROJECT_ID`
- `BQ_DATASET`
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

If required secrets/variables are missing, the workflow fails before running the pull.

## Windows scheduler setup

```powershell
python .\scripts\setup_full_pipeline_task.py --start-time "09:10"
```

This creates/updates task `OSHA Full Pipeline Daily` and disables legacy per-step tasks by default.
