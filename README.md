# OSHA API Pull Pipeline (Python)

This repo is focused on pulling OSHA and related public/API data using environment-provided API keys.

## What is kept

- `pipeline/`: API clients, extraction logic, rate limiting, compliance checks, workflows
- `scripts/*.py`: direct entrypoints to run API pull stages
- `sql/*.sql`: refresh logic for downstream BigQuery tables used by pipeline stages
- `tests/`: unit tests for API pull and refresh workflows

## Required setup

1. Create and populate `.env.local`.
2. Set required API and compliance values.
3. Ensure `bq` and `gcloud` are authenticated.
4. Use Python 3.10+.

## API key and credential env vars

Required:

- `DOL_API_KEY`

Optional (source-specific):

- `OPENFDA_API_KEY`
- `CENSUS_API_KEY`
- `BLS_API_KEY`
- `CA_SOS_SUBSCRIPTION_KEY`

Common BigQuery/env config:

- `PROJECT_ID`
- `BQ_DATASET`
- `PUBLIC_PROJECT_ID`
- `PUBLIC_BQ_DATASET`
- `FDA_PROJECT_ID`
- `FDA_BQ_DATASET`
- `EPA_PROJECT_ID`
- `EPA_BQ_DATASET`
- `NIH_PROJECT_ID`
- `NIH_BQ_DATASET`
- `RSS_PROJECT_ID`
- `RSS_BQ_DATASET`

## Main commands

Run full pipeline:

```powershell
python -m pipeline.cli run-full
```

Run pull stages individually:

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

Ad-hoc endpoint pulls:

```powershell
python .\scripts\query-osha-inspection.py --geo-profile socal --out-csv .\data\inspection_socal_incremental.csv --checkpoint-path .\data\inspection_checkpoint.json
python .\scripts\query-osha-endpoint.py --endpoint violation --out-csv .\data\violation_recent.csv --filter-object-json '{"field":"load_dt","operator":"gt","value":"2025-03-04"}'
```

## Compliance

See `COMPLIANCE.md` for API usage and retention requirements.

## Dashboard (Metabase)

For a ready local dashboard shell, use:

- `metabase/README.md`
