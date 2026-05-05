# Dashboard

This dashboard is a Vite + React + MUI frontend with a local API server for working the sales lead pipeline.

## Run locally

```powershell
npm install
npm run dev
```

This starts:

- UI: `http://localhost:4173`
- API: `http://127.0.0.1:8787`

The UI proxies `/api/*` requests to the local API server.

## One-click desktop startup (Windows)

Use `start-dashboard.cmd` as your desktop shortcut target. The launcher will:

- load env vars from `../.env.local` (or `../.env` fallback)
- set `GOOGLE_APPLICATION_CREDENTIALS` from a configured key path or inline key value
- run a BigQuery auth preflight and trigger `gcloud auth application-default login` if needed
- start the dashboard with `npm run dev`

Suggested shortcut target:

```text
C:\Users\jonat\osso-sales-dashboard\osha-data\dashboard\start-dashboard.cmd
```

If you keep a service account key in `.env.local`, these are supported:

- `GOOGLE_APPLICATION_CREDENTIALS` (path)
- `BIGQUERY_SERVICE_ACCOUNT_KEY_PATH` (path)
- `BIGQUERY_SERVICE_ACCOUNT_KEY_FILE` (path)
- `BIGQUERY_SERVICE_ACCOUNT_KEY_JSON` (inline JSON)
- `GOOGLE_SERVICE_ACCOUNT_JSON` (inline JSON)
- `GCP_SERVICE_ACCOUNT_JSON` (inline JSON)
- `BigQuery_Service_Account_Key` (legacy path/inline JSON/base64 JSON)

## What is included

- Left-side navigation for lead workflows
- Global search across company, source, and call reasons
- Lead queue filters for region, priority, and source
- Settings view for queue behavior and density
- Live lead loading from BigQuery tables
- Pipeline refresh trigger (`python -m pipeline.cli run-full`)
- Pull status and pull history tracking

## Live data requirements

The API uses `../.env.local` and expects:

- `PROJECT_ID`
- `BQ_DATASET`

To run pipeline refresh successfully, you also need valid pipeline credentials/compliance fields in `../.env.local`, including:

- `DOL_API_KEY`
- compliance flags/metadata (`DOL_API_TERMS_ACCEPTED`, policy/contact fields, etc.)
