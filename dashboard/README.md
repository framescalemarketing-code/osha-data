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
