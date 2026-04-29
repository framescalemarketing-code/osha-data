# Metabase Quickstart (Local)

This folder gives you a local Metabase instance for your OSHA sales pipeline data in BigQuery.

## 1) Start Metabase

From this folder:

```powershell
docker compose up -d
```

Then open:

`http://localhost:3000`

## 2) First-time setup in Metabase UI

1. Create your admin login.
2. Add data source: `BigQuery`.
3. Use a Google service account JSON key with read access to your BigQuery project/datasets.
4. Choose your billing project.
5. Save connection.

## 3) Build your first sales dashboard

Start from these tables:

- `osha_raw.sales_call_now_current`
- `osha_raw.eyewear_opportunity_actionable_current`
- `osha_raw.sales_followup_cross_source_current`

Recommended first cards:

- Top 25 accounts by `Overall Sales Score`
- Priority breakdown (`Overall Sales Priority`)
- Region split (`Region`)
- Immediate call list (`Should Look At Now` = `Call Now`)

## 4) Stop Metabase

```powershell
docker compose down
```
