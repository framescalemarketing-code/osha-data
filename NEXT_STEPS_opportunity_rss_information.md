# Next Steps: Opportunity RSS Information

Branch: `feature/opportunity-rss-information`

## What is already in place

- New Python sales-intelligence flow entry points in `pipeline.cli`
- Local SQLite-backed sales-intel store and JSON snapshot export
- RSS ingestion, filtering, scoring, review queue handling, and basic contact enrichment
- Current BigQuery sales snapshot sync into the local sales-intel store
- New Next.js dashboard skeleton under `dashboard/`
- README updates and unit coverage for the new backend logic

## What still needs to be finished

1. Validate the Next.js app with actual installed dependencies
   - Run `npm install` inside `dashboard/`
   - Run `npm run build` and fix any TypeScript / Next App Router issues
   - Confirm responsive layout and empty-state behavior with real snapshot files

2. Run the new data flow end to end with real credentials
   - Run `python -m pipeline.cli sync-sales-intel-current`
   - Run `python -m pipeline.cli ingest-opportunity-rss`
   - Run `python -m pipeline.cli run-sales-intel`
   - Inspect generated files under `data/dashboard/`

3. Tighten RSS source handling with live feeds
   - Verify which provided feed URLs return direct RSS vs feed-index HTML
   - Adjust discovery rules for BioSpace and any publication that exposes multiple feed endpoints
   - Confirm publication date parsing is consistent enough for date-range filters

4. Improve company extraction and matching quality
   - Review false positives / false negatives on company extraction from headlines
   - Decide whether intersection matching should stay normalized-name only or add ZIP / location context
   - Add a manual override path for merges/splits when two companies normalize similarly

5. Improve contact enrichment quality
   - Measure hit rate for website/contact-page extraction on live article pages
   - Decide whether to add a safer second-pass company website lookup strategy
   - Add richer provenance notes when contacts come from article HTML vs company pages

6. Decide whether the new sales-intel flow should become scheduled
   - If yes, add a separate workflow instead of folding it into `run-full`
   - Keep the current OSHA/FDA/public-records schedule untouched unless explicitly approved

## Recommended immediate next move

Run the new backend flow once with real BigQuery and internet access, then validate the dashboard against the generated snapshots before expanding the architecture any further.
