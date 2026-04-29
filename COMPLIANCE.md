# Compliance Controls

This pipeline is designed to enforce operational controls around OSHA/DOL API usage.

## Required controls

Set these in `.env.local` before any production run:

- `DOL_API_TERMS_ACCEPTED=true`
- `DOL_API_POLICY_REVIEWED_ON=YYYY-MM-DD`
- `DOL_API_CONTACT_EMAIL=team-or-owner@example.com`
- `DOL_API_INTENDED_USE=short business purpose statement`

If any required field is missing and `COMPLIANCE_STRICT_MODE=true`, the run fails before calling the API.

## API safety controls

- Global cross-process pacing through lock/state files:
  - `API_MIN_INTERVAL_SECONDS` (default `1.0`)
- Retry handling for `429/5xx`:
  - Honors `Retry-After` header when present
  - Exponential backoff when absent
  - `API_MAX_RETRIES` (default `5`)
- Hard per-run budget cap:
  - `API_MAX_REQUESTS_PER_RUN` (default `600`)
  - If exceeded, run aborts

## Data governance checklist

- Keep `DOL_API_KEY` in `.env.local` only, never in source control.
- Set `DATA_RETENTION_DAYS` to your approved retention period.
- Restrict who can run scheduler tasks and read local `data/` files.
- Review DOL API policy periodically and update `DOL_API_POLICY_REVIEWED_ON`.
- Keep an auditable changelog of scoring SQL and outbound usage of derived tables.

## Public enrichment sources

When using Census/BLS/USAspending public APIs:

- Use public endpoints only (no sensitive/FOUO endpoints).
- Follow source attribution/disclaimer language required by each provider.
- Do not imply agency endorsement of your product or analysis.
- Do not alter source data and present it as unchanged official output.
- Keep per-source access dates in run logs or reports for auditability.

## Operational note

Compliance controls in code reduce accidental misuse but do not replace legal review. Your organization remains responsible for confirming permitted use under DOL terms and applicable law.

