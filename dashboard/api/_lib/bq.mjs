// BigQuery client and data-fetching logic for Vercel serverless functions.
// Uses @google-cloud/bigquery instead of the bq CLI (which isn't available on Vercel).

import { BigQuery } from "@google-cloud/bigquery";
import { toLeadRecord } from "./transforms.mjs";
import { buildLeadsSql, DEFAULT_LEADS_LIMIT } from "../../shared/leads_sql.mjs";

const PROJECT_ID = process.env.PROJECT_ID || "cold-lead-pipeline-dashboard";
const DATASET = process.env.BQ_DATASET || "osha_raw";
const ENABLE_LEAD_TABLE_COUNT = String(process.env.ENABLE_LEAD_TABLE_COUNT || "0") === "1";

// In-memory cache (survives warm serverless instance, reset on cold start)
const leadsCacheByMode = {
  primary: { leads: [], generatedAt: null, totalAvailable: null, cacheUntil: 0 },
  secondary: { leads: [], generatedAt: null, totalAvailable: null, cacheUntil: 0 },
};
const CACHE_TTL_MS = 60_000;

function getCredentials() {
  const candidates = [
    process.env.BIGQUERY_SERVICE_ACCOUNT_KEY_JSON,
    process.env.GCP_SERVICE_ACCOUNT_JSON,
    process.env.GOOGLE_SERVICE_ACCOUNT_JSON,
    process.env.BigQuery_Service_Account_Key,
  ];
  for (const candidate of candidates) {
    if (!candidate || !candidate.trim()) continue;
    const trimmed = candidate.trim();
    // Try inline JSON
    if (trimmed.startsWith("{")) {
      try { return JSON.parse(trimmed); } catch (_) { /* continue */ }
    }
    // Try base64-encoded JSON
    try {
      const decoded = Buffer.from(trimmed, "base64").toString("utf8").trim();
      if (decoded.startsWith("{")) return JSON.parse(decoded);
    } catch (_) { /* continue */ }
  }
  return null;
}

let _client = null;
function getBigQueryClient() {
  if (_client) return _client;
  const credentials = getCredentials();
  _client = credentials
    ? new BigQuery({ projectId: PROJECT_ID, credentials })
    : new BigQuery({ projectId: PROJECT_ID }); // ADC for local dev
  return _client;
}

function getMode(includeSecondary) {
  return includeSecondary ? "secondary" : "primary";
}

async function fetchLiveLeads({ includeSecondary = false } = {}) {
  const bq = getBigQueryClient();
  const query = buildLeadsSql({
    projectId: PROJECT_ID,
    dataset: DATASET,
    includeSecondary,
    limit: DEFAULT_LEADS_LIMIT,
  });
  const [rows] = await bq.query({ query, useLegacySql: false });
  const records = rows.map(toLeadRecord);
  // Safety-net dedup: one record per normalized company name (SQL dedup handles location priority).
  const seen = new Set();
  return records.filter((r) => {
    const key = (r.company || '').replace(/[^A-Z0-9]/gi, '').toUpperCase();
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

async function fetchLeadTableCount() {
  const bq = getBigQueryClient();
  const [rows] = await bq.query({
    query: `SELECT COUNT(*) AS total_rows FROM \`${PROJECT_ID}.${DATASET}.dashboard_leads_current\``,
    useLegacySql: false,
  });
  const total = Number(rows?.[0]?.total_rows ?? 0);
  return Number.isFinite(total) ? total : 0;
}

export async function fetchLeadsCached({ force = false, includeSecondary = false } = {}) {
  const mode = getMode(includeSecondary);
  const cache = leadsCacheByMode[mode];
  const now = Date.now();
  if (!force && cache.generatedAt && now < cache.cacheUntil) {
    return {
      leads: cache.leads,
      generatedAt: cache.generatedAt,
      totalAvailable: cache.totalAvailable,
      cacheHit: true,
    };
  }
  const leads = await fetchLiveLeads({ includeSecondary });
  const totalAvailable = ENABLE_LEAD_TABLE_COUNT
    ? await fetchLeadTableCount().catch(() => null)
    : null;
  const generatedAt = new Date().toISOString();
  leadsCacheByMode[mode] = {
    leads,
    generatedAt,
    totalAvailable,
    cacheUntil: Date.now() + CACHE_TTL_MS,
  };
  return { leads, generatedAt, totalAvailable, cacheHit: false };
}
