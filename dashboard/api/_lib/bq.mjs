// BigQuery client and data-fetching logic for Vercel serverless functions.
// Uses @google-cloud/bigquery instead of the bq CLI (which isn't available on Vercel).

import { BigQuery } from "@google-cloud/bigquery";
import { toLeadRecord } from "./transforms.mjs";

const PROJECT_ID = process.env.PROJECT_ID || "cold-lead-pipeline-dashboard";
const DATASET = process.env.BQ_DATASET || "osha_raw";

const BAY_AREA_ANCHOR_LAT = 37.6776;
const BAY_AREA_ANCHOR_LON = -122.1297;
const BAY_AREA_RADIUS_MILES = 50;
const SAN_DIEGO_ANCHOR_LAT = 32.873;
const SAN_DIEGO_ANCHOR_LON = -117.1604;
const SAN_DIEGO_RADIUS_MILES = 50;

// In-memory cache (survives warm serverless instance, reset on cold start)
let leadsCache = { leads: [], generatedAt: null, totalAvailable: null, cacheUntil: 0 };
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

const LEADS_SQL = `
WITH base AS (
  SELECT
    inspection_id, account_name, region, county, site_city, site_state, site_zip,
    distance_from_miramar_miles, naics_code, industry_segment, ownership_type,
    employee_band, nr_employees, open_case_date, close_case_date,
    last_eye_injury_date, last_violation_date, last_violation_event_date,
    eye_lead_score, ppe_score, final_score, lead_tier, pitch_recommendation,
    eye_injury_count, fatality_count, face_head_injury_count, eye_injury_descriptions,
    eye_violation_count, prescription_violation_count, side_protection_violation_count,
    open_eye_violation_count, general_ppe_violation_count, open_general_ppe_violation_count,
    willful_violation_count, repeat_violation_count, total_current_penalty,
    standards_cited, violation_event_count, contested_violation_count,
    eye_emphasis_count, emphasis_code_list, related_inspection_count,
    formal_followup_count, total_inspection_count, has_open_violations
  FROM \`${PROJECT_ID}.${DATASET}.dashboard_leads_current\`
),
geo_enriched AS (
  SELECT
    b.*,
    ROUND(SAFE_DIVIDE(ST_DISTANCE(zg.internal_point_geom, ST_GEOGPOINT(${BAY_AREA_ANCHOR_LON}, ${BAY_AREA_ANCHOR_LAT})), 1609.344), 1) AS bay_area_distance_miles,
    ROUND(SAFE_DIVIDE(ST_DISTANCE(zg.internal_point_geom, ST_GEOGPOINT(${SAN_DIEGO_ANCHOR_LON}, ${SAN_DIEGO_ANCHOR_LAT})), 1609.344), 1) AS san_diego_distance_miles
  FROM base b
  LEFT JOIN \`bigquery-public-data.geo_us_boundaries.zip_codes\` zg
    ON zg.zip_code = LPAD(REGEXP_EXTRACT(COALESCE(b.site_zip, ''), r'(\\d{5})'), 5, '0')
),
eligible_geo AS (
  SELECT
    ge.*,
    (COALESCE(ge.bay_area_distance_miles, 999999) <= ${BAY_AREA_RADIUS_MILES}) AS is_within_bay_area_50mi,
    (COALESCE(ge.san_diego_distance_miles, 999999) <= ${SAN_DIEGO_RADIUS_MILES}) AS is_san_diego_area,
    CASE
      WHEN COALESCE(ge.bay_area_distance_miles, 999999) <= ${BAY_AREA_RADIUS_MILES}
           AND COALESCE(ge.san_diego_distance_miles, 999999) <= ${SAN_DIEGO_RADIUS_MILES} THEN 'bay_radius|san_diego_area'
      WHEN COALESCE(ge.bay_area_distance_miles, 999999) <= ${BAY_AREA_RADIUS_MILES} THEN 'bay_radius'
      WHEN COALESCE(ge.san_diego_distance_miles, 999999) <= ${SAN_DIEGO_RADIUS_MILES} THEN 'san_diego_area'
      ELSE 'none'
    END AS geo_match_source
  FROM geo_enriched ge
  WHERE UPPER(TRIM(COALESCE(ge.site_state, ''))) = 'CA'
    AND (COALESCE(ge.bay_area_distance_miles, 999999) <= ${BAY_AREA_RADIUS_MILES}
         OR COALESCE(ge.san_diego_distance_miles, 999999) <= ${SAN_DIEGO_RADIUS_MILES})
),
classified AS (
  SELECT eg.*,
    IFNULL((
      (COALESCE(eg.eye_injury_count, 0) > 0 OR COALESCE(eg.face_head_injury_count, 0) > 0
       OR COALESCE(eg.eye_violation_count, 0) > 0 OR COALESCE(eg.prescription_violation_count, 0) > 0
       OR COALESCE(eg.open_eye_violation_count, 0) > 0 OR COALESCE(eg.general_ppe_violation_count, 0) > 0
       OR COALESCE(eg.open_general_ppe_violation_count, 0) > 0)
      AND COALESCE(eg.last_eye_injury_date, eg.last_violation_event_date, eg.last_violation_date)
        >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
    ), FALSE) AS qualifies_incident_3yr
  FROM eligible_geo eg
),
all_valid AS (
  SELECT *, CASE WHEN qualifies_incident_3yr THEN 'incident' ELSE 'profile_fit' END AS lead_type
  FROM classified
  WHERE UPPER(TRIM(COALESCE(account_name, ''))) NOT IN ('', 'NA', 'N/A', 'UNKNOWN', 'NONE', 'NULL')
),
deduped AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT av.*,
      ROW_NUMBER() OVER (
        PARTITION BY UPPER(COALESCE(av.account_name, '')), COALESCE(av.site_zip, '')
        ORDER BY IF(av.qualifies_incident_3yr, 0, 1) ASC, av.final_score DESC
      ) AS rn
    FROM all_valid av
  )
  WHERE rn = 1
)
SELECT * FROM deduped
ORDER BY
  IF(qualifies_incident_3yr, 0, 1) ASC,
  CASE lead_tier WHEN 'P0 Hot Eye' THEN 0 WHEN 'P1 Eye Violation' THEN 1 WHEN 'P2 PPE Opportunity' THEN 2 ELSE 3 END ASC,
  final_score DESC,
  IF(has_open_violations, 1, 0) DESC
LIMIT 600
`;

async function fetchLiveLeads() {
  const bq = getBigQueryClient();
  const [rows] = await bq.query({ query: LEADS_SQL, useLegacySql: false });
  return rows.map(toLeadRecord);
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

export async function fetchLeadsCached({ force = false } = {}) {
  const now = Date.now();
  if (!force && leadsCache.generatedAt && now < leadsCache.cacheUntil) {
    return { leads: leadsCache.leads, generatedAt: leadsCache.generatedAt, totalAvailable: leadsCache.totalAvailable, cacheHit: true };
  }
  const [leads, totalAvailable] = await Promise.all([
    fetchLiveLeads(),
    fetchLeadTableCount().catch(() => null),
  ]);
  const generatedAt = new Date().toISOString();
  leadsCache = { leads, generatedAt, totalAvailable, cacheUntil: Date.now() + CACHE_TTL_MS };
  return { leads, generatedAt, totalAvailable, cacheHit: false };
}
