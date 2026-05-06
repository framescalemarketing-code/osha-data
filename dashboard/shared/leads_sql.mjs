const BAY_AREA_ANCHOR_LAT = 37.6776;
const BAY_AREA_ANCHOR_LON = -122.1297;
const BAY_AREA_RADIUS_MILES = 50;
const SAN_DIEGO_ANCHOR_LAT = 32.873;
const SAN_DIEGO_ANCHOR_LON = -117.1604;
const SAN_DIEGO_RADIUS_MILES = 50;
export const DEFAULT_LEADS_LIMIT = 2000;

const CONSUMER_NAME_REGEX =
  "\\b(LLC\\s+DBA|DBA\\s+|SALON|BARBERSHOP|BARBER|NAIL|SPA|BOUTIQUE|RESTAURANT|CAFE|COFFEE|PIZZA|TAQUERIA|DELI|BAKERY|DONUT|YOGA|FITNESS|GYM|SMOKE\\s*SHOP|VAPE|CONVENIENCE|GROCERY|MARKET|LIQUOR|OPTICAL\\s+SHOP|EYEWEAR\\s+SHOP|PET\\s+GROOMING|AUTO\\s+DETAIL|CAR\\s+WASH|TATTOO|MASSAGE|DAYCARE|CHILDCARE|BEAUTY)\\b";
const CONSUMER_INDUSTRY_REGEX =
  "\\b(RETAIL|FOOD\\s+SERVICE|RESTAURANT|ACCOMMODATION|PERSONAL\\s+CARE|BEAUTY|SALON|BARBER|CONSUMER|HOSPITALITY|COSMETOLOGY)\\b";
const B2B_EXCEPTION_REGEX =
  "\\b(MANUFACTUR|FABRICATION|ASSEMBLY|INDUSTRIAL|LABORATOR|RESEARCH|AEROSPACE|DEFENSE|CONSTRUCTION|CONTRACTOR|UTILITY|ENERGY|CHEMICAL|BIO|PHARMA|WAREHOUSE|DISTRIBUTION)\\b";
const STRATEGIC_BOOST_REGEX =
  "\\b(PHARMA|PHARMACEUT|BIOTECH|LIFE\\s*SCIENCE|LAB|LABORATOR|RESEARCH|R&D|AEROSPACE|DEFENSE|DEFENCE|AVIATION|SPACE)\\b";

function esc(value) {
  return String(value).replaceAll("`", "");
}

export function buildLeadsSql({
  projectId,
  dataset,
  includeSecondary = false,
  limit = DEFAULT_LEADS_LIMIT,
  bayTarget = 700,
  sanDiegoTarget = 700,
  restTarget = 600,
} = {}) {
  const safeProject = esc(projectId || "cold-lead-pipeline-dashboard");
  const safeDataset = esc(dataset || "osha_raw");
  const cap = Math.max(1, Number(limit) || DEFAULT_LEADS_LIMIT);
  const safeBayTarget = Math.max(0, Number(bayTarget) || 0);
  const safeSanDiegoTarget = Math.max(0, Number(sanDiegoTarget) || 0);
  const safeRestTarget = Math.max(0, Number(restTarget) || 0);
  const includeSecondarySql = includeSecondary ? "TRUE" : "FALSE";

  return `
WITH suppression_rules AS (
  SELECT
    r'${CONSUMER_NAME_REGEX}' AS consumer_name_regex,
    r'${CONSUMER_INDUSTRY_REGEX}' AS consumer_industry_regex,
    r'${B2B_EXCEPTION_REGEX}' AS b2b_exception_regex,
    r'${STRATEGIC_BOOST_REGEX}' AS strategic_boost_regex
),
base AS (
  SELECT
    inspection_id,
    account_name,
    region,
    county,
    site_city,
    site_state,
    site_zip,
    distance_from_miramar_miles,
    naics_code,
    industry_segment,
    ownership_type,
    employee_band,
    nr_employees,
    open_case_date,
    close_case_date,
    last_eye_injury_date,
    last_violation_date,
    last_violation_event_date,
    eye_lead_score,
    ppe_score,
    final_score,
    lead_tier,
    pitch_recommendation,
    eye_injury_count,
    fatality_count,
    face_head_injury_count,
    eye_injury_descriptions,
    eye_violation_count,
    prescription_violation_count,
    side_protection_violation_count,
    open_eye_violation_count,
    general_ppe_violation_count,
    open_general_ppe_violation_count,
    willful_violation_count,
    repeat_violation_count,
    total_current_penalty,
    standards_cited,
    violation_event_count,
    contested_violation_count,
    eye_emphasis_count,
    emphasis_code_list,
    related_inspection_count,
    formal_followup_count,
    total_inspection_count,
    has_open_violations
  FROM \`${safeProject}.${safeDataset}.dashboard_leads_current\`
),
geo_enriched AS (
  SELECT
    b.*,
    ROUND(
      SAFE_DIVIDE(
        ST_DISTANCE(zg.internal_point_geom, ST_GEOGPOINT(${BAY_AREA_ANCHOR_LON}, ${BAY_AREA_ANCHOR_LAT})),
        1609.344
      ),
      1
    ) AS bay_area_distance_miles,
    ROUND(
      SAFE_DIVIDE(
        ST_DISTANCE(zg.internal_point_geom, ST_GEOGPOINT(${SAN_DIEGO_ANCHOR_LON}, ${SAN_DIEGO_ANCHOR_LAT})),
        1609.344
      ),
      1
    ) AS san_diego_distance_miles
  FROM base b
  LEFT JOIN \`bigquery-public-data.geo_us_boundaries.zip_codes\` zg
    ON zg.zip_code = LPAD(REGEXP_EXTRACT(COALESCE(b.site_zip, ''), r'(\\d{5})'), 5, '0')
),
classified AS (
  SELECT
    ge.*,
    COALESCE(ge.bay_area_distance_miles, 999999) <= ${BAY_AREA_RADIUS_MILES} AS is_within_bay_area_50mi,
    COALESCE(ge.san_diego_distance_miles, 999999) <= ${SAN_DIEGO_RADIUS_MILES} AS is_san_diego_area,
    CASE
      WHEN COALESCE(ge.bay_area_distance_miles, 999999) <= ${BAY_AREA_RADIUS_MILES}
        AND COALESCE(ge.san_diego_distance_miles, 999999) <= ${SAN_DIEGO_RADIUS_MILES}
      THEN 'bay_radius|san_diego_area'
      WHEN COALESCE(ge.bay_area_distance_miles, 999999) <= ${BAY_AREA_RADIUS_MILES}
      THEN 'bay_radius'
      WHEN COALESCE(ge.san_diego_distance_miles, 999999) <= ${SAN_DIEGO_RADIUS_MILES}
      THEN 'san_diego_area'
      ELSE 'none'
    END AS geo_match_source,
    IFNULL((
      (
        COALESCE(ge.eye_injury_count, 0) > 0
        OR COALESCE(ge.face_head_injury_count, 0) > 0
        OR COALESCE(ge.eye_violation_count, 0) > 0
        OR COALESCE(ge.prescription_violation_count, 0) > 0
        OR COALESCE(ge.open_eye_violation_count, 0) > 0
        OR COALESCE(ge.general_ppe_violation_count, 0) > 0
        OR COALESCE(ge.open_general_ppe_violation_count, 0) > 0
      )
      AND COALESCE(ge.last_eye_injury_date, ge.last_violation_event_date, ge.last_violation_date)
        >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
    ), FALSE) AS qualifies_incident_3yr
  FROM geo_enriched ge
  WHERE UPPER(TRIM(COALESCE(ge.site_state, ''))) = 'CA'
),
suppressed AS (
  SELECT
    c.*,
    REGEXP_REPLACE(COALESCE(CAST(c.naics_code AS STRING), ''), r'\\D', '') AS naics_digits,
    (
      STARTS_WITH(REGEXP_REPLACE(COALESCE(CAST(c.naics_code AS STRING), ''), r'\\D', ''), '44')
      OR STARTS_WITH(REGEXP_REPLACE(COALESCE(CAST(c.naics_code AS STRING), ''), r'\\D', ''), '45')
      OR STARTS_WITH(REGEXP_REPLACE(COALESCE(CAST(c.naics_code AS STRING), ''), r'\\D', ''), '72')
    ) AS blocked_naics_family,
    REGEXP_CONTAINS(UPPER(COALESCE(c.account_name, '')), sr.consumer_name_regex) AS blocked_name,
    REGEXP_CONTAINS(UPPER(COALESCE(c.industry_segment, '')), sr.consumer_industry_regex) AS blocked_industry_text,
    REGEXP_CONTAINS(UPPER(CONCAT(COALESCE(c.account_name, ''), ' ', COALESCE(c.industry_segment, ''))), sr.b2b_exception_regex) AS b2b_exception,
    REGEXP_CONTAINS(UPPER(COALESCE(c.industry_segment, '')), sr.strategic_boost_regex)
      OR STARTS_WITH(REGEXP_REPLACE(COALESCE(CAST(c.naics_code AS STRING), ''), r'\\D', ''), '3254')
      OR STARTS_WITH(REGEXP_REPLACE(COALESCE(CAST(c.naics_code AS STRING), ''), r'\\D', ''), '3364')
      OR STARTS_WITH(REGEXP_REPLACE(COALESCE(CAST(c.naics_code AS STRING), ''), r'\\D', ''), '5417')
      OR STARTS_WITH(REGEXP_REPLACE(COALESCE(CAST(c.naics_code AS STRING), ''), r'\\D', ''), '541380')
    AS strategic_boost_match
  FROM classified c
  CROSS JOIN suppression_rules sr
),
eligible AS (
  SELECT
    s.*,
    CASE
      WHEN s.strategic_boost_match THEN 8
      WHEN STARTS_WITH(s.naics_digits, '21')
        OR STARTS_WITH(s.naics_digits, '22')
        OR STARTS_WITH(s.naics_digits, '23')
        OR STARTS_WITH(s.naics_digits, '31')
        OR STARTS_WITH(s.naics_digits, '32')
        OR STARTS_WITH(s.naics_digits, '33')
      THEN 4
      ELSE 0
    END AS strategic_boost_score,
    CASE
      WHEN s.inspection_id IS NULL THEN 'city_license'
      WHEN s.qualifies_incident_3yr THEN 'osha_incident'
      ELSE 'osha_profile'
    END AS lead_source_type,
    CAST(NULL AS STRING) AS company_domain,
    CAST(NULL AS STRING) AS website,
    CAST(0 AS INT64) AS contactability_score,
    'unresearched' AS contact_research_status,
    '' AS contact_research_notes,
    CASE
      WHEN s.inspection_id IS NULL THEN 2
      WHEN s.qualifies_incident_3yr THEN 0
      ELSE 1
    END AS source_priority,
    CASE
      WHEN COALESCE(s.san_diego_distance_miles, 999999) <= ${SAN_DIEGO_RADIUS_MILES} THEN 'san_diego'
      WHEN COALESCE(s.bay_area_distance_miles, 999999) <= ${BAY_AREA_RADIUS_MILES} THEN 'bay_area'
      ELSE 'rest_ca'
    END AS geo_bucket
  FROM suppressed s
  WHERE UPPER(TRIM(COALESCE(s.account_name, ''))) NOT IN ('', 'NA', 'N/A', 'UNKNOWN', 'NONE', 'NULL')
    -- Dashboard queue should only include companies within the active 50-mile SD/Bay territories.
    AND (
      COALESCE(s.san_diego_distance_miles, 999999) <= ${SAN_DIEGO_RADIUS_MILES}
      OR COALESCE(s.bay_area_distance_miles, 999999) <= ${BAY_AREA_RADIUS_MILES}
    )
    -- Hard-block consumer-heavy NAICS families in the primary queue.
    AND NOT s.blocked_naics_family
    AND (
      NOT s.blocked_name
      OR s.b2b_exception
    )
    AND (
      NOT s.blocked_industry_text
      OR s.b2b_exception
    )
    AND (${includeSecondarySql} OR s.inspection_id IS NOT NULL)
),
deduped AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      e.*,
      ROW_NUMBER() OVER (
        PARTITION BY REGEXP_REPLACE(UPPER(COALESCE(e.account_name, '')), r'[^A-Z0-9]', '')
        ORDER BY
          e.source_priority ASC,
          IF(e.qualifies_incident_3yr, 0, 1) ASC,
          CASE e.lead_tier
            WHEN 'P0 Hot Eye' THEN 0
            WHEN 'P1 Eye Violation' THEN 1
            WHEN 'P2 PPE Opportunity' THEN 2
            ELSE 3
          END ASC,
          (COALESCE(e.final_score, 0) + COALESCE(e.strategic_boost_score, 0)) DESC
      ) AS rn
    FROM eligible e
  )
  WHERE rn = 1
),
ranked AS (
  SELECT
    d.*,
    ROW_NUMBER() OVER (
      PARTITION BY d.geo_bucket
      ORDER BY
        d.source_priority ASC,
        IF(d.qualifies_incident_3yr, 0, 1) ASC,
        CASE d.lead_tier
          WHEN 'P0 Hot Eye' THEN 0
          WHEN 'P1 Eye Violation' THEN 1
          WHEN 'P2 PPE Opportunity' THEN 2
          ELSE 3
        END ASC,
        (COALESCE(d.final_score, 0) + COALESCE(d.strategic_boost_score, 0)) DESC,
        IF(d.has_open_violations, 1, 0) DESC
    ) AS geo_bucket_rank,
    ROW_NUMBER() OVER (
      ORDER BY
        d.source_priority ASC,
        IF(d.qualifies_incident_3yr, 0, 1) ASC,
        CASE d.lead_tier
          WHEN 'P0 Hot Eye' THEN 0
          WHEN 'P1 Eye Violation' THEN 1
          WHEN 'P2 PPE Opportunity' THEN 2
          ELSE 3
        END ASC,
        (COALESCE(d.final_score, 0) + COALESCE(d.strategic_boost_score, 0)) DESC,
        IF(d.has_open_violations, 1, 0) DESC
    ) AS overall_rank
  FROM deduped d
),
bucket_selected AS (
  SELECT *
  FROM ranked
  WHERE
    (geo_bucket = 'san_diego' AND geo_bucket_rank <= ${safeSanDiegoTarget})
    OR (geo_bucket = 'bay_area' AND geo_bucket_rank <= ${safeBayTarget})
    OR (geo_bucket = 'rest_ca' AND geo_bucket_rank <= ${safeRestTarget})
),
bucket_counts AS (
  SELECT COUNT(*) AS selected_count FROM bucket_selected
),
backfill AS (
  SELECT * EXCEPT(backfill_rank)
  FROM (
    SELECT
      r.*,
      ROW_NUMBER() OVER (ORDER BY r.overall_rank ASC) AS backfill_rank
    FROM ranked r
    LEFT JOIN bucket_selected b
      ON REGEXP_REPLACE(UPPER(COALESCE(r.account_name, '')), r'[^A-Z0-9]', '')
        = REGEXP_REPLACE(UPPER(COALESCE(b.account_name, '')), r'[^A-Z0-9]', '')
    WHERE b.account_name IS NULL
  )
  WHERE backfill_rank <= GREATEST(${cap} - (SELECT selected_count FROM bucket_counts), 0)
),
final_selection AS (
  SELECT * FROM bucket_selected
  UNION ALL
  SELECT * FROM backfill
)
SELECT
  * EXCEPT(geo_bucket_rank, overall_rank, blocked_naics_family, blocked_name, blocked_industry_text, b2b_exception, strategic_boost_match, naics_digits)
FROM final_selection
ORDER BY
  source_priority ASC,
  IF(qualifies_incident_3yr, 0, 1) ASC,
  CASE lead_tier
    WHEN 'P0 Hot Eye' THEN 0
    WHEN 'P1 Eye Violation' THEN 1
    WHEN 'P2 PPE Opportunity' THEN 2
    ELSE 3
  END ASC,
  (COALESCE(final_score, 0) + COALESCE(strategic_boost_score, 0)) DESC,
  IF(has_open_violations, 1, 0) DESC
LIMIT ${cap}
`;
}
