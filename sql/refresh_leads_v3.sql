-- =============================================================================
-- refresh_leads_v3.sql
-- Eye-focused lead scoring pipeline.
-- Scoring is entirely evidence-based using all 6 OSHA enrichment endpoints.
-- Date window: rolling 1 year from today (works in any future year).
-- =============================================================================

-- Ensure California incremental table exists for statewide ingest command.
CREATE TABLE IF NOT EXISTS `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.inspection_california_incremental` AS
SELECT * FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.inspection_socal_incremental` WHERE 1 = 0;

-- ???????????????????????????????????????????????????????????????????????????
-- STEP 1 - INSPECTION BASE (all California, rolling 1-year window)
-- ???????????????????????????????????????????????????????????????????????????
CREATE TEMP TABLE inspection_base_v3 AS
WITH source_union AS (
  SELECT * FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.inspection_california_incremental`
  UNION ALL
  SELECT * FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.inspection_socal_incremental`
  UNION ALL
  SELECT * FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.inspection_bayarea_incremental`
),
deduped AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      su.*,
      ROW_NUMBER() OVER (
        PARTITION BY SAFE_CAST(activity_nr AS INT64)
        ORDER BY DATE(load_dt) DESC NULLS LAST
      ) AS rn
    FROM source_union su
  )
  WHERE rn = 1
),
zip_geo AS (
  SELECT
    zip_code,
    ANY_VALUE(REGEXP_REPLACE(UPPER(county), r' COUNTY$', '')) AS county,
    ANY_VALUE(internal_point_geom) AS internal_point_geom
  FROM `bigquery-public-data.geo_us_boundaries.zip_codes`
  WHERE state_code = 'CA'
  GROUP BY zip_code
),
miramar_anchor AS (
  SELECT ANY_VALUE(internal_point_geom) AS anchor_geom
  FROM zip_geo
  WHERE zip_code = '92121'
)
SELECT
  TRIM(d.estab_name) AS company_name,
  UPPER(REGEXP_REPLACE(TRIM(d.estab_name), r'[^A-Z0-9]', '')) AS company_key,
  d.site_address AS address,
  d.site_city AS city,
  d.site_state AS state,
  LPAD(CAST(d.site_zip AS STRING), 5, '0') AS zip5,
  COALESCE(zg.county, 'UNKNOWN') AS county,
  ROUND(
    SAFE_DIVIDE(ST_DISTANCE(zg.internal_point_geom, ma.anchor_geom), 1609.344),
    1
  ) AS distance_from_miramar_miles,
  CAST(d.naics_code AS STRING) AS naics_code,
  COALESCE(CAST(d.naics_code AS STRING), '') AS naics_prefix,
  d.owner_type,
  d.insp_type,
  d.safety_hlth,
  COALESCE(SAFE_CAST(d.nr_in_estab AS INT64), 0) AS nr_in_estab,
  DATE(d.open_date) AS open_case_date,
  DATE(d.close_case_date) AS close_case_date,
  CAST(d.activity_nr AS INT64) AS activity_nr,
  CASE
    WHEN COALESCE(zg.county, '') IN (
      'IMPERIAL',
      'KERN',
      'LOS ANGELES',
      'ORANGE',
      'RIVERSIDE',
      'SAN BERNARDINO',
      'SAN DIEGO',
      'SANTA BARBARA',
      'VENTURA'
    ) THEN 'Southern California'
    WHEN COALESCE(zg.county, '') = '' OR COALESCE(zg.county, '') = 'UNKNOWN' THEN 'Unknown California'
    ELSE 'Northern California'
  END AS region,
  d.load_dt
FROM deduped d
LEFT JOIN zip_geo zg
  ON LPAD(CAST(d.site_zip AS STRING), 5, '0') = zg.zip_code
CROSS JOIN miramar_anchor ma
WHERE (
    DATE(d.open_date)  >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
    OR DATE(d.close_case_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  )
  AND UPPER(COALESCE(d.site_state, '')) = 'CA'
;

-- ???????????????????????????????????????????????????????????????????????????
-- STEP 2 - PER-INSPECTION SIGNALS FROM ENRICHMENT ENDPOINTS
-- ???????????????????????????????????????????????????????????????????????????

-- 2a. Eye injury signals from accident_injury + accident (part_of_body = 13 = Eye)
CREATE TEMP TABLE eye_injury_signals_v3 AS
WITH ai AS (
  SELECT
    SAFE_CAST(ai.rel_insp_nr AS INT64) AS activity_nr,
    ai.degree_of_inj,
    ai.nature_of_inj,
    ai.src_of_injury,
    ai.event_type,
    a.event_date,
    a.event_desc,
    a.fatality
  FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.accident_injury_recent` ai
  LEFT JOIN `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.accident_recent` a
    ON SAFE_CAST(ai.summary_nr AS INT64) = SAFE_CAST(a.summary_nr AS INT64)
  WHERE ai.part_of_body = '13'                                -- OSHA code 13 = Eye(s)
    AND (
      DATE(a.event_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
      OR a.event_date IS NULL
    )
),
face_head AS (
  SELECT
    SAFE_CAST(ai.rel_insp_nr AS INT64) AS activity_nr
  FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.accident_injury_recent` ai
  LEFT JOIN `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.accident_recent` a
    ON SAFE_CAST(ai.summary_nr AS INT64) = SAFE_CAST(a.summary_nr AS INT64)
  WHERE ai.part_of_body IN ('10','11','14','15','16','17','19')  -- head, face, neck, ear, scalp, skull, eye orbit
    AND (
      DATE(a.event_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
      OR a.event_date IS NULL
    )
)
SELECT
  a.activity_nr,
  COUNT(*) AS eye_injury_count,
  COUNTIF(UPPER(a.fatality) = 'X') AS fatality_count,
  STRING_AGG(
    CONCAT(
      COALESCE(a.event_type, ''),
      CASE WHEN a.degree_of_inj IS NOT NULL THEN CONCAT(' - ', a.degree_of_inj) ELSE '' END,
      CASE WHEN a.src_of_injury IS NOT NULL THEN CONCAT(' (', a.src_of_injury, ')') ELSE '' END
    ),
    ' | ' ORDER BY a.event_date DESC
    LIMIT 5
  ) AS eye_injury_descriptions,
  MAX(DATE(a.event_date)) AS last_eye_injury_date,
  COALESCE((SELECT COUNT(DISTINCT fh.activity_nr) FROM face_head fh WHERE fh.activity_nr = a.activity_nr), 0) AS face_head_injury_count
FROM ai a
GROUP BY a.activity_nr
;

-- 2b. Eye/face and general PPE violation signals
CREATE TEMP TABLE violation_signals_v3 AS
SELECT
  SAFE_CAST(activity_nr AS INT64) AS activity_nr,

  -- Eye/face protection citations (1910.133 general industry, 1926.102 construction)
  COUNTIF(
    REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.133')
    OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.102')
  ) AS eye_violation_count,

  -- Prescription-specific citations
  COUNTIF(
    REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.133\(a\)\(3\)')
    OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.102\(a\)\(3\)')
  ) AS prescription_violation_count,

  -- Side protection
  COUNTIF(
    REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.133\(a\)\(2\)')
    OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.102\(a\)\(2\)')
  ) AS side_protection_violation_count,

  -- Open / unabated eye violations (no abatement completion, no final order)
  COUNTIF((
    REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.133')
    OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.102')
  ) AND (
    (abate_complete IS NULL OR UPPER(CAST(abate_complete AS STRING)) NOT IN ('Y','YES','1','TRUE'))
    AND final_order_date IS NULL
  )) AS open_eye_violation_count,

  -- General PPE (1910.132 general industry, 1926.95 construction)
  COUNTIF(
    REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.132')
    OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.95')
  ) AS general_ppe_violation_count,

  -- Open general PPE violations
  COUNTIF((
    REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.132')
    OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.95')
  ) AND (
    (abate_complete IS NULL OR UPPER(CAST(abate_complete AS STRING)) NOT IN ('Y','YES','1','TRUE'))
    AND final_order_date IS NULL
  )) AS open_general_ppe_violation_count,

  -- Total penalty
  SUM(COALESCE(SAFE_CAST(current_penalty AS FLOAT64), 0)) AS total_current_penalty,
  SUM(COALESCE(SAFE_CAST(initial_penalty AS FLOAT64), 0)) AS total_initial_penalty,

  -- All cited standards (pipe separated) for display
  STRING_AGG(DISTINCT CAST(standard AS STRING), ' | ' ORDER BY CAST(standard AS STRING)) AS standards_cited,

  -- Willful / repeat flags
  COUNTIF(UPPER(CAST(viol_type AS STRING)) IN ('W','WILLFUL')) AS willful_violation_count,
  COUNTIF(UPPER(CAST(viol_type AS STRING)) IN ('R','REPEAT')) AS repeat_violation_count,

  -- Most recent issuance date
  MAX(DATE(issuance_date)) AS last_violation_date

FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.violation_recent`
GROUP BY 1
;

-- 2c. Violation events (contest / penalty history - shows engagement and persistence)
CREATE TEMP TABLE violation_event_signals_v3 AS
SELECT
  SAFE_CAST(activity_nr AS INT64) AS activity_nr,
  COUNT(*) AS violation_event_count,
  COUNTIF(
    UPPER(CAST(hist_event AS STRING)) IN ('C','CONTEST','CONTESTED','INFORMAL','FORMAL')
  ) AS contested_violation_count,
  MAX(DATE(hist_date)) AS last_violation_event_date
FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.violation_event_recent`
GROUP BY 1
;

-- 2d. Emphasis codes (OSHA National/Local Emphasis Programs active at the site)
CREATE TEMP TABLE emphasis_signals_v3 AS
SELECT
  SAFE_CAST(activity_nr AS INT64) AS activity_nr,
  COUNT(*) AS emphasis_code_count,
  STRING_AGG(CAST(prog_value AS STRING), ' | ' ORDER BY CAST(prog_value AS STRING)) AS emphasis_code_list,
  COUNTIF(
    REGEXP_CONTAINS(LOWER(CAST(prog_value AS STRING)), r'eye|face|vision|optical|goggle|shield')
    OR REGEXP_CONTAINS(LOWER(CAST(prog_type AS STRING)), r'eye|face|vision|optical|goggle|shield')
  ) AS eye_emphasis_count
FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.emphasis_codes_recent`
GROUP BY 1
;

-- 2e. Related activity (follow-up inspections - persistent hazard signal)
CREATE TEMP TABLE related_activity_signals_v3 AS
SELECT
  SAFE_CAST(activity_nr AS INT64) AS activity_nr,
  COUNT(*) AS related_inspection_count,
  COUNTIF(UPPER(CAST(rel_type AS STRING)) = 'F') AS formal_followup_count
FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.related_activity_recent`
GROUP BY 1
;

-- ???????????????????????????????????????????????????????????????????????????
-- STEP 3 - JOIN ALL SIGNALS ONTO INSPECTION BASE
-- ???????????????????????????????????????????????????????????????????????????
CREATE TEMP TABLE inspection_scored_v3 AS
SELECT
  ib.*,

  -- Eye injury signals
  COALESCE(ei.eye_injury_count, 0)           AS eye_injury_count,
  COALESCE(ei.fatality_count, 0)             AS fatality_count,
  COALESCE(ei.face_head_injury_count, 0)     AS face_head_injury_count,
  COALESCE(ei.eye_injury_descriptions, '')   AS eye_injury_descriptions,
  ei.last_eye_injury_date,

  -- Violation signals
  COALESCE(vs.eye_violation_count, 0)            AS eye_violation_count,
  COALESCE(vs.prescription_violation_count, 0)   AS prescription_violation_count,
  COALESCE(vs.side_protection_violation_count, 0) AS side_protection_violation_count,
  COALESCE(vs.open_eye_violation_count, 0)       AS open_eye_violation_count,
  COALESCE(vs.general_ppe_violation_count, 0)    AS general_ppe_violation_count,
  COALESCE(vs.open_general_ppe_violation_count, 0) AS open_general_ppe_violation_count,
  COALESCE(vs.total_current_penalty, 0)          AS total_current_penalty,
  COALESCE(vs.total_initial_penalty, 0)          AS total_initial_penalty,
  COALESCE(vs.willful_violation_count, 0)        AS willful_violation_count,
  COALESCE(vs.repeat_violation_count, 0)         AS repeat_violation_count,
  COALESCE(vs.standards_cited, '')               AS standards_cited,
  vs.last_violation_date,

  -- Violation event signals
  COALESCE(ve.violation_event_count, 0)    AS violation_event_count,
  COALESCE(ve.contested_violation_count, 0) AS contested_violation_count,
  ve.last_violation_event_date,

  -- Emphasis code signals
  COALESCE(em.emphasis_code_count, 0)  AS emphasis_code_count,
  COALESCE(em.eye_emphasis_count, 0)   AS eye_emphasis_count,
  COALESCE(em.emphasis_code_list, '')  AS emphasis_code_list,

  -- Related activity signals
  COALESCE(ra.related_inspection_count, 0) AS related_inspection_count,
  COALESCE(ra.formal_followup_count, 0)    AS formal_followup_count

FROM inspection_base_v3 ib
LEFT JOIN eye_injury_signals_v3       ei ON ei.activity_nr = ib.activity_nr
LEFT JOIN violation_signals_v3        vs ON vs.activity_nr = ib.activity_nr
LEFT JOIN violation_event_signals_v3  ve ON ve.activity_nr = ib.activity_nr
LEFT JOIN emphasis_signals_v3         em ON em.activity_nr = ib.activity_nr
LEFT JOIN related_activity_signals_v3 ra ON ra.activity_nr = ib.activity_nr
;

-- ???????????????????????????????????????????????????????????????????????????
-- STEP 4 - ROLL UP TO COMPANY LEVEL (deduplicate to 1 row per company site)
-- ???????????????????????????????????????????????????????????????????????????
CREATE TEMP TABLE company_scored_v3 AS
WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY company_key, zip5
      ORDER BY
        eye_injury_count DESC,
        eye_violation_count DESC,
        COALESCE(close_case_date, open_case_date) DESC,
        activity_nr DESC
    ) AS rn,

    -- Company-level roll-ups across all inspections in window
    SUM(eye_injury_count)          OVER w AS co_eye_injury_count,
    SUM(fatality_count)            OVER w AS co_fatality_count,
    SUM(face_head_injury_count)    OVER w AS co_face_head_injury_count,
    SUM(eye_violation_count)       OVER w AS co_eye_violation_count,
    SUM(prescription_violation_count) OVER w AS co_prescription_violation_count,
    SUM(open_eye_violation_count)  OVER w AS co_open_eye_violation_count,
    SUM(general_ppe_violation_count) OVER w AS co_general_ppe_violation_count,
    SUM(open_general_ppe_violation_count) OVER w AS co_open_general_ppe_violation_count,
    SUM(total_current_penalty)     OVER w AS co_total_current_penalty,
    SUM(willful_violation_count)   OVER w AS co_willful_count,
    SUM(repeat_violation_count)    OVER w AS co_repeat_count,
    SUM(violation_event_count)     OVER w AS co_violation_event_count,
    SUM(contested_violation_count) OVER w AS co_contested_count,
    MAX(eye_emphasis_count)        OVER w AS co_eye_emphasis_count,
    SUM(related_inspection_count)  OVER w AS co_related_inspection_count,
    COUNT(*)                       OVER w AS co_inspection_count,
    MAX(COALESCE(nr_in_estab, 0))  OVER w AS co_max_employees

  FROM inspection_scored_v3
  WINDOW w AS (PARTITION BY company_key, zip5)
),
scored AS (
  SELECT
    *,

    -- ?? EYE LEAD SCORE (0-100) ????????????????????????????????????????????
    -- Direct eye injury evidence
    LEAST(50, co_eye_injury_count * 50)                         -- +50 first eye injury
    + LEAST(30, GREATEST(0, co_eye_injury_count - 1) * 15)     -- +15 each additional (max 30)
    + IF(co_fatality_count > 0, 20, 0)                          -- +20 fatality

    -- Eye/face protection citations
    + LEAST(30, co_eye_violation_count * 30)                    -- +30 first eye cite
    + IF(co_prescription_violation_count > 0, 20, 0)            -- +20 prescription-specific
    + IF(co_open_eye_violation_count > 0, 15, 0)               -- +15 still open/unabated

    -- Program engagement signals
    + IF(co_eye_emphasis_count > 0, 10, 0)                     -- +10 eye/face emphasis program
    + LEAST(10, co_related_inspection_count * 10)               -- +10 follow-up inspection
    + IF(co_contested_count > 0, 5, 0)                         -- +5 contested = engaged
    + IF(co_willful_count > 0, 10, 0)                          -- +10 willful = severe
    + IF(co_repeat_count > 0, 8, 0)                            -- +8 repeat = pattern
    AS eye_lead_score_raw,

    -- ?? PPE OPPORTUNITY SCORE (0-50) ??????????????????????????????????????
    -- General PPE violation evidence
    IF(co_general_ppe_violation_count > 0, 25, 0)              -- +25 general PPE cite
    + IF(co_open_general_ppe_violation_count > 0, 10, 0)       -- +10 still open
    + IF(co_face_head_injury_count > 0, 10, 0)                 -- +10 face/head injury

    -- Company profile fit
    + CASE
        WHEN STARTS_WITH(naics_code, '31')
          OR STARTS_WITH(naics_code, '32')
          OR STARTS_WITH(naics_code, '33') THEN 15             -- Manufacturing
        WHEN STARTS_WITH(naics_code, '23') THEN 15             -- Construction
        WHEN STARTS_WITH(naics_code, '21') THEN 15             -- Mining/Oil & Gas
        WHEN STARTS_WITH(naics_code, '48')
          OR STARTS_WITH(naics_code, '49') THEN 10             -- Transportation/Warehousing
        WHEN STARTS_WITH(naics_code, '54')
          OR STARTS_WITH(naics_code, '56') THEN 8              -- Professional/Facility Services
        WHEN STARTS_WITH(naics_code, '62') THEN 8              -- Healthcare
        ELSE 5
      END
    + CASE
        WHEN co_max_employees >= 250 THEN 10
        WHEN co_max_employees >= 50  THEN 7
        WHEN co_max_employees >= 20  THEN 4
        ELSE 0
      END
    + IF(co_inspection_count >= 3, 5, 0)                       -- +5 multi-inspection history
    AS ppe_score_raw

  FROM ranked
  WHERE rn = 1
)
SELECT
  * EXCEPT(eye_lead_score_raw, ppe_score_raw),
  LEAST(100, eye_lead_score_raw)  AS eye_lead_score,
  LEAST(50,  ppe_score_raw)       AS ppe_score,
  LEAST(100, eye_lead_score_raw) * 2 + LEAST(50, ppe_score_raw) AS final_score,

  -- ?? LEAD TIER ?????????????????????????????????????????????????????????
  CASE
    WHEN LEAST(100, eye_lead_score_raw) >= 50 THEN 'P0 Hot Eye'
    WHEN LEAST(100, eye_lead_score_raw) >= 30 THEN 'P1 Eye Violation'
    WHEN LEAST(50,  ppe_score_raw) >= 25      THEN 'P2 PPE Opportunity'
    ELSE                                           'P3 Industry Fit'
  END AS lead_tier,

  -- ?? PITCH RECOMMENDATION ??????????????????????????????????????????????
  CASE
    WHEN LEAST(100, eye_lead_score_raw) >= 50 THEN
      'Direct eye injury on record - prescription safety eyewear program is urgent. Reference the incident.'
    WHEN co_prescription_violation_count > 0 THEN
      'Cited for prescription lens protection failure - prescription safety eyewear program directly addresses the citation.'
    WHEN LEAST(100, eye_lead_score_raw) >= 30 THEN
      'Cited for eye/face protection failure - program compliance upgrade or prescription eyewear program opportunity.'
    WHEN co_open_general_ppe_violation_count > 0 THEN
      'Open PPE violations on record - prescription safety eyewear program can close the compliance gap.'
    WHEN LEAST(50, ppe_score_raw) >= 25 THEN
      'General PPE violations in high-hazard industry - prescription safety eyewear is a natural program add.'
    ELSE
      'High-hazard industry profile - proactive prescription safety eyewear outreach.'
  END AS pitch_recommendation,

  -- ?? EMPLOYEE BAND ?????????????????????????????????????????????????????
  CASE
    WHEN co_max_employees >= 500 THEN '500+'
    WHEN co_max_employees >= 250 THEN '250-499'
    WHEN co_max_employees >= 100 THEN '100-249'
    WHEN co_max_employees >= 50  THEN '50-99'
    WHEN co_max_employees >= 20  THEN '20-49'
    WHEN co_max_employees >= 1   THEN '1-19'
    ELSE 'Unknown'
  END AS employee_band,

  -- ?? NAICS INDUSTRY LABEL ??????????????????????????????????????????????
  CASE
    WHEN STARTS_WITH(naics_code, '11') THEN 'Agriculture / Forestry / Fishing'
    WHEN STARTS_WITH(naics_code, '21') THEN 'Mining / Oil & Gas'
    WHEN STARTS_WITH(naics_code, '22') THEN 'Utilities'
    WHEN STARTS_WITH(naics_code, '23') THEN 'Construction'
    WHEN STARTS_WITH(naics_code, '31')
      OR STARTS_WITH(naics_code, '32')
      OR STARTS_WITH(naics_code, '33') THEN 'Manufacturing'
    WHEN STARTS_WITH(naics_code, '42') THEN 'Wholesale Trade'
    WHEN STARTS_WITH(naics_code, '44')
      OR STARTS_WITH(naics_code, '45') THEN 'Retail Trade'
    WHEN STARTS_WITH(naics_code, '48')
      OR STARTS_WITH(naics_code, '49') THEN 'Transportation / Warehousing'
    WHEN STARTS_WITH(naics_code, '51') THEN 'Information / Technology'
    WHEN STARTS_WITH(naics_code, '54') THEN 'Professional Services'
    WHEN STARTS_WITH(naics_code, '56') THEN 'Facility / Support Services'
    WHEN STARTS_WITH(naics_code, '61') THEN 'Education'
    WHEN STARTS_WITH(naics_code, '62') THEN 'Healthcare / Social Assistance'
    WHEN STARTS_WITH(naics_code, '72') THEN 'Food Service / Accommodation'
    WHEN STARTS_WITH(naics_code, '81') THEN 'Repair / Personal Services'
    WHEN STARTS_WITH(naics_code, '92') THEN 'Government / Public Admin'
    ELSE 'Other'
  END AS industry_label

FROM scored
;

-- ???????????????????????????????????????????????????????????????????????????
-- STEP 5 - FINAL DASHBOARD TABLE
-- Replaces dashboard_leads_current with the new eye-focused schema.
-- ???????????????????????????????????????????????????????????????????????????
DROP TABLE IF EXISTS `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.dashboard_leads_current`;
CREATE TABLE `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.dashboard_leads_current`
CLUSTER BY region, lead_tier, final_score
AS
WITH osha_leads AS (
  SELECT
    CAST(activity_nr AS STRING)          AS inspection_id,
    company_name                         AS account_name,
    region,
    county,
    city                                 AS site_city,
    state                                AS site_state,
    zip5                                 AS site_zip,
    distance_from_miramar_miles,
    naics_code,
    industry_label                       AS industry_segment,
    owner_type                           AS ownership_type,
    employee_band,
    co_max_employees                     AS nr_employees,
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
    co_eye_injury_count                  AS eye_injury_count,
    co_fatality_count                    AS fatality_count,
    co_face_head_injury_count            AS face_head_injury_count,
    eye_injury_descriptions,
    co_eye_violation_count               AS eye_violation_count,
    co_prescription_violation_count      AS prescription_violation_count,
    side_protection_violation_count,
    co_open_eye_violation_count          AS open_eye_violation_count,
    co_general_ppe_violation_count       AS general_ppe_violation_count,
    co_open_general_ppe_violation_count  AS open_general_ppe_violation_count,
    co_willful_count                     AS willful_violation_count,
    co_repeat_count                      AS repeat_violation_count,
    ROUND(co_total_current_penalty, 2)   AS total_current_penalty,
    standards_cited,
    co_violation_event_count             AS violation_event_count,
    co_contested_count                   AS contested_violation_count,
    co_eye_emphasis_count                AS eye_emphasis_count,
    emphasis_code_list,
    co_related_inspection_count          AS related_inspection_count,
    formal_followup_count,
    co_inspection_count                  AS total_inspection_count,
    IF(co_open_eye_violation_count > 0 OR co_open_general_ppe_violation_count > 0, TRUE, FALSE) AS has_open_violations
  FROM company_scored_v3
),
-- City business license leads: active hazardous-industry businesses from SF + LA
-- that do NOT already appear as OSHA-inspected companies (anti-join by name + zip).
city_leads AS (
  SELECT
    CAST(NULL AS STRING)                  AS inspection_id,
    COALESCE(NULLIF(TRIM(dba_name), ''), TRIM(business_name)) AS account_name,
    CASE source
      WHEN 'sf_biz_license' THEN 'Bay Area'
      WHEN 'la_biz_license' THEN 'Los Angeles'
      ELSE 'California'
    END                                   AS region,
    UPPER(city)                           AS county,
    UPPER(city)                           AS site_city,
    COALESCE(NULLIF(TRIM(state), ''), 'CA') AS site_state,
    LEFT(TRIM(zip_code), 5)               AS site_zip,
    CAST(NULL AS FLOAT64)                 AS distance_from_miramar_miles,
    naics_code,
    industry_category                     AS industry_segment,
    CAST(NULL AS STRING)                  AS ownership_type,
    'Unknown'                             AS employee_band,
    CAST(NULL AS INT64)                   AS nr_employees,
    CAST(NULL AS DATE)                    AS open_case_date,
    CAST(NULL AS DATE)                    AS close_case_date,
    CAST(NULL AS DATE)                    AS last_eye_injury_date,
    CAST(NULL AS DATE)                    AS last_violation_date,
    CAST(NULL AS DATE)                    AS last_violation_event_date,
    CAST(0 AS INT64)                      AS eye_lead_score,
    CASE industry_category
      -- High-priority hazard industries
      WHEN 'Construction'          THEN IF(is_new_business, 42, 36)
      WHEN 'Manufacturing'         THEN IF(is_new_business, 40, 34)
      WHEN 'Chemical Manufacturing' THEN IF(is_new_business, 42, 38)
      WHEN 'Machinery Manufacturing' THEN IF(is_new_business, 38, 32)
      -- Pharma/Biotech/Healthcare (premium targets)
      WHEN 'Professional/Scientific Services' THEN IF(is_new_business, 40, 36)
      WHEN 'Healthcare/Social Services' THEN IF(is_new_business, 38, 32)
      -- Hazardous/Specialized Work
      WHEN 'Repair/Maintenance/Personal Services' THEN IF(is_new_business, 32, 26)
      WHEN 'Warehousing/Transport' THEN IF(is_new_business, 30, 24)
      WHEN 'Computer/Electronics Manufacturing' THEN IF(is_new_business, 28, 22)
      WHEN 'Utilities'             THEN IF(is_new_business, 26, 20)
      WHEN 'Waste Management/Remediation' THEN IF(is_new_business, 26, 20)
      WHEN 'Wholesale Trade'       THEN IF(is_new_business, 20, 14)
      WHEN 'Education'             THEN IF(is_new_business, 18, 12)
      WHEN 'Food/Beverage Manufacturing' THEN IF(is_new_business, 22, 16)
      -- Lower priority
      ELSE IF(is_new_business, 16, 10)
    END                                   AS ppe_score,
    CASE industry_category
      -- High-priority hazard industries
      WHEN 'Construction'          THEN IF(is_new_business, 42, 36)
      WHEN 'Manufacturing'         THEN IF(is_new_business, 40, 34)
      WHEN 'Chemical Manufacturing' THEN IF(is_new_business, 42, 38)
      WHEN 'Machinery Manufacturing' THEN IF(is_new_business, 38, 32)
      -- Pharma/Biotech/Healthcare (premium targets)
      WHEN 'Professional/Scientific Services' THEN IF(is_new_business, 40, 36)
      WHEN 'Healthcare/Social Services' THEN IF(is_new_business, 38, 32)
      -- Hazardous/Specialized Work
      WHEN 'Repair/Maintenance/Personal Services' THEN IF(is_new_business, 32, 26)
      WHEN 'Warehousing/Transport' THEN IF(is_new_business, 30, 24)
      WHEN 'Computer/Electronics Manufacturing' THEN IF(is_new_business, 28, 22)
      WHEN 'Utilities'             THEN IF(is_new_business, 26, 20)
      WHEN 'Waste Management/Remediation' THEN IF(is_new_business, 26, 20)
      WHEN 'Wholesale Trade'       THEN IF(is_new_business, 20, 14)
      WHEN 'Education'             THEN IF(is_new_business, 18, 12)
      WHEN 'Food/Beverage Manufacturing' THEN IF(is_new_business, 22, 16)
      -- Lower priority
      ELSE IF(is_new_business, 16, 10)
    END                                   AS final_score,
    CASE industry_category
      WHEN 'Construction'          THEN 'P2 PPE Opportunity'
      WHEN 'Manufacturing'         THEN 'P2 PPE Opportunity'
      WHEN 'Chemical Manufacturing' THEN 'P1 Eye Violation'
      WHEN 'Machinery Manufacturing' THEN 'P2 PPE Opportunity'
      WHEN 'Professional/Scientific Services' THEN 'P2 PPE Opportunity'
      WHEN 'Healthcare/Social Services' THEN 'P2 PPE Opportunity'
      WHEN 'Repair/Maintenance/Personal Services' THEN 'P2 PPE Opportunity'
      WHEN 'Warehousing/Transport' THEN 'P2 PPE Opportunity'
      WHEN 'Computer/Electronics Manufacturing' THEN 'P3 Industry Fit'
      WHEN 'Utilities'             THEN 'P3 Industry Fit'
      WHEN 'Waste Management/Remediation' THEN 'P3 Industry Fit'
      ELSE 'P3 Industry Fit'
    END                                   AS lead_tier,
    CASE industry_category
      WHEN 'Construction'
        THEN 'High-hazard construction site — prescription eyewear program opportunity.'
      WHEN 'Manufacturing'
        THEN 'Manufacturing environment — eye protection requirements, prescription program fit.'
      WHEN 'Chemical Manufacturing'
        THEN 'Chemical plant operations — splash hazards and prescription eyewear critical.'
      WHEN 'Machinery Manufacturing'
        THEN 'Industrial machinery operation — impact and debris exposure, prescription eyewear program opportunity.'
      WHEN 'Professional/Scientific Services'
        THEN 'Research and professional services — specialized eye protection for precision/lab work.'
      WHEN 'Healthcare/Social Services'
        THEN 'Healthcare facility — biological/chemical exposure, prescription eyewear for clinical staff.'
      WHEN 'Computer/Electronics Manufacturing'
        THEN 'Electronics manufacturing — precision work requiring prescription eyewear.'
      WHEN 'Repair/Maintenance/Personal Services'
        THEN 'Repair/maintenance operations — impact and chemical exposure risks, prescription eyewear opportunity.'
      WHEN 'Warehousing/Transport'
        THEN 'Warehouse / transport operation — prescription eyewear program prospect.'
      WHEN 'Utilities'
        THEN 'Utility company — industrial operations with eye protection requirements.'
      WHEN 'Waste Management/Remediation'
        THEN 'Waste/remediation site — hazardous exposure, prescription eyewear required.'
      WHEN 'Food/Beverage Manufacturing'
        THEN 'Food processing facility — chemical and environmental hazards, prescription eyewear program fit.'
      ELSE 'Business operating with potential eye hazards — proactive prescription eyewear outreach.'
    END                                   AS pitch_recommendation,
    CAST(0 AS INT64)                      AS eye_injury_count,
    CAST(0 AS INT64)                      AS fatality_count,
    CAST(0 AS INT64)                      AS face_head_injury_count,
    CAST(NULL AS STRING)                  AS eye_injury_descriptions,
    CAST(0 AS INT64)                      AS eye_violation_count,
    CAST(0 AS INT64)                      AS prescription_violation_count,
    CAST(0 AS INT64)                      AS side_protection_violation_count,
    CAST(0 AS INT64)                      AS open_eye_violation_count,
    CAST(0 AS INT64)                      AS general_ppe_violation_count,
    CAST(0 AS INT64)                      AS open_general_ppe_violation_count,
    CAST(0 AS INT64)                      AS willful_violation_count,
    CAST(0 AS INT64)                      AS repeat_violation_count,
    CAST(0.0 AS FLOAT64)                  AS total_current_penalty,
    CAST(NULL AS STRING)                  AS standards_cited,
    CAST(0 AS INT64)                      AS violation_event_count,
    CAST(0 AS INT64)                      AS contested_violation_count,
    CAST(0 AS INT64)                      AS eye_emphasis_count,
    CAST(NULL AS STRING)                  AS emphasis_code_list,
    CAST(0 AS INT64)                      AS related_inspection_count,
    CAST(0 AS INT64)                      AS formal_followup_count,
    CAST(0 AS INT64)                      AS total_inspection_count,
    FALSE                                 AS has_open_violations
  FROM `{{CITY_PROJECT_ID}}.{{CITY_DATASET}}.city_biz_license_current` c
  WHERE COALESCE(NULLIF(TRIM(c.dba_name), ''), NULLIF(TRIM(c.business_name), '')) IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 FROM osha_leads ol
      WHERE UPPER(REGEXP_REPLACE(
              COALESCE(NULLIF(TRIM(c.dba_name), ''), TRIM(c.business_name)),
              r'[^A-Z0-9]', ''))
            = UPPER(REGEXP_REPLACE(ol.account_name, r'[^A-Z0-9]', ''))
        AND LEFT(TRIM(c.zip_code), 5) = ol.site_zip
    )
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      UPPER(REGEXP_REPLACE(
        COALESCE(NULLIF(TRIM(c.dba_name), ''), TRIM(c.business_name)),
        r'[^A-Z0-9]', ''
      )),
      LEFT(TRIM(c.zip_code), 5)
    ORDER BY c.start_date DESC NULLS LAST
  ) = 1
)
SELECT * FROM osha_leads
UNION ALL
SELECT * FROM city_leads
;
