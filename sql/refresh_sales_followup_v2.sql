CREATE OR REPLACE VIEW osha_raw.v_sales_followup_sandiego_v2 AS
WITH inspection_base AS (
  SELECT
    TRIM(estab_name) AS company_name,
    site_address AS address,
    site_city AS city,
    site_state AS state,
    CAST(site_zip AS STRING) AS zip,
    CAST(naics_code AS STRING) AS naics_code,
    owner_type,
    insp_type,
    safety_hlth,
    DATE(close_case_date) AS close_case_date,
    DATE_DIFF(CURRENT_DATE(), DATE(close_case_date), DAY) AS days_since_close,
    CAST(activity_nr AS INT64) AS activity_nr,
    load_dt
  FROM osha_raw.inspection_socal_incremental
  WHERE DATE(close_case_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
    AND REGEXP_CONTAINS(CAST(site_zip AS STRING), r'^(919|920|921)')
),
inspection_company AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY company_name, address, city, state, zip
      ORDER BY load_dt DESC, activity_nr DESC
    ) AS rn,
    COUNT(*) OVER (
      PARTITION BY company_name, address, city, state, zip
    ) AS inspection_count,
    COUNTIF(close_case_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) OVER (
      PARTITION BY company_name, address, city, state, zip
    ) AS inspections_90d,
    MAX(close_case_date) OVER (
      PARTITION BY company_name, address, city, state, zip
    ) AS company_latest_close_date,
    MAX(load_dt) OVER (
      PARTITION BY company_name, address, city, state, zip
    ) AS company_latest_load_dt
  FROM inspection_base
),
violation_metrics AS (
  SELECT
    SAFE_CAST(activity_nr AS INT64) AS activity_nr,
    COUNT(*) AS violation_count,
    SUM(
      COALESCE(SAFE_CAST(current_penalty AS FLOAT64), 0)
      + COALESCE(SAFE_CAST(initial_penalty AS FLOAT64), 0)
      + COALESCE(SAFE_CAST(fta_penalty AS FLOAT64), 0)
    ) AS total_penalties,
    COUNTIF(
      (abate_complete IS NULL OR UPPER(CAST(abate_complete AS STRING)) NOT IN ('Y', 'YES', '1', 'TRUE'))
      AND final_order_date IS NULL
    ) AS open_violation_count
  FROM osha_raw.violation_recent
  GROUP BY 1
),
ppe_focus_metrics AS (
  SELECT
    SAFE_CAST(activity_nr AS INT64) AS activity_nr,
    COUNTIF(
      REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.133')
      OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.102')
      OR REGEXP_CONTAINS(
        LOWER(CONCAT(
          ' ',
          COALESCE(CAST(hazsub1 AS STRING), ''),
          ' ',
          COALESCE(CAST(hazsub2 AS STRING), ''),
          ' ',
          COALESCE(CAST(hazsub3 AS STRING), ''),
          ' ',
          COALESCE(CAST(hazsub4 AS STRING), ''),
          ' ',
          COALESCE(CAST(hazsub5 AS STRING), '')
        )),
        r'eye\s*hazard|eye\s*injur|eye\s*expos|face\s*hazard|face\s*injur|goggles?|face\s*shield|protective\s*eyewear|eye\s*and\s*face'
      )
    ) AS eye_face_violation_count,
    COUNTIF(
      REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.132')
      OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.95')
    ) AS general_ppe_violation_count,
    COUNTIF(
      REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.133\(a\)\(3\)')
      OR (
        (REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.133')
        OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.102'))
        AND REGEXP_CONTAINS(
          LOWER(CONCAT(
            ' ',
            COALESCE(CAST(hazsub1 AS STRING), ''),
            ' ',
            COALESCE(CAST(hazsub2 AS STRING), ''),
            ' ',
            COALESCE(CAST(hazsub3 AS STRING), ''),
            ' ',
            COALESCE(CAST(hazsub4 AS STRING), ''),
            ' ',
            COALESCE(CAST(hazsub5 AS STRING), '')
          )),
          r'prescription|lenses?|eyewear'
        )
      )
    ) AS prescription_lens_violation_count,
    COUNTIF(
      REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.133')
      OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.102')
      OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.132')
      OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.95')
      OR REGEXP_CONTAINS(
        LOWER(CONCAT(
          ' ',
          COALESCE(CAST(hazsub1 AS STRING), ''),
          ' ',
          COALESCE(CAST(hazsub2 AS STRING), ''),
          ' ',
          COALESCE(CAST(hazsub3 AS STRING), ''),
          ' ',
          COALESCE(CAST(hazsub4 AS STRING), ''),
          ' ',
          COALESCE(CAST(hazsub5 AS STRING), '')
        )),
        r'prescription|lenses?|eyewear|face shield|eye protection|eye\s*hazard|face\s*hazard|goggles?|personal protective equipment|ppe'
      )
    ) AS ppe_focus_violation_count
  FROM osha_raw.violation_recent
  GROUP BY 1
),
violation_details AS (
  SELECT
    SAFE_CAST(activity_nr AS INT64) AS activity_nr,
    STRING_AGG(
      DISTINCT NULLIF(TRIM(CAST(citation_id AS STRING)), ''),
      ' | ' ORDER BY NULLIF(TRIM(CAST(citation_id AS STRING)), '') LIMIT 12
    ) AS violation_items,
    STRING_AGG(
      DISTINCT NULLIF(TRIM(CAST(standard AS STRING)), ''),
      ' | ' ORDER BY NULLIF(TRIM(CAST(standard AS STRING)), '') LIMIT 12
    ) AS standards_cited,
    STRING_AGG(
      DISTINCT CASE
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(1910133|1926102)')
          THEN 'Eye and Face Protection'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(1910132|192695|1514)')
          THEN 'PPE Selection and Use'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(1509|3203|6401)')
          THEN 'Safety Program and Training (IIPP)'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^1512')
          THEN 'Medical/First Aid Readiness'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(15411|15410001|1541|1629|1632)')
          THEN 'Fall Protection and Safe Access'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^3395')
          THEN 'Heat Illness and Environmental Controls'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^3396')
          THEN 'Heat Illness and Environmental Controls'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(5162|5193)')
          THEN 'Health Exposure and Respiratory/Biological Controls'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(3382|3384|3227)')
          THEN 'PPE Selection and Use'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(6151|6175|3664|3666|1630)')
          THEN 'Fall Protection and Safe Access'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(19100157|19260150)')
          THEN 'Fire Protection and Emergency Readiness'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^19260701')
          THEN 'Construction Work Practice Controls'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^542')
          THEN 'Permit and Regulatory Process'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^341')
          THEN 'Permit and Regulatory Process'
        WHEN UPPER(CAST(viol_type AS STRING)) = 'WILLFUL'
          THEN 'High-Risk Compliance Behavior'
        WHEN UPPER(CAST(viol_type AS STRING)) = 'REPEAT'
          THEN 'Repeat Compliance Gaps'
        WHEN UPPER(CAST(viol_type AS STRING)) = 'SERIOUS'
          THEN 'Serious Safety Risk'
        ELSE NULL
      END,
      ' | ' LIMIT 4
    ) AS citation_sales_category,
    STRING_AGG(
      DISTINCT CASE
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(1910133|1926102)')
          THEN 'Eye/face hazard controls were not adequate; strong opportunity for protective eyewear standardization and fit testing.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(1910132|192695|1514)')
          THEN 'PPE selection/enforcement gap; position safety eyewear as part of a broader PPE compliance rollout.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(1509|3203|6401)')
          THEN 'Safety program/training process gap; bundle eyewear program support with onboarding and recurring safety training.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^1512')
          THEN 'Medical readiness and response expectations are in focus; emphasize incident prevention through reliable daily eye protection.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(15411|15410001|1541|1629|1632)')
          THEN 'Work-at-height/access risk context; position durable, secure-fit eyewear for active and elevated work conditions.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^3395')
          THEN 'Heat or environmental exposure controls are in focus; position anti-fog and all-day wearable protective eyewear.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^3396')
          THEN 'Heat or environmental exposure controls are in focus; position anti-fog and all-day wearable protective eyewear.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(5162|5193)')
          THEN 'Health exposure controls are in focus; position sealed or specialty protective eyewear matched to exposure risk.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(3382|3384|3227)')
          THEN 'PPE performance and enforcement gaps exist; position a standardized safety eyewear rollout with manager accountability.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(6151|6175|3664|3666|1630)')
          THEN 'Active work-at-height or access-control issues are present; lead with secure-fit, durable eyewear suited to mobile crews.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(19100157|19260150)')
          THEN 'Emergency/fire readiness is in scope; align eyewear program with broader emergency preparedness and PPE compliance.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^19260701')
          THEN 'Construction work-practice controls are under review; position site-ready eyewear standards for consistent field execution.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^542')
          THEN 'Regulatory process scrutiny is present; offer documented eyewear policy and audit trail to reduce repeat citations.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^341')
          THEN 'Permit/regulatory process scrutiny; offer a documented eyewear policy to reduce repeat findings during inspections.'
        WHEN UPPER(CAST(viol_type AS STRING)) = 'WILLFUL'
          THEN 'High urgency account: prioritize immediate outreach with a compliance-first eyewear implementation plan.'
        WHEN UPPER(CAST(viol_type AS STRING)) = 'REPEAT'
          THEN 'Recurring issue account: pitch a standardized multi-site eyewear program with measurable compliance tracking.'
        WHEN UPPER(CAST(viol_type AS STRING)) = 'SERIOUS'
          THEN 'Serious risk account: lead with fast deployment and manager controls for eye/face PPE consistency.'
        ELSE NULL
      END,
      ' || ' LIMIT 4
    ) AS citation_sales_explanation,
    STRING_AGG(
      DISTINCT CASE
        WHEN REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.133\(a\)\(3\)')
          THEN 'Prescription lenses: eye protection must fit and work safely with prescription lenses.'
        WHEN REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.133')
          OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.102')
          THEN 'Eye/face protection: provide and enforce proper eye and face PPE for identified hazards.'
        WHEN REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.132')
          OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.95')
          THEN 'General PPE requirement: assess hazards and provide suitable protective equipment.'
        WHEN UPPER(CAST(viol_type AS STRING)) = 'WILLFUL'
          THEN 'Willful citation: OSHA indicates the requirement was knowingly disregarded.'
        WHEN UPPER(CAST(viol_type AS STRING)) = 'REPEAT'
          THEN 'Repeat citation: similar OSHA requirement was cited previously.'
        WHEN UPPER(CAST(viol_type AS STRING)) = 'SERIOUS'
          THEN 'Serious citation: substantial probability of serious physical harm.'
        WHEN UPPER(CAST(viol_type AS STRING)) IN ('OTHER-THAN-SERIOUS', 'OTHER THAN SERIOUS')
          THEN 'Other-than-serious citation: related to safety and health but lower immediate severity.'
        WHEN NULLIF(TRIM(CAST(standard AS STRING)), '') IS NOT NULL
          THEN CONCAT('Cited OSHA standard ', TRIM(CAST(standard AS STRING)), '; review full case details for exact language.')
        ELSE 'OSHA requirement cited; review inspection case details for exact language.'
      END,
      ' || ' LIMIT 6
    ) AS citation_excerpt
  FROM osha_raw.violation_recent
  GROUP BY 1
),
violation_event_metrics AS (
  SELECT
    SAFE_CAST(activity_nr AS INT64) AS activity_nr,
    COUNT(*) AS violation_event_count,
    MAX(DATE(hist_date)) AS last_violation_event_date
  FROM osha_raw.violation_event_recent
  GROUP BY 1
),
related_activity_metrics AS (
  SELECT
    SAFE_CAST(activity_nr AS INT64) AS activity_nr,
    COUNT(*) AS related_activity_count,
    COUNTIF(UPPER(CAST(rel_type AS STRING)) IN ('B', 'COMPLAINT', '2')) AS complaint_related_count
  FROM osha_raw.related_activity_recent
  GROUP BY 1
),
emphasis_metrics AS (
  SELECT
    SAFE_CAST(activity_nr AS INT64) AS activity_nr,
    COUNT(*) AS emphasis_code_count
  FROM osha_raw.emphasis_codes_recent
  GROUP BY 1
),
injury_metrics AS (
  SELECT
    SAFE_CAST(rel_insp_nr AS INT64) AS activity_nr,
    COUNT(*) AS injury_count,
    COUNTIF(SAFE_CAST(degree_of_inj AS INT64) IN (1, 2)) AS severe_injury_count
  FROM osha_raw.accident_injury_recent
  GROUP BY 1
),
accident_metrics AS (
  SELECT
    ai.activity_nr,
    COUNT(DISTINCT a.summary_nr) AS accident_case_count,
    COUNTIF(SAFE_CAST(a.fatality AS INT64) > 0) AS fatality_case_count,
    MAX(DATE(a.event_date)) AS last_accident_date
  FROM (
    SELECT DISTINCT
      SAFE_CAST(rel_insp_nr AS INT64) AS activity_nr,
      CAST(summary_nr AS STRING) AS summary_nr
    FROM osha_raw.accident_injury_recent
    WHERE rel_insp_nr IS NOT NULL AND summary_nr IS NOT NULL
  ) ai
  LEFT JOIN (
    SELECT
      CAST(summary_nr AS STRING) AS summary_nr,
      fatality,
      event_date
    FROM osha_raw.accident_recent
  ) a
  ON ai.summary_nr = a.summary_nr
  GROUP BY 1
),
scored AS (
  SELECT
    'San Diego' AS region_label,
    ic.company_name,
    ic.address,
    ic.city,
    ic.state,
    ic.zip,
    ic.naics_code,
    ic.owner_type,
    ic.insp_type,
    ic.safety_hlth,
    CASE
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^(31|32|33)') THEN 18
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^23') THEN 18
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^22') THEN 15
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^(42|48|49)') THEN 16
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^62') THEN 14
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^(5417|54138|3254)') THEN 17
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^92') OR ic.owner_type IN ('B', 'C', 'D') THEN 10
      ELSE 8
    END AS industry_fit_points,
    CASE
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^(31|32|33)') THEN 'Manufacturing & Production'
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^23') THEN 'Construction & Field Work'
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^22') THEN 'Utilities & Field Services'
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^(42|48|49)') THEN 'Warehouse & Distribution'
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^62') THEN 'Healthcare & Clinical'
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^(5417|54138|3254)') THEN 'Laboratory & Research'
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^92') OR ic.owner_type IN ('B', 'C', 'D') THEN 'Public Sector & Municipal'
      ELSE 'Specialized / Mixed Environment'
    END AS industry_segment,
    CASE ic.owner_type
      WHEN 'A' THEN 'Private'
      WHEN 'B' THEN 'Local Government'
      WHEN 'C' THEN 'State Government'
      WHEN 'D' THEN 'Federal'
      ELSE 'Unknown'
    END AS owner_type_label,
    CASE ic.insp_type
      WHEN 'A' THEN 'Accident'
      WHEN 'B' THEN 'Complaint'
      WHEN 'C' THEN 'Referral'
      WHEN 'D' THEN 'Monitoring'
      WHEN 'E' THEN 'Variance'
      WHEN 'F' THEN 'Follow-up'
      WHEN 'G' THEN 'Unprogrammed Related'
      WHEN 'H' THEN 'Planned'
      WHEN 'I' THEN 'Program Related'
      WHEN 'J' THEN 'Unprogrammed Other'
      WHEN 'K' THEN 'Program Other'
      WHEN 'L' THEN 'Other'
      WHEN 'M' THEN 'Fatality/Catastrophe'
      WHEN 'N' THEN 'Unprogrammed Emphasis'
      ELSE 'Other'
    END AS inspection_type_label,
    ic.close_case_date,
    ic.days_since_close,
    CASE
      WHEN ic.days_since_close <= 30 THEN '0-30 days'
      WHEN ic.days_since_close <= 90 THEN '31-90 days'
      ELSE '91+ days'
    END AS recency_band,
    ic.activity_nr AS latest_activity_nr,
    ic.inspection_count,
    ic.inspections_90d,
    COALESCE(vm.violation_count, 0) AS violation_count,
    ROUND(COALESCE(vm.total_penalties, 0), 2) AS total_penalties,
    (COALESCE(vm.open_violation_count, 0) > 0) AS open_violation_status,
    COALESCE(vm.open_violation_count, 0) AS open_violation_count,
    COALESCE(vd.violation_items, '') AS violation_items,
    COALESCE(vd.standards_cited, '') AS standards_cited,
    COALESCE(vd.citation_sales_category, 'General OSHA Compliance') AS citation_sales_category,
    COALESCE(vd.citation_sales_explanation, 'OSHA cited a compliance gap; use discovery questions to map hazards and recommend the right safety eyewear program.') AS citation_sales_explanation,
    COALESCE(vd.citation_excerpt, '') AS citation_excerpt,
    COALESCE(vem.violation_event_count, 0) AS violation_event_count,
    vem.last_violation_event_date,
    COALESCE(ram.related_activity_count, 0) AS related_activity_count,
    (ic.insp_type = 'B' OR COALESCE(ram.complaint_related_count, 0) > 0) AS complaint_activity,
    COALESCE(ram.complaint_related_count, 0) AS complaint_activity_count,
    COALESCE(em.emphasis_code_count, 0) AS emphasis_code_count,
    COALESCE(im.injury_count, 0) AS injury_count,
    (COALESCE(im.severe_injury_count, 0) > 0 OR COALESCE(am.fatality_case_count, 0) > 0 OR ic.insp_type = 'M') AS severe_injury_indicator,
    COALESCE(im.severe_injury_count, 0) AS severe_injury_count,
    COALESCE(am.accident_case_count, 0) AS accident_case_count,
    COALESCE(am.fatality_case_count, 0) AS fatality_case_count,
    am.last_accident_date,
    ic.company_latest_close_date,
    ic.company_latest_load_dt,
    (
      CASE
        WHEN ic.days_since_close <= 14 THEN 30
        WHEN ic.days_since_close <= 30 THEN 24
        WHEN ic.days_since_close <= 60 THEN 16
        WHEN ic.days_since_close <= 90 THEN 8
        ELSE 2
      END
      + CASE
          WHEN REGEXP_CONTAINS(ic.naics_code, r'^(31|32|33|23)') THEN 18
          WHEN REGEXP_CONTAINS(ic.naics_code, r'^(42|48|49)') THEN 16
          WHEN REGEXP_CONTAINS(ic.naics_code, r'^22') THEN 15
          WHEN REGEXP_CONTAINS(ic.naics_code, r'^62') THEN 14
          WHEN REGEXP_CONTAINS(ic.naics_code, r'^(5417|54138|3254)') THEN 17
          WHEN REGEXP_CONTAINS(ic.naics_code, r'^92') OR ic.owner_type IN ('B', 'C', 'D') THEN 10
          ELSE 8
        END
      + CASE
          WHEN ic.safety_hlth = 'S' THEN 10
          WHEN ic.safety_hlth = 'H' THEN 4
          ELSE 2
        END
      + LEAST(COALESCE(vm.violation_count, 0) * 4, 20)
      + CASE
          WHEN COALESCE(vm.total_penalties, 0) >= 100000 THEN 20
          WHEN COALESCE(vm.total_penalties, 0) >= 25000 THEN 14
          WHEN COALESCE(vm.total_penalties, 0) >= 5000 THEN 8
          WHEN COALESCE(vm.total_penalties, 0) > 0 THEN 4
          ELSE 0
        END
      + CASE WHEN COALESCE(vm.open_violation_count, 0) > 0 THEN 12 ELSE 0 END
      + CASE WHEN (ic.insp_type = 'B' OR COALESCE(ram.complaint_related_count, 0) > 0) THEN 8 ELSE 0 END
      + CASE WHEN (COALESCE(im.severe_injury_count, 0) > 0 OR COALESCE(am.fatality_case_count, 0) > 0 OR ic.insp_type = 'M') THEN 15 ELSE 0 END
      + CASE WHEN COALESCE(em.emphasis_code_count, 0) > 0 THEN 5 ELSE 0 END
      + CASE WHEN COALESCE(pfm.eye_face_violation_count, 0) > 0 THEN 8 ELSE 0 END
      + CASE WHEN COALESCE(pfm.prescription_lens_violation_count, 0) > 0 THEN 6 ELSE 0 END
      + CASE WHEN COALESCE(pfm.general_ppe_violation_count, 0) > 0 THEN 4 ELSE 0 END
      + LEAST(COALESCE(pfm.ppe_focus_violation_count, 0) * 2, 8)
      + CASE
          WHEN ic.inspections_90d >= 3 THEN 10
          WHEN ic.inspections_90d = 2 THEN 7
          WHEN ic.inspections_90d = 1 THEN 4
          ELSE 0
        END
    ) AS followup_score
  FROM inspection_company ic
  LEFT JOIN violation_metrics vm ON ic.activity_nr = vm.activity_nr
  LEFT JOIN ppe_focus_metrics pfm ON ic.activity_nr = pfm.activity_nr
  LEFT JOIN violation_details vd ON ic.activity_nr = vd.activity_nr
  LEFT JOIN violation_event_metrics vem ON ic.activity_nr = vem.activity_nr
  LEFT JOIN related_activity_metrics ram ON ic.activity_nr = ram.activity_nr
  LEFT JOIN emphasis_metrics em ON ic.activity_nr = em.activity_nr
  LEFT JOIN injury_metrics im ON ic.activity_nr = im.activity_nr
  LEFT JOIN accident_metrics am ON ic.activity_nr = am.activity_nr
  WHERE ic.rn = 1
)
SELECT
  region_label AS `Region`,
  company_name AS `Account Name`,
  address AS `Site Address`,
  city AS `Site City`,
  state AS `Site State`,
  zip AS `Site ZIP`,
  naics_code AS `NAICS Code`,
  industry_segment AS `Industry Segment`,
  owner_type_label AS `Ownership Type`,
  inspection_type_label AS `Inspection Type`,
  close_case_date AS `Latest Case Close Date`,
  days_since_close AS `Days Since Last Case Close`,
  recency_band AS `Recency Window`,
  latest_activity_nr AS `Latest Inspection ID`,
  inspection_count AS `Inspections Total`,
  inspections_90d AS `Inspections Last 90 Days`,
  violation_count AS `Violations Total`,
  open_violation_count AS `Open Violations Total`,
  CASE WHEN open_violation_count > 0 THEN 'Yes' ELSE 'No' END AS `Has Open Violations`,
  ROUND(total_penalties, 2) AS `Penalties Total USD`,
  violation_items AS `Violation Items`,
  standards_cited AS `Standards Cited`,
  citation_sales_category AS `Citation Category`,
  citation_sales_explanation AS `Citation Sales Explanation`,
  citation_excerpt AS `Citation Excerpt`,
  violation_event_count AS `Violation Events Total`,
  last_violation_event_date AS `Last Violation Event Date`,
  related_activity_count AS `Related Activities Total`,
  complaint_activity_count AS `Complaint Related Activities`,
  CASE WHEN complaint_activity_count > 0 THEN 'Yes' ELSE 'No' END AS `Has Complaint Signal`,
  emphasis_code_count AS `Emphasis Codes Total`,
  injury_count AS `Injuries Total`,
  severe_injury_count AS `Severe Injuries Total`,
  fatality_case_count AS `Fatality Cases Total`,
  accident_case_count AS `Accident Cases Total`,
  last_accident_date AS `Last Accident Date`,
  company_latest_close_date AS `Company Latest Case Close Date`,
  company_latest_load_dt AS `Company Latest Load Timestamp`,
  followup_score AS `Follow-up Score`,
  CASE
    WHEN followup_score >= 85 THEN 'Priority 1'
    WHEN followup_score >= 55 THEN 'Priority 2'
    ELSE 'Priority 3'
  END AS `Follow-up Priority`,
  CASE
    WHEN severe_injury_indicator OR open_violation_status OR followup_score >= 85 THEN 'Call within 24 hours'
    WHEN followup_score >= 55 THEN 'Call this week'
    ELSE 'Nurture this month'
  END AS `Suggested Action`,
  CASE
    WHEN followup_score >= 85 THEN 'High'
    WHEN followup_score >= 55 THEN 'Medium'
    ELSE 'Low'
  END AS `Buying Likelihood`,
  CASE
    WHEN followup_score >= 85 THEN 'RED'
    WHEN followup_score >= 55 THEN 'YELLOW'
    ELSE 'GREEN'
  END AS `Urgency Band`,
  CASE
    WHEN followup_score >= 85 THEN '#F97066'
    WHEN followup_score >= 55 THEN '#F6C344'
    ELSE '#5BB974'
  END AS `Urgency Color`,
  CASE
    WHEN severe_injury_indicator THEN 'Yes'
    ELSE 'No'
  END AS `Severe Incident Signal`
FROM scored
WHERE industry_segment IN (
  'Manufacturing & Production',
  'Construction & Field Work',
  'Utilities & Field Services',
  'Warehouse & Distribution',
  'Healthcare & Clinical',
  'Public Sector & Municipal',
  'Laboratory & Research',
  'Specialized / Mixed Environment'
);

CREATE OR REPLACE VIEW osha_raw.v_sales_followup_bayarea_v2 AS
WITH inspection_base AS (
  SELECT
    TRIM(estab_name) AS company_name,
    site_address AS address,
    site_city AS city,
    site_state AS state,
    CAST(site_zip AS STRING) AS zip,
    CAST(naics_code AS STRING) AS naics_code,
    owner_type,
    insp_type,
    safety_hlth,
    DATE(close_case_date) AS close_case_date,
    DATE_DIFF(CURRENT_DATE(), DATE(close_case_date), DAY) AS days_since_close,
    CAST(activity_nr AS INT64) AS activity_nr,
    load_dt
  FROM osha_raw.inspection_bayarea_incremental
  WHERE DATE(close_case_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
    AND REGEXP_CONTAINS(CAST(site_zip AS STRING), r'^(940|941|943|944|945|946|947|948|949|950|951|954)')
),
inspection_company AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY company_name, address, city, state, zip
      ORDER BY load_dt DESC, activity_nr DESC
    ) AS rn,
    COUNT(*) OVER (
      PARTITION BY company_name, address, city, state, zip
    ) AS inspection_count,
    COUNTIF(close_case_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) OVER (
      PARTITION BY company_name, address, city, state, zip
    ) AS inspections_90d,
    MAX(close_case_date) OVER (
      PARTITION BY company_name, address, city, state, zip
    ) AS company_latest_close_date,
    MAX(load_dt) OVER (
      PARTITION BY company_name, address, city, state, zip
    ) AS company_latest_load_dt
  FROM inspection_base
),
violation_metrics AS (
  SELECT
    SAFE_CAST(activity_nr AS INT64) AS activity_nr,
    COUNT(*) AS violation_count,
    SUM(
      COALESCE(SAFE_CAST(current_penalty AS FLOAT64), 0)
      + COALESCE(SAFE_CAST(initial_penalty AS FLOAT64), 0)
      + COALESCE(SAFE_CAST(fta_penalty AS FLOAT64), 0)
    ) AS total_penalties,
    COUNTIF(
      (abate_complete IS NULL OR UPPER(CAST(abate_complete AS STRING)) NOT IN ('Y', 'YES', '1', 'TRUE'))
      AND final_order_date IS NULL
    ) AS open_violation_count
  FROM osha_raw.violation_recent
  GROUP BY 1
),
ppe_focus_metrics AS (
  SELECT
    SAFE_CAST(activity_nr AS INT64) AS activity_nr,
    COUNTIF(
      REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.133')
      OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.102')
      OR REGEXP_CONTAINS(
        LOWER(CONCAT(
          ' ',
          COALESCE(CAST(hazsub1 AS STRING), ''),
          ' ',
          COALESCE(CAST(hazsub2 AS STRING), ''),
          ' ',
          COALESCE(CAST(hazsub3 AS STRING), ''),
          ' ',
          COALESCE(CAST(hazsub4 AS STRING), ''),
          ' ',
          COALESCE(CAST(hazsub5 AS STRING), '')
        )),
        r'eye\s*hazard|eye\s*injur|eye\s*expos|face\s*hazard|face\s*injur|goggles?|face\s*shield|protective\s*eyewear|eye\s*and\s*face'
      )
    ) AS eye_face_violation_count,
    COUNTIF(
      REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.132')
      OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.95')
    ) AS general_ppe_violation_count,
    COUNTIF(
      REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.133\(a\)\(3\)')
      OR (
        (REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.133')
        OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.102'))
        AND REGEXP_CONTAINS(
          LOWER(CONCAT(
            ' ',
            COALESCE(CAST(hazsub1 AS STRING), ''),
            ' ',
            COALESCE(CAST(hazsub2 AS STRING), ''),
            ' ',
            COALESCE(CAST(hazsub3 AS STRING), ''),
            ' ',
            COALESCE(CAST(hazsub4 AS STRING), ''),
            ' ',
            COALESCE(CAST(hazsub5 AS STRING), '')
          )),
          r'prescription|lenses?|eyewear'
        )
      )
    ) AS prescription_lens_violation_count,
    COUNTIF(
      REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.133')
      OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.102')
      OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.132')
      OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.95')
      OR REGEXP_CONTAINS(
        LOWER(CONCAT(
          ' ',
          COALESCE(CAST(hazsub1 AS STRING), ''),
          ' ',
          COALESCE(CAST(hazsub2 AS STRING), ''),
          ' ',
          COALESCE(CAST(hazsub3 AS STRING), ''),
          ' ',
          COALESCE(CAST(hazsub4 AS STRING), ''),
          ' ',
          COALESCE(CAST(hazsub5 AS STRING), '')
        )),
        r'prescription|lenses?|eyewear|face shield|eye protection|eye\s*hazard|face\s*hazard|goggles?|personal protective equipment|ppe'
      )
    ) AS ppe_focus_violation_count
  FROM osha_raw.violation_recent
  GROUP BY 1
),
violation_details AS (
  SELECT
    SAFE_CAST(activity_nr AS INT64) AS activity_nr,
    STRING_AGG(
      DISTINCT NULLIF(TRIM(CAST(citation_id AS STRING)), ''),
      ' | ' ORDER BY NULLIF(TRIM(CAST(citation_id AS STRING)), '') LIMIT 12
    ) AS violation_items,
    STRING_AGG(
      DISTINCT NULLIF(TRIM(CAST(standard AS STRING)), ''),
      ' | ' ORDER BY NULLIF(TRIM(CAST(standard AS STRING)), '') LIMIT 12
    ) AS standards_cited,
    STRING_AGG(
      DISTINCT CASE
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(1910133|1926102)')
          THEN 'Eye and Face Protection'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(1910132|192695|1514)')
          THEN 'PPE Selection and Use'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(1509|3203|6401)')
          THEN 'Safety Program and Training (IIPP)'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^1512')
          THEN 'Medical/First Aid Readiness'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(15411|15410001|1541|1629|1632)')
          THEN 'Fall Protection and Safe Access'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^3395')
          THEN 'Heat Illness and Environmental Controls'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^3396')
          THEN 'Heat Illness and Environmental Controls'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(5162|5193)')
          THEN 'Health Exposure and Respiratory/Biological Controls'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(3382|3384|3227)')
          THEN 'PPE Selection and Use'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(6151|6175|3664|3666|1630)')
          THEN 'Fall Protection and Safe Access'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(19100157|19260150)')
          THEN 'Fire Protection and Emergency Readiness'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^19260701')
          THEN 'Construction Work Practice Controls'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^542')
          THEN 'Permit and Regulatory Process'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^341')
          THEN 'Permit and Regulatory Process'
        WHEN UPPER(CAST(viol_type AS STRING)) = 'WILLFUL'
          THEN 'High-Risk Compliance Behavior'
        WHEN UPPER(CAST(viol_type AS STRING)) = 'REPEAT'
          THEN 'Repeat Compliance Gaps'
        WHEN UPPER(CAST(viol_type AS STRING)) = 'SERIOUS'
          THEN 'Serious Safety Risk'
        ELSE NULL
      END,
      ' | ' LIMIT 4
    ) AS citation_sales_category,
    STRING_AGG(
      DISTINCT CASE
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(1910133|1926102)')
          THEN 'Eye/face hazard controls were not adequate; strong opportunity for protective eyewear standardization and fit testing.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(1910132|192695|1514)')
          THEN 'PPE selection/enforcement gap; position safety eyewear as part of a broader PPE compliance rollout.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(1509|3203|6401)')
          THEN 'Safety program/training process gap; bundle eyewear program support with onboarding and recurring safety training.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^1512')
          THEN 'Medical readiness and response expectations are in focus; emphasize incident prevention through reliable daily eye protection.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(15411|15410001|1541|1629|1632)')
          THEN 'Work-at-height/access risk context; position durable, secure-fit eyewear for active and elevated work conditions.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^3395')
          THEN 'Heat or environmental exposure controls are in focus; position anti-fog and all-day wearable protective eyewear.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^3396')
          THEN 'Heat or environmental exposure controls are in focus; position anti-fog and all-day wearable protective eyewear.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(5162|5193)')
          THEN 'Health exposure controls are in focus; position sealed or specialty protective eyewear matched to exposure risk.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(3382|3384|3227)')
          THEN 'PPE performance and enforcement gaps exist; position a standardized safety eyewear rollout with manager accountability.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(6151|6175|3664|3666|1630)')
          THEN 'Active work-at-height or access-control issues are present; lead with secure-fit, durable eyewear suited to mobile crews.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^(19100157|19260150)')
          THEN 'Emergency/fire readiness is in scope; align eyewear program with broader emergency preparedness and PPE compliance.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^19260701')
          THEN 'Construction work-practice controls are under review; position site-ready eyewear standards for consistent field execution.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^542')
          THEN 'Regulatory process scrutiny is present; offer documented eyewear policy and audit trail to reduce repeat citations.'
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(UPPER(CAST(standard AS STRING)), r'[^A-Z0-9]', ''), r'^341')
          THEN 'Permit/regulatory process scrutiny; offer a documented eyewear policy to reduce repeat findings during inspections.'
        WHEN UPPER(CAST(viol_type AS STRING)) = 'WILLFUL'
          THEN 'High urgency account: prioritize immediate outreach with a compliance-first eyewear implementation plan.'
        WHEN UPPER(CAST(viol_type AS STRING)) = 'REPEAT'
          THEN 'Recurring issue account: pitch a standardized multi-site eyewear program with measurable compliance tracking.'
        WHEN UPPER(CAST(viol_type AS STRING)) = 'SERIOUS'
          THEN 'Serious risk account: lead with fast deployment and manager controls for eye/face PPE consistency.'
        ELSE NULL
      END,
      ' || ' LIMIT 4
    ) AS citation_sales_explanation,
    STRING_AGG(
      DISTINCT CASE
        WHEN REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.133\(a\)\(3\)')
          THEN 'Prescription lenses: eye protection must fit and work safely with prescription lenses.'
        WHEN REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.133')
          OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.102')
          THEN 'Eye/face protection: provide and enforce proper eye and face PPE for identified hazards.'
        WHEN REGEXP_CONTAINS(CAST(standard AS STRING), r'^1910\.132')
          OR REGEXP_CONTAINS(CAST(standard AS STRING), r'^1926\.95')
          THEN 'General PPE requirement: assess hazards and provide suitable protective equipment.'
        WHEN UPPER(CAST(viol_type AS STRING)) = 'WILLFUL'
          THEN 'Willful citation: OSHA indicates the requirement was knowingly disregarded.'
        WHEN UPPER(CAST(viol_type AS STRING)) = 'REPEAT'
          THEN 'Repeat citation: similar OSHA requirement was cited previously.'
        WHEN UPPER(CAST(viol_type AS STRING)) = 'SERIOUS'
          THEN 'Serious citation: substantial probability of serious physical harm.'
        WHEN UPPER(CAST(viol_type AS STRING)) IN ('OTHER-THAN-SERIOUS', 'OTHER THAN SERIOUS')
          THEN 'Other-than-serious citation: related to safety and health but lower immediate severity.'
        WHEN NULLIF(TRIM(CAST(standard AS STRING)), '') IS NOT NULL
          THEN CONCAT('Cited OSHA standard ', TRIM(CAST(standard AS STRING)), '; review full case details for exact language.')
        ELSE 'OSHA requirement cited; review inspection case details for exact language.'
      END,
      ' || ' LIMIT 6
    ) AS citation_excerpt
  FROM osha_raw.violation_recent
  GROUP BY 1
),
violation_event_metrics AS (
  SELECT
    SAFE_CAST(activity_nr AS INT64) AS activity_nr,
    COUNT(*) AS violation_event_count,
    MAX(DATE(hist_date)) AS last_violation_event_date
  FROM osha_raw.violation_event_recent
  GROUP BY 1
),
related_activity_metrics AS (
  SELECT
    SAFE_CAST(activity_nr AS INT64) AS activity_nr,
    COUNT(*) AS related_activity_count,
    COUNTIF(UPPER(CAST(rel_type AS STRING)) IN ('B', 'COMPLAINT', '2')) AS complaint_related_count
  FROM osha_raw.related_activity_recent
  GROUP BY 1
),
emphasis_metrics AS (
  SELECT
    SAFE_CAST(activity_nr AS INT64) AS activity_nr,
    COUNT(*) AS emphasis_code_count
  FROM osha_raw.emphasis_codes_recent
  GROUP BY 1
),
injury_metrics AS (
  SELECT
    SAFE_CAST(rel_insp_nr AS INT64) AS activity_nr,
    COUNT(*) AS injury_count,
    COUNTIF(SAFE_CAST(degree_of_inj AS INT64) IN (1, 2)) AS severe_injury_count
  FROM osha_raw.accident_injury_recent
  GROUP BY 1
),
accident_metrics AS (
  SELECT
    ai.activity_nr,
    COUNT(DISTINCT a.summary_nr) AS accident_case_count,
    COUNTIF(SAFE_CAST(a.fatality AS INT64) > 0) AS fatality_case_count,
    MAX(DATE(a.event_date)) AS last_accident_date
  FROM (
    SELECT DISTINCT
      SAFE_CAST(rel_insp_nr AS INT64) AS activity_nr,
      CAST(summary_nr AS STRING) AS summary_nr
    FROM osha_raw.accident_injury_recent
    WHERE rel_insp_nr IS NOT NULL AND summary_nr IS NOT NULL
  ) ai
  LEFT JOIN (
    SELECT
      CAST(summary_nr AS STRING) AS summary_nr,
      fatality,
      event_date
    FROM osha_raw.accident_recent
  ) a
  ON ai.summary_nr = a.summary_nr
  GROUP BY 1
),
scored AS (
  SELECT
    'Bay Area' AS region_label,
    ic.company_name,
    ic.address,
    ic.city,
    ic.state,
    ic.zip,
    ic.naics_code,
    ic.owner_type,
    ic.insp_type,
    ic.safety_hlth,
    CASE
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^(31|32|33)') THEN 18
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^23') THEN 18
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^22') THEN 15
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^(42|48|49)') THEN 16
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^62') THEN 14
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^(5417|54138|3254)') THEN 17
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^92') OR ic.owner_type IN ('B', 'C', 'D') THEN 10
      ELSE 8
    END AS industry_fit_points,
    CASE
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^(31|32|33)') THEN 'Manufacturing & Production'
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^23') THEN 'Construction & Field Work'
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^22') THEN 'Utilities & Field Services'
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^(42|48|49)') THEN 'Warehouse & Distribution'
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^62') THEN 'Healthcare & Clinical'
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^(5417|54138|3254)') THEN 'Laboratory & Research'
      WHEN REGEXP_CONTAINS(ic.naics_code, r'^92') OR ic.owner_type IN ('B', 'C', 'D') THEN 'Public Sector & Municipal'
      ELSE 'Specialized / Mixed Environment'
    END AS industry_segment,
    CASE ic.owner_type
      WHEN 'A' THEN 'Private'
      WHEN 'B' THEN 'Local Government'
      WHEN 'C' THEN 'State Government'
      WHEN 'D' THEN 'Federal'
      ELSE 'Unknown'
    END AS owner_type_label,
    CASE ic.insp_type
      WHEN 'A' THEN 'Accident'
      WHEN 'B' THEN 'Complaint'
      WHEN 'C' THEN 'Referral'
      WHEN 'D' THEN 'Monitoring'
      WHEN 'E' THEN 'Variance'
      WHEN 'F' THEN 'Follow-up'
      WHEN 'G' THEN 'Unprogrammed Related'
      WHEN 'H' THEN 'Planned'
      WHEN 'I' THEN 'Program Related'
      WHEN 'J' THEN 'Unprogrammed Other'
      WHEN 'K' THEN 'Program Other'
      WHEN 'L' THEN 'Other'
      WHEN 'M' THEN 'Fatality/Catastrophe'
      WHEN 'N' THEN 'Unprogrammed Emphasis'
      ELSE 'Other'
    END AS inspection_type_label,
    ic.close_case_date,
    ic.days_since_close,
    CASE
      WHEN ic.days_since_close <= 30 THEN '0-30 days'
      WHEN ic.days_since_close <= 90 THEN '31-90 days'
      ELSE '91+ days'
    END AS recency_band,
    ic.activity_nr AS latest_activity_nr,
    ic.inspection_count,
    ic.inspections_90d,
    COALESCE(vm.violation_count, 0) AS violation_count,
    ROUND(COALESCE(vm.total_penalties, 0), 2) AS total_penalties,
    (COALESCE(vm.open_violation_count, 0) > 0) AS open_violation_status,
    COALESCE(vm.open_violation_count, 0) AS open_violation_count,
    COALESCE(vd.violation_items, '') AS violation_items,
    COALESCE(vd.standards_cited, '') AS standards_cited,
    COALESCE(vd.citation_sales_category, 'General OSHA Compliance') AS citation_sales_category,
    COALESCE(vd.citation_sales_explanation, 'OSHA cited a compliance gap; use discovery questions to map hazards and recommend the right safety eyewear program.') AS citation_sales_explanation,
    COALESCE(vd.citation_excerpt, '') AS citation_excerpt,
    COALESCE(vem.violation_event_count, 0) AS violation_event_count,
    vem.last_violation_event_date,
    COALESCE(ram.related_activity_count, 0) AS related_activity_count,
    (ic.insp_type = 'B' OR COALESCE(ram.complaint_related_count, 0) > 0) AS complaint_activity,
    COALESCE(ram.complaint_related_count, 0) AS complaint_activity_count,
    COALESCE(em.emphasis_code_count, 0) AS emphasis_code_count,
    COALESCE(im.injury_count, 0) AS injury_count,
    (COALESCE(im.severe_injury_count, 0) > 0 OR COALESCE(am.fatality_case_count, 0) > 0 OR ic.insp_type = 'M') AS severe_injury_indicator,
    COALESCE(im.severe_injury_count, 0) AS severe_injury_count,
    COALESCE(am.accident_case_count, 0) AS accident_case_count,
    COALESCE(am.fatality_case_count, 0) AS fatality_case_count,
    am.last_accident_date,
    ic.company_latest_close_date,
    ic.company_latest_load_dt,
    (
      CASE
        WHEN ic.days_since_close <= 14 THEN 30
        WHEN ic.days_since_close <= 30 THEN 24
        WHEN ic.days_since_close <= 60 THEN 16
        WHEN ic.days_since_close <= 90 THEN 8
        ELSE 2
      END
      + CASE
          WHEN REGEXP_CONTAINS(ic.naics_code, r'^(31|32|33|23)') THEN 18
          WHEN REGEXP_CONTAINS(ic.naics_code, r'^(42|48|49)') THEN 16
          WHEN REGEXP_CONTAINS(ic.naics_code, r'^22') THEN 15
          WHEN REGEXP_CONTAINS(ic.naics_code, r'^62') THEN 14
          WHEN REGEXP_CONTAINS(ic.naics_code, r'^(5417|54138|3254)') THEN 17
          WHEN REGEXP_CONTAINS(ic.naics_code, r'^92') OR ic.owner_type IN ('B', 'C', 'D') THEN 10
          ELSE 8
        END
      + CASE
          WHEN ic.safety_hlth = 'S' THEN 10
          WHEN ic.safety_hlth = 'H' THEN 4
          ELSE 2
        END
      + LEAST(COALESCE(vm.violation_count, 0) * 4, 20)
      + CASE
          WHEN COALESCE(vm.total_penalties, 0) >= 100000 THEN 20
          WHEN COALESCE(vm.total_penalties, 0) >= 25000 THEN 14
          WHEN COALESCE(vm.total_penalties, 0) >= 5000 THEN 8
          WHEN COALESCE(vm.total_penalties, 0) > 0 THEN 4
          ELSE 0
        END
      + CASE WHEN COALESCE(vm.open_violation_count, 0) > 0 THEN 12 ELSE 0 END
      + CASE WHEN (ic.insp_type = 'B' OR COALESCE(ram.complaint_related_count, 0) > 0) THEN 8 ELSE 0 END
      + CASE WHEN (COALESCE(im.severe_injury_count, 0) > 0 OR COALESCE(am.fatality_case_count, 0) > 0 OR ic.insp_type = 'M') THEN 15 ELSE 0 END
      + CASE WHEN COALESCE(em.emphasis_code_count, 0) > 0 THEN 5 ELSE 0 END
      + CASE WHEN COALESCE(pfm.eye_face_violation_count, 0) > 0 THEN 8 ELSE 0 END
      + CASE WHEN COALESCE(pfm.prescription_lens_violation_count, 0) > 0 THEN 6 ELSE 0 END
      + CASE WHEN COALESCE(pfm.general_ppe_violation_count, 0) > 0 THEN 4 ELSE 0 END
      + LEAST(COALESCE(pfm.ppe_focus_violation_count, 0) * 2, 8)
      + CASE
          WHEN ic.inspections_90d >= 3 THEN 10
          WHEN ic.inspections_90d = 2 THEN 7
          WHEN ic.inspections_90d = 1 THEN 4
          ELSE 0
        END
    ) AS followup_score
  FROM inspection_company ic
  LEFT JOIN violation_metrics vm ON ic.activity_nr = vm.activity_nr
  LEFT JOIN ppe_focus_metrics pfm ON ic.activity_nr = pfm.activity_nr
  LEFT JOIN violation_details vd ON ic.activity_nr = vd.activity_nr
  LEFT JOIN violation_event_metrics vem ON ic.activity_nr = vem.activity_nr
  LEFT JOIN related_activity_metrics ram ON ic.activity_nr = ram.activity_nr
  LEFT JOIN emphasis_metrics em ON ic.activity_nr = em.activity_nr
  LEFT JOIN injury_metrics im ON ic.activity_nr = im.activity_nr
  LEFT JOIN accident_metrics am ON ic.activity_nr = am.activity_nr
  WHERE ic.rn = 1
)
SELECT
  region_label AS `Region`,
  company_name AS `Account Name`,
  address AS `Site Address`,
  city AS `Site City`,
  state AS `Site State`,
  zip AS `Site ZIP`,
  naics_code AS `NAICS Code`,
  industry_segment AS `Industry Segment`,
  owner_type_label AS `Ownership Type`,
  inspection_type_label AS `Inspection Type`,
  close_case_date AS `Latest Case Close Date`,
  days_since_close AS `Days Since Last Case Close`,
  recency_band AS `Recency Window`,
  latest_activity_nr AS `Latest Inspection ID`,
  inspection_count AS `Inspections Total`,
  inspections_90d AS `Inspections Last 90 Days`,
  violation_count AS `Violations Total`,
  open_violation_count AS `Open Violations Total`,
  CASE WHEN open_violation_count > 0 THEN 'Yes' ELSE 'No' END AS `Has Open Violations`,
  ROUND(total_penalties, 2) AS `Penalties Total USD`,
  violation_items AS `Violation Items`,
  standards_cited AS `Standards Cited`,
  citation_sales_category AS `Citation Category`,
  citation_sales_explanation AS `Citation Sales Explanation`,
  citation_excerpt AS `Citation Excerpt`,
  violation_event_count AS `Violation Events Total`,
  last_violation_event_date AS `Last Violation Event Date`,
  related_activity_count AS `Related Activities Total`,
  complaint_activity_count AS `Complaint Related Activities`,
  CASE WHEN complaint_activity_count > 0 THEN 'Yes' ELSE 'No' END AS `Has Complaint Signal`,
  emphasis_code_count AS `Emphasis Codes Total`,
  injury_count AS `Injuries Total`,
  severe_injury_count AS `Severe Injuries Total`,
  fatality_case_count AS `Fatality Cases Total`,
  accident_case_count AS `Accident Cases Total`,
  last_accident_date AS `Last Accident Date`,
  company_latest_close_date AS `Company Latest Case Close Date`,
  company_latest_load_dt AS `Company Latest Load Timestamp`,
  followup_score AS `Follow-up Score`,
  CASE
    WHEN followup_score >= 85 THEN 'Priority 1'
    WHEN followup_score >= 55 THEN 'Priority 2'
    ELSE 'Priority 3'
  END AS `Follow-up Priority`,
  CASE
    WHEN severe_injury_indicator OR open_violation_status OR followup_score >= 85 THEN 'Call within 24 hours'
    WHEN followup_score >= 55 THEN 'Call this week'
    ELSE 'Nurture this month'
  END AS `Suggested Action`,
  CASE
    WHEN followup_score >= 85 THEN 'High'
    WHEN followup_score >= 55 THEN 'Medium'
    ELSE 'Low'
  END AS `Buying Likelihood`,
  CASE
    WHEN followup_score >= 85 THEN 'RED'
    WHEN followup_score >= 55 THEN 'YELLOW'
    ELSE 'GREEN'
  END AS `Urgency Band`,
  CASE
    WHEN followup_score >= 85 THEN '#F97066'
    WHEN followup_score >= 55 THEN '#F6C344'
    ELSE '#5BB974'
  END AS `Urgency Color`,
  CASE
    WHEN severe_injury_indicator THEN 'Yes'
    ELSE 'No'
  END AS `Severe Incident Signal`
FROM scored
WHERE industry_segment IN (
  'Manufacturing & Production',
  'Construction & Field Work',
  'Utilities & Field Services',
  'Warehouse & Distribution',
  'Healthcare & Clinical',
  'Public Sector & Municipal',
  'Laboratory & Research',
  'Specialized / Mixed Environment'
);

CREATE OR REPLACE TABLE osha_raw.sales_followup_sandiego_current AS
SELECT * FROM osha_raw.v_sales_followup_sandiego_v2;

CREATE OR REPLACE TABLE osha_raw.sales_followup_bayarea_current AS
SELECT * FROM osha_raw.v_sales_followup_bayarea_v2;

