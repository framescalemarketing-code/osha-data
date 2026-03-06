CREATE OR REPLACE TABLE `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.device_registration_current` AS
WITH normalized AS (
  SELECT
    region_label,
    UPPER(TRIM(facility_name)) AS company_key,
    TRIM(facility_name) AS facility_name,
    TRIM(address_line_1) AS address_line_1,
    TRIM(address_line_2) AS address_line_2,
    TRIM(city) AS city,
    UPPER(TRIM(state_code)) AS state_code,
    REGEXP_EXTRACT(CAST(zip_code AS STRING), r'^(\d{5})') AS zip5,
    TRIM(registration_number) AS registration_number,
    TRIM(fei_number) AS fei_number,
    TRIM(status_code) AS status_code,
    SAFE_CAST(reg_expiry_year AS INT64) AS reg_expiry_year,
    TRIM(initial_importer_flag) AS initial_importer_flag,
    TRIM(owner_operator_number) AS owner_operator_number,
    TRIM(owner_operator_firm_name) AS owner_operator_firm_name,
    TRIM(establishment_types) AS establishment_types,
    NULLIF(TRIM(product_code), '') AS product_code,
    NULLIF(TRIM(device_class), '') AS device_class,
    NULLIF(TRIM(medical_specialty), '') AS medical_specialty,
    NULLIF(TRIM(device_name), '') AS device_name,
    SAFE_CAST(load_dt AS TIMESTAMP) AS load_ts
  FROM `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.fda_device_registration_raw`
  WHERE UPPER(TRIM(state_code)) = 'CA'
),
dedup AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      n.*,
      ROW_NUMBER() OVER (
        PARTITION BY
          region_label,
          COALESCE(registration_number, ''),
          COALESCE(fei_number, ''),
          COALESCE(company_key, ''),
          COALESCE(zip5, ''),
          COALESCE(product_code, ''),
          COALESCE(device_name, '')
        ORDER BY load_ts DESC
      ) AS rn
    FROM normalized n
    WHERE zip5 IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  region_label,
  company_key,
  facility_name,
  address_line_1,
  address_line_2,
  city,
  state_code,
  zip5,
  registration_number,
  fei_number,
  MAX(status_code) AS status_code,
  MAX(reg_expiry_year) AS reg_expiry_year,
  MAX(initial_importer_flag) AS initial_importer_flag,
  ANY_VALUE(owner_operator_number) AS owner_operator_number,
  ANY_VALUE(owner_operator_firm_name) AS owner_operator_firm_name,
  STRING_AGG(DISTINCT NULLIF(establishment_types, ''), ' | ' LIMIT 8) AS establishment_types,
  COUNT(DISTINCT product_code) AS product_code_count,
  COUNT(DISTINCT IF(device_class = '3', product_code, NULL)) AS class3_product_count,
  COUNT(DISTINCT IF(device_class = '2', product_code, NULL)) AS class2_product_count,
  STRING_AGG(DISTINCT product_code, ' | ' ORDER BY product_code LIMIT 30) AS product_codes,
  STRING_AGG(DISTINCT device_class, ' | ' ORDER BY device_class LIMIT 6) AS device_classes,
  STRING_AGG(DISTINCT medical_specialty, ' | ' ORDER BY medical_specialty LIMIT 20) AS medical_specialties,
  STRING_AGG(DISTINCT device_name, ' | ' ORDER BY device_name LIMIT 20) AS device_names,
  MAX(load_ts) AS source_load_ts
FROM dedup
GROUP BY
  region_label,
  company_key,
  facility_name,
  address_line_1,
  address_line_2,
  city,
  state_code,
  zip5,
  registration_number,
  fei_number;

CREATE OR REPLACE VIEW `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.v_sales_followup_facility_v1` AS
WITH facility_base AS (
  SELECT
    region_label,
    company_key,
    facility_name,
    address_line_1,
    address_line_2,
    city,
    state_code,
    zip5,
    registration_number,
    fei_number,
    status_code,
    reg_expiry_year,
    owner_operator_number,
    owner_operator_firm_name,
    COALESCE(establishment_types, '') AS establishment_types,
    COALESCE(product_code_count, 0) AS product_code_count,
    COALESCE(class3_product_count, 0) AS class3_product_count,
    COALESCE(class2_product_count, 0) AS class2_product_count,
    COALESCE(product_codes, '') AS product_codes,
    COALESCE(device_classes, '') AS device_classes,
    COALESCE(medical_specialties, '') AS medical_specialties,
    COALESCE(device_names, '') AS device_names,
    source_load_ts,
    FARM_FINGERPRINT(
      CONCAT(
        COALESCE(registration_number, ''), '|',
        COALESCE(fei_number, ''), '|',
        COALESCE(company_key, ''), '|',
        COALESCE(zip5, '')
      )
    ) AS facility_id
  FROM `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.device_registration_current`
  WHERE region_label IN ('San Diego', 'Bay Area')
),
facility_keys AS (
  SELECT facility_id, 'registration' AS join_key_type, registration_number AS join_key
  FROM facility_base
  WHERE registration_number IS NOT NULL AND registration_number != ''
  UNION ALL
  SELECT facility_id, 'fei' AS join_key_type, fei_number AS join_key
  FROM facility_base
  WHERE fei_number IS NOT NULL AND fei_number != ''
),
k510_linked AS (
  SELECT
    fk.facility_id,
    NULLIF(TRIM(k.k_number), '') AS k_number,
    SAFE_CAST(k.decision_date AS DATE) AS decision_date
  FROM facility_keys fk
  JOIN `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.fda_device_510k_raw` k
    ON fk.join_key_type = k.join_key_type
   AND fk.join_key = k.join_key
  WHERE NULLIF(TRIM(k.k_number), '') IS NOT NULL
),
k510_metrics AS (
  SELECT
    facility_id,
    COUNT(DISTINCT k_number) AS k510_count_total,
    COUNT(DISTINCT IF(decision_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR), k_number, NULL)) AS k510_count_5y,
    MAX(decision_date) AS last_k510_decision_date
  FROM k510_linked
  GROUP BY 1
),
pma_linked AS (
  SELECT
    fk.facility_id,
    NULLIF(TRIM(p.pma_number), '') AS pma_number,
    SAFE_CAST(p.decision_date AS DATE) AS decision_date
  FROM facility_keys fk
  JOIN `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.fda_device_pma_raw` p
    ON fk.join_key_type = p.join_key_type
   AND fk.join_key = p.join_key
  WHERE NULLIF(TRIM(p.pma_number), '') IS NOT NULL
),
pma_metrics AS (
  SELECT
    facility_id,
    COUNT(DISTINCT pma_number) AS pma_count_total,
    COUNT(DISTINCT IF(decision_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR), pma_number, NULL)) AS pma_count_5y,
    MAX(decision_date) AS last_pma_decision_date
  FROM pma_linked
  GROUP BY 1
),
osha_signals AS (
  SELECT
    UPPER(TRIM(`Account Name`)) AS company_key,
    REGEXP_EXTRACT(CAST(`Site ZIP` AS STRING), r'^(\d{5})') AS zip5,
    MAX(
      CASE `Follow-up Priority`
        WHEN 'Priority 1' THEN 3
        WHEN 'Priority 2' THEN 2
        WHEN 'Priority 3' THEN 1
        ELSE 0
      END
    ) AS osha_priority_rank,
    MAX(CASE WHEN `Has Open Violations` = 'Yes' THEN 1 ELSE 0 END) AS has_open_violations,
    MAX(CASE WHEN `Severe Incident Signal` = 'Yes' THEN 1 ELSE 0 END) AS severe_incident_signal,
    MAX(SAFE_CAST(`Follow-up Score` AS FLOAT64)) AS max_osha_followup_score
  FROM (
    SELECT * FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.sales_followup_sandiego_current`
    UNION ALL
    SELECT * FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.sales_followup_bayarea_current`
  )
  GROUP BY 1, 2
),
scored AS (
  SELECT
    fb.region_label,
    fb.company_key,
    fb.facility_name,
    fb.address_line_1,
    fb.address_line_2,
    fb.city,
    fb.state_code,
    fb.zip5,
    fb.registration_number,
    fb.fei_number,
    fb.status_code,
    fb.reg_expiry_year,
    fb.owner_operator_number,
    fb.owner_operator_firm_name,
    fb.establishment_types,
    fb.product_code_count,
    fb.class3_product_count,
    fb.class2_product_count,
    fb.product_codes,
    fb.device_classes,
    fb.medical_specialties,
    fb.device_names,
    COALESCE(km.k510_count_total, 0) AS k510_count_total,
    COALESCE(km.k510_count_5y, 0) AS k510_count_5y,
    km.last_k510_decision_date,
    COALESCE(pm.pma_count_total, 0) AS pma_count_total,
    COALESCE(pm.pma_count_5y, 0) AS pma_count_5y,
    pm.last_pma_decision_date,
    COALESCE(os.osha_priority_rank, 0) AS osha_priority_rank,
    COALESCE(os.has_open_violations, 0) AS osha_open_violation_signal,
    COALESCE(os.severe_incident_signal, 0) AS osha_severe_signal,
    COALESCE(os.max_osha_followup_score, 0) AS osha_followup_score,
    fb.source_load_ts,
    LOWER(
      CONCAT(
        ' ', fb.facility_name,
        ' ', COALESCE(fb.owner_operator_firm_name, ''),
        ' ', COALESCE(fb.establishment_types, ''),
        ' ', COALESCE(fb.device_names, ''),
        ' ', COALESCE(fb.medical_specialties, ''),
        ' ', COALESCE(fb.product_codes, '')
      )
    ) AS text_blob
  FROM facility_base fb
  LEFT JOIN k510_metrics km ON fb.facility_id = km.facility_id
  LEFT JOIN pma_metrics pm ON fb.facility_id = pm.facility_id
  LEFT JOIN osha_signals os
    ON fb.company_key = os.company_key
   AND fb.zip5 = os.zip5
),
components AS (
  SELECT
    s.*,
    REGEXP_CONTAINS(s.text_blob, r'biotech|biopharm|biolog|genom|cell\s*therap|molecular|therapeutic|reagent') AS biotech_signal,
    REGEXP_CONTAINS(s.text_blob, r'pharma|drug|sterile|aseptic|inject|formulation|lyophil|api\b') AS pharma_signal,
    REGEXP_CONTAINS(s.text_blob, r'\blab\b|laborator|diagnostic|clinical chemistry|microbiology|immunology|pathology') AS lab_signal,
    CASE
      WHEN REGEXP_CONTAINS(UPPER(s.establishment_types), r'CONTRACT MANUFACTURER') THEN 20
      WHEN REGEXP_CONTAINS(UPPER(s.establishment_types), r'MANUFACTURE MEDICAL DEVICE') THEN 25
      WHEN REGEXP_CONTAINS(UPPER(s.establishment_types), r'CONTRACT STERILIZER') THEN 16
      WHEN REGEXP_CONTAINS(UPPER(s.establishment_types), r'REPACK OR RELABEL') THEN 10
      WHEN REGEXP_CONTAINS(UPPER(s.establishment_types), r'DEVELOP SPECIFICATIONS') THEN 8
      ELSE 4
    END AS role_points,
    LEAST(
      CASE
        WHEN s.class3_product_count > 0 THEN 18
        WHEN s.class2_product_count > 0 THEN 10
        ELSE 4
      END + LEAST(s.product_code_count, 7),
      25
    ) AS device_risk_points,
    LEAST(
      (CASE WHEN REGEXP_CONTAINS(s.text_blob, r'biotech|biopharm|biolog|genom|cell\s*therap|molecular|therapeutic|reagent') THEN 8 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(s.text_blob, r'pharma|drug|sterile|aseptic|inject|formulation|lyophil|api\b') THEN 7 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(s.text_blob, r'\blab\b|laborator|diagnostic|clinical chemistry|microbiology|immunology|pathology') THEN 7 ELSE 0 END),
      20
    ) AS vertical_relevance_points,
    CASE
      WHEN s.region_label = 'San Diego' AND REGEXP_CONTAINS(s.zip5, r'^921') THEN 15
      WHEN s.region_label = 'San Diego' AND REGEXP_CONTAINS(s.zip5, r'^920') THEN 11
      WHEN s.region_label = 'San Diego' AND REGEXP_CONTAINS(s.zip5, r'^919') THEN 7
      WHEN s.region_label = 'Bay Area' AND REGEXP_CONTAINS(s.zip5, r'^(945|946|947|948)') THEN 15
      WHEN s.region_label = 'Bay Area' AND REGEXP_CONTAINS(s.zip5, r'^(949|950|951)') THEN 11
      WHEN s.region_label = 'Bay Area' AND REGEXP_CONTAINS(s.zip5, r'^(940|941|943|944|954)') THEN 8
      ELSE 4
    END AS regional_fit_points,
    (
      CASE WHEN s.status_code = '1' THEN 3 ELSE 0 END
      + CASE
          WHEN s.reg_expiry_year >= EXTRACT(YEAR FROM CURRENT_DATE()) THEN 2
          WHEN s.reg_expiry_year = EXTRACT(YEAR FROM CURRENT_DATE()) - 1 THEN 1
          ELSE 0
        END
    ) AS freshness_points,
    LEAST(
      CASE
        WHEN s.k510_count_5y >= 10 THEN 6
        WHEN s.k510_count_5y >= 4 THEN 4
        WHEN s.k510_count_5y >= 1 THEN 2
        ELSE 0
      END
      + CASE
          WHEN s.pma_count_5y >= 5 THEN 4
          WHEN s.pma_count_5y >= 1 THEN 2
          ELSE 0
        END,
      10
    ) AS regulatory_activity_points,
    LEAST(
      CASE s.osha_priority_rank
        WHEN 3 THEN 7
        WHEN 2 THEN 4
        WHEN 1 THEN 2
        ELSE 0
      END
      + CASE WHEN s.osha_open_violation_signal = 1 THEN 2 ELSE 0 END
      + CASE WHEN s.osha_severe_signal = 1 THEN 2 ELSE 0 END,
      10
    ) AS osha_cross_signal_points
  FROM scored s
),
ranked AS (
  SELECT
    c.*,
    LEAST(
      c.role_points
      + c.device_risk_points
      + c.vertical_relevance_points
      + c.regional_fit_points
      + c.freshness_points
      + c.regulatory_activity_points
      + c.osha_cross_signal_points,
      100
    ) AS followup_score,
    (
      CASE WHEN c.role_points >= 16 THEN 1 ELSE 0 END
      + CASE WHEN c.class3_product_count > 0 THEN 1 ELSE 0 END
      + CASE WHEN c.regulatory_activity_points >= 4 THEN 1 ELSE 0 END
      + CASE WHEN c.biotech_signal OR c.pharma_signal OR c.lab_signal THEN 1 ELSE 0 END
      + CASE WHEN c.osha_priority_rank >= 2 THEN 1 ELSE 0 END
    ) AS quality_signal_count
  FROM components c
)
SELECT
  region_label AS `Region`,
  facility_name AS `Account Name`,
  address_line_1 AS `Site Address`,
  city AS `Site City`,
  state_code AS `Site State`,
  zip5 AS `Site ZIP`,
  registration_number AS `FDA Registration Number`,
  fei_number AS `FEI Number`,
  establishment_types AS `Establishment Types`,
  device_classes AS `Device Classes`,
  product_code_count AS `Product Codes Total`,
  product_codes AS `Product Codes`,
  medical_specialties AS `Medical Specialties`,
  device_names AS `Device Names`,
  k510_count_total AS `510k Count Total`,
  k510_count_5y AS `510k Count 5Y`,
  last_k510_decision_date AS `Last 510k Decision Date`,
  pma_count_total AS `PMA Count Total`,
  pma_count_5y AS `PMA Count 5Y`,
  last_pma_decision_date AS `Last PMA Decision Date`,
  CASE WHEN biotech_signal THEN 'Yes' ELSE 'No' END AS `Biotech Signal`,
  CASE WHEN pharma_signal THEN 'Yes' ELSE 'No' END AS `Pharma Signal`,
  CASE WHEN lab_signal THEN 'Yes' ELSE 'No' END AS `Lab Signal`,
  role_points AS `Role Points`,
  device_risk_points AS `Device Risk Points`,
  vertical_relevance_points AS `Vertical Relevance Points`,
  regional_fit_points AS `Regional Fit Points`,
  freshness_points AS `Freshness Points`,
  regulatory_activity_points AS `Regulatory Activity Points`,
  osha_cross_signal_points AS `OSHA Cross-Signal Points`,
  osha_priority_rank AS `Matched OSHA Priority Rank`,
  osha_followup_score AS `Matched OSHA Follow-up Score`,
  quality_signal_count AS `Quality Signal Count`,
  followup_score AS `Follow-up Score`,
  CASE
    WHEN followup_score >= 75
      OR (role_points >= 20 AND device_risk_points >= 18 AND regulatory_activity_points >= 4)
      THEN 'Priority 1'
    WHEN followup_score >= 50
      OR ((biotech_signal OR pharma_signal OR lab_signal) AND followup_score >= 40)
      THEN 'Priority 2'
    ELSE 'Priority 3'
  END AS `Follow-up Priority`,
  CASE
    WHEN followup_score >= 75
      OR (role_points >= 20 AND device_risk_points >= 18 AND regulatory_activity_points >= 4)
      THEN 'Call within 24 hours'
    WHEN followup_score >= 50
      OR ((biotech_signal OR pharma_signal OR lab_signal) AND followup_score >= 40)
      THEN 'Call this week'
    ELSE 'Nurture this month'
  END AS `Suggested Action`,
  CASE
    WHEN followup_score >= 75 THEN 'High'
    WHEN followup_score >= 50 THEN 'Medium'
    ELSE 'Low'
  END AS `Buying Likelihood`,
  CASE
    WHEN followup_score >= 75 THEN 'RED'
    WHEN followup_score >= 50 THEN 'YELLOW'
    ELSE 'GREEN'
  END AS `Urgency Band`,
  CASE
    WHEN followup_score >= 75 THEN '#F97066'
    WHEN followup_score >= 50 THEN '#F6C344'
    ELSE '#5BB974'
  END AS `Urgency Color`,
  source_load_ts AS `Source Load Timestamp`
FROM ranked
WHERE
  (
    role_points >= 10
    OR class3_product_count > 0
    OR biotech_signal
    OR pharma_signal
    OR lab_signal
    OR pma_count_5y > 0
    OR k510_count_5y > 0
    OR osha_priority_rank >= 1
  );

CREATE OR REPLACE TABLE `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.sales_followup_facility_current` AS
SELECT * FROM `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.v_sales_followup_facility_v1`;

CREATE OR REPLACE TABLE `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.sales_followup_facility_sandiego_current` AS
SELECT * FROM `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.v_sales_followup_facility_v1`
WHERE `Region` = 'San Diego';

CREATE OR REPLACE TABLE `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.sales_followup_facility_bayarea_current` AS
SELECT * FROM `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.v_sales_followup_facility_v1`
WHERE `Region` = 'Bay Area';
