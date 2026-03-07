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
    REGEXP_CONTAINS(s.text_blob, r'uv|ultraviolet|laser|bright light|photolith|weld|welding|arc flash') AS uv_bright_light_signal,
    REGEXP_CONTAINS(s.text_blob, r'chemical|acid|caustic|corrosive|solvent|reagent|sterile|aseptic|biohazard|disinfect') AS splash_chemical_signal,
    REGEXP_CONTAINS(s.text_blob, r'dust|powder|particulate|debris|abrasive|grind|grinding|blast|machin|saw|cutting') AS dust_debris_signal,
    REGEXP_CONTAINS(s.text_blob, r'impact|press|stamp|mill|machin|fabricat|tooling|metal') AS high_impact_signal,
    REGEXP_CONTAINS(s.text_blob, r'humid|humidity|condens|washdown|fog|anti-fog|cold storage|freezer') AS fog_humidity_signal,
    REGEXP_CONTAINS(s.text_blob, r'heat|hot|furnace|kiln|autoclave|steriliz|cold|cryo|freezer|thermal') AS extreme_temp_signal,
    REGEXP_CONTAINS(s.text_blob, r'computer|screen|display|monitor|inspection|microscope|quality control|metrology|diagnostic|assembly') AS computer_visual_signal,
    REGEXP_CONTAINS(s.text_blob, r'lab|laborator|diagnostic|clinical|research|inspection|microscope|cleanroom|sterile|aseptic') AS prescription_support_signal,
    LEAST(
      20,
      CASE
        WHEN REGEXP_CONTAINS(UPPER(s.establishment_types), r'MANUFACTURE MEDICAL DEVICE') THEN 18
        WHEN REGEXP_CONTAINS(UPPER(s.establishment_types), r'CONTRACT MANUFACTURER') THEN 16
        WHEN REGEXP_CONTAINS(UPPER(s.establishment_types), r'CONTRACT STERILIZER') THEN 14
        WHEN REGEXP_CONTAINS(UPPER(s.establishment_types), r'DEVELOP SPECIFICATIONS') THEN 10
        WHEN REGEXP_CONTAINS(UPPER(s.establishment_types), r'REPACK OR RELABEL') THEN 8
        ELSE 4
      END
      + CASE
          WHEN REGEXP_CONTAINS(s.text_blob, r'lab|laborator|diagnostic|clinical|research') THEN 4
          ELSE 0
        END
    ) AS role_points,
    LEAST(
      40,
      (CASE WHEN REGEXP_CONTAINS(s.text_blob, r'uv|ultraviolet|laser|bright light|photolith|weld|welding|arc flash') THEN 14 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(s.text_blob, r'chemical|acid|caustic|corrosive|solvent|reagent|sterile|aseptic|biohazard|disinfect') THEN 16 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(s.text_blob, r'dust|powder|particulate|debris|abrasive|grind|grinding|blast|machin|saw|cutting') THEN 14 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(s.text_blob, r'impact|press|stamp|mill|machin|fabricat|tooling|metal') THEN 16 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(s.text_blob, r'humid|humidity|condens|washdown|fog|anti-fog|cold storage|freezer') THEN 8 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(s.text_blob, r'heat|hot|furnace|kiln|autoclave|steriliz|cold|cryo|freezer|thermal') THEN 8 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(s.text_blob, r'computer|screen|display|monitor|inspection|microscope|quality control|metrology|diagnostic|assembly') THEN 6 ELSE 0 END)
    ) AS environment_points,
    LEAST(
      25,
      (CASE WHEN s.product_code_count >= 20 THEN 10 WHEN s.product_code_count >= 8 THEN 7 WHEN s.product_code_count >= 3 THEN 4 ELSE 1 END)
      + (CASE WHEN s.class3_product_count > 0 THEN 6 WHEN s.class2_product_count > 0 THEN 3 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(s.text_blob, r'quality control|inspection|cleanroom|sterile|aseptic|diagnostic|research') THEN 5 ELSE 0 END)
      + (CASE WHEN s.k510_count_5y > 0 OR s.pma_count_5y > 0 THEN 4 ELSE 0 END)
    ) AS operational_fit_points,
    LEAST(
      18,
      (CASE WHEN s.k510_count_5y >= 10 THEN 8 WHEN s.k510_count_5y >= 4 THEN 5 WHEN s.k510_count_5y >= 1 THEN 2 ELSE 0 END)
      + (CASE WHEN s.pma_count_5y >= 5 THEN 6 WHEN s.pma_count_5y >= 1 THEN 3 ELSE 0 END)
      + (CASE WHEN s.status_code = '1' THEN 2 ELSE 0 END)
      + (CASE
          WHEN s.reg_expiry_year >= EXTRACT(YEAR FROM CURRENT_DATE()) THEN 2
          WHEN s.reg_expiry_year = EXTRACT(YEAR FROM CURRENT_DATE()) - 1 THEN 1
          ELSE 0
        END)
    ) AS regulatory_support_points,
    LEAST(
      100,
      (CASE WHEN REGEXP_CONTAINS(s.text_blob, r'lab|laborator|diagnostic|clinical|research|inspection|microscope|cleanroom|sterile|aseptic') THEN 40 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(s.text_blob, r'computer|screen|display|monitor|inspection|microscope|quality control|metrology|diagnostic|assembly') THEN 12 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(s.text_blob, r'humid|humidity|condens|washdown|fog|anti-fog|cold storage|freezer') THEN 10 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(s.text_blob, r'chemical|acid|caustic|corrosive|solvent|reagent|sterile|aseptic|biohazard|disinfect') THEN 10 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(s.text_blob, r'heat|hot|furnace|kiln|autoclave|steriliz|cold|cryo|freezer|thermal') THEN 6 ELSE 0 END)
      + (CASE WHEN s.osha_priority_rank >= 2 THEN 8 ELSE 0 END)
      + (CASE WHEN s.osha_open_violation_signal = 1 THEN 6 ELSE 0 END)
      + (CASE WHEN s.osha_severe_signal = 1 THEN 8 ELSE 0 END)
    ) AS prescription_program_score,
    LEAST(
      30,
      (CASE s.osha_priority_rank WHEN 3 THEN 18 WHEN 2 THEN 12 WHEN 1 THEN 6 ELSE 0 END)
      + (CASE WHEN s.osha_open_violation_signal = 1 THEN 6 ELSE 0 END)
      + (CASE WHEN s.osha_severe_signal = 1 THEN 6 ELSE 0 END)
    ) AS osha_cross_signal_points
  FROM scored s
),
ranked AS (
  SELECT
    c.*,
    CAST(
      ROUND(
        LEAST(
          100,
          c.environment_points
          + CAST(ROUND(c.role_points * 1.5) AS INT64)
          + CAST(ROUND(c.regulatory_support_points * 1.5) AS INT64)
          + c.osha_cross_signal_points
        )
      ) AS INT64
    ) AS program_need_score,
    CAST(
      ROUND(
        LEAST(
          100,
          c.role_points * 2
          + c.operational_fit_points * 2
          + c.regulatory_support_points
          + c.osha_cross_signal_points * 0.5
        )
      ) AS INT64
    ) AS commercial_fit_support_score,
    CAST(
      ROUND(
        LEAST(
          100,
          (LEAST(
            100,
            c.environment_points
            + CAST(ROUND(c.role_points * 1.5) AS INT64)
            + CAST(ROUND(c.regulatory_support_points * 1.5) AS INT64)
            + c.osha_cross_signal_points
          ) * 0.45)
          + (c.prescription_program_score * 0.30)
          + (
            LEAST(
              100,
              c.role_points * 2
              + c.operational_fit_points * 2
              + c.regulatory_support_points
              + c.osha_cross_signal_points * 0.5
            ) * 0.25
          )
        )
      ) AS INT64
    ) AS fda_support_score
  FROM components c
)
SELECT
  region_label AS `Region`,
  facility_name AS `Account Name`,
  owner_operator_firm_name AS `Owner Operator Name`,
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
  CASE WHEN uv_bright_light_signal THEN 'Yes' ELSE 'No' END AS `UV Bright Light Signal`,
  CASE WHEN splash_chemical_signal THEN 'Yes' ELSE 'No' END AS `Splash Chemical Signal`,
  CASE WHEN dust_debris_signal THEN 'Yes' ELSE 'No' END AS `Dust Debris Signal`,
  CASE WHEN high_impact_signal THEN 'Yes' ELSE 'No' END AS `High Impact Signal`,
  CASE WHEN fog_humidity_signal THEN 'Yes' ELSE 'No' END AS `Fog Humidity Signal`,
  CASE WHEN extreme_temp_signal THEN 'Yes' ELSE 'No' END AS `Extreme Temperature Signal`,
  CASE WHEN computer_visual_signal THEN 'Yes' ELSE 'No' END AS `Computer Visual Task Signal`,
  CASE WHEN prescription_support_signal THEN 'Yes' ELSE 'No' END AS `Prescription Program Support Signal`,
  program_need_score AS `Program Need Score`,
  prescription_program_score AS `Prescription Program Score`,
  commercial_fit_support_score AS `Commercial Fit Support Score`,
  osha_priority_rank AS `Matched OSHA Priority Rank`,
  osha_followup_score AS `Matched OSHA Follow-up Score`,
  fda_support_score AS `FDA Support Score`,
  CASE
    WHEN fda_support_score >= 78
      OR (prescription_program_score >= 70 AND program_need_score >= 60)
      THEN 'Priority 1'
    WHEN fda_support_score >= 55
      OR program_need_score >= 60
      OR prescription_program_score >= 55
      THEN 'Priority 2'
    ELSE 'Priority 3'
  END AS `Follow-up Priority`,
  CASE
    WHEN fda_support_score >= 78
      OR (prescription_program_score >= 70 AND program_need_score >= 60)
      THEN 'Call within 24 hours'
    WHEN fda_support_score >= 55
      OR program_need_score >= 60
      OR prescription_program_score >= 55
      THEN 'Call this week'
    ELSE 'Nurture this month'
  END AS `Suggested Action`,
  CASE
    WHEN fda_support_score >= 78 THEN 'High'
    WHEN fda_support_score >= 55 THEN 'Medium'
    ELSE 'Low'
  END AS `Buying Likelihood`,
  CASE
    WHEN fda_support_score >= 78 THEN 'RED'
    WHEN fda_support_score >= 55 THEN 'YELLOW'
    ELSE 'GREEN'
  END AS `Urgency Band`,
  CASE
    WHEN fda_support_score >= 78 THEN '#F97066'
    WHEN fda_support_score >= 55 THEN '#F6C344'
    ELSE '#5BB974'
  END AS `Urgency Color`,
  source_load_ts AS `Source Load Timestamp`
FROM ranked
WHERE
  program_need_score >= 40
  OR prescription_program_score >= 45
  OR commercial_fit_support_score >= 45
  OR osha_priority_rank >= 1;

CREATE OR REPLACE TABLE `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.sales_followup_facility_current` AS
SELECT * FROM `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.v_sales_followup_facility_v1`;

CREATE OR REPLACE TABLE `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.sales_followup_facility_sandiego_current` AS
SELECT * FROM `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.v_sales_followup_facility_v1`
WHERE `Region` = 'San Diego';

CREATE OR REPLACE TABLE `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.sales_followup_facility_bayarea_current` AS
SELECT * FROM `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.v_sales_followup_facility_v1`
WHERE `Region` = 'Bay Area';
