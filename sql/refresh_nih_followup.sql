CREATE OR REPLACE TABLE `{{NIH_PROJECT_ID}}.{{NIH_DATASET}}.nih_project_current` AS
SELECT
  region_label,
  UPPER(TRIM(org_name)) AS company_key,
  TRIM(org_name) AS org_name,
  TRIM(org_city) AS org_city,
  UPPER(TRIM(org_state)) AS org_state,
  REGEXP_EXTRACT(CAST(org_zipcode AS STRING), r'^(\d{5})') AS zip5,
  TRIM(org_country) AS org_country,
  TRIM(org_type) AS org_type,
  TRIM(appl_id) AS appl_id,
  TRIM(project_num) AS project_num,
  TRIM(project_title) AS project_title,
  TRIM(activity_code) AS activity_code,
  SAFE_CAST(fiscal_year AS INT64) AS fiscal_year,
  SAFE_CAST(award_amount AS FLOAT64) AS award_amount,
  SAFE_CAST(direct_cost_amt AS FLOAT64) AS direct_cost_amt,
  SAFE_CAST(indirect_cost_amt AS FLOAT64) AS indirect_cost_amt,
  SAFE_CAST(award_notice_date AS DATE) AS award_notice_date,
  SAFE_CAST(project_start_date AS DATE) AS project_start_date,
  SAFE_CAST(project_end_date AS DATE) AS project_end_date,
  TRIM(principal_investigators) AS principal_investigators,
  TRIM(agency_ic_admin) AS agency_ic_admin,
  TRIM(project_terms) AS project_terms,
  TRIM(abstract_text) AS abstract_text,
  SAFE_CAST(load_dt AS TIMESTAMP) AS load_ts
FROM `{{NIH_PROJECT_ID}}.{{NIH_DATASET}}.nih_project_raw`
WHERE UPPER(TRIM(org_state)) = 'CA'
  AND region_label IN ('San Diego', 'Bay Area')
  AND NULLIF(TRIM(org_name), '') IS NOT NULL;

CREATE OR REPLACE VIEW `{{NIH_PROJECT_ID}}.{{NIH_DATASET}}.v_sales_followup_org_v1` AS
WITH org_rollup AS (
  SELECT
    region_label,
    company_key,
    org_name,
    org_city,
    org_state,
    zip5,
    ANY_VALUE(org_type) AS org_type,
    COUNT(DISTINCT appl_id) AS award_count,
    COUNT(DISTINCT IF(project_end_date >= CURRENT_DATE(), appl_id, NULL)) AS active_project_count,
    SUM(COALESCE(award_amount, 0)) AS total_award_amount,
    SUM(COALESCE(direct_cost_amt, 0)) AS total_direct_cost_amount,
    SUM(COALESCE(indirect_cost_amt, 0)) AS total_indirect_cost_amount,
    MAX(award_notice_date) AS latest_award_notice_date,
    STRING_AGG(DISTINCT NULLIF(activity_code, ''), ' | ' LIMIT 15) AS activity_codes,
    STRING_AGG(DISTINCT NULLIF(project_title, ''), ' | ' LIMIT 30) AS project_titles,
    STRING_AGG(DISTINCT NULLIF(principal_investigators, ''), ' | ' LIMIT 20) AS principal_investigators,
    STRING_AGG(DISTINCT NULLIF(project_terms, ''), ' | ' LIMIT 30) AS project_terms,
    STRING_AGG(DISTINCT NULLIF(agency_ic_admin, ''), ' | ' LIMIT 15) AS agency_ic_admin,
    STRING_AGG(DISTINCT NULLIF(abstract_text, ''), ' | ' LIMIT 10) AS abstract_text,
    MAX(load_ts) AS source_load_ts
  FROM `{{NIH_PROJECT_ID}}.{{NIH_DATASET}}.nih_project_current`
  GROUP BY 1, 2, 3, 4, 5, 6
),
scored AS (
  SELECT
    o.*,
    LOWER(
      CONCAT(
        ' ', org_name,
        ' ', COALESCE(org_type, ''),
        ' ', COALESCE(project_titles, ''),
        ' ', COALESCE(project_terms, ''),
        ' ', COALESCE(abstract_text, '')
      )
    ) AS text_blob
  FROM org_rollup o
),
ranked AS (
  SELECT
    s.*,
    REGEXP_CONTAINS(text_blob, r'uv|ultraviolet|laser|photonic|optic|microscopy|imaging') AS uv_bright_light_signal,
    REGEXP_CONTAINS(text_blob, r'chemical|chemistry|reagent|solvent|biohazard|assay|toxic|pharma|drug') AS splash_chemical_signal,
    REGEXP_CONTAINS(text_blob, r'dust|particulate|powder|aerosol|airborne|nanoparticle') AS dust_debris_signal,
    REGEXP_CONTAINS(text_blob, r'prototype|manufactur|fabricat|robotic|device|engineering') AS high_impact_signal,
    REGEXP_CONTAINS(text_blob, r'humid|humidity|condens|cleanroom|sterile|aseptic|washdown') AS fog_humidity_signal,
    REGEXP_CONTAINS(text_blob, r'thermal|heat|cold|cryo|freezer|temperature') AS extreme_temp_signal,
    REGEXP_CONTAINS(text_blob, r'computer|screen|display|monitor|imaging|microscope|analysis|visual|inspection') AS computer_visual_signal,
    REGEXP_CONTAINS(text_blob, r'clinical|diagnostic|laborator|research|microscope|imaging|visual|screen|inspection') AS prescription_support_signal,
    LEAST(
      38,
      (CASE WHEN REGEXP_CONTAINS(text_blob, r'clinical|diagnostic|laborator|research|biotech|pharma|medical device|cleanroom') THEN 18 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(text_blob, r'chemical|chemistry|reagent|solvent|biohazard|assay|toxic|pharma|drug') THEN 10 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(text_blob, r'prototype|manufactur|fabricat|robotic|device|engineering') THEN 10 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(text_blob, r'uv|ultraviolet|laser|photonic|optic|microscopy|imaging') THEN 8 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(text_blob, r'computer|screen|display|monitor|imaging|microscope|analysis|visual|inspection') THEN 6 ELSE 0 END)
    ) AS environment_points,
    LEAST(
      32,
      (CASE WHEN active_project_count >= 10 THEN 14 WHEN active_project_count >= 4 THEN 10 WHEN active_project_count >= 1 THEN 5 ELSE 0 END)
      + (CASE WHEN total_award_amount >= 50000000 THEN 14 WHEN total_award_amount >= 10000000 THEN 10 WHEN total_award_amount >= 2500000 THEN 6 WHEN total_award_amount >= 500000 THEN 3 ELSE 0 END)
      + (CASE WHEN org_type IS NOT NULL AND org_type != '' THEN 4 ELSE 0 END)
      + (CASE WHEN latest_award_notice_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY) THEN 4 ELSE 0 END)
    ) AS operational_fit_points,
    LEAST(
      100,
      (CASE WHEN REGEXP_CONTAINS(text_blob, r'clinical|diagnostic|laborator|research|microscope|imaging|visual|screen|inspection') THEN 40 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(text_blob, r'computer|screen|display|monitor|imaging|microscope|analysis|visual|inspection') THEN 16 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(text_blob, r'chemical|chemistry|reagent|solvent|biohazard|assay|toxic|pharma|drug') THEN 12 ELSE 0 END)
      + (CASE WHEN total_award_amount >= 10000000 THEN 8 WHEN total_award_amount >= 2500000 THEN 4 ELSE 0 END)
      + (CASE WHEN active_project_count >= 4 THEN 8 WHEN active_project_count >= 1 THEN 4 ELSE 0 END)
    ) AS prescription_program_score
  FROM scored s
)
SELECT
  region_label,
  org_name AS account_name,
  org_city AS site_city,
  org_state AS site_state,
  zip5 AS site_zip,
  org_type AS organization_type,
  award_count,
  active_project_count,
  total_award_amount,
  total_direct_cost_amount,
  total_indirect_cost_amount,
  latest_award_notice_date,
  activity_codes,
  project_titles,
  project_terms,
  CASE WHEN uv_bright_light_signal THEN 'Yes' ELSE 'No' END AS uv_bright_light_signal,
  CASE WHEN splash_chemical_signal THEN 'Yes' ELSE 'No' END AS splash_chemical_signal,
  CASE WHEN dust_debris_signal THEN 'Yes' ELSE 'No' END AS dust_debris_signal,
  CASE WHEN high_impact_signal THEN 'Yes' ELSE 'No' END AS high_impact_signal,
  CASE WHEN fog_humidity_signal THEN 'Yes' ELSE 'No' END AS fog_humidity_signal,
  CASE WHEN extreme_temp_signal THEN 'Yes' ELSE 'No' END AS extreme_temperature_signal,
  CASE WHEN computer_visual_signal THEN 'Yes' ELSE 'No' END AS computer_visual_task_signal,
  CASE WHEN prescription_support_signal THEN 'Yes' ELSE 'No' END AS prescription_program_support_signal,
  CAST(ROUND(LEAST(100, environment_points * 1.5 + operational_fit_points * 0.4)) AS INT64) AS program_need_score,
  prescription_program_score,
  CAST(ROUND(LEAST(100, operational_fit_points * 2.6 + environment_points * 0.3)) AS INT64) AS commercial_fit_support_score,
  CAST(
    ROUND(
      LEAST(
        100,
        (environment_points * 1.5 + operational_fit_points * 0.4) * 0.35
        + prescription_program_score * 0.35
        + (operational_fit_points * 2.6 + environment_points * 0.3) * 0.30
      )
    ) AS INT64
  ) AS nih_support_score,
  CASE
    WHEN total_award_amount >= 10000000 OR prescription_program_score >= 68 THEN 'Priority 1'
    WHEN total_award_amount >= 2500000 OR prescription_program_score >= 50 OR environment_points >= 18 THEN 'Priority 2'
    ELSE 'Priority 3'
  END AS followup_priority,
  CASE
    WHEN total_award_amount >= 10000000 OR prescription_program_score >= 68 THEN 'Review for immediate outreach'
    WHEN total_award_amount >= 2500000 OR prescription_program_score >= 50 OR environment_points >= 18 THEN 'Research this week'
    ELSE 'Monitor this month'
  END AS suggested_action,
  source_load_ts AS source_load_timestamp
FROM ranked
WHERE
  total_award_amount >= 500000
  OR active_project_count > 0
  OR prescription_program_score >= 35;

CREATE OR REPLACE TABLE `{{NIH_PROJECT_ID}}.{{NIH_DATASET}}.sales_followup_org_current` AS
SELECT * FROM `{{NIH_PROJECT_ID}}.{{NIH_DATASET}}.v_sales_followup_org_v1`;

CREATE OR REPLACE TABLE `{{NIH_PROJECT_ID}}.{{NIH_DATASET}}.sales_followup_org_sandiego_current` AS
SELECT * FROM `{{NIH_PROJECT_ID}}.{{NIH_DATASET}}.v_sales_followup_org_v1`
WHERE region_label = 'San Diego';

CREATE OR REPLACE TABLE `{{NIH_PROJECT_ID}}.{{NIH_DATASET}}.sales_followup_org_bayarea_current` AS
SELECT * FROM `{{NIH_PROJECT_ID}}.{{NIH_DATASET}}.v_sales_followup_org_v1`
WHERE region_label = 'Bay Area';
