CREATE OR REPLACE TABLE `{{EPA_PROJECT_ID}}.{{EPA_DATASET}}.epa_facility_current` AS
SELECT
  region_label,
  UPPER(TRIM(fac_name)) AS company_key,
  TRIM(fac_name) AS fac_name,
  TRIM(fac_street) AS fac_street,
  TRIM(fac_city) AS fac_city,
  UPPER(TRIM(fac_state)) AS fac_state,
  REGEXP_EXTRACT(CAST(fac_zip AS STRING), r'^(\d{5})') AS zip5,
  UPPER(TRIM(fac_county)) AS fac_county,
  TRIM(registry_id) AS registry_id,
  TRIM(fac_federal_flg) AS fac_federal_flg,
  TRIM(fac_active_flag) AS fac_active_flag,
  SAFE_CAST(fac_programs_in_snc AS INT64) AS fac_programs_in_snc,
  SAFE_CAST(fac_qtrs_in_nc AS INT64) AS fac_qtrs_in_nc,
  TRIM(fac_curr_compliance_status) AS fac_curr_compliance_status,
  TRIM(fac_curr_snc_flag) AS fac_curr_snc_flag,
  TRIM(air_flag) AS air_flag,
  TRIM(npdes_flag) AS npdes_flag,
  TRIM(sdwis_flag) AS sdwis_flag,
  TRIM(rcra_flag) AS rcra_flag,
  SAFE_CAST(caa_qtrs_in_nc AS INT64) AS caa_qtrs_in_nc,
  TRIM(caa_curr_compliance_status) AS caa_curr_compliance_status,
  TRIM(caa_curr_hpv_flag) AS caa_curr_hpv_flag,
  SAFE_CAST(cwa_inspection_count AS INT64) AS cwa_inspection_count,
  SAFE_CAST(cwa_formal_action_count AS INT64) AS cwa_formal_action_count,
  SAFE_CAST(cwa_qtrs_in_nc AS INT64) AS cwa_qtrs_in_nc,
  TRIM(cwa_curr_compliance_status) AS cwa_curr_compliance_status,
  TRIM(cwa_curr_snc_flag) AS cwa_curr_snc_flag,
  SAFE_CAST(rcra_inspection_count AS INT64) AS rcra_inspection_count,
  SAFE_CAST(rcra_formal_action_count AS INT64) AS rcra_formal_action_count,
  SAFE_CAST(rcra_qtrs_in_nc AS INT64) AS rcra_qtrs_in_nc,
  TRIM(rcra_curr_compliance_status) AS rcra_curr_compliance_status,
  TRIM(rcra_curr_snc_flag) AS rcra_curr_snc_flag,
  SAFE_CAST(sdwa_formal_action_count AS INT64) AS sdwa_formal_action_count,
  TRIM(sdwa_curr_compliance_status) AS sdwa_curr_compliance_status,
  TRIM(sdwa_curr_snc_flag) AS sdwa_curr_snc_flag,
  TRIM(tri_ids) AS tri_ids,
  SAFE_CAST(tri_releases_transfers AS FLOAT64) AS tri_releases_transfers,
  SAFE_CAST(tri_on_site_releases AS FLOAT64) AS tri_on_site_releases,
  TRIM(tri_reporter_in_past) AS tri_reporter_in_past,
  SAFE_CAST(fec_number_of_cases AS INT64) AS fec_number_of_cases,
  SAFE_CAST(fec_total_penalties AS FLOAT64) AS fec_total_penalties,
  TRIM(fac_naics_codes) AS fac_naics_codes,
  TRIM(fac_sic_codes) AS fac_sic_codes,
  SAFE_CAST(fac_date_last_inspection_epa AS DATE) AS fac_date_last_inspection_epa,
  SAFE_CAST(fac_date_last_inspection_state AS DATE) AS fac_date_last_inspection_state,
  SAFE_CAST(fac_date_last_formal_act_epa AS DATE) AS fac_date_last_formal_act_epa,
  SAFE_CAST(fac_date_last_formal_act_state AS DATE) AS fac_date_last_formal_act_state,
  TRIM(fac_federal_agency) AS fac_federal_agency,
  TRIM(dfr_url) AS dfr_url,
  SAFE_CAST(load_dt AS TIMESTAMP) AS load_ts
FROM `{{EPA_PROJECT_ID}}.{{EPA_DATASET}}.epa_facility_raw`
WHERE UPPER(TRIM(fac_state)) = 'CA'
  AND region_label IN ('San Diego', 'Bay Area')
  AND NULLIF(TRIM(fac_name), '') IS NOT NULL;

CREATE OR REPLACE VIEW `{{EPA_PROJECT_ID}}.{{EPA_DATASET}}.v_sales_followup_facility_v1` AS
WITH base AS (
  SELECT
    *,
    LOWER(
      CONCAT(
        ' ', fac_name,
        ' ', COALESCE(fac_federal_agency, ''),
        ' ', COALESCE(fac_naics_codes, ''),
        ' ', COALESCE(fac_sic_codes, '')
      )
    ) AS text_blob
  FROM `{{EPA_PROJECT_ID}}.{{EPA_DATASET}}.epa_facility_current`
),
scored AS (
  SELECT
    b.*,
    REGEXP_CONTAINS(text_blob, r'chemical|acid|caustic|corrosive|solvent|coating|plating|paint|refin|waste|hazard') AS splash_chemical_signal,
    REGEXP_CONTAINS(text_blob, r'dust|debris|cement|concrete|aggregate|sand|powder|particulate|wood|sawmill|foundry') AS dust_debris_signal,
    REGEXP_CONTAINS(text_blob, r'weld|welding|forge|foundry|metal|ship|aerospace|fabricat|machin|mill|tool|stamp') AS high_impact_signal,
    REGEXP_CONTAINS(text_blob, r'uv|ultraviolet|photolith|laser|weld|arc') AS uv_bright_light_signal,
    REGEXP_CONTAINS(text_blob, r'water|wastewater|wash|steam|humid|cooling|food|beverage') AS fog_humidity_signal,
    REGEXP_CONTAINS(text_blob, r'furnace|kiln|forge|thermal|hot|heat|cold storage|freezer|cryo') AS extreme_temp_signal,
    REGEXP_CONTAINS(text_blob, r'laborator|analysis|monitor|inspection|diagnostic|cleanroom|semiconductor') AS computer_visual_signal,
    REGEXP_CONTAINS(text_blob, r'laborator|diagnostic|clinical|research|inspection|cleanroom|monitor') AS prescription_support_signal,
    LEAST(
      36,
      (CASE WHEN COALESCE(tri_releases_transfers, 0) > 0 OR COALESCE(tri_on_site_releases, 0) > 0 THEN 16 ELSE 0 END)
      + (CASE WHEN COALESCE(rcra_qtrs_in_nc, 0) > 0 OR rcra_curr_snc_flag IN ('Y', 'Yes') THEN 10 ELSE 0 END)
      + (CASE WHEN COALESCE(cwa_qtrs_in_nc, 0) > 0 OR cwa_curr_snc_flag IN ('Y', 'Yes') THEN 8 ELSE 0 END)
      + (CASE WHEN COALESCE(fac_programs_in_snc, 0) > 0 THEN 6 ELSE 0 END)
      + (CASE WHEN fac_curr_snc_flag IN ('Y', 'Yes') THEN 6 ELSE 0 END)
    ) AS compliance_points,
    LEAST(
      34,
      (CASE WHEN REGEXP_CONTAINS(text_blob, r'chemical|acid|caustic|corrosive|solvent|coating|plating|paint|refin|waste|hazard') THEN 14 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(text_blob, r'dust|debris|cement|concrete|aggregate|sand|powder|particulate|wood|sawmill|foundry') THEN 10 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(text_blob, r'weld|welding|forge|foundry|metal|ship|aerospace|fabricat|machin|mill|tool|stamp') THEN 12 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(text_blob, r'uv|ultraviolet|photolith|laser|weld|arc') THEN 8 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(text_blob, r'water|wastewater|wash|steam|humid|cooling|food|beverage') THEN 5 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(text_blob, r'furnace|kiln|forge|thermal|hot|heat|cold storage|freezer|cryo') THEN 5 ELSE 0 END)
    ) AS exposure_points,
    LEAST(
      24,
      (CASE WHEN COALESCE(cwa_inspection_count, 0) >= 5 OR COALESCE(rcra_inspection_count, 0) >= 5 THEN 8 WHEN COALESCE(cwa_inspection_count, 0) + COALESCE(rcra_inspection_count, 0) >= 2 THEN 4 ELSE 0 END)
      + (CASE WHEN COALESCE(cwa_formal_action_count, 0) + COALESCE(rcra_formal_action_count, 0) + COALESCE(sdwa_formal_action_count, 0) >= 2 THEN 8 WHEN COALESCE(cwa_formal_action_count, 0) + COALESCE(rcra_formal_action_count, 0) + COALESCE(sdwa_formal_action_count, 0) = 1 THEN 4 ELSE 0 END)
      + (CASE WHEN COALESCE(fec_number_of_cases, 0) > 0 THEN 4 ELSE 0 END)
      + (CASE WHEN COALESCE(fec_total_penalties, 0) >= 100000 THEN 4 WHEN COALESCE(fec_total_penalties, 0) > 0 THEN 2 ELSE 0 END)
      + (CASE WHEN fac_federal_flg IN ('Y', 'Yes') THEN 3 ELSE 0 END)
      + (CASE WHEN tri_reporter_in_past IN ('Y', 'Yes') THEN 3 ELSE 0 END)
    ) AS operational_fit_points,
    LEAST(
      100,
      (CASE WHEN REGEXP_CONTAINS(text_blob, r'laborator|diagnostic|clinical|research|inspection|cleanroom|monitor') THEN 38 ELSE 0 END)
      + (CASE WHEN REGEXP_CONTAINS(text_blob, r'analysis|monitor|inspection|diagnostic|semiconductor') THEN 12 ELSE 0 END)
      + (CASE WHEN COALESCE(tri_releases_transfers, 0) > 0 OR COALESCE(tri_on_site_releases, 0) > 0 THEN 10 ELSE 0 END)
      + (CASE WHEN COALESCE(cwa_qtrs_in_nc, 0) > 0 OR COALESCE(rcra_qtrs_in_nc, 0) > 0 THEN 10 ELSE 0 END)
      + (CASE WHEN fac_curr_snc_flag IN ('Y', 'Yes') THEN 8 ELSE 0 END)
      + (CASE WHEN COALESCE(fec_number_of_cases, 0) > 0 THEN 6 ELSE 0 END)
    ) AS prescription_program_score
  FROM base b
),
ranked AS (
  SELECT
    s.*,
    CAST(
      ROUND(
        LEAST(
          100,
          exposure_points * 1.2
          + compliance_points * 0.9
          + operational_fit_points * 0.5
        )
      ) AS INT64
    ) AS program_need_score,
    CAST(
      ROUND(
        LEAST(
          100,
          operational_fit_points * 2.2
          + compliance_points * 0.7
        )
      ) AS INT64
    ) AS commercial_fit_support_score,
    CAST(
      ROUND(
        LEAST(
          100,
          (exposure_points * 1.2 + compliance_points * 0.9 + operational_fit_points * 0.5) * 0.45
          + prescription_program_score * 0.25
          + (operational_fit_points * 2.2 + compliance_points * 0.7) * 0.30
        )
      ) AS INT64
    ) AS epa_support_score
  FROM scored s
)
SELECT
  region_label,
  fac_name AS account_name,
  fac_street AS site_address,
  fac_city AS site_city,
  fac_state AS site_state,
  zip5 AS site_zip,
  registry_id AS epa_registry_id,
  fac_county AS county,
  fac_naics_codes AS naics_codes,
  fac_curr_compliance_status AS facility_compliance_status,
  cwa_curr_compliance_status AS water_compliance_status,
  rcra_curr_compliance_status AS rcra_compliance_status,
  COALESCE(tri_releases_transfers, 0) AS tri_releases_transfers,
  COALESCE(tri_on_site_releases, 0) AS tri_on_site_releases,
  COALESCE(fec_total_penalties, 0) AS federal_penalties,
  CASE WHEN uv_bright_light_signal THEN 'Yes' ELSE 'No' END AS uv_bright_light_signal,
  CASE WHEN splash_chemical_signal THEN 'Yes' ELSE 'No' END AS splash_chemical_signal,
  CASE WHEN dust_debris_signal THEN 'Yes' ELSE 'No' END AS dust_debris_signal,
  CASE WHEN high_impact_signal THEN 'Yes' ELSE 'No' END AS high_impact_signal,
  CASE WHEN fog_humidity_signal THEN 'Yes' ELSE 'No' END AS fog_humidity_signal,
  CASE WHEN extreme_temp_signal THEN 'Yes' ELSE 'No' END AS extreme_temperature_signal,
  CASE WHEN computer_visual_signal THEN 'Yes' ELSE 'No' END AS computer_visual_task_signal,
  CASE WHEN prescription_support_signal THEN 'Yes' ELSE 'No' END AS prescription_program_support_signal,
  program_need_score,
  prescription_program_score,
  commercial_fit_support_score,
  epa_support_score,
  CASE
    WHEN epa_support_score >= 72 OR (program_need_score >= 62 AND prescription_program_score >= 50) THEN 'Priority 1'
    WHEN epa_support_score >= 52 OR program_need_score >= 52 OR prescription_program_score >= 45 THEN 'Priority 2'
    ELSE 'Priority 3'
  END AS followup_priority,
  CASE
    WHEN epa_support_score >= 72 OR (program_need_score >= 62 AND prescription_program_score >= 50) THEN 'Review for immediate outreach'
    WHEN epa_support_score >= 52 OR program_need_score >= 52 OR prescription_program_score >= 45 THEN 'Research this week'
    ELSE 'Monitor this month'
  END AS suggested_action,
  load_ts AS source_load_timestamp
FROM ranked
WHERE
  program_need_score >= 35
  OR prescription_program_score >= 40
  OR commercial_fit_support_score >= 40
  OR COALESCE(fac_qtrs_in_nc, 0) > 0
  OR COALESCE(fec_number_of_cases, 0) > 0;

CREATE OR REPLACE TABLE `{{EPA_PROJECT_ID}}.{{EPA_DATASET}}.sales_followup_facility_current` AS
SELECT * FROM `{{EPA_PROJECT_ID}}.{{EPA_DATASET}}.v_sales_followup_facility_v1`;

CREATE OR REPLACE TABLE `{{EPA_PROJECT_ID}}.{{EPA_DATASET}}.sales_followup_facility_sandiego_current` AS
SELECT * FROM `{{EPA_PROJECT_ID}}.{{EPA_DATASET}}.v_sales_followup_facility_v1`
WHERE region_label = 'San Diego';

CREATE OR REPLACE TABLE `{{EPA_PROJECT_ID}}.{{EPA_DATASET}}.sales_followup_facility_bayarea_current` AS
SELECT * FROM `{{EPA_PROJECT_ID}}.{{EPA_DATASET}}.v_sales_followup_facility_v1`
WHERE region_label = 'Bay Area';
