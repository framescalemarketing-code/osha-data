CREATE TABLE IF NOT EXISTS `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.osha_downloads_company_signals_current` (
  company_key_norm STRING,
  zip5 STRING,
  ita_annual_average_employees INT64,
  ita_recordable_case_count INT64,
  ita_total_dafw_cases INT64,
  ita_total_djtr_cases INT64,
  ita_total_injuries INT64,
  ita_skin_disorders INT64,
  ita_respiratory_conditions INT64,
  ita_poisonings INT64,
  ita_case_detail_count INT64,
  ita_eye_face_case_count INT64,
  ita_prescription_case_count INT64,
  ita_uv_case_count INT64,
  ita_chemical_case_count INT64,
  ita_dust_case_count INT64,
  ita_impact_case_count INT64,
  ita_fog_case_count INT64,
  ita_temp_case_count INT64,
  ita_visual_case_count INT64,
  severe_injury_count INT64,
  severe_eye_face_case_count INT64,
  severe_loss_of_eye_count FLOAT64,
  severe_amputation_count FLOAT64,
  severe_hospitalized_count FLOAT64,
  severe_prescription_signal_count INT64,
  severe_uv_case_count INT64,
  severe_chemical_case_count INT64,
  severe_dust_case_count INT64,
  severe_impact_case_count INT64,
  severe_fog_case_count INT64,
  severe_temp_case_count INT64,
  severe_visual_case_count INT64,
  health_sample_count INT64,
  health_uv_signal_count INT64,
  health_chemical_signal_count INT64,
  health_dust_signal_count INT64,
  health_impact_signal_count INT64
);

CREATE SCHEMA IF NOT EXISTS `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}`;
CREATE SCHEMA IF NOT EXISTS `{{EPA_PROJECT_ID}}.{{EPA_DATASET}}`;
CREATE SCHEMA IF NOT EXISTS `{{NIH_PROJECT_ID}}.{{NIH_DATASET}}`;

CREATE OR REPLACE TABLE `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.sales_followup_all_current` AS
SELECT * FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.sales_followup_sandiego_current`
UNION ALL
SELECT * FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.sales_followup_bayarea_current`;

BEGIN
  DECLARE has_fda_source BOOL DEFAULT EXISTS (
    SELECT 1
    FROM `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'sales_followup_facility_current'
  );

  IF has_fda_source THEN
    EXECUTE IMMEDIATE """
      CREATE OR REPLACE TABLE `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.sales_followup_all_current` AS
      SELECT
        `Region` AS region_label,
        `Account Name` AS account_name,
        `Owner Operator Name` AS alt_account_name,
        `Site Address` AS site_address,
        `Site City` AS site_city,
        `Site State` AS site_state,
        `Site ZIP` AS site_zip,
        SAFE_CAST(`Program Need Score` AS FLOAT64) AS program_need_score,
        SAFE_CAST(`Prescription Program Score` AS FLOAT64) AS prescription_program_score,
        SAFE_CAST(`Commercial Fit Support Score` AS FLOAT64) AS commercial_fit_support_score,
        SAFE_CAST(`FDA Support Score` AS FLOAT64) AS source_support_score,
        `Follow-up Priority` AS followup_priority,
        `Suggested Action` AS suggested_action,
        ARRAY_TO_STRING(
          ARRAY(
            SELECT signal
            FROM UNNEST([
              IF(`UV Bright Light Signal` = 'Yes', 'UV / Bright Light', NULL),
              IF(`Splash Chemical Signal` = 'Yes', 'Splash / Chemical', NULL),
              IF(`Dust Debris Signal` = 'Yes', 'Dust / Debris', NULL),
              IF(`High Impact Signal` = 'Yes', 'High Impact', NULL),
              IF(`Fog Humidity Signal` = 'Yes', 'Fog / Humidity', NULL),
              IF(`Extreme Temperature Signal` = 'Yes', 'Extreme Temperature', NULL),
              IF(`Computer Visual Task Signal` = 'Yes', 'Computer / Visual Task', NULL),
              IF(`Prescription Program Support Signal` = 'Yes', 'Prescription Program Support', NULL)
            ]) signal
            WHERE signal IS NOT NULL
          ),
          ' | '
        ) AS signal_summary
      FROM `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.sales_followup_facility_current`
    """;
  ELSE
    EXECUTE IMMEDIATE """
      CREATE OR REPLACE TABLE `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.sales_followup_all_current` AS
      SELECT
        CAST(NULL AS STRING) AS region_label,
        CAST(NULL AS STRING) AS account_name,
        CAST(NULL AS STRING) AS alt_account_name,
        CAST(NULL AS STRING) AS site_address,
        CAST(NULL AS STRING) AS site_city,
        CAST(NULL AS STRING) AS site_state,
        CAST(NULL AS STRING) AS site_zip,
        CAST(NULL AS FLOAT64) AS program_need_score,
        CAST(NULL AS FLOAT64) AS prescription_program_score,
        CAST(NULL AS FLOAT64) AS commercial_fit_support_score,
        CAST(NULL AS FLOAT64) AS source_support_score,
        CAST(NULL AS STRING) AS followup_priority,
        CAST(NULL AS STRING) AS suggested_action,
        CAST(NULL AS STRING) AS signal_summary
      LIMIT 0
    """;
  END IF;
END;

BEGIN
  DECLARE has_epa_source BOOL DEFAULT EXISTS (
    SELECT 1
    FROM `{{EPA_PROJECT_ID}}.{{EPA_DATASET}}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'sales_followup_facility_current'
  );

  IF has_epa_source THEN
    EXECUTE IMMEDIATE """
      CREATE OR REPLACE TABLE `{{EPA_PROJECT_ID}}.{{EPA_DATASET}}.sales_followup_all_current` AS
      SELECT
        region_label,
        account_name,
        CAST(NULL AS STRING) AS alt_account_name,
        site_address,
        site_city,
        site_state,
        site_zip,
        SAFE_CAST(program_need_score AS FLOAT64) AS program_need_score,
        SAFE_CAST(prescription_program_score AS FLOAT64) AS prescription_program_score,
        SAFE_CAST(commercial_fit_support_score AS FLOAT64) AS commercial_fit_support_score,
        SAFE_CAST(epa_support_score AS FLOAT64) AS source_support_score,
        followup_priority,
        suggested_action,
        ARRAY_TO_STRING(
          ARRAY(
            SELECT signal
            FROM UNNEST([
              IF(uv_bright_light_signal = 'Yes', 'UV / Bright Light', NULL),
              IF(splash_chemical_signal = 'Yes', 'Splash / Chemical', NULL),
              IF(dust_debris_signal = 'Yes', 'Dust / Debris', NULL),
              IF(high_impact_signal = 'Yes', 'High Impact', NULL),
              IF(fog_humidity_signal = 'Yes', 'Fog / Humidity', NULL),
              IF(extreme_temperature_signal = 'Yes', 'Extreme Temperature', NULL),
              IF(computer_visual_task_signal = 'Yes', 'Computer / Visual Task', NULL),
              IF(prescription_program_support_signal = 'Yes', 'Prescription Program Support', NULL)
            ]) signal
            WHERE signal IS NOT NULL
          ),
          ' | '
        ) AS signal_summary
      FROM `{{EPA_PROJECT_ID}}.{{EPA_DATASET}}.sales_followup_facility_current`
    """;
  ELSE
    EXECUTE IMMEDIATE """
      CREATE OR REPLACE TABLE `{{EPA_PROJECT_ID}}.{{EPA_DATASET}}.sales_followup_all_current` AS
      SELECT
        CAST(NULL AS STRING) AS region_label,
        CAST(NULL AS STRING) AS account_name,
        CAST(NULL AS STRING) AS alt_account_name,
        CAST(NULL AS STRING) AS site_address,
        CAST(NULL AS STRING) AS site_city,
        CAST(NULL AS STRING) AS site_state,
        CAST(NULL AS STRING) AS site_zip,
        CAST(NULL AS FLOAT64) AS program_need_score,
        CAST(NULL AS FLOAT64) AS prescription_program_score,
        CAST(NULL AS FLOAT64) AS commercial_fit_support_score,
        CAST(NULL AS FLOAT64) AS source_support_score,
        CAST(NULL AS STRING) AS followup_priority,
        CAST(NULL AS STRING) AS suggested_action,
        CAST(NULL AS STRING) AS signal_summary
      LIMIT 0
    """;
  END IF;
END;

BEGIN
  DECLARE has_nih_source BOOL DEFAULT EXISTS (
    SELECT 1
    FROM `{{NIH_PROJECT_ID}}.{{NIH_DATASET}}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'sales_followup_org_current'
  );

  IF has_nih_source THEN
    EXECUTE IMMEDIATE """
      CREATE OR REPLACE TABLE `{{NIH_PROJECT_ID}}.{{NIH_DATASET}}.sales_followup_all_current` AS
      SELECT
        region_label,
        account_name,
        CAST(NULL AS STRING) AS alt_account_name,
        CAST(NULL AS STRING) AS site_address,
        site_city,
        site_state,
        site_zip,
        SAFE_CAST(program_need_score AS FLOAT64) AS program_need_score,
        SAFE_CAST(prescription_program_score AS FLOAT64) AS prescription_program_score,
        SAFE_CAST(commercial_fit_support_score AS FLOAT64) AS commercial_fit_support_score,
        SAFE_CAST(nih_support_score AS FLOAT64) AS source_support_score,
        followup_priority,
        suggested_action,
        ARRAY_TO_STRING(
          ARRAY(
            SELECT signal
            FROM UNNEST([
              IF(uv_bright_light_signal = 'Yes', 'UV / Bright Light', NULL),
              IF(splash_chemical_signal = 'Yes', 'Splash / Chemical', NULL),
              IF(dust_debris_signal = 'Yes', 'Dust / Debris', NULL),
              IF(high_impact_signal = 'Yes', 'High Impact', NULL),
              IF(fog_humidity_signal = 'Yes', 'Fog / Humidity', NULL),
              IF(extreme_temperature_signal = 'Yes', 'Extreme Temperature', NULL),
              IF(computer_visual_task_signal = 'Yes', 'Computer / Visual Task', NULL),
              IF(prescription_program_support_signal = 'Yes', 'Prescription Program Support', NULL)
            ]) signal
            WHERE signal IS NOT NULL
          ),
          ' | '
        ) AS signal_summary
      FROM `{{NIH_PROJECT_ID}}.{{NIH_DATASET}}.sales_followup_org_current`
    """;
  ELSE
    EXECUTE IMMEDIATE """
      CREATE OR REPLACE TABLE `{{NIH_PROJECT_ID}}.{{NIH_DATASET}}.sales_followup_all_current` AS
      SELECT
        CAST(NULL AS STRING) AS region_label,
        CAST(NULL AS STRING) AS account_name,
        CAST(NULL AS STRING) AS alt_account_name,
        CAST(NULL AS STRING) AS site_address,
        CAST(NULL AS STRING) AS site_city,
        CAST(NULL AS STRING) AS site_state,
        CAST(NULL AS STRING) AS site_zip,
        CAST(NULL AS FLOAT64) AS program_need_score,
        CAST(NULL AS FLOAT64) AS prescription_program_score,
        CAST(NULL AS FLOAT64) AS commercial_fit_support_score,
        CAST(NULL AS FLOAT64) AS source_support_score,
        CAST(NULL AS STRING) AS followup_priority,
        CAST(NULL AS STRING) AS suggested_action,
        CAST(NULL AS STRING) AS signal_summary
      LIMIT 0
    """;
  END IF;
END;

CREATE OR REPLACE TABLE `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.sales_followup_cross_source_current` AS
WITH osha_base AS (
  SELECT
    o.*,
    UPPER(REGEXP_REPLACE(COALESCE(`Account Name`, ''), r'[^A-Z0-9]', '')) AS osha_company_key_norm,
    UPPER(REGEXP_REPLACE(COALESCE(`Site City`, ''), r'[^A-Z0-9]', '')) AS osha_city_key_norm,
    REGEXP_EXTRACT(CAST(`Site ZIP` AS STRING), r'^(\d{5})') AS osha_zip5
  FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.sales_followup_all_current` o
),
fda_base AS (
  SELECT
    account_name,
    alt_account_name,
    site_address,
    site_city,
    site_state,
    REGEXP_EXTRACT(CAST(site_zip AS STRING), r'^(\d{5})') AS site_zip5,
    UPPER(REGEXP_REPLACE(COALESCE(account_name, ''), r'[^A-Z0-9]', '')) AS account_key_norm,
    UPPER(REGEXP_REPLACE(COALESCE(alt_account_name, ''), r'[^A-Z0-9]', '')) AS alt_account_key_norm,
    UPPER(REGEXP_REPLACE(COALESCE(site_city, ''), r'[^A-Z0-9]', '')) AS city_key_norm,
    program_need_score,
    prescription_program_score,
    commercial_fit_support_score,
    source_support_score,
    followup_priority,
    suggested_action,
    signal_summary
  FROM `{{FDA_PROJECT_ID}}.{{FDA_DATASET}}.sales_followup_all_current`
),
epa_base AS (
  SELECT
    account_name,
    alt_account_name,
    site_address,
    site_city,
    site_state,
    REGEXP_EXTRACT(CAST(site_zip AS STRING), r'^(\d{5})') AS site_zip5,
    UPPER(REGEXP_REPLACE(COALESCE(account_name, ''), r'[^A-Z0-9]', '')) AS account_key_norm,
    UPPER(REGEXP_REPLACE(COALESCE(site_city, ''), r'[^A-Z0-9]', '')) AS city_key_norm,
    program_need_score,
    prescription_program_score,
    commercial_fit_support_score,
    source_support_score,
    followup_priority,
    suggested_action,
    signal_summary
  FROM `{{EPA_PROJECT_ID}}.{{EPA_DATASET}}.sales_followup_all_current`
),
nih_base AS (
  SELECT
    account_name,
    alt_account_name,
    site_address,
    site_city,
    site_state,
    REGEXP_EXTRACT(CAST(site_zip AS STRING), r'^(\d{5})') AS site_zip5,
    UPPER(REGEXP_REPLACE(COALESCE(account_name, ''), r'[^A-Z0-9]', '')) AS account_key_norm,
    UPPER(REGEXP_REPLACE(COALESCE(site_city, ''), r'[^A-Z0-9]', '')) AS city_key_norm,
    program_need_score,
    prescription_program_score,
    commercial_fit_support_score,
    source_support_score,
    followup_priority,
    suggested_action,
    signal_summary
  FROM `{{NIH_PROJECT_ID}}.{{NIH_DATASET}}.sales_followup_all_current`
),
candidate_matches AS (
  SELECT
    o.`Latest Inspection ID` AS latest_inspection_id,
    o.`Region`,
    o.`Account Name`,
    o.`Site Address`,
    o.`Site City`,
    o.`Site State`,
    o.`Site ZIP`,
    'FDA' AS matched_source,
    'facility_name_zip_exact' AS match_rule,
    100 AS match_confidence,
    f.account_name AS source_account_name,
    f.alt_account_name AS source_alt_name,
    f.program_need_score AS source_program_need_score,
    f.prescription_program_score AS source_prescription_program_score,
    f.commercial_fit_support_score AS source_commercial_fit_support_score,
    f.source_support_score AS source_support_score,
    f.followup_priority AS source_followup_priority,
    f.suggested_action AS source_suggested_action,
    f.signal_summary AS source_signal_summary
  FROM osha_base o
  JOIN fda_base f
    ON o.osha_company_key_norm = f.account_key_norm
   AND o.osha_zip5 = f.site_zip5

  UNION ALL

  SELECT
    o.`Latest Inspection ID`,
    o.`Region`,
    o.`Account Name`,
    o.`Site Address`,
    o.`Site City`,
    o.`Site State`,
    o.`Site ZIP`,
    'FDA',
    'owner_operator_zip_exact',
    94,
    f.account_name,
    f.alt_account_name,
    f.program_need_score,
    f.prescription_program_score,
    f.commercial_fit_support_score,
    f.source_support_score,
    f.followup_priority,
    f.suggested_action,
    f.signal_summary
  FROM osha_base o
  JOIN fda_base f
    ON o.osha_company_key_norm = f.alt_account_key_norm
   AND o.osha_zip5 = f.site_zip5

  UNION ALL

  SELECT
    o.`Latest Inspection ID`,
    o.`Region`,
    o.`Account Name`,
    o.`Site Address`,
    o.`Site City`,
    o.`Site State`,
    o.`Site ZIP`,
    'EPA',
    'facility_name_zip_exact',
    96,
    e.account_name,
    e.alt_account_name,
    e.program_need_score,
    e.prescription_program_score,
    e.commercial_fit_support_score,
    e.source_support_score,
    e.followup_priority,
    e.suggested_action,
    e.signal_summary
  FROM osha_base o
  JOIN epa_base e
    ON o.osha_company_key_norm = e.account_key_norm
   AND o.osha_zip5 = e.site_zip5

  UNION ALL

  SELECT
    o.`Latest Inspection ID`,
    o.`Region`,
    o.`Account Name`,
    o.`Site Address`,
    o.`Site City`,
    o.`Site State`,
    o.`Site ZIP`,
    'EPA',
    'facility_name_city_exact',
    88,
    e.account_name,
    e.alt_account_name,
    e.program_need_score,
    e.prescription_program_score,
    e.commercial_fit_support_score,
    e.source_support_score,
    e.followup_priority,
    e.suggested_action,
    e.signal_summary
  FROM osha_base o
  JOIN epa_base e
    ON o.osha_company_key_norm = e.account_key_norm
   AND o.osha_city_key_norm = e.city_key_norm
  WHERE o.osha_zip5 IS NULL OR e.site_zip5 IS NULL

  UNION ALL

  SELECT
    o.`Latest Inspection ID`,
    o.`Region`,
    o.`Account Name`,
    o.`Site Address`,
    o.`Site City`,
    o.`Site State`,
    o.`Site ZIP`,
    'NIH',
    'org_name_zip_exact',
    95,
    n.account_name,
    n.alt_account_name,
    n.program_need_score,
    n.prescription_program_score,
    n.commercial_fit_support_score,
    n.source_support_score,
    n.followup_priority,
    n.suggested_action,
    n.signal_summary
  FROM osha_base o
  JOIN nih_base n
    ON o.osha_company_key_norm = n.account_key_norm
   AND o.osha_zip5 = n.site_zip5

  UNION ALL

  SELECT
    o.`Latest Inspection ID`,
    o.`Region`,
    o.`Account Name`,
    o.`Site Address`,
    o.`Site City`,
    o.`Site State`,
    o.`Site ZIP`,
    'NIH',
    'org_name_city_exact',
    86,
    n.account_name,
    n.alt_account_name,
    n.program_need_score,
    n.prescription_program_score,
    n.commercial_fit_support_score,
    n.source_support_score,
    n.followup_priority,
    n.suggested_action,
    n.signal_summary
  FROM osha_base o
  JOIN nih_base n
    ON o.osha_company_key_norm = n.account_key_norm
   AND o.osha_city_key_norm = n.city_key_norm
  WHERE o.osha_zip5 IS NULL OR n.site_zip5 IS NULL
),
best_match_per_inspection_source AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      c.*,
      ROW_NUMBER() OVER (
        PARTITION BY latest_inspection_id, matched_source
        ORDER BY match_confidence DESC, COALESCE(source_support_score, 0) DESC, source_account_name
      ) AS rn
    FROM candidate_matches c
  )
  WHERE rn = 1
)
SELECT
  latest_inspection_id AS `Latest Inspection ID`,
  `Region`,
  `Account Name`,
  `Site Address`,
  `Site City`,
  `Site State`,
  `Site ZIP`,
  matched_source AS `Matched Source`,
  match_rule AS `Match Rule`,
  match_confidence AS `Match Confidence`,
  source_account_name AS `Source Account Name`,
  source_alt_name AS `Source Alt Name`,
  source_program_need_score AS `Source Program Need Score`,
  source_prescription_program_score AS `Source Prescription Program Score`,
  source_commercial_fit_support_score AS `Source Commercial Fit Support Score`,
  source_support_score AS `Source Support Score`,
  source_followup_priority AS `Source Follow-up Priority`,
  source_suggested_action AS `Source Suggested Action`,
  source_signal_summary AS `Source Signal Summary`
FROM best_match_per_inspection_source;

CREATE OR REPLACE TABLE `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.sales_call_now_current` AS
WITH osha_base AS (
  SELECT
    o.*,
    UPPER(REGEXP_REPLACE(COALESCE(`Account Name`, ''), r'[^A-Z0-9]', '')) AS osha_company_key_norm,
    REGEXP_EXTRACT(CAST(`Site ZIP` AS STRING), r'^(\d{5})') AS osha_zip5
  FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.sales_followup_all_current` o
),
source_rollup AS (
  SELECT
    `Latest Inspection ID`,
    STRING_AGG(`Matched Source`, ' | ' ORDER BY `Matched Source`) AS matched_sources,
    COUNT(*) AS matched_source_count,
    MAX(`Match Confidence`) AS max_match_confidence,
    MAX(IF(`Matched Source` = 'FDA', `Source Account Name`, NULL)) AS fda_account_name,
    MAX(IF(`Matched Source` = 'FDA', `Source Alt Name`, NULL)) AS fda_alt_name,
    MAX(IF(`Matched Source` = 'FDA', SAFE_CAST(`Source Program Need Score` AS FLOAT64), NULL)) AS fda_program_need_score,
    MAX(IF(`Matched Source` = 'FDA', SAFE_CAST(`Source Prescription Program Score` AS FLOAT64), NULL)) AS fda_prescription_program_score,
    MAX(IF(`Matched Source` = 'FDA', SAFE_CAST(`Source Commercial Fit Support Score` AS FLOAT64), NULL)) AS fda_commercial_fit_support_score,
    MAX(IF(`Matched Source` = 'FDA', SAFE_CAST(`Source Support Score` AS FLOAT64), NULL)) AS fda_support_score,
    MAX(IF(`Matched Source` = 'FDA', `Source Follow-up Priority`, NULL)) AS fda_followup_priority,
    MAX(IF(`Matched Source` = 'FDA', `Source Suggested Action`, NULL)) AS fda_suggested_action,
    MAX(IF(`Matched Source` = 'FDA', `Source Signal Summary`, NULL)) AS fda_signal_summary,
    MAX(IF(`Matched Source` = 'EPA', `Source Account Name`, NULL)) AS epa_account_name,
    MAX(IF(`Matched Source` = 'EPA', SAFE_CAST(`Source Program Need Score` AS FLOAT64), NULL)) AS epa_program_need_score,
    MAX(IF(`Matched Source` = 'EPA', SAFE_CAST(`Source Prescription Program Score` AS FLOAT64), NULL)) AS epa_prescription_program_score,
    MAX(IF(`Matched Source` = 'EPA', SAFE_CAST(`Source Commercial Fit Support Score` AS FLOAT64), NULL)) AS epa_commercial_fit_support_score,
    MAX(IF(`Matched Source` = 'EPA', SAFE_CAST(`Source Support Score` AS FLOAT64), NULL)) AS epa_support_score,
    MAX(IF(`Matched Source` = 'EPA', `Source Follow-up Priority`, NULL)) AS epa_followup_priority,
    MAX(IF(`Matched Source` = 'EPA', `Source Suggested Action`, NULL)) AS epa_suggested_action,
    MAX(IF(`Matched Source` = 'EPA', `Source Signal Summary`, NULL)) AS epa_signal_summary,
    MAX(IF(`Matched Source` = 'NIH', `Source Account Name`, NULL)) AS nih_account_name,
    MAX(IF(`Matched Source` = 'NIH', SAFE_CAST(`Source Program Need Score` AS FLOAT64), NULL)) AS nih_program_need_score,
    MAX(IF(`Matched Source` = 'NIH', SAFE_CAST(`Source Prescription Program Score` AS FLOAT64), NULL)) AS nih_prescription_program_score,
    MAX(IF(`Matched Source` = 'NIH', SAFE_CAST(`Source Commercial Fit Support Score` AS FLOAT64), NULL)) AS nih_commercial_fit_support_score,
    MAX(IF(`Matched Source` = 'NIH', SAFE_CAST(`Source Support Score` AS FLOAT64), NULL)) AS nih_support_score,
    MAX(IF(`Matched Source` = 'NIH', `Source Follow-up Priority`, NULL)) AS nih_followup_priority,
    MAX(IF(`Matched Source` = 'NIH', `Source Suggested Action`, NULL)) AS nih_suggested_action,
    MAX(IF(`Matched Source` = 'NIH', `Source Signal Summary`, NULL)) AS nih_signal_summary
  FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.sales_followup_cross_source_current`
  GROUP BY 1
),
local_osha_exact AS (
  SELECT *
  FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.osha_downloads_company_signals_current`
  WHERE company_key_norm IS NOT NULL
    AND company_key_norm != ''
    AND zip5 IS NOT NULL
    AND zip5 != ''
),
local_osha_company AS (
  SELECT
    company_key_norm,
    MAX(ita_annual_average_employees) AS ita_annual_average_employees,
    MAX(ita_recordable_case_count) AS ita_recordable_case_count,
    MAX(ita_total_dafw_cases) AS ita_total_dafw_cases,
    MAX(ita_total_djtr_cases) AS ita_total_djtr_cases,
    MAX(ita_total_injuries) AS ita_total_injuries,
    MAX(ita_skin_disorders) AS ita_skin_disorders,
    MAX(ita_respiratory_conditions) AS ita_respiratory_conditions,
    MAX(ita_poisonings) AS ita_poisonings,
    SUM(ita_case_detail_count) AS ita_case_detail_count,
    SUM(ita_eye_face_case_count) AS ita_eye_face_case_count,
    SUM(ita_prescription_case_count) AS ita_prescription_case_count,
    SUM(ita_uv_case_count) AS ita_uv_case_count,
    SUM(ita_chemical_case_count) AS ita_chemical_case_count,
    SUM(ita_dust_case_count) AS ita_dust_case_count,
    SUM(ita_impact_case_count) AS ita_impact_case_count,
    SUM(ita_fog_case_count) AS ita_fog_case_count,
    SUM(ita_temp_case_count) AS ita_temp_case_count,
    SUM(ita_visual_case_count) AS ita_visual_case_count,
    SUM(severe_injury_count) AS severe_injury_count,
    SUM(severe_eye_face_case_count) AS severe_eye_face_case_count,
    SUM(severe_loss_of_eye_count) AS severe_loss_of_eye_count,
    SUM(severe_amputation_count) AS severe_amputation_count,
    SUM(severe_hospitalized_count) AS severe_hospitalized_count,
    SUM(severe_prescription_signal_count) AS severe_prescription_signal_count,
    SUM(severe_uv_case_count) AS severe_uv_case_count,
    SUM(severe_chemical_case_count) AS severe_chemical_case_count,
    SUM(severe_dust_case_count) AS severe_dust_case_count,
    SUM(severe_impact_case_count) AS severe_impact_case_count,
    SUM(severe_fog_case_count) AS severe_fog_case_count,
    SUM(severe_temp_case_count) AS severe_temp_case_count,
    SUM(severe_visual_case_count) AS severe_visual_case_count,
    SUM(health_sample_count) AS health_sample_count,
    SUM(health_uv_signal_count) AS health_uv_signal_count,
    SUM(health_chemical_signal_count) AS health_chemical_signal_count,
    SUM(health_dust_signal_count) AS health_dust_signal_count,
    SUM(health_impact_signal_count) AS health_impact_signal_count
  FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.osha_downloads_company_signals_current`
  WHERE company_key_norm IS NOT NULL
    AND company_key_norm != ''
  GROUP BY 1
),
combined AS (
  SELECT
    o.*,
    COALESCE(s.matched_sources, '') AS matched_sources,
    COALESCE(s.matched_source_count, 0) AS matched_source_count,
    COALESCE(s.max_match_confidence, 0) AS max_match_confidence,
    s.fda_account_name,
    s.fda_alt_name,
    COALESCE(s.fda_program_need_score, 0) AS fda_program_need_score,
    COALESCE(s.fda_prescription_program_score, 0) AS fda_prescription_program_score,
    COALESCE(s.fda_commercial_fit_support_score, 0) AS fda_commercial_fit_support_score,
    COALESCE(s.fda_support_score, 0) AS fda_support_score,
    s.fda_followup_priority,
    s.fda_suggested_action,
    COALESCE(s.fda_signal_summary, '') AS fda_signal_summary,
    s.epa_account_name,
    COALESCE(s.epa_program_need_score, 0) AS epa_program_need_score,
    COALESCE(s.epa_prescription_program_score, 0) AS epa_prescription_program_score,
    COALESCE(s.epa_commercial_fit_support_score, 0) AS epa_commercial_fit_support_score,
    COALESCE(s.epa_support_score, 0) AS epa_support_score,
    s.epa_followup_priority,
    s.epa_suggested_action,
    COALESCE(s.epa_signal_summary, '') AS epa_signal_summary,
    s.nih_account_name,
    COALESCE(s.nih_program_need_score, 0) AS nih_program_need_score,
    COALESCE(s.nih_prescription_program_score, 0) AS nih_prescription_program_score,
    COALESCE(s.nih_commercial_fit_support_score, 0) AS nih_commercial_fit_support_score,
    COALESCE(s.nih_support_score, 0) AS nih_support_score,
    s.nih_followup_priority,
    s.nih_suggested_action,
    COALESCE(s.nih_signal_summary, '') AS nih_signal_summary,
    SAFE_CAST(o.`Employee Count Estimate` AS FLOAT64) AS employee_count_estimate_num,
    SAFE_CAST(o.`Company Sites 5Y` AS FLOAT64) AS company_sites_5y_num,
    SAFE_CAST(o.`Follow-up Score` AS FLOAT64) AS osha_followup_score_num,
    SAFE_CAST(o.`Direct Prescription Citation Count` AS FLOAT64) AS direct_rx_count_num,
    SAFE_CAST(o.`Prescription Signal Count` AS FLOAT64) AS rx_signal_count_num,
    COALESCE(lz.ita_annual_average_employees, lc.ita_annual_average_employees, 0) AS ita_annual_average_employees_num,
    COALESCE(lz.ita_recordable_case_count, lc.ita_recordable_case_count, 0) AS ita_recordable_case_count_num,
    COALESCE(lz.ita_total_dafw_cases, lc.ita_total_dafw_cases, 0) AS ita_total_dafw_cases_num,
    COALESCE(lz.ita_total_djtr_cases, lc.ita_total_djtr_cases, 0) AS ita_total_djtr_cases_num,
    COALESCE(lz.ita_total_injuries, lc.ita_total_injuries, 0) AS ita_total_injuries_num,
    COALESCE(lz.ita_case_detail_count, lc.ita_case_detail_count, 0) AS ita_case_detail_count_num,
    COALESCE(lz.ita_eye_face_case_count, lc.ita_eye_face_case_count, 0) AS ita_eye_face_case_count_num,
    COALESCE(lz.ita_prescription_case_count, lc.ita_prescription_case_count, 0) AS ita_prescription_case_count_num,
    COALESCE(lz.ita_uv_case_count, lc.ita_uv_case_count, 0) AS ita_uv_case_count_num,
    COALESCE(lz.ita_chemical_case_count, lc.ita_chemical_case_count, 0) AS ita_chemical_case_count_num,
    COALESCE(lz.ita_dust_case_count, lc.ita_dust_case_count, 0) AS ita_dust_case_count_num,
    COALESCE(lz.ita_impact_case_count, lc.ita_impact_case_count, 0) AS ita_impact_case_count_num,
    COALESCE(lz.ita_fog_case_count, lc.ita_fog_case_count, 0) AS ita_fog_case_count_num,
    COALESCE(lz.ita_temp_case_count, lc.ita_temp_case_count, 0) AS ita_temp_case_count_num,
    COALESCE(lz.ita_visual_case_count, lc.ita_visual_case_count, 0) AS ita_visual_case_count_num,
    COALESCE(lz.severe_injury_count, lc.severe_injury_count, 0) AS severe_injury_count_num,
    COALESCE(lz.severe_eye_face_case_count, lc.severe_eye_face_case_count, 0) AS severe_eye_face_case_count_num,
    COALESCE(lz.severe_loss_of_eye_count, lc.severe_loss_of_eye_count, 0) AS severe_loss_of_eye_count_num,
    COALESCE(lz.severe_amputation_count, lc.severe_amputation_count, 0) AS severe_amputation_count_num,
    COALESCE(lz.severe_hospitalized_count, lc.severe_hospitalized_count, 0) AS severe_hospitalized_count_num,
    COALESCE(lz.severe_prescription_signal_count, lc.severe_prescription_signal_count, 0) AS severe_prescription_signal_count_num,
    COALESCE(lz.severe_uv_case_count, lc.severe_uv_case_count, 0) AS severe_uv_case_count_num,
    COALESCE(lz.severe_chemical_case_count, lc.severe_chemical_case_count, 0) AS severe_chemical_case_count_num,
    COALESCE(lz.severe_dust_case_count, lc.severe_dust_case_count, 0) AS severe_dust_case_count_num,
    COALESCE(lz.severe_impact_case_count, lc.severe_impact_case_count, 0) AS severe_impact_case_count_num,
    COALESCE(lz.severe_fog_case_count, lc.severe_fog_case_count, 0) AS severe_fog_case_count_num,
    COALESCE(lz.severe_temp_case_count, lc.severe_temp_case_count, 0) AS severe_temp_case_count_num,
    COALESCE(lz.severe_visual_case_count, lc.severe_visual_case_count, 0) AS severe_visual_case_count_num,
    COALESCE(lz.health_sample_count, lc.health_sample_count, 0) AS health_sample_count_num,
    COALESCE(lz.health_uv_signal_count, lc.health_uv_signal_count, 0) AS health_uv_signal_count_num,
    COALESCE(lz.health_chemical_signal_count, lc.health_chemical_signal_count, 0) AS health_chemical_signal_count_num,
    COALESCE(lz.health_dust_signal_count, lc.health_dust_signal_count, 0) AS health_dust_signal_count_num,
    COALESCE(lz.health_impact_signal_count, lc.health_impact_signal_count, 0) AS health_impact_signal_count_num,
    CASE
      WHEN lz.company_key_norm IS NOT NULL THEN 'company_zip_exact'
      WHEN lc.company_key_norm IS NOT NULL THEN 'company_name_only'
      ELSE 'none'
    END AS osha_download_match_rule
  FROM osha_base o
  LEFT JOIN source_rollup s
    ON o.`Latest Inspection ID` = s.`Latest Inspection ID`
  LEFT JOIN local_osha_exact lz
    ON o.osha_company_key_norm = lz.company_key_norm
   AND o.osha_zip5 = lz.zip5
  LEFT JOIN local_osha_company lc
    ON o.osha_company_key_norm = lc.company_key_norm
),
heuristics AS (
  SELECT
    c.*,
    GREATEST(COALESCE(employee_count_estimate_num, 0), COALESCE(ita_annual_average_employees_num, 0)) AS employee_size_proxy_num,
    CAST(LEAST(30,
      CASE WHEN COALESCE(severe_loss_of_eye_count_num, 0) > 0 THEN 14 ELSE 0 END
      + CASE WHEN COALESCE(severe_eye_face_case_count_num, 0) > 0 THEN 6 ELSE 0 END
      + CASE WHEN COALESCE(ita_eye_face_case_count_num, 0) > 0 THEN 5 ELSE 0 END
      + LEAST(COALESCE(ita_chemical_case_count_num, 0) + COALESCE(severe_chemical_case_count_num, 0) + COALESCE(health_chemical_signal_count_num, 0), 4) * 2
      + LEAST(COALESCE(ita_dust_case_count_num, 0) + COALESCE(severe_dust_case_count_num, 0) + COALESCE(health_dust_signal_count_num, 0), 3) * 2
      + LEAST(COALESCE(ita_impact_case_count_num, 0) + COALESCE(severe_impact_case_count_num, 0) + COALESCE(health_impact_signal_count_num, 0), 3) * 2
      + CASE WHEN COALESCE(ita_uv_case_count_num, 0) + COALESCE(severe_uv_case_count_num, 0) + COALESCE(health_uv_signal_count_num, 0) > 0 THEN 4 ELSE 0 END
      + CASE WHEN COALESCE(ita_fog_case_count_num, 0) + COALESCE(severe_fog_case_count_num, 0) > 0 THEN 2 ELSE 0 END
      + CASE WHEN COALESCE(ita_temp_case_count_num, 0) + COALESCE(severe_temp_case_count_num, 0) > 0 THEN 2 ELSE 0 END
      + CASE WHEN COALESCE(ita_visual_case_count_num, 0) + COALESCE(severe_visual_case_count_num, 0) > 0 THEN 2 ELSE 0 END
    ) AS INT64) AS osha_download_program_support_score,
    CAST(LEAST(24,
      LEAST(COALESCE(ita_prescription_case_count_num, 0), 2) * 6
      + LEAST(COALESCE(severe_prescription_signal_count_num, 0), 2) * 5
      + CASE WHEN COALESCE(severe_loss_of_eye_count_num, 0) > 0 THEN 8 ELSE 0 END
      + CASE WHEN COALESCE(ita_visual_case_count_num, 0) + COALESCE(severe_visual_case_count_num, 0) > 0 THEN 2 ELSE 0 END
    ) AS INT64) AS osha_download_prescription_support_score,
    CAST(LEAST(32,
      CASE WHEN COALESCE(severe_injury_count_num, 0) > 0 THEN 8 ELSE 0 END
      + LEAST(CAST(ROUND(COALESCE(severe_hospitalized_count_num, 0)) AS INT64), 3) * 4
      + CASE WHEN COALESCE(severe_loss_of_eye_count_num, 0) > 0 THEN 10 ELSE 0 END
      + CASE WHEN COALESCE(severe_amputation_count_num, 0) > 0 THEN 6 ELSE 0 END
      + CASE WHEN COALESCE(ita_recordable_case_count_num, 0) >= 10 THEN 4 WHEN COALESCE(ita_recordable_case_count_num, 0) >= 5 THEN 2 ELSE 0 END
      + CASE WHEN COALESCE(ita_total_dafw_cases_num, 0) + COALESCE(ita_total_djtr_cases_num, 0) >= 5 THEN 4 WHEN COALESCE(ita_total_dafw_cases_num, 0) + COALESCE(ita_total_djtr_cases_num, 0) >= 2 THEN 2 ELSE 0 END
    ) AS INT64) AS osha_download_urgency_support_score,
    CAST(LEAST(18,
      CASE WHEN COALESCE(ita_annual_average_employees_num, 0) >= 50 THEN 8 WHEN COALESCE(ita_annual_average_employees_num, 0) > 0 THEN 4 ELSE 0 END
      + CASE WHEN COALESCE(health_sample_count_num, 0) > 0 THEN 4 ELSE 0 END
      + CASE WHEN COALESCE(ita_case_detail_count_num, 0) >= 3 THEN 3 WHEN COALESCE(ita_case_detail_count_num, 0) >= 1 THEN 1 ELSE 0 END
      + CASE WHEN COALESCE(severe_injury_count_num, 0) > 0 THEN 3 ELSE 0 END
    ) AS INT64) AS osha_download_fit_support_score
  FROM combined c
)
,
scored AS (
  SELECT
    h.*,
    CASE
      WHEN employee_size_proxy_num >= 500 THEN '500+'
      WHEN employee_size_proxy_num >= 250 THEN '250-499'
      WHEN employee_size_proxy_num >= 100 THEN '100-249'
      WHEN employee_size_proxy_num >= 50 THEN '50-99'
      WHEN employee_size_proxy_num >= 20 THEN '20-49'
      WHEN employee_size_proxy_num > 0 THEN '1-19'
      ELSE 'Unknown'
    END AS estimated_employee_band,
    CASE
      WHEN COALESCE(ita_annual_average_employees_num, 0) >= 50 THEN 'High'
      WHEN COALESCE(ita_annual_average_employees_num, 0) > 0 THEN 'Medium'
      WHEN employee_size_proxy_num >= 50 THEN 'Medium'
      WHEN employee_size_proxy_num > 0 THEN 'Low'
      WHEN `CA Employees Signal` != 'Unknown' AND company_sites_5y_num >= 2 THEN 'Low'
      ELSE 'Unknown'
    END AS employee_estimate_confidence,
    CAST(
      ROUND(
        LEAST(
          100,
          COALESCE(osha_followup_score_num, 0) * 0.50
          + osha_download_program_support_score * 0.85
          + fda_program_need_score * 0.18
          + epa_program_need_score * 0.14
          + nih_program_need_score * 0.10
        )
      ) AS INT64
    ) AS program_need_score,
    CAST(
      ROUND(
        LEAST(
          100,
          CASE WHEN `Program Relevance` = 'Prescription Safety' THEN 44 ELSE 14 END
          + LEAST(COALESCE(direct_rx_count_num, 0) * 18, 28)
          + LEAST(COALESCE(rx_signal_count_num, 0) * 8, 18)
          + osha_download_prescription_support_score
          + fda_prescription_program_score * 0.18
          + epa_prescription_program_score * 0.06
          + nih_prescription_program_score * 0.20
        )
      ) AS INT64
    ) AS prescription_program_score,
    CAST(
      ROUND(
        LEAST(
          100,
          CASE `Follow-up Priority`
            WHEN 'Priority 1' THEN 60
            WHEN 'Priority 2' THEN 42
            ELSE 20
          END
          + CASE WHEN `Has Open Violations` = 'Yes' THEN 12 ELSE 0 END
          + CASE WHEN `Severe Incident Signal` = 'Yes' THEN 12 ELSE 0 END
          + CASE WHEN `Has Complaint Signal` = 'Yes' THEN 6 ELSE 0 END
          + osha_download_urgency_support_score * 0.75
          + fda_support_score * 0.06
          + epa_support_score * 0.12
          + nih_support_score * 0.03
        )
      ) AS INT64
    ) AS urgency_score,
    CAST(
      ROUND(
        LEAST(
          100,
          CASE
            WHEN employee_size_proxy_num >= 500 THEN 34
            WHEN employee_size_proxy_num >= 250 THEN 28
            WHEN employee_size_proxy_num >= 100 THEN 22
            WHEN employee_size_proxy_num >= 50 THEN 18
            WHEN employee_size_proxy_num >= 20 THEN 8
            WHEN employee_size_proxy_num > 0 THEN 3
            ELSE 0
          END
          + CASE WHEN company_sites_5y_num >= 5 THEN 12 WHEN company_sites_5y_num >= 3 THEN 8 WHEN company_sites_5y_num >= 2 THEN 5 ELSE 0 END
          + CASE
              WHEN COALESCE(ita_annual_average_employees_num, 0) >= 50 THEN 12
              WHEN COALESCE(ita_annual_average_employees_num, 0) > 0 THEN 8
              WHEN employee_size_proxy_num >= 50 THEN 8
              WHEN employee_size_proxy_num > 0 THEN 4
              WHEN `CA Employees Signal` != 'Unknown' AND company_sites_5y_num >= 2 THEN 4
              ELSE 0
            END
          + CASE `CA Federal Spend Signal`
              WHEN 'Very High' THEN 12
              WHEN 'High' THEN 8
              WHEN 'Medium' THEN 5
              WHEN 'Low' THEN 2
              ELSE 0
            END
          + osha_download_fit_support_score
          + fda_commercial_fit_support_score * 0.14
          + epa_commercial_fit_support_score * 0.10
          + nih_commercial_fit_support_score * 0.18
          + LEAST(matched_source_count, 3) * 2
        )
      ) AS INT64
    ) AS commercial_fit_score
  FROM heuristics h
),
ranked AS (
  SELECT
    s.*,
    CASE
      WHEN employee_size_proxy_num >= 500 THEN '$100M+'
      WHEN employee_size_proxy_num >= 250 THEN '$50M-$100M'
      WHEN employee_size_proxy_num >= 100 THEN '$25M-$50M'
      WHEN employee_size_proxy_num >= 50 THEN '$10M-$25M'
      WHEN employee_size_proxy_num >= 20 THEN '$5M-$10M'
      ELSE 'Unknown'
    END AS estimated_revenue_band,
    CASE
      WHEN COALESCE(ita_annual_average_employees_num, 0) >= 50 THEN 'Medium'
      WHEN COALESCE(ita_annual_average_employees_num, 0) > 0 THEN 'Low'
      WHEN employee_size_proxy_num > 0 THEN 'Low'
      ELSE 'Unknown'
    END AS revenue_estimate_confidence,
    CAST(
      ROUND(
        LEAST(
          100,
          program_need_score * 0.33
          + prescription_program_score * 0.21
          + urgency_score * 0.16
          + commercial_fit_score * 0.30
        )
      ) AS INT64
    ) AS overall_sales_score
  FROM scored s
),
deduped AS (
  SELECT *
  FROM ranked
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      UPPER(REGEXP_REPLACE(COALESCE(`Account Name`, ''), r'[^A-Z0-9]', '')),
      REGEXP_EXTRACT(CAST(`Site ZIP` AS STRING), r'^(\d{5})'),
      UPPER(
        REGEXP_REPLACE(
          COALESCE(NULLIF(`Site Address`, ''), `Site City`, ''),
          r'[^A-Z0-9]',
          ''
        )
      )
    ORDER BY
      overall_sales_score DESC,
      urgency_score DESC,
      program_need_score DESC,
      commercial_fit_score DESC,
      `Latest Inspection ID` DESC
  ) = 1
)
SELECT
  `Region`,
  `Account Name`,
  `Site Address`,
  `Site City`,
  `Site State`,
  `Site ZIP`,
  `NAICS Code`,
  `Industry Segment`,
  `Ownership Type`,
  `Latest Inspection ID`,
  `Program Relevance`,
  `Follow-up Priority` AS `OSHA Follow-up Priority`,
  `Suggested Action` AS `OSHA Suggested Action`,
  `Follow-up Score` AS `OSHA Follow-up Score`,
  matched_sources AS `Matched Sources`,
  matched_source_count AS `Matched Source Count`,
  max_match_confidence AS `Match Confidence`,
  fda_account_name AS `FDA Account Name`,
  fda_alt_name AS `FDA Owner Operator Name`,
  epa_account_name AS `EPA Account Name`,
  nih_account_name AS `NIH Account Name`,
  estimated_employee_band AS `Estimated Employee Band`,
  CASE
    WHEN COALESCE(ita_annual_average_employees_num, 0) >= 50 THEN 'High'
    WHEN COALESCE(ita_annual_average_employees_num, 0) > 0 THEN 'Medium'
    WHEN employee_size_proxy_num >= 50 THEN 'Medium'
    WHEN employee_size_proxy_num > 0 THEN 'Low'
    WHEN `CA Employees Signal` != 'Unknown' AND company_sites_5y_num >= 2 THEN 'Low'
    ELSE 'Unknown'
  END AS `Employee Estimate Confidence`,
  estimated_revenue_band AS `Estimated Revenue Band`,
  revenue_estimate_confidence AS `Revenue Estimate Confidence`,
  commercial_fit_score AS `Commercial Fit Score`,
  CASE
    WHEN commercial_fit_score >= 60 AND estimated_employee_band IN ('100-249', '250-499', '500+') THEN 'Ideal ICP'
    WHEN commercial_fit_score >= 45 AND estimated_employee_band IN ('50-99', '100-249', '250-499', '500+') THEN 'Strong ICP'
    WHEN commercial_fit_score >= 30 AND estimated_employee_band NOT IN ('1-19', 'Unknown') THEN 'Possible ICP'
    ELSE 'Weak ICP'
  END AS `ICP Fit Band`,
  program_need_score AS `Program Need Score`,
  prescription_program_score AS `Prescription Program Score`,
  urgency_score AS `Urgency Score`,
  overall_sales_score AS `Overall Sales Score`,
  CASE
    WHEN overall_sales_score >= 58 AND commercial_fit_score >= 54 AND estimated_employee_band IN ('100-249', '250-499', '500+') AND program_need_score >= 56 AND (urgency_score >= 30 OR prescription_program_score >= 24 OR matched_source_count >= 2) THEN 'Ideal Call Now'
    WHEN overall_sales_score >= 46 AND commercial_fit_score >= 42 AND estimated_employee_band IN ('50-99', '100-249', '250-499', '500+') AND (program_need_score >= 48 OR prescription_program_score >= 24) AND (urgency_score >= 24 OR matched_source_count >= 1 OR company_sites_5y_num >= 2) THEN 'Call Now'
    WHEN overall_sales_score >= 38 AND commercial_fit_score >= 18 THEN 'Research Then Call'
    ELSE 'Monitor / Nurture'
  END AS `Should Look At Now`,
  CASE
    WHEN overall_sales_score >= 58 AND commercial_fit_score >= 54 AND estimated_employee_band IN ('100-249', '250-499', '500+') AND program_need_score >= 56 AND (urgency_score >= 30 OR prescription_program_score >= 24 OR matched_source_count >= 2) THEN 'P0 Ideal'
    WHEN overall_sales_score >= 46 AND commercial_fit_score >= 42 AND estimated_employee_band IN ('50-99', '100-249', '250-499', '500+') AND (program_need_score >= 48 OR prescription_program_score >= 24) AND (urgency_score >= 24 OR matched_source_count >= 1 OR company_sites_5y_num >= 2) THEN 'P1 Active'
    WHEN overall_sales_score >= 38 AND commercial_fit_score >= 18 THEN 'P2 Research'
    ELSE 'P3 Monitor'
  END AS `Overall Sales Priority`,
  ARRAY_TO_STRING(ARRAY(
    SELECT reason
    FROM UNNEST([
      IF(COALESCE(ita_eye_face_case_count_num, 0) + COALESCE(severe_eye_face_case_count_num, 0) > 0, 'Eye / face injury evidence', NULL),
      IF(COALESCE(ita_prescription_case_count_num, 0) + COALESCE(severe_prescription_signal_count_num, 0) > 0 OR `Program Relevance` = 'Prescription Safety', 'Prescription safety eyewear need indicated', NULL),
      IF(COALESCE(ita_chemical_case_count_num, 0) + COALESCE(severe_chemical_case_count_num, 0) + COALESCE(health_chemical_signal_count_num, 0) > 0, 'Chemical / splash exposure signals', NULL),
      IF(COALESCE(ita_dust_case_count_num, 0) + COALESCE(severe_dust_case_count_num, 0) + COALESCE(health_dust_signal_count_num, 0) > 0, 'Dust / debris exposure signals', NULL),
      IF(COALESCE(ita_impact_case_count_num, 0) + COALESCE(severe_impact_case_count_num, 0) + COALESCE(health_impact_signal_count_num, 0) > 0, 'High-impact work environment', NULL),
      IF(COALESCE(ita_uv_case_count_num, 0) + COALESCE(severe_uv_case_count_num, 0) + COALESCE(health_uv_signal_count_num, 0) > 0, 'UV / bright-light exposure signals', NULL),
      IF(estimated_employee_band IN ('50-99', '100-249', '250-499', '500+'), 'Employee count likely fits managed program', NULL),
      IF(company_sites_5y_num >= 2, 'Multi-site employer', NULL),
      IF(fda_support_score >= 55, 'FDA environment supports program fit', NULL),
      IF(epa_support_score >= 52, 'EPA compliance context supports PPE need', NULL),
      IF(nih_support_score >= 50, 'NIH research profile supports eyewear program fit', NULL)
    ]) reason
    WHERE reason IS NOT NULL
    LIMIT 3
  ), ' | ') AS `Reason To Contact`,
  ARRAY_TO_STRING(ARRAY(
    SELECT reason
    FROM UNNEST([
      IF(`Has Open Violations` = 'Yes', 'Open OSHA violations', NULL),
      IF(`Severe Incident Signal` = 'Yes', 'Severe incident signal', NULL),
      IF(COALESCE(severe_loss_of_eye_count_num, 0) > 0, 'Loss-of-eye severe injury history', NULL),
      IF(COALESCE(severe_injury_count_num, 0) > 0, 'Recent severe injury reporting', NULL),
      IF(COALESCE(ita_prescription_case_count_num, 0) + COALESCE(severe_prescription_signal_count_num, 0) > 0 OR `Program Relevance` = 'Prescription Safety', 'Prescription program urgency', NULL),
      IF(epa_support_score >= 60, 'EPA compliance pressure adds urgency', NULL),
      IF(nih_support_score >= 60, 'Active funded research environment adds urgency', NULL)
    ]) reason
    WHERE reason IS NOT NULL
    LIMIT 3
  ), ' | ') AS `Reason To Call Now`,
  ARRAY_TO_STRING(ARRAY(
    SELECT reason
    FROM UNNEST([
      IF(commercial_fit_score >= 60 AND estimated_employee_band IN ('100-249', '250-499', '500+'), 'Profile matches ideal managed-program account size', NULL),
      IF(estimated_employee_band IN ('50-99', '100-249', '250-499', '500+'), 'Likely enough employees for a managed eyewear program', NULL),
      IF(ita_annual_average_employees_num > 0, 'Official OSHA ITA filing provides employee-size support', NULL),
      IF(estimated_revenue_band IN ('$25M-$50M', '$50M-$100M', '$100M+'), 'Estimated budget profile fits higher-ticket program', NULL),
      IF(company_sites_5y_num >= 2, 'Multi-site footprint increases program-management value', NULL),
      IF(health_sample_count_num > 0, 'Health sampling activity suggests a more controlled PPE environment', NULL),
      IF(fda_support_score >= 55, 'FDA environment reinforces structured PPE / eyewear need', NULL),
      IF(epa_support_score >= 52, 'EPA facility compliance data reinforces operational complexity', NULL),
      IF(nih_support_score >= 50, 'NIH-funded research profile reinforces program fit', NULL)
    ]) reason
    WHERE reason IS NOT NULL
  ), ' | ') AS `Why Fit`,
  ARRAY_TO_STRING(ARRAY(
    SELECT reason
    FROM UNNEST([
      IF(`Has Open Violations` = 'Yes', 'Open violations are still active', NULL),
      IF(`Severe Incident Signal` = 'Yes', 'Severe incident signal present', NULL),
      IF(COALESCE(severe_loss_of_eye_count_num, 0) > 0, 'Severe injury reporting includes loss-of-eye cases', NULL),
      IF(COALESCE(severe_injury_count_num, 0) > 0, 'Severe injury reports match this company', NULL),
      IF(COALESCE(ita_eye_face_case_count_num, 0) > 0, 'ITA case detail includes eye / face injury context', NULL),
      IF(`Program Relevance` = 'Prescription Safety', 'Prescription safety need is indicated', NULL),
      IF(fda_signal_summary != '', CONCAT('FDA signals: ', fda_signal_summary), NULL),
      IF(epa_signal_summary != '', CONCAT('EPA signals: ', epa_signal_summary), NULL),
      IF(nih_signal_summary != '', CONCAT('NIH signals: ', nih_signal_summary), NULL),
      NULLIF(`Citation Sales Explanation`, '')
    ]) reason
    WHERE reason IS NOT NULL
  ), ' | ') AS `Why Now`,
  osha_download_match_rule AS `OSHA Download Match Rule`,
  osha_download_program_support_score AS `OSHA Download Program Support Score`,
  osha_download_prescription_support_score AS `OSHA Download Prescription Support Score`,
  osha_download_urgency_support_score AS `OSHA Download Urgency Support Score`,
  osha_download_fit_support_score AS `OSHA Download Fit Support Score`,
  fda_followup_priority AS `FDA Follow-up Priority`,
  fda_suggested_action AS `FDA Suggested Action`,
  fda_signal_summary AS `FDA Signal Summary`,
  epa_followup_priority AS `EPA Follow-up Priority`,
  epa_suggested_action AS `EPA Suggested Action`,
  epa_signal_summary AS `EPA Signal Summary`,
  nih_followup_priority AS `NIH Follow-up Priority`,
  nih_suggested_action AS `NIH Suggested Action`,
  nih_signal_summary AS `NIH Signal Summary`,
  `Has Open Violations`,
  `Severe Incident Signal`,
  `Prescription Signal Count`,
  `Direct Prescription Citation Count`,
  `Eye Face Citation Count`,
  `General PPE Citation Count`,
  `Fit Selection Citation Count`,
  ita_eye_face_case_count_num AS `ITA Eye Face Case Count`,
  ita_prescription_case_count_num AS `ITA Prescription Case Count`,
  severe_eye_face_case_count_num AS `Severe Eye Face Case Count`,
  severe_loss_of_eye_count_num AS `Severe Loss Of Eye Count`,
  severe_injury_count_num AS `Severe Injury Count`,
  `Company Sites 5Y`,
  `Employee Count Estimate`,
  `CA Employees Signal`,
  `CA Federal Spend Signal`
FROM deduped
WHERE overall_sales_score >= 36;

CREATE OR REPLACE TABLE `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.eyewear_opportunity_current` AS
WITH base AS (
  SELECT
    s.*,
    CASE
      WHEN COALESCE(`Direct Prescription Citation Count`, 0) > 0
        OR COALESCE(`Prescription Signal Count`, 0) > 0
        OR COALESCE(`Eye Face Citation Count`, 0) > 0
        OR COALESCE(`Fit Selection Citation Count`, 0) > 0
        OR COALESCE(`ITA Eye Face Case Count`, 0) > 0
        OR COALESCE(`ITA Prescription Case Count`, 0) > 0
        OR COALESCE(`Severe Eye Face Case Count`, 0) > 0
        OR COALESCE(`Severe Loss Of Eye Count`, 0) > 0
      THEN 'Direct Need'
      WHEN COALESCE(`General PPE Citation Count`, 0) > 0
        OR COALESCE(`OSHA Download Program Support Score`, 0) >= 10
        OR COALESCE(`OSHA Download Prescription Support Score`, 0) >= 6
        OR (
          `Severe Incident Signal` = 'Yes'
          AND COALESCE(`OSHA Download Program Support Score`, 0) >= 8
        )
        OR (
          COALESCE(`Matched Source Count`, 0) > 0
          AND (
            COALESCE(`FDA Signal Summary`, '') != ''
            OR COALESCE(`EPA Signal Summary`, '') != ''
            OR COALESCE(`NIH Signal Summary`, '') != ''
          )
        )
      THEN 'Probable Need'
      ELSE 'Fit Only'
    END AS eyewear_need_tier,
    CAST(
      ROUND(
        LEAST(
          100,
          CASE WHEN COALESCE(`Direct Prescription Citation Count`, 0) > 0 THEN 34 ELSE 0 END
          + CASE WHEN COALESCE(`Prescription Signal Count`, 0) > 0 THEN 22 ELSE 0 END
          + CASE WHEN COALESCE(`Eye Face Citation Count`, 0) > 0 THEN 18 ELSE 0 END
          + CASE WHEN COALESCE(`Fit Selection Citation Count`, 0) > 0 THEN 16 ELSE 0 END
          + CASE WHEN COALESCE(`ITA Eye Face Case Count`, 0) > 0 THEN 18 ELSE 0 END
          + CASE WHEN COALESCE(`ITA Prescription Case Count`, 0) > 0 THEN 16 ELSE 0 END
          + CASE WHEN COALESCE(`Severe Eye Face Case Count`, 0) > 0 THEN 20 ELSE 0 END
          + CASE WHEN COALESCE(`Severe Loss Of Eye Count`, 0) > 0 THEN 26 ELSE 0 END
          + LEAST(COALESCE(`OSHA Download Prescription Support Score`, 0), 12)
          + LEAST(COALESCE(`OSHA Download Program Support Score`, 0), 10)
          + CASE WHEN `Severe Incident Signal` = 'Yes' THEN 8 ELSE 0 END
          + CASE WHEN `Has Open Violations` = 'Yes' THEN 6 ELSE 0 END
          + CASE WHEN COALESCE(`Matched Source Count`, 0) > 0 THEN 4 ELSE 0 END
        )
      ) AS INT64
    ) AS eyewear_evidence_score
  FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.sales_call_now_current` s
),
classified AS (
  SELECT
    b.*,
    CASE
      WHEN eyewear_need_tier = 'Direct Need'
        AND (
          `Overall Sales Priority` IN ('P0 Ideal', 'P1 Active', 'P2 Research')
          OR `Severe Incident Signal` = 'Yes'
          OR `Has Open Violations` = 'Yes'
          OR eyewear_evidence_score >= 24
        )
      THEN 'Call Now'
      WHEN eyewear_need_tier IN ('Direct Need', 'Probable Need')
        AND `Overall Sales Priority` IN ('P0 Ideal', 'P1 Active', 'P2 Research')
      THEN 'Research Then Call'
      ELSE 'Monitor / Nurture'
    END AS eyewear_outreach_recommendation,
    ARRAY_TO_STRING(
      ARRAY(
        SELECT reason
        FROM UNNEST([
          IF(COALESCE(`Direct Prescription Citation Count`, 0) > 0, 'Direct prescription-related citation present', NULL),
          IF(COALESCE(`Prescription Signal Count`, 0) > 0, 'Prescription program signal present in OSHA inspection data', NULL),
          IF(COALESCE(`Eye Face Citation Count`, 0) > 0, 'Eye / face citation present in OSHA inspection data', NULL),
          IF(COALESCE(`Fit Selection Citation Count`, 0) > 0, 'Fit / selection citation suggests eyewear program gap', NULL),
          IF(COALESCE(`ITA Eye Face Case Count`, 0) > 0, 'ITA downloadable data includes eye / face injury cases', NULL),
          IF(COALESCE(`ITA Prescription Case Count`, 0) > 0, 'ITA downloadable data includes prescription-related cases', NULL),
          IF(COALESCE(`Severe Eye Face Case Count`, 0) > 0, 'Severe injury reports include eye / face cases', NULL),
          IF(COALESCE(`Severe Loss Of Eye Count`, 0) > 0, 'Severe injury reports include loss-of-eye cases', NULL),
          IF(COALESCE(`OSHA Download Prescription Support Score`, 0) >= 10, 'Downloadable OSHA records show strong prescription-support evidence', NULL),
          IF(COALESCE(`OSHA Download Program Support Score`, 0) >= 10, 'Downloadable OSHA records show probable eye / face hazard exposure', NULL),
          IF(`Severe Incident Signal` = 'Yes', 'Severe incident context raises urgency', NULL),
          IF(`Has Open Violations` = 'Yes', 'Open OSHA violations remain unresolved', NULL),
          IF(COALESCE(`FDA Signal Summary`, '') != '', CONCAT('FDA signals: ', `FDA Signal Summary`), NULL),
          IF(COALESCE(`EPA Signal Summary`, '') != '', CONCAT('EPA signals: ', `EPA Signal Summary`), NULL),
          IF(COALESCE(`NIH Signal Summary`, '') != '', CONCAT('NIH signals: ', `NIH Signal Summary`), NULL)
        ]) reason
        WHERE reason IS NOT NULL
        LIMIT 4
      ),
      ' | '
    ) AS eyewear_evidence_summary
  FROM base b
)
SELECT
  `Region`,
  `Account Name`,
  `Site Address`,
  `Site City`,
  `Site State`,
  `Site ZIP`,
  `NAICS Code`,
  `Industry Segment`,
  `Ownership Type`,
  `Latest Inspection ID`,
  `Program Relevance`,
  eyewear_need_tier AS `Eyewear Need Tier`,
  eyewear_outreach_recommendation AS `Eyewear Outreach Recommendation`,
  eyewear_evidence_score AS `Eyewear Evidence Score`,
  eyewear_evidence_summary AS `Eyewear Evidence Summary`,
  `Overall Sales Priority`,
  `Should Look At Now`,
  `Reason To Contact`,
  `Reason To Call Now`,
  `Why Fit`,
  `Why Now`,
  `OSHA Follow-up Priority`,
  `OSHA Suggested Action`,
  `OSHA Follow-up Score`,
  `Program Need Score`,
  `Prescription Program Score`,
  `Urgency Score`,
  `Overall Sales Score`,
  `Matched Sources`,
  `Matched Source Count`,
  `Match Confidence`,
  `OSHA Download Match Rule`,
  `OSHA Download Program Support Score`,
  `OSHA Download Prescription Support Score`,
  `OSHA Download Urgency Support Score`,
  `OSHA Download Fit Support Score`,
  `Has Open Violations`,
  `Severe Incident Signal`,
  `Prescription Signal Count`,
  `Direct Prescription Citation Count`,
  `Eye Face Citation Count`,
  `General PPE Citation Count`,
  `Fit Selection Citation Count`,
  `ITA Eye Face Case Count`,
  `ITA Prescription Case Count`,
  `Severe Eye Face Case Count`,
  `Severe Loss Of Eye Count`,
  `Severe Injury Count`,
  `FDA Account Name`,
  `FDA Owner Operator Name`,
  `FDA Follow-up Priority`,
  `FDA Suggested Action`,
  `FDA Signal Summary`,
  `EPA Account Name`,
  `EPA Follow-up Priority`,
  `EPA Suggested Action`,
  `EPA Signal Summary`,
  `NIH Account Name`,
  `NIH Follow-up Priority`,
  `NIH Suggested Action`,
  `NIH Signal Summary`,
  `Estimated Employee Band`,
  `Employee Estimate Confidence`,
  `Estimated Revenue Band`,
  `Revenue Estimate Confidence`,
  `Commercial Fit Score`,
  `ICP Fit Band`,
  `Company Sites 5Y`,
  `Employee Count Estimate`,
  `CA Employees Signal`,
  `CA Federal Spend Signal`
FROM classified;

CREATE OR REPLACE TABLE `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.eyewear_opportunity_actionable_current` AS
SELECT *
FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.eyewear_opportunity_current`
WHERE `Eyewear Need Tier` IN ('Direct Need', 'Probable Need');

CREATE OR REPLACE TABLE `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.sales_call_now_sandiego_current` AS
SELECT * FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.sales_call_now_current`
WHERE `Region` = 'San Diego';

CREATE OR REPLACE TABLE `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.sales_call_now_bayarea_current` AS
SELECT * FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.sales_call_now_current`
WHERE `Region` = 'Bay Area';

CREATE OR REPLACE TABLE `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.eyewear_opportunity_sandiego_current` AS
SELECT * FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.eyewear_opportunity_current`
WHERE `Region` = 'San Diego';

CREATE OR REPLACE TABLE `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.eyewear_opportunity_bayarea_current` AS
SELECT * FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.eyewear_opportunity_current`
WHERE `Region` = 'Bay Area';
