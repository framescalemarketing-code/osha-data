CREATE OR REPLACE TABLE `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.ita_300a_summary_ca_current` AS
SELECT
  id,
  establishment_id,
  establishment_name,
  company_name,
  street_address,
  city,
  state,
  REGEXP_EXTRACT(CAST(zip_code AS STRING), r'^(\d{5})') AS zip5,
  CAST(naics_code AS STRING) AS naics_code,
  industry_description,
  SAFE_CAST(size AS INT64) AS size_band_code,
  SAFE_CAST(annual_average_employees AS INT64) AS annual_average_employees,
  SAFE_CAST(total_hours_worked AS INT64) AS total_hours_worked,
  SAFE_CAST(no_injuries_illnesses AS INT64) AS recordable_case_count,
  SAFE_CAST(total_deaths AS INT64) AS total_deaths,
  SAFE_CAST(total_dafw_cases AS INT64) AS total_dafw_cases,
  SAFE_CAST(total_djtr_cases AS INT64) AS total_djtr_cases,
  SAFE_CAST(total_other_cases AS INT64) AS total_other_cases,
  SAFE_CAST(total_injuries AS INT64) AS total_injuries,
  SAFE_CAST(total_skin_disorders AS INT64) AS total_skin_disorders,
  SAFE_CAST(total_respiratory_conditions AS INT64) AS total_respiratory_conditions,
  SAFE_CAST(total_poisonings AS INT64) AS total_poisonings,
  SAFE_CAST(total_hearing_loss AS INT64) AS total_hearing_loss,
  SAFE_CAST(total_other_illnesses AS INT64) AS total_other_illnesses,
  SAFE_CAST(year_filing_for AS INT64) AS year_filing_for,
  sector,
  SAFE.PARSE_TIMESTAMP('%d%b%y:%H:%M:%S', created_timestamp) AS created_ts
FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.ita_300a_summary_2024_2025_raw`
WHERE UPPER(TRIM(state)) = 'CA';

CREATE OR REPLACE TABLE `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.ita_case_detail_ca_current` AS
WITH base AS (
  SELECT
    id,
    establishment_id,
    establishment_name,
    company_name,
    street_address,
    city,
    state,
    REGEXP_EXTRACT(CAST(zip_code AS STRING), r'^(\d{5})') AS zip5,
    CAST(naics_code AS STRING) AS naics_code,
    industry_description,
    SAFE_CAST(annual_average_employees AS INT64) AS annual_average_employees,
    case_number,
    job_description,
    soc_description,
    SAFE.PARSE_DATE('%m/%d/%Y', CAST(date_of_incident AS STRING)) AS incident_date,
    SAFE_CAST(incident_outcome AS INT64) AS incident_outcome,
    SAFE_CAST(dafw_num_away AS INT64) AS dafw_num_away,
    SAFE_CAST(djtr_num_tr AS INT64) AS djtr_num_tr,
    SAFE_CAST(type_of_incident AS INT64) AS type_of_incident,
    NEW_NAR_WHAT_HAPPENED,
    NEW_NAR_BEFORE_INCIDENT,
    NEW_INCIDENT_LOCATION,
    NEW_NAR_INJURY_ILLNESS,
    NEW_NAR_OBJECT_SUBSTANCE,
    NEW_INCIDENT_DESCRIPTION,
    nature_title_pred,
    part_title_pred,
    event_title_pred,
    source_title_pred,
    sec_source_title_pred,
    LOWER(
      CONCAT(
        ' ', COALESCE(NEW_NAR_WHAT_HAPPENED, ''),
        ' ', COALESCE(NEW_NAR_BEFORE_INCIDENT, ''),
        ' ', COALESCE(NEW_INCIDENT_LOCATION, ''),
        ' ', COALESCE(NEW_NAR_INJURY_ILLNESS, ''),
        ' ', COALESCE(NEW_NAR_OBJECT_SUBSTANCE, ''),
        ' ', COALESCE(NEW_INCIDENT_DESCRIPTION, ''),
        ' ', COALESCE(nature_title_pred, ''),
        ' ', COALESCE(part_title_pred, ''),
        ' ', COALESCE(event_title_pred, ''),
        ' ', COALESCE(source_title_pred, ''),
        ' ', COALESCE(sec_source_title_pred, '')
      )
    ) AS text_blob
  FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.ita_case_detail_2023_raw`
  WHERE UPPER(TRIM(state)) = 'CA'
)
SELECT
  *,
  REGEXP_CONTAINS(text_blob, r'eye|face|goggle|shield|vision|visual') AS eye_face_signal,
  REGEXP_CONTAINS(text_blob, r'prescription|corrective lens|glasses|eyewear') AS prescription_signal,
  REGEXP_CONTAINS(text_blob, r'uv|ultraviolet|laser|bright light|weld|welding|arc flash|torch') AS uv_bright_light_signal,
  REGEXP_CONTAINS(text_blob, r'chemical|acid|caustic|corrosive|solvent|reagent|biohazard|fluid|splash') AS splash_chemical_signal,
  REGEXP_CONTAINS(text_blob, r'dust|debris|particulate|silica|sandblast|grind|grinding|cutting|saw|metal shard|flying particle') AS dust_debris_signal,
  REGEXP_CONTAINS(text_blob, r'struck|impact|projectile|caught in|compressed by|machine|press|forklift') AS high_impact_signal,
  REGEXP_CONTAINS(text_blob, r'humidity|humid|fog|anti-fog|condens|steam|washdown') AS fog_humidity_signal,
  REGEXP_CONTAINS(text_blob, r'heat|hot|thermal|burn|cold|freezer|cryo|furnace') AS extreme_temp_signal,
  REGEXP_CONTAINS(text_blob, r'computer|screen|monitor|display|microscope|inspection|quality control|assembly') AS computer_visual_signal,
  (
    REGEXP_CONTAINS(text_blob, r'eye|face|goggle|shield|vision|visual')
    OR REGEXP_CONTAINS(text_blob, r'uv|ultraviolet|laser|bright light|weld|welding|arc flash|torch')
    OR REGEXP_CONTAINS(text_blob, r'chemical|acid|caustic|corrosive|solvent|reagent|biohazard|fluid|splash')
    OR REGEXP_CONTAINS(text_blob, r'dust|debris|particulate|silica|sandblast|grind|grinding|cutting|saw|metal shard|flying particle')
    OR REGEXP_CONTAINS(text_blob, r'struck|impact|projectile|caught in|compressed by|machine|press|forklift')
  ) AS eyewear_program_signal
FROM base;

CREATE OR REPLACE TABLE `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.severe_injury_ca_current` AS
WITH base AS (
  SELECT
    ID,
    UPA,
    SAFE.PARSE_DATE('%m/%d/%Y', CAST(EventDate AS STRING)) AS event_date,
    Employer,
    Address1,
    Address2,
    City,
    State,
    REGEXP_EXTRACT(CAST(Zip AS STRING), r'^(\d{5})') AS zip5,
    CAST(`Primary NAICS` AS STRING) AS naics_code,
    SAFE_CAST(Hospitalized AS FLOAT64) AS hospitalized_count,
    SAFE_CAST(Amputation AS FLOAT64) AS amputation_count,
    SAFE_CAST(`Loss of Eye` AS FLOAT64) AS loss_of_eye_count,
    Inspection,
    `Final Narrative` AS final_narrative,
    NatureTitle,
    `Part of Body Title` AS part_of_body_title,
    EventTitle,
    SourceTitle,
    `Secondary Source Title` AS secondary_source_title,
    FederalState,
    LOWER(
      CONCAT(
        ' ', COALESCE(`Final Narrative`, ''),
        ' ', COALESCE(NatureTitle, ''),
        ' ', COALESCE(`Part of Body Title`, ''),
        ' ', COALESCE(EventTitle, ''),
        ' ', COALESCE(SourceTitle, ''),
        ' ', COALESCE(`Secondary Source Title`, '')
      )
    ) AS text_blob
  FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.severe_injury_2015_2025_raw`
  WHERE UPPER(TRIM(State)) = 'CALIFORNIA'
)
SELECT
  *,
  REGEXP_CONTAINS(text_blob, r'eye|face|goggle|shield|vision|visual') AS eye_face_signal,
  REGEXP_CONTAINS(text_blob, r'loss of eye|prescription|corrective lens|glasses|eyewear') AS prescription_signal,
  REGEXP_CONTAINS(text_blob, r'uv|ultraviolet|laser|bright light|weld|welding|arc flash|torch') AS uv_bright_light_signal,
  REGEXP_CONTAINS(text_blob, r'chemical|acid|caustic|corrosive|solvent|reagent|biohazard|fluid|splash') AS splash_chemical_signal,
  REGEXP_CONTAINS(text_blob, r'dust|debris|particulate|silica|sandblast|grind|grinding|cutting|saw|metal shard|flying particle') AS dust_debris_signal,
  REGEXP_CONTAINS(text_blob, r'struck|impact|projectile|caught in|compressed by|machine|press|explosion') AS high_impact_signal,
  REGEXP_CONTAINS(text_blob, r'humidity|humid|fog|anti-fog|condens|steam|washdown') AS fog_humidity_signal,
  REGEXP_CONTAINS(text_blob, r'heat|hot|thermal|burn|cold|freezer|cryo|furnace') AS extreme_temp_signal,
  REGEXP_CONTAINS(text_blob, r'computer|screen|monitor|display|microscope|inspection|quality control|assembly') AS computer_visual_signal
FROM base;

CREATE OR REPLACE TABLE `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.health_samples_focus_current` AS
WITH base AS (
  SELECT
    CAST(INSPECTION AS STRING) AS inspection_id,
    CAST(SAMPLING_NUMBER AS STRING) AS sampling_number,
    OFFICE,
    ESTABLISHMENT,
    CAST(NAICS AS STRING) AS naics_code,
    SAFE.PARSE_DATE('%d-%b-%y', CAST(DATE_SAMPLED AS STRING)) AS date_sampled,
    SAFE.PARSE_DATE('%d-%b-%y', CAST(DATE_REPORTED AS STRING)) AS date_reported,
    SUBSTANCE,
    SAFE_CAST(RESULT AS FLOAT64) AS result_value,
    UOM,
    QUALIFIER,
    LOWER(
      CONCAT(
        ' ', COALESCE(ESTABLISHMENT, ''),
        ' ', COALESCE(SUBSTANCE, '')
      )
    ) AS text_blob
  FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.health_samples_sample_raw`
)
SELECT
  *,
  REGEXP_CONTAINS(text_blob, r'weld|welding|fume|arc flash|torch|bright light|uv') AS uv_bright_light_signal,
  REGEXP_CONTAINS(text_blob, r'chemical|acid|caustic|corrosive|solvent|arsenic|cadmium|lead|nickel|chromium|biohazard') AS splash_chemical_signal,
  REGEXP_CONTAINS(text_blob, r'dust|silica|particulate|sandblast|grinding|metal dust|fume') AS dust_debris_signal,
  REGEXP_CONTAINS(text_blob, r'weld|grinding|metal|machin|sandblast') AS high_impact_signal
FROM base;

CREATE OR REPLACE TABLE `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.osha_downloads_company_signals_current` AS
WITH ita_summary AS (
  SELECT
    UPPER(REGEXP_REPLACE(COALESCE(company_name, establishment_name, ''), r'[^A-Z0-9]', '')) AS company_key_norm,
    zip5,
    MAX(annual_average_employees) AS ita_annual_average_employees,
    MAX(recordable_case_count) AS ita_recordable_case_count,
    MAX(total_dafw_cases) AS ita_total_dafw_cases,
    MAX(total_djtr_cases) AS ita_total_djtr_cases,
    MAX(total_injuries) AS ita_total_injuries,
    MAX(total_skin_disorders) AS ita_skin_disorders,
    MAX(total_respiratory_conditions) AS ita_respiratory_conditions,
    MAX(total_poisonings) AS ita_poisonings
  FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.ita_300a_summary_ca_current`
  GROUP BY 1, 2
),
ita_case AS (
  SELECT
    UPPER(REGEXP_REPLACE(COALESCE(company_name, establishment_name, ''), r'[^A-Z0-9]', '')) AS company_key_norm,
    zip5,
    COUNT(*) AS ita_case_detail_count,
    COUNTIF(eye_face_signal) AS ita_eye_face_case_count,
    COUNTIF(prescription_signal) AS ita_prescription_case_count,
    COUNTIF(uv_bright_light_signal) AS ita_uv_case_count,
    COUNTIF(splash_chemical_signal) AS ita_chemical_case_count,
    COUNTIF(dust_debris_signal) AS ita_dust_case_count,
    COUNTIF(high_impact_signal) AS ita_impact_case_count,
    COUNTIF(fog_humidity_signal) AS ita_fog_case_count,
    COUNTIF(extreme_temp_signal) AS ita_temp_case_count,
    COUNTIF(computer_visual_signal) AS ita_visual_case_count
  FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.ita_case_detail_ca_current`
  GROUP BY 1, 2
),
severe AS (
  SELECT
    UPPER(REGEXP_REPLACE(COALESCE(Employer, ''), r'[^A-Z0-9]', '')) AS company_key_norm,
    zip5,
    COUNT(*) AS severe_injury_count,
    COUNTIF(eye_face_signal) AS severe_eye_face_case_count,
    SUM(COALESCE(loss_of_eye_count, 0)) AS severe_loss_of_eye_count,
    SUM(COALESCE(amputation_count, 0)) AS severe_amputation_count,
    SUM(COALESCE(hospitalized_count, 0)) AS severe_hospitalized_count,
    COUNTIF(prescription_signal) AS severe_prescription_signal_count,
    COUNTIF(uv_bright_light_signal) AS severe_uv_case_count,
    COUNTIF(splash_chemical_signal) AS severe_chemical_case_count,
    COUNTIF(dust_debris_signal) AS severe_dust_case_count,
    COUNTIF(high_impact_signal) AS severe_impact_case_count,
    COUNTIF(fog_humidity_signal) AS severe_fog_case_count,
    COUNTIF(extreme_temp_signal) AS severe_temp_case_count,
    COUNTIF(computer_visual_signal) AS severe_visual_case_count
  FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.severe_injury_ca_current`
  GROUP BY 1, 2
),
health AS (
  SELECT
    UPPER(REGEXP_REPLACE(COALESCE(ESTABLISHMENT, ''), r'[^A-Z0-9]', '')) AS company_key_norm,
    COUNT(*) AS health_sample_count,
    COUNTIF(uv_bright_light_signal) AS health_uv_signal_count,
    COUNTIF(splash_chemical_signal) AS health_chemical_signal_count,
    COUNTIF(dust_debris_signal) AS health_dust_signal_count,
    COUNTIF(high_impact_signal) AS health_impact_signal_count
  FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.health_samples_focus_current`
  GROUP BY 1
)
SELECT
  COALESCE(s.company_key_norm, c.company_key_norm, v.company_key_norm, h.company_key_norm) AS company_key_norm,
  COALESCE(s.zip5, c.zip5, v.zip5) AS zip5,
  COALESCE(s.ita_annual_average_employees, 0) AS ita_annual_average_employees,
  COALESCE(s.ita_recordable_case_count, 0) AS ita_recordable_case_count,
  COALESCE(s.ita_total_dafw_cases, 0) AS ita_total_dafw_cases,
  COALESCE(s.ita_total_djtr_cases, 0) AS ita_total_djtr_cases,
  COALESCE(s.ita_total_injuries, 0) AS ita_total_injuries,
  COALESCE(s.ita_skin_disorders, 0) AS ita_skin_disorders,
  COALESCE(s.ita_respiratory_conditions, 0) AS ita_respiratory_conditions,
  COALESCE(s.ita_poisonings, 0) AS ita_poisonings,
  COALESCE(c.ita_case_detail_count, 0) AS ita_case_detail_count,
  COALESCE(c.ita_eye_face_case_count, 0) AS ita_eye_face_case_count,
  COALESCE(c.ita_prescription_case_count, 0) AS ita_prescription_case_count,
  COALESCE(c.ita_uv_case_count, 0) AS ita_uv_case_count,
  COALESCE(c.ita_chemical_case_count, 0) AS ita_chemical_case_count,
  COALESCE(c.ita_dust_case_count, 0) AS ita_dust_case_count,
  COALESCE(c.ita_impact_case_count, 0) AS ita_impact_case_count,
  COALESCE(c.ita_fog_case_count, 0) AS ita_fog_case_count,
  COALESCE(c.ita_temp_case_count, 0) AS ita_temp_case_count,
  COALESCE(c.ita_visual_case_count, 0) AS ita_visual_case_count,
  COALESCE(v.severe_injury_count, 0) AS severe_injury_count,
  COALESCE(v.severe_eye_face_case_count, 0) AS severe_eye_face_case_count,
  COALESCE(v.severe_loss_of_eye_count, 0) AS severe_loss_of_eye_count,
  COALESCE(v.severe_amputation_count, 0) AS severe_amputation_count,
  COALESCE(v.severe_hospitalized_count, 0) AS severe_hospitalized_count,
  COALESCE(v.severe_prescription_signal_count, 0) AS severe_prescription_signal_count,
  COALESCE(v.severe_uv_case_count, 0) AS severe_uv_case_count,
  COALESCE(v.severe_chemical_case_count, 0) AS severe_chemical_case_count,
  COALESCE(v.severe_dust_case_count, 0) AS severe_dust_case_count,
  COALESCE(v.severe_impact_case_count, 0) AS severe_impact_case_count,
  COALESCE(v.severe_fog_case_count, 0) AS severe_fog_case_count,
  COALESCE(v.severe_temp_case_count, 0) AS severe_temp_case_count,
  COALESCE(v.severe_visual_case_count, 0) AS severe_visual_case_count,
  COALESCE(h.health_sample_count, 0) AS health_sample_count,
  COALESCE(h.health_uv_signal_count, 0) AS health_uv_signal_count,
  COALESCE(h.health_chemical_signal_count, 0) AS health_chemical_signal_count,
  COALESCE(h.health_dust_signal_count, 0) AS health_dust_signal_count,
  COALESCE(h.health_impact_signal_count, 0) AS health_impact_signal_count
FROM ita_summary s
FULL OUTER JOIN ita_case c
  ON s.company_key_norm = c.company_key_norm
 AND s.zip5 = c.zip5
FULL OUTER JOIN severe v
  ON COALESCE(s.company_key_norm, c.company_key_norm) = v.company_key_norm
 AND COALESCE(s.zip5, c.zip5) = v.zip5
FULL OUTER JOIN health h
  ON COALESCE(s.company_key_norm, c.company_key_norm, v.company_key_norm) = h.company_key_norm;
