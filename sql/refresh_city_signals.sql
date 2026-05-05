-- Refresh city business license signals (SF + LA Socrata feeds)
-- Filtered to eye-hazard industries: Construction, Manufacturing,
-- Warehousing/Transport, Auto/Repair

CREATE SCHEMA IF NOT EXISTS `{{CITY_PROJECT_ID}}.{{CITY_DATASET}}`;

CREATE OR REPLACE TABLE `{{CITY_PROJECT_ID}}.{{CITY_DATASET}}.city_biz_license_current`
CLUSTER BY naics_code, zip_code, source
AS
WITH ranked AS (
  SELECT
    source,
    UPPER(TRIM(business_name))                       AS business_name,
    UPPER(TRIM(dba_name))                            AS dba_name,
    TRIM(address)                                    AS address,
    UPPER(TRIM(city))                                AS city,
    UPPER(TRIM(state))                               AS state,
    LEFT(TRIM(zip_code), 5)                          AS zip_code,
    TRIM(naics_code)                                 AS naics_code,
    TRIM(naics_description)                          AS naics_description,
    SAFE.PARSE_DATE('%Y-%m-%dT%H:%M:%S', start_date) AS start_date,
    load_dt,
    -- New business flag: started within the last 24 months
    CASE
      WHEN SAFE.PARSE_DATE('%Y-%m-%dT%H:%M:%S', start_date)
           >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
      THEN TRUE
      ELSE FALSE
    END AS is_new_business,
  -- Industry category for outreach prioritization (all active businesses, not restricted by NAICS)
  CASE
    WHEN REGEXP_CONTAINS(naics_code, r'^325') THEN 'Chemical Manufacturing'
    WHEN REGEXP_CONTAINS(naics_code, r'^333') THEN 'Machinery Manufacturing'
    WHEN REGEXP_CONTAINS(naics_code, r'^334') THEN 'Computer/Electronics Manufacturing'
    WHEN REGEXP_CONTAINS(naics_code, r'^(335|336)') THEN 'Transportation Equipment'
    WHEN REGEXP_CONTAINS(naics_code, r'^(311|312)') THEN 'Food/Beverage Manufacturing'
    WHEN REGEXP_CONTAINS(naics_code, r'^22') THEN 'Utilities'
    WHEN REGEXP_CONTAINS(naics_code, r'^23') THEN 'Construction'
    WHEN REGEXP_CONTAINS(naics_code, r'^(31|32|33)') THEN 'Manufacturing'
    WHEN REGEXP_CONTAINS(naics_code, r'^42') THEN 'Wholesale Trade'
    WHEN REGEXP_CONTAINS(naics_code, r'^(48|49)') THEN 'Warehousing/Transport'
    WHEN REGEXP_CONTAINS(naics_code, r'^51') THEN 'Information Services'
    WHEN REGEXP_CONTAINS(naics_code, r'^52') THEN 'Finance/Insurance'
    WHEN REGEXP_CONTAINS(naics_code, r'^54') THEN 'Professional/Scientific Services'
    WHEN REGEXP_CONTAINS(naics_code, r'^56') THEN 'Waste Management/Remediation'
    WHEN REGEXP_CONTAINS(naics_code, r'^61') THEN 'Education'
    WHEN REGEXP_CONTAINS(naics_code, r'^62') THEN 'Healthcare/Social Services'
    WHEN REGEXP_CONTAINS(naics_code, r'^71') THEN 'Arts/Entertainment'
    WHEN REGEXP_CONTAINS(naics_code, r'^72') THEN 'Hospitality'
    WHEN REGEXP_CONTAINS(naics_code, r'^81') THEN 'Repair/Maintenance/Personal Services'
    ELSE 'Other Industries'
  END AS industry_category,
    -- Dedup rank: keep only the most recent record per business name + zip
    ROW_NUMBER() OVER (
      PARTITION BY
        UPPER(REGEXP_REPLACE(
          COALESCE(NULLIF(TRIM(dba_name), ''), TRIM(business_name)),
          r'[^A-Z0-9]', ''
        )),
        LEFT(TRIM(zip_code), 5)
      ORDER BY
        SAFE.PARSE_DATE('%Y-%m-%dT%H:%M:%S', start_date) DESC NULLS LAST,
        load_dt DESC NULLS LAST
    ) AS _rn
  FROM `{{CITY_PROJECT_ID}}.{{CITY_DATASET}}.city_biz_license_raw`
  WHERE
    TRIM(business_name) != ''
    OR TRIM(dba_name) != ''
)
SELECT * EXCEPT (_rn) FROM ranked WHERE _rn = 1;
