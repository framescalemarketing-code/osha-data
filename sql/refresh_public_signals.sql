CREATE OR REPLACE TABLE osha_raw.census_cbp_ca_naics2_current AS
WITH base AS (
  SELECT
    SUBSTR(REGEXP_REPLACE(naics2017, r'[^0-9]', ''), 1, 2) AS naics2,
    SAFE_CAST(estab AS FLOAT64) AS estab_num,
    SAFE_CAST(emp AS FLOAT64) AS emp_num,
    SAFE_CAST(payann AS FLOAT64) AS payann_num,
    SAFE_CAST(load_dt AS TIMESTAMP) AS load_ts
  FROM osha_raw.census_cbp_ca_raw
  WHERE REGEXP_CONTAINS(naics2017, r'^\d{2}$')
)
SELECT
  naics2,
  CAST(SUM(estab_num) AS INT64) AS establishments_ca,
  CAST(SUM(emp_num) AS INT64) AS employees_ca,
  SUM(payann_num) AS annual_payroll_ca,
  MAX(load_ts) AS source_load_ts
FROM base
GROUP BY 1;

CREATE OR REPLACE TABLE osha_raw.usaspending_naics2_ca_current AS
WITH base AS (
  SELECT
    SUBSTR(REGEXP_REPLACE(naics_code, r'[^0-9]', ''), 1, 2) AS naics2,
    SAFE_CAST(amount AS FLOAT64) AS amount_num,
    SAFE_CAST(load_dt AS TIMESTAMP) AS load_ts
  FROM osha_raw.usaspending_naics_ca_raw
  WHERE REGEXP_CONTAINS(naics_code, r'^\d{2,6}$')
)
SELECT
  naics2,
  SUM(amount_num) AS federal_amount_ca,
  MAX(load_ts) AS source_load_ts
FROM base
GROUP BY 1;

CREATE OR REPLACE TABLE osha_raw.bls_segment_growth_ca_current AS
WITH ordered AS (
  SELECT
    segment,
    SAFE_CAST(year AS INT64) AS yr,
    SAFE_CAST(SUBSTR(period, 2) AS INT64) AS mo,
    SAFE_CAST(value AS FLOAT64) AS value_num,
    ROW_NUMBER() OVER (
      PARTITION BY segment
      ORDER BY SAFE_CAST(year AS INT64) DESC, SAFE_CAST(SUBSTR(period, 2) AS INT64) DESC
    ) AS rn
  FROM osha_raw.bls_ca_series_raw
  WHERE REGEXP_CONTAINS(period, r'^M\d{2}$')
)
SELECT
  latest.segment,
  latest.value_num AS latest_value,
  prev.value_num AS prior_12m_value,
  SAFE_DIVIDE(latest.value_num - prev.value_num, NULLIF(prev.value_num, 0)) AS pct_change_12m,
  CURRENT_TIMESTAMP() AS updated_at
FROM ordered AS latest
LEFT JOIN ordered AS prev
  ON latest.segment = prev.segment
 AND prev.rn = 13
WHERE latest.rn = 1;

CREATE OR REPLACE TABLE osha_raw.public_enrichment_naics2_current AS
WITH combined AS (
  SELECT
    COALESCE(c.naics2, u.naics2) AS naics2,
    c.establishments_ca,
    c.employees_ca,
    c.annual_payroll_ca,
    u.federal_amount_ca,
    GREATEST(COALESCE(c.source_load_ts, TIMESTAMP '1970-01-01'), COALESCE(u.source_load_ts, TIMESTAMP '1970-01-01')) AS source_load_ts
  FROM osha_raw.census_cbp_ca_naics2_current AS c
  FULL OUTER JOIN osha_raw.usaspending_naics2_ca_current AS u
    ON c.naics2 = u.naics2
)
SELECT
  naics2,
  COALESCE(establishments_ca, 0) AS establishments_ca,
  COALESCE(employees_ca, 0) AS employees_ca,
  COALESCE(annual_payroll_ca, 0) AS annual_payroll_ca,
  COALESCE(federal_amount_ca, 0) AS federal_amount_ca,
  CASE
    WHEN COALESCE(employees_ca, 0) >= 500000 THEN 8
    WHEN COALESCE(employees_ca, 0) >= 200000 THEN 6
    WHEN COALESCE(employees_ca, 0) >= 50000 THEN 4
    WHEN COALESCE(employees_ca, 0) >= 10000 THEN 2
    ELSE 1
  END AS census_size_points,
  CASE
    WHEN COALESCE(federal_amount_ca, 0) >= 5000000000 THEN 8
    WHEN COALESCE(federal_amount_ca, 0) >= 1000000000 THEN 6
    WHEN COALESCE(federal_amount_ca, 0) >= 200000000 THEN 4
    WHEN COALESCE(federal_amount_ca, 0) >= 50000000 THEN 2
    ELSE 0
  END AS federal_spend_points,
  LEAST(
    CASE
      WHEN COALESCE(employees_ca, 0) >= 500000 THEN 8
      WHEN COALESCE(employees_ca, 0) >= 200000 THEN 6
      WHEN COALESCE(employees_ca, 0) >= 50000 THEN 4
      WHEN COALESCE(employees_ca, 0) >= 10000 THEN 2
      ELSE 1
    END
    + CASE
        WHEN COALESCE(federal_amount_ca, 0) >= 5000000000 THEN 8
        WHEN COALESCE(federal_amount_ca, 0) >= 1000000000 THEN 6
        WHEN COALESCE(federal_amount_ca, 0) >= 200000000 THEN 4
        WHEN COALESCE(federal_amount_ca, 0) >= 50000000 THEN 2
        ELSE 0
      END,
    12
  ) AS external_signal_points,
  source_load_ts,
  CURRENT_TIMESTAMP() AS updated_at
FROM combined;
