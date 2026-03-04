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
    CAST(_activity_nr_ AS INT64) AS activity_nr,
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
      + CASE
          WHEN ic.inspections_90d >= 3 THEN 10
          WHEN ic.inspections_90d = 2 THEN 7
          WHEN ic.inspections_90d = 1 THEN 4
          ELSE 0
        END
    ) AS followup_score
  FROM inspection_company ic
  LEFT JOIN violation_metrics vm ON ic.activity_nr = vm.activity_nr
  LEFT JOIN violation_event_metrics vem ON ic.activity_nr = vem.activity_nr
  LEFT JOIN related_activity_metrics ram ON ic.activity_nr = ram.activity_nr
  LEFT JOIN emphasis_metrics em ON ic.activity_nr = em.activity_nr
  LEFT JOIN injury_metrics im ON ic.activity_nr = im.activity_nr
  LEFT JOIN accident_metrics am ON ic.activity_nr = am.activity_nr
  WHERE ic.rn = 1
)
SELECT
  *,
  CASE
    WHEN followup_score >= 85 THEN 'Priority 1'
    WHEN followup_score >= 55 THEN 'Priority 2'
    ELSE 'Priority 3'
  END AS followup_priority,
  CASE
    WHEN severe_injury_indicator OR open_violation_status OR followup_score >= 85 THEN 'Call within 24 hours'
    WHEN followup_score >= 55 THEN 'Call this week'
    ELSE 'Nurture this month'
  END AS suggested_action,
  CASE
    WHEN followup_score >= 85 THEN 'High'
    WHEN followup_score >= 55 THEN 'Medium'
    ELSE 'Low'
  END AS eyewear_buy_likelihood,
  CASE
    WHEN followup_score >= 85 THEN 'RED'
    WHEN followup_score >= 55 THEN 'YELLOW'
    ELSE 'GREEN'
  END AS score_band,
  CASE
    WHEN followup_score >= 85 THEN '#F97066'
    WHEN followup_score >= 55 THEN '#F6C344'
    ELSE '#5BB974'
  END AS score_color
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
    CAST(_activity_nr_ AS INT64) AS activity_nr,
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
      + CASE
          WHEN ic.inspections_90d >= 3 THEN 10
          WHEN ic.inspections_90d = 2 THEN 7
          WHEN ic.inspections_90d = 1 THEN 4
          ELSE 0
        END
    ) AS followup_score
  FROM inspection_company ic
  LEFT JOIN violation_metrics vm ON ic.activity_nr = vm.activity_nr
  LEFT JOIN violation_event_metrics vem ON ic.activity_nr = vem.activity_nr
  LEFT JOIN related_activity_metrics ram ON ic.activity_nr = ram.activity_nr
  LEFT JOIN emphasis_metrics em ON ic.activity_nr = em.activity_nr
  LEFT JOIN injury_metrics im ON ic.activity_nr = im.activity_nr
  LEFT JOIN accident_metrics am ON ic.activity_nr = am.activity_nr
  WHERE ic.rn = 1
)
SELECT
  *,
  CASE
    WHEN followup_score >= 85 THEN 'Priority 1'
    WHEN followup_score >= 55 THEN 'Priority 2'
    ELSE 'Priority 3'
  END AS followup_priority,
  CASE
    WHEN severe_injury_indicator OR open_violation_status OR followup_score >= 85 THEN 'Call within 24 hours'
    WHEN followup_score >= 55 THEN 'Call this week'
    ELSE 'Nurture this month'
  END AS suggested_action,
  CASE
    WHEN followup_score >= 85 THEN 'High'
    WHEN followup_score >= 55 THEN 'Medium'
    ELSE 'Low'
  END AS eyewear_buy_likelihood,
  CASE
    WHEN followup_score >= 85 THEN 'RED'
    WHEN followup_score >= 55 THEN 'YELLOW'
    ELSE 'GREEN'
  END AS score_band,
  CASE
    WHEN followup_score >= 85 THEN '#F97066'
    WHEN followup_score >= 55 THEN '#F6C344'
    ELSE '#5BB974'
  END AS score_color
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

