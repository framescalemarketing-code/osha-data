"""Temporary debug script to trace the pipeline funnel counts."""
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.config import load_pipeline_config
from pipeline.bigquery import _resolve_command

config = load_pipeline_config(Path(__file__).resolve().parent.parent)
pid = config.project_id
ds = config.dataset


def bq(sql):
    cmd = _resolve_command(
        ["bq", "query", f"--project_id={pid}", "--use_legacy_sql=false", "--format=prettyjson"]
    )
    r = subprocess.run(cmd, input=sql, capture_output=True, text=True, cwd=str(Path(__file__).resolve().parent.parent))
    if r.returncode != 0:
        print(f"ERROR: {r.stderr.strip()}")
        return []
    return json.loads(r.stdout.strip()) if r.stdout.strip() else []


# Funnel counts
rows = bq(f"""
SELECT 'A. sales_followup_all' AS stage, COUNT(*) AS total_rows, COUNT(DISTINCT `Account Name`) AS unique_companies
FROM `{pid}.{ds}.sales_followup_all_current`
UNION ALL
SELECT 'B. sales_call_now', COUNT(*), COUNT(DISTINCT `Account Name`)
FROM `{pid}.{ds}.sales_call_now_current`
UNION ALL
SELECT 'C. eyewear_opportunity', COUNT(*), COUNT(DISTINCT `Account Name`)
FROM `{pid}.{ds}.eyewear_opportunity_current`
UNION ALL
SELECT 'D. eyewear_actionable', COUNT(*), COUNT(DISTINCT `Account Name`)
FROM `{pid}.{ds}.eyewear_opportunity_actionable_current`
ORDER BY stage
""")
print("=== PIPELINE FUNNEL ===")
for r in rows:
    print(f"  {r['stage']}: {r['total_rows']} rows, {r['unique_companies']} companies")

# What's cut at sales_call_now (overall_sales_score >= 36)?
rows2 = bq(f"""
WITH scored AS (
  SELECT `Account Name`, `Region`,
    CASE WHEN overall_sales_score >= 36 THEN 'passes' ELSE 'filtered' END AS status,
    overall_sales_score
  FROM (
    SELECT *, CAST(ROUND(LEAST(100,
      program_need_score * 0.33 + prescription_program_score * 0.21 + urgency_score * 0.16 + commercial_fit_score * 0.30
    )) AS INT64) AS overall_sales_score
    FROM `{pid}.{ds}.sales_call_now_current`
  )
)
SELECT 'Below score 36' AS reason, COUNT(*) AS cnt, COUNT(DISTINCT `Account Name`) AS companies
FROM `{pid}.{ds}.sales_followup_all_current`
WHERE `Account Name` NOT IN (SELECT `Account Name` FROM `{pid}.{ds}.sales_call_now_current`)
""")
# Actually let me just check what the score cutoff does
rows2 = bq(f"""
SELECT
  `Region`,
  COUNT(*) AS total,
  COUNT(DISTINCT `Account Name`) AS companies,
  MIN(SAFE_CAST(`Follow-up Score` AS FLOAT64)) AS min_score,
  MAX(SAFE_CAST(`Follow-up Score` AS FLOAT64)) AS max_score,
  AVG(SAFE_CAST(`Follow-up Score` AS FLOAT64)) AS avg_score
FROM `{pid}.{ds}.sales_followup_all_current`
GROUP BY 1
""")
print("\n=== SALES FOLLOWUP ALL (base inspections) ===")
for r in rows2:
    print(f"  {r['Region']}: {r['total']} rows, {r['companies']} companies, score range {r['min_score']}-{r['max_score']}, avg {r['avg_score']}")

# Eyewear need tiers
rows3 = bq(f"""
SELECT `Eyewear Need Tier`, COUNT(*) AS cnt, COUNT(DISTINCT `Account Name`) AS companies
FROM `{pid}.{ds}.eyewear_opportunity_current`
GROUP BY 1 ORDER BY 1
""")
print("\n=== EYEWEAR OPPORTUNITY BY NEED TIER ===")
for r in rows3:
    print(f"  {r['Eyewear Need Tier']}: {r['cnt']} rows, {r['companies']} companies")

# Region breakdown in actionable
rows4 = bq(f"""
SELECT `Region`, `Overall Sales Priority`, COUNT(*) AS cnt, COUNT(DISTINCT `Account Name`) AS companies
FROM `{pid}.{ds}.eyewear_opportunity_actionable_current`
GROUP BY 1, 2 ORDER BY 1, 2
""")
print("\n=== EYEWEAR ACTIONABLE BY REGION + PRIORITY ===")
for r in rows4:
    print(f"  {r['Region']} / {r['Overall Sales Priority']}: {r['cnt']} rows, {r['companies']} companies")

# What industries are in salesfollowup but NOT in call_now?
rows5 = bq(f"""
SELECT `Industry Segment`, COUNT(*) AS cnt
FROM `{pid}.{ds}.sales_followup_all_current`
GROUP BY 1 ORDER BY cnt DESC LIMIT 15
""")
print("\n=== ALL INDUSTRIES in sales_followup_all ===")
for r in rows5:
    print(f"  {r['Industry Segment']}: {r['cnt']}")

# WHY only 28 pass into sales_call_now?
# sales_call_now = deduped ranked rows WHERE overall_sales_score >= 36
# overall_sales_score = program_need * 0.33 + prescription * 0.21 + urgency * 0.16 + commercial_fit * 0.30
# The key driver is commercial_fit_score which depends heavily on employee count + multi-site + ITA data
rows6 = bq(f"""
WITH base AS (
  SELECT
    `Account Name`,
    `Region`,
    SAFE_CAST(`Follow-up Score` AS FLOAT64) AS followup_score,
    SAFE_CAST(`Employee Count Estimate` AS FLOAT64) AS emp_est,
    SAFE_CAST(`Company Sites 5Y` AS FLOAT64) AS sites_5y,
    `Industry Segment`,
    `Has Open Violations`,
    `Severe Incident Signal`,
    `Program Relevance`
  FROM `{pid}.{ds}.sales_followup_all_current`
)
SELECT
  CASE
    WHEN emp_est >= 50 THEN '50+'
    WHEN emp_est >= 20 THEN '20-49'
    WHEN emp_est > 0 THEN '1-19'
    ELSE 'Unknown / 0'
  END AS employee_band,
  COUNT(*) AS cnt,
  COUNT(DISTINCT `Account Name`) AS companies,
  COUNTIF(`Has Open Violations` = 'Yes') AS has_open_viol,
  COUNTIF(`Severe Incident Signal` = 'Yes') AS has_severe,
  COUNTIF(sites_5y >= 2) AS multi_site,
  ROUND(AVG(followup_score), 1) AS avg_followup
FROM base
GROUP BY 1
ORDER BY 1
""")
print("\n=== EMPLOYEE BAND DISTRIBUTION (sales_followup_all) ===")
for r in rows6:
    print(f"  {r['employee_band']}: {r['cnt']} rows, {r['companies']} cos, open_viol={r['has_open_viol']}, severe={r['has_severe']}, multi_site={r['multi_site']}, avg_score={r['avg_followup']}")

# Check: how many have OSHA download data (ITA/severe injury)?
rows7 = bq(f"""
SELECT
  `OSHA Download Match Rule` AS match_status,
  COUNT(*) AS cnt,
  COUNT(DISTINCT `Account Name`) AS companies
FROM `{pid}.{ds}.sales_call_now_current`
GROUP BY 1
""")
print("\n=== OSHA DOWNLOAD MATCH in sales_call_now ===")
for r in rows7:
    print(f"  {r['match_status']}: {r['cnt']} rows, {r['companies']} cos")

# Check the score distribution to see where the cutoff bites
rows8 = bq(f"""
SELECT
  MIN(`Overall Sales Score`) AS min_overall,
  MAX(`Overall Sales Score`) AS max_overall,
  ROUND(AVG(SAFE_CAST(`Overall Sales Score` AS FLOAT64)), 1) AS avg_overall,
  MIN(`Commercial Fit Score`) AS min_fit,
  MAX(`Commercial Fit Score`) AS max_fit,
  ROUND(AVG(SAFE_CAST(`Commercial Fit Score` AS FLOAT64)), 1) AS avg_fit,
  MIN(`Program Need Score`) AS min_need,
  MAX(`Program Need Score`) AS max_need,
  MIN(`Urgency Score`) AS min_urg,
  MAX(`Urgency Score`) AS max_urg
FROM `{pid}.{ds}.sales_call_now_current`
""")
print("\n=== SCORE RANGES in sales_call_now ===")
for r in rows8:
    print(f"  Overall:        {r['min_overall']}-{r['max_overall']} (avg {r['avg_overall']})")
    print(f"  Commercial fit: {r['min_fit']}-{r['max_fit']} (avg {r['avg_fit']})")
    print(f"  Program need:   {r['min_need']}-{r['max_need']}")
    print(f"  Urgency:        {r['min_urg']}-{r['max_urg']}")

# How many had OSHA download data in the full base?
rows9 = bq(f"""
SELECT
  COUNT(*) AS total,
  COUNTIF(company_key_norm IS NOT NULL AND company_key_norm != '') AS with_data
FROM `{pid}.{ds}.osha_downloads_company_signals_current`
""")
print("\n=== OSHA DOWNLOADS TABLE (ITA/severe injury lookup) ===")
for r in rows9:
    print(f"  {r['total']} total rows, {r['with_data']} with data")

# What does the open_violation_count actually look like?
rows10 = bq(f"""
SELECT
  `Has Open Violations`,
  COUNT(*) AS cnt
FROM `{pid}.{ds}.sales_followup_all_current`
GROUP BY 1
""")
print("\n=== OPEN VIOLATIONS in sales_followup_all ===")
for r in rows10:
    print(f"  {r['Has Open Violations']}: {r['cnt']}")

# What about severe incident?
rows11 = bq(f"""
SELECT
  `Severe Incident Signal`,
  COUNT(*) AS cnt
FROM `{pid}.{ds}.sales_followup_all_current`
GROUP BY 1
""")
print("\n=== SEVERE INCIDENT in sales_followup_all ===")
for r in rows11:
    print(f"  {r['Severe Incident Signal']}: {r['cnt']}")

# Cross-source matches?
rows12 = bq(f"""
SELECT COUNT(*) AS cnt
FROM `{pid}.{ds}.sales_followup_cross_source_current`
""")
print("\n=== CROSS-SOURCE MATCHES ===")
for r in rows12:
    print(f"  {r['cnt']} rows")

# Check SEVERE incident signal distribution
rows13 = bq(f"""
SELECT `Severe Incident Signal`, COUNT(*) AS cnt
FROM `{pid}.{ds}.sales_followup_all_current`
GROUP BY 1
""")
print("\n=== SEVERE INCIDENT in sales_followup_all ===")
for r in rows13:
    print(f"  {r['Severe Incident Signal']}: {r['cnt']}")

# The big question: why do only 28 pass the overall_sales_score >= 36 filter?
# Let me check what the score distribution looks like BEFORE the cutoff
rows14 = bq(f"""
WITH osha_base AS (
  SELECT
    o.*,
    UPPER(REGEXP_REPLACE(COALESCE(\`Account Name\`, ''), r'[^A-Z0-9]', '')) AS osha_company_key_norm,
    REGEXP_EXTRACT(CAST(\`Site ZIP\` AS STRING, r'^(\\d{{5}})')) AS osha_zip5
  FROM \`{pid}.{ds}.sales_followup_all_current\` o
)
SELECT 'placeholder' AS x
LIMIT 0
""")
# That's too complex. Let me just check the existing score distribution of the 28 that pass
rows14 = bq(f"""
SELECT
  \`Account Name\`,
  \`Region\`,
  \`Overall Sales Score\`,
  \`Commercial Fit Score\`,
  \`Program Need Score\`,
  \`Urgency Score\`,
  \`Prescription Program Score\`,
  \`Overall Sales Priority\`,
  \`OSHA Download Match Rule\`,
  \`Estimated Employee Band\`
FROM \`{pid}.{ds}.sales_call_now_current\`
ORDER BY \`Overall Sales Score\` DESC
LIMIT 30
""")
print("\n=== ALL 28 sales_call_now rows (top scores) ===")
for r in rows14:
    print(f"  {r['Account Name'][:35]:35s} | {r['Region']:10s} | score={r['Overall Sales Score']:>3s} fit={r['Commercial Fit Score']:>3s} need={r['Program Need Score']:>3s} urg={r['Urgency Score']:>3s} | emp={r['Estimated Employee Band']:>8s} | match={r['OSHA Download Match Rule']}")
