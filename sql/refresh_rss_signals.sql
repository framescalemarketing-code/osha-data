CREATE SCHEMA IF NOT EXISTS `{{RSS_PROJECT_ID}}.{{RSS_DATASET}}`;

CREATE OR REPLACE TABLE `{{RSS_PROJECT_ID}}.{{RSS_DATASET}}.rss_articles_current` AS
WITH normalized AS (
  SELECT
    feed_key,
    feed_url,
    feed_title,
    article_guid,
    article_link,
    article_title,
    article_summary,
    article_text,
    article_author,
    COALESCE(article_published_at, load_dt) AS article_published_at,
    SAFE_CAST(eyewear_relevance_score AS INT64) AS eyewear_relevance_score,
    SAFE_CAST(urgency_score AS INT64) AS urgency_score,
    article_priority,
    signal_summary,
    load_dt,
    COALESCE(
      NULLIF(article_link, ''),
      NULLIF(article_guid, ''),
      TO_HEX(MD5(CONCAT(feed_key, '|', article_title, '|', COALESCE(CAST(article_published_at AS STRING), ''))))
    ) AS article_key,
    TRIM(
      REGEXP_REPLACE(
        LOWER(CONCAT(COALESCE(article_title, ''), ' ', COALESCE(article_summary, ''), ' ', COALESCE(article_text, ''))),
        r'[^a-z0-9]+',
        ' '
      )
    ) AS article_text_norm
  FROM `{{RSS_PROJECT_ID}}.{{RSS_DATASET}}.feed_items_raw`
  WHERE COALESCE(article_published_at, load_dt)
    >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{RSS_LOOKBACK_DAYS}} DAY)
),
deduped AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      n.*,
      ROW_NUMBER() OVER (
        PARTITION BY article_key
        ORDER BY article_published_at DESC, load_dt DESC, article_title
      ) AS rn
    FROM normalized n
  )
  WHERE rn = 1
)
SELECT
  feed_key,
  feed_url,
  feed_title,
  article_key,
  article_guid,
  article_link,
  article_title,
  article_summary,
  article_text,
  article_author,
  article_published_at,
  DATE_DIFF(CURRENT_DATE(), DATE(article_published_at), DAY) AS article_age_days,
  eyewear_relevance_score,
  urgency_score,
  article_priority,
  signal_summary,
  article_text_norm,
  load_dt
FROM deduped;

BEGIN
  DECLARE has_eyewear_source BOOL DEFAULT EXISTS (
    SELECT 1
    FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'eyewear_opportunity_actionable_current'
  );

  IF has_eyewear_source THEN
    EXECUTE IMMEDIATE """
      CREATE OR REPLACE TABLE `{{RSS_PROJECT_ID}}.{{RSS_DATASET}}.rss_company_matches_current` AS
      WITH companies AS (
        SELECT DISTINCT
          `Account Name` AS account_name,
          `Region` AS region_label,
          `Site City` AS site_city,
          `Site ZIP` AS site_zip,
          `Eyewear Need Tier` AS eyewear_need_tier,
          `Eyewear Outreach Recommendation` AS eyewear_outreach_recommendation,
          `Overall Sales Priority` AS overall_sales_priority,
          `Eyewear Evidence Score` AS eyewear_evidence_score,
          `Reason To Contact` AS reason_to_contact,
          TRIM(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                LOWER(REGEXP_REPLACE(`Account Name`, r'[^a-z0-9]+', ' ')),
                r'\\b(inc|incorporated|llc|ltd|limited|lp|corp|corporation|co|company)\\b',
                ' '
              ),
              r'\\s+',
              ' '
            )
          ) AS company_match_key
        FROM `{{OSHA_PROJECT_ID}}.{{OSHA_DATASET}}.eyewear_opportunity_actionable_current`
      ),
      articles AS (
        SELECT *
        FROM `{{RSS_PROJECT_ID}}.{{RSS_DATASET}}.rss_articles_current`
        WHERE eyewear_relevance_score >= 8
           OR urgency_score >= 8
           OR STARTS_WITH(feed_key, 'company_news_')
      ),
      matches AS (
        SELECT
          a.article_key,
          a.article_published_at,
          a.article_age_days,
          a.article_title,
          a.article_summary,
          a.article_link,
          a.feed_key,
          a.feed_title,
          a.eyewear_relevance_score,
          a.urgency_score,
          a.article_priority,
          a.signal_summary,
          c.account_name,
          c.region_label,
          c.site_city,
          c.site_zip,
          c.eyewear_need_tier,
          c.eyewear_outreach_recommendation,
          c.overall_sales_priority,
          c.eyewear_evidence_score,
          c.reason_to_contact,
          CASE
            WHEN LENGTH(c.company_match_key) >= 10 THEN 'company_name_normalized'
            ELSE 'company_name_short'
          END AS company_match_rule
        FROM articles a
        JOIN companies c
          ON LENGTH(c.company_match_key) >= 6
         AND STRPOS(a.article_text_norm, c.company_match_key) > 0
      )
      SELECT *
      FROM matches
    """;
  ELSE
    EXECUTE IMMEDIATE """
      CREATE OR REPLACE TABLE `{{RSS_PROJECT_ID}}.{{RSS_DATASET}}.rss_company_matches_current` AS
      SELECT
        CAST(NULL AS STRING) AS article_key,
        CAST(NULL AS TIMESTAMP) AS article_published_at,
        CAST(NULL AS INT64) AS article_age_days,
        CAST(NULL AS STRING) AS article_title,
        CAST(NULL AS STRING) AS article_summary,
        CAST(NULL AS STRING) AS article_link,
        CAST(NULL AS STRING) AS feed_key,
        CAST(NULL AS STRING) AS feed_title,
        CAST(NULL AS INT64) AS eyewear_relevance_score,
        CAST(NULL AS INT64) AS urgency_score,
        CAST(NULL AS STRING) AS article_priority,
        CAST(NULL AS STRING) AS signal_summary,
        CAST(NULL AS STRING) AS account_name,
        CAST(NULL AS STRING) AS region_label,
        CAST(NULL AS STRING) AS site_city,
        CAST(NULL AS STRING) AS site_zip,
        CAST(NULL AS STRING) AS eyewear_need_tier,
        CAST(NULL AS STRING) AS eyewear_outreach_recommendation,
        CAST(NULL AS STRING) AS overall_sales_priority,
        CAST(NULL AS INT64) AS eyewear_evidence_score,
        CAST(NULL AS STRING) AS reason_to_contact,
        CAST(NULL AS STRING) AS company_match_rule
      LIMIT 0
    """;
  END IF;
END;

CREATE OR REPLACE TABLE `{{RSS_PROJECT_ID}}.{{RSS_DATASET}}.rss_watchlist_current` AS
SELECT
  account_name AS `Account Name`,
  region_label AS `Region`,
  site_city AS `Site City`,
  site_zip AS `Site ZIP`,
  eyewear_need_tier AS `Eyewear Need Tier`,
  eyewear_outreach_recommendation AS `Eyewear Outreach Recommendation`,
  overall_sales_priority AS `Overall Sales Priority`,
  eyewear_evidence_score AS `Eyewear Evidence Score`,
  article_published_at AS `Article Published At`,
  article_age_days AS `Article Age Days`,
  article_title AS `Article Title`,
  article_summary AS `Article Summary`,
  article_link AS `Article Link`,
  feed_title AS `Feed Title`,
  eyewear_relevance_score AS `Article Eyewear Relevance Score`,
  urgency_score AS `Article Urgency Score`,
  article_priority AS `Article Priority`,
  signal_summary AS `Article Signal Summary`,
  reason_to_contact AS `Reason To Contact`,
  company_match_rule AS `Company Match Rule`
FROM `{{RSS_PROJECT_ID}}.{{RSS_DATASET}}.rss_company_matches_current`
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY account_name, article_key
  ORDER BY article_published_at DESC, eyewear_relevance_score DESC, urgency_score DESC
) = 1;
