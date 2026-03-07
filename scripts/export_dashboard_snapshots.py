from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.bigquery import _resolve_command
from pipeline.config import load_pipeline_config


def _run_bq_json(*, project_id: str, repo_root: Path, sql: str) -> list[dict[str, object]]:
    command = _resolve_command(
        [
            "bq",
            "query",
            f"--project_id={project_id}",
            "--use_legacy_sql=false",
            "--format=prettyjson",
            "--max_rows=500",
        ]
    )
    proc = subprocess.run(
        command,
        cwd=str(repo_root),
        check=False,
        capture_output=True,
        text=True,
        input=sql,
    )
    if proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip()
        raise RuntimeError(f"BigQuery export query failed: {detail}")
    payload = proc.stdout.strip()
    if not payload:
        return []
    return json.loads(payload)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    config = load_pipeline_config(repo_root)
    exported_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    rss_articles = _run_bq_json(
        project_id=config.rss_project_id,
        repo_root=repo_root,
        sql=f"""
        SELECT
          feed_title,
          article_title,
          article_link,
          article_priority,
          eyewear_relevance_score AS opportunity_signal_score,
          urgency_score AS momentum_score,
          signal_summary,
          CAST(article_published_at AS STRING) AS article_published_at
        FROM `{config.rss_project_id}.{config.rss_dataset}.rss_articles_current`
        ORDER BY article_published_at DESC
        LIMIT 80
        """,
    )
    alignment_watchlist = _run_bq_json(
        project_id=config.rss_project_id,
        repo_root=repo_root,
        sql=f"""
        SELECT
          `Account Name`,
          `Region`,
          `Eyewear Need Tier`,
          `Article Title`,
          `Article Link`,
          `Feed Title`,
          `Article Priority`,
          `Article Opportunity Signal Score`,
          `Article Momentum Score`,
          `Article Signal Summary`
        FROM `{config.rss_project_id}.{config.rss_dataset}.alignment_watchlist_current`
        ORDER BY `Article Published At` DESC
        LIMIT 40
        """,
    )
    local_target_summary = _run_bq_json(
        project_id=config.project_id,
        repo_root=repo_root,
        sql=f"""
        SELECT
          `Region` AS region,
          SUBSTR(REGEXP_REPLACE(COALESCE(`NAICS Code`, ''), r'[^0-9]', ''), 1, 2) AS naics2,
          `Industry Segment` AS industry_segment,
          COUNT(*) AS account_count,
          COUNTIF(`Overall Sales Priority` = 'Priority 1') AS priority_1_count
        FROM `{config.project_id}.{config.dataset}.eyewear_opportunity_actionable_current`
        WHERE `Region` IN ('San Diego', 'Bay Area')
        GROUP BY 1, 2, 3
        """,
    )
    public_freshness = _run_bq_json(
        project_id=config.public_project_id,
        repo_root=repo_root,
        sql=f"""
        SELECT 'public_enrichment_naics2_current' AS source_name, COUNT(*) AS record_count, CAST(MAX(updated_at) AS STRING) AS latest_timestamp
        FROM `{config.public_project_id}.{config.public_dataset}.public_enrichment_naics2_current`
        UNION ALL
        SELECT 'bls_segment_growth_ca_current', COUNT(*), CAST(MAX(updated_at) AS STRING)
        FROM `{config.public_project_id}.{config.public_dataset}.bls_segment_growth_ca_current`
        UNION ALL
        SELECT 'census_cbp_ca_raw', COUNT(*), CAST(MAX(CAST(load_dt AS TIMESTAMP)) AS STRING)
        FROM `{config.public_project_id}.{config.public_dataset}.census_cbp_ca_raw`
        UNION ALL
        SELECT 'usaspending_naics_ca_raw', COUNT(*), CAST(MAX(CAST(load_dt AS TIMESTAMP)) AS STRING)
        FROM `{config.public_project_id}.{config.public_dataset}.usaspending_naics_ca_raw`
        """,
    )
    public_naics = _run_bq_json(
        project_id=config.public_project_id,
        repo_root=repo_root,
        sql=f"""
        WITH local_targets AS (
          SELECT
            `Region` AS region,
            SUBSTR(REGEXP_REPLACE(COALESCE(`NAICS Code`, ''), r'[^0-9]', ''), 1, 2) AS naics2,
            `Industry Segment` AS industry_segment,
            COUNT(*) AS account_count,
            COUNTIF(`Overall Sales Priority` = 'Priority 1') AS priority_1_count
          FROM `{config.project_id}.{config.dataset}.eyewear_opportunity_actionable_current`
          WHERE `Region` IN ('San Diego', 'Bay Area')
          GROUP BY 1, 2, 3
        )
        SELECT
          lt.region,
          lt.industry_segment,
          lt.naics2,
          lt.account_count,
          lt.priority_1_count,
          pe.establishments_ca,
          pe.employees_ca,
          pe.annual_payroll_ca,
          pe.federal_amount_ca,
          pe.external_signal_points
        FROM local_targets lt
        LEFT JOIN `{config.public_project_id}.{config.public_dataset}.public_enrichment_naics2_current` pe
          ON lt.naics2 = pe.naics2
        ORDER BY lt.region, pe.external_signal_points DESC, lt.account_count DESC, lt.naics2
        """,
    )
    bls_growth = _run_bq_json(
        project_id=config.public_project_id,
        repo_root=repo_root,
        sql=f"""
        WITH local_segments AS (
          SELECT DISTINCT LOWER(`Industry Segment`) AS industry_segment
          FROM `{config.project_id}.{config.dataset}.eyewear_opportunity_actionable_current`
          WHERE `Region` IN ('San Diego', 'Bay Area')
            AND `Industry Segment` IS NOT NULL
            AND `Industry Segment` != ''
        ),
        matched_segments AS (
          SELECT DISTINCT
            b.segment,
            b.latest_value,
            b.prior_12m_value,
            b.pct_change_12m
          FROM `{config.public_project_id}.{config.public_dataset}.bls_segment_growth_ca_current` b
          JOIN local_segments ls
            ON ls.industry_segment LIKE CONCAT('%', LOWER(b.segment), '%')
            OR LOWER(b.segment) LIKE CONCAT('%', ls.industry_segment, '%')
        )
        SELECT
          segment,
          latest_value,
          prior_12m_value,
          pct_change_12m
        FROM matched_segments
        ORDER BY segment
        """,
    )

    top_accounts = _run_bq_json(
        project_id=config.project_id,
        repo_root=repo_root,
        sql=f"""
        SELECT
          `Account Name` AS account_name,
          `Region` AS region,
          SUBSTR(REGEXP_REPLACE(COALESCE(`NAICS Code`, ''), r'[^0-9]', ''), 1, 2) AS naics2,
          `Industry Segment` AS industry_segment,
          `Overall Sales Priority` AS overall_sales_priority,
          `Recent Inspection Context` AS recent_inspection_context,
          `Overall History` AS overall_history,
          `Reason To Contact` AS reason_to_contact
        FROM `{config.project_id}.{config.dataset}.eyewear_opportunity_actionable_current`
        WHERE `Region` IN ('San Diego', 'Bay Area')
        ORDER BY CASE WHEN `Overall Sales Priority` IN ('P0 Ideal','P1 Active','P2 Research','Priority 1') THEN 1 ELSE 2 END,
          `Overall Sales Priority` NULLS LAST, account_name
        LIMIT 250
        """,
    )

    dashboard_public = repo_root / "dashboard" / "public" / "data"
    _write_json(
        dashboard_public / "rss-feed.json",
        {
          "exported_at": exported_at,
          "article_count": len(rss_articles),
          "alignment_watchlist_count": len(alignment_watchlist),
          "alignment_watchlist": alignment_watchlist,
          "articles": rss_articles,
        },
    )
    _write_json(
        dashboard_public / "public-sources.json",
        {
          "exported_at": exported_at,
          "regions": ["San Diego", "Bay Area"],
          "source_freshness": public_freshness,
          "local_target_summary": local_target_summary,
          "naics_enrichment": public_naics,
          "top_accounts": top_accounts,
          "bls_growth": bls_growth,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
