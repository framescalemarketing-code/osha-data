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
          eyewear_relevance_score,
          urgency_score,
          CAST(article_published_at AS STRING) AS article_published_at
        FROM `{config.rss_project_id}.{config.rss_dataset}.rss_articles_current`
        ORDER BY article_published_at DESC
        LIMIT 80
        """,
    )
    rss_watchlist = _run_bq_json(
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
          `Article Eyewear Relevance Score`,
          `Article Urgency Score`
        FROM `{config.rss_project_id}.{config.rss_dataset}.rss_watchlist_current`
        ORDER BY `Article Published At` DESC
        LIMIT 40
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
        SELECT
          naics2,
          establishments_ca,
          employees_ca,
          annual_payroll_ca,
          federal_amount_ca,
          external_signal_points
        FROM `{config.public_project_id}.{config.public_dataset}.public_enrichment_naics2_current`
        ORDER BY external_signal_points DESC, employees_ca DESC
        LIMIT 25
        """,
    )
    bls_growth = _run_bq_json(
        project_id=config.public_project_id,
        repo_root=repo_root,
        sql=f"""
        SELECT
          segment,
          latest_value,
          prior_12m_value,
          pct_change_12m
        FROM `{config.public_project_id}.{config.public_dataset}.bls_segment_growth_ca_current`
        ORDER BY segment
        """,
    )

    dashboard_public = repo_root / "dashboard" / "public" / "data"
    _write_json(
        dashboard_public / "rss-feed.json",
        {
          "exported_at": exported_at,
          "article_count": len(rss_articles),
          "watchlist_count": len(rss_watchlist),
          "watchlist": rss_watchlist,
          "articles": rss_articles,
        },
    )
    _write_json(
        dashboard_public / "public-sources.json",
        {
          "exported_at": exported_at,
          "source_freshness": public_freshness,
          "naics_enrichment": public_naics,
          "bls_growth": bls_growth,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
