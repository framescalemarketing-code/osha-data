from __future__ import annotations

import logging

from pipeline.bigquery import bq_query_sql
from pipeline.config import PipelineConfig


def run_sql_refresh(
    *,
    config: PipelineConfig,
    sql_filename: str,
    project_id: str,
) -> None:
    sql_file = config.paths.sql_dir / sql_filename
    if not sql_file.exists():
        logging.warning("SQL refresh file not found: %s", sql_file)
        return

    sql_text = sql_file.read_text(encoding="utf-8-sig")
    sql_text = (
        sql_text.replace("{{OSHA_PROJECT_ID}}", config.project_id)
        .replace("{{OSHA_DATASET}}", config.dataset)
        .replace("{{PUBLIC_PROJECT_ID}}", config.public_project_id)
        .replace("{{PUBLIC_DATASET}}", config.public_dataset)
        .replace("{{RSS_PROJECT_ID}}", config.rss_project_id)
        .replace("{{RSS_DATASET}}", config.rss_dataset)
        .replace("{{RSS_LOOKBACK_DAYS}}", str(config.rss_lookback_days))
        .replace("{{FDA_PROJECT_ID}}", config.fda_project_id)
        .replace("{{FDA_DATASET}}", config.fda_dataset)
        .replace("{{EPA_PROJECT_ID}}", config.epa_project_id)
        .replace("{{EPA_DATASET}}", config.epa_dataset)
        .replace("{{NIH_PROJECT_ID}}", config.nih_project_id)
        .replace("{{NIH_DATASET}}", config.nih_dataset)
        .replace("{{CA_SOS_PROJECT_ID}}", config.ca_sos_project_id)
        .replace("{{CA_SOS_DATASET}}", config.ca_sos_dataset)
    )
    bq_query_sql(
        repo_root=config.paths.repo_root,
        project_id=project_id,
        sql_text=sql_text,
    )
