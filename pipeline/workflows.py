from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path

from pipeline.bigquery import bq_load_csv, bq_query_sql
from pipeline.ca_sos_signals import run_ca_sos_signals_ingest
from pipeline.compliance import validate_compliance
from pipeline.config import PipelineConfig
from pipeline.csv_utils import csv_data_row_count
from pipeline.dol_api import DolApiClient
from pipeline.epa_signals import run_epa_signals_ingest
from pipeline.extract import query_endpoint_to_csv, query_inspection_incremental
from pipeline.fda_signals import run_fda_signals_ingest
from pipeline.nih_signals import run_nih_signals_ingest
from pipeline.osha_local_downloads import run_osha_local_downloads_ingest
from pipeline.public_signals import run_public_signals_ingest


ENDPOINT_SCHEMAS = {
    "violation": "activity_nr:STRING,citation_id:STRING,delete_flag:STRING,standard:STRING,viol_type:STRING,issuance_date:STRING,abate_date:STRING,abate_complete:STRING,current_penalty:STRING,initial_penalty:STRING,contest_date:STRING,final_order_date:STRING,nr_instances:STRING,nr_exposed:STRING,rec:STRING,gravity:STRING,emphasis:STRING,hazcat:STRING,fta_insp_nr:STRING,fta_issuance_date:STRING,fta_penalty:STRING,fta_contest_date:STRING,fta_final_order_date:STRING,hazsub1:STRING,hazsub2:STRING,hazsub3:STRING,hazsub4:STRING,hazsub5:STRING,load_dt:STRING",
    "violation_event": "activity_nr:STRING,citation_id:STRING,pen_fta:STRING,hist_event:STRING,hist_date:STRING,hist_penalty:STRING,hist_abate_date:STRING,hist_vtype:STRING,hist_insp_nr:STRING,load_dt:STRING",
    "related_activity": "activity_nr:STRING,rel_type:STRING,rel_act_nr:STRING,rel_safety:STRING,rel_health:STRING,load_dt:STRING",
    "emphasis_codes": "activity_nr:STRING,prog_type:STRING,prog_value:STRING,load_dt:STRING",
    "accident_injury": "summary_nr:STRING,rel_insp_nr:STRING,age:STRING,sex:STRING,nature_of_inj:STRING,part_of_body:STRING,src_of_injury:STRING,event_type:STRING,evn_factor:STRING,hum_factor:STRING,occ_code:STRING,degree_of_inj:STRING,task_assigned:STRING,hazsub:STRING,const_op:STRING,const_op_cause:STRING,fat_cause:STRING,fall_distance:STRING,fall_ht:STRING,injury_line_nr:STRING,load_dt:STRING",
    "accident": "summary_nr:STRING,report_id:STRING,event_date:STRING,event_time:STRING,event_desc:STRING,event_keyword:STRING,const_end_use:STRING,build_stories:STRING,nonbuild_ht:STRING,project_cost:STRING,project_type:STRING,sic_list:STRING,fatality:STRING,state_flag:STRING,abstract_text:STRING,load_dt:STRING",
}


def _schema_columns(schema: str) -> list[str]:
    columns: list[str] = []
    for token in schema.split(","):
        name = token.split(":", 1)[0].strip()
        if name:
            columns.append(name)
    return columns


@dataclass(frozen=True)
class IngestResult:
    rows_pulled: int
    loaded_to_bigquery: bool


def run_preflight_checks(config: PipelineConfig) -> None:
    if not config.api_key.strip():
        raise RuntimeError("DOL_API_KEY is missing. Set it in environment or .env.")
    config.paths.data_dir.mkdir(parents=True, exist_ok=True)
    report = validate_compliance(config.compliance)
    if report.checks_failed > 0:
        logging.warning(
            "Compliance checks had %s warnings (strict mode disabled).",
            report.checks_failed,
        )


def run_inspection_ingest(
    *,
    config: PipelineConfig,
    client: DolApiClient,
    geo_profile: str,
    table: str,
    csv_file: str,
    checkpoint_file: str,
    max_pages: int = 1,
) -> IngestResult:
    out_csv = config.paths.data_dir / csv_file
    checkpoint = config.paths.data_dir / checkpoint_file

    try:
        rows = query_inspection_incremental(
            client=client,
            geo_profile=geo_profile,
            out_csv=out_csv,
            checkpoint_path=checkpoint,
            since_date=config.since_date,
            limit=config.api_limit,
            max_pages=max_pages,
        )
    except Exception as exc:
        logging.warning(
            "Inspection pull failed for %s (%s). Falling back to existing CSV snapshot.",
            geo_profile,
            exc,
        )
        if not out_csv.exists():
            raise
        rows = 0

    if not out_csv.exists():
        raise RuntimeError(f"CSV not found for load step: {out_csv}")

    bq_load_csv(
        repo_root=config.paths.repo_root,
        project_id=config.project_id,
        dataset=config.dataset,
        table=table,
        csv_path=out_csv,
        autodetect=True,
    )
    return IngestResult(rows_pulled=rows, loaded_to_bigquery=True)


def run_enrichment_ingest(*, config: PipelineConfig, client: DolApiClient) -> None:
    endpoint_plan = [
        ("violation", "violation_recent.csv", "violation_recent", config.api_limit, config.api_max_pages),
        (
            "violation_event",
            "violation_event_recent.csv",
            "violation_event_recent",
            config.api_limit,
            config.api_max_pages,
        ),
        (
            "related_activity",
            "related_activity_recent.csv",
            "related_activity_recent",
            config.api_limit,
            config.api_max_pages,
        ),
        (
            "emphasis_codes",
            "emphasis_codes_recent.csv",
            "emphasis_codes_recent",
            config.api_limit,
            config.api_max_pages,
        ),
        (
            "accident_injury",
            "accident_injury_recent.csv",
            "accident_injury_recent",
            config.api_limit,
            config.api_max_pages,
        ),
        (
            "accident",
            "accident_recent.csv",
            "accident_recent",
            config.accident_api_limit,
            config.accident_api_max_pages,
        ),
    ]

    extraction_results: dict[str, bool] = {}
    filter_object = {"field": "load_dt", "operator": "gt", "value": config.since_date}

    for endpoint, csv_name, _table, limit, max_pages in endpoint_plan:
        out_csv = config.paths.data_dir / csv_name
        try:
            if endpoint == "accident":
                logging.info(
                    "[accident] using conservative settings limit=%s max_pages=%s",
                    limit,
                    max_pages,
                )
            query_endpoint_to_csv(
                client=client,
                endpoint=endpoint,
                out_csv=out_csv,
                limit=limit,
                max_pages=max_pages,
                sort="desc",
                sort_by="load_dt",
                filter_object=filter_object,
                append=False,
                columns_override=_schema_columns(ENDPOINT_SCHEMAS[endpoint]),
            )
            extraction_results[endpoint] = True
        except Exception as exc:
            extraction_results[endpoint] = False
            logging.warning("[%s] pull skipped due to API error: %s", endpoint, exc)

    logging.info(
        "Enrichment pull summary: %s",
        ", ".join(f"{name}={ok}" for name, ok in extraction_results.items()),
    )

    for endpoint, csv_name, table_name, _limit, _max_pages in endpoint_plan:
        csv_path = config.paths.data_dir / csv_name
        row_count = csv_data_row_count(csv_path)
        if row_count <= 0:
            logging.warning("[%s] CSV empty; skipping BigQuery load.", endpoint)
            continue
        bq_load_csv(
            repo_root=config.paths.repo_root,
            project_id=config.project_id,
            dataset=config.dataset,
            table=table_name,
            csv_path=csv_path,
            autodetect=False,
            schema=ENDPOINT_SCHEMAS[endpoint],
            allow_quoted_newlines=True,
        )

    # Use utf-8-sig so that a UTF-8 BOM at the start of the file is stripped
    # transparently; BigQuery rejects the BOM character (\357) as illegal input.
    refresh_sql = config.paths.sql_refresh_file.read_text(encoding="utf-8-sig")
    bq_query_sql(
        repo_root=config.paths.repo_root,
        project_id=config.project_id,
        sql_text=refresh_sql,
    )


def run_local_download_ingest(config: PipelineConfig) -> None:
    run_osha_local_downloads_ingest(config)


def run_epa_source_ingest(config: PipelineConfig) -> None:
    run_epa_signals_ingest(config)


def run_nih_source_ingest(config: PipelineConfig) -> None:
    run_nih_signals_ingest(config)


def run_ca_sos_source_ingest(config: PipelineConfig) -> None:
    run_ca_sos_signals_ingest(config)


def run_full_pipeline(config: PipelineConfig, client: DolApiClient) -> None:
    start = time.perf_counter()
    logging.info("OSHA full pipeline started.")

    logging.info("Stage 1/8: Ingest SoCal inspection.")
    run_inspection_ingest(
        config=config,
        client=client,
        geo_profile="socal",
        table="inspection_socal_incremental",
        csv_file="inspection_socal_incremental.csv",
        checkpoint_file="inspection_checkpoint.json",
        max_pages=1,
    )

    logging.info("Stage 2/8: Ingest Bay Area inspection.")
    run_inspection_ingest(
        config=config,
        client=client,
        geo_profile="bay_area",
        table="inspection_bayarea_incremental",
        csv_file="inspection_bayarea_incremental.csv",
        checkpoint_file="inspection_bayarea_checkpoint.json",
        max_pages=1,
    )

    logging.info("Stage 3/8: Ingest enrichment endpoints and refresh sales outputs.")
    run_enrichment_ingest(config=config, client=client)

    logging.info("Stage 4/8: Pull public enrichment signals (Census, BLS, USAspending).")
    try:
        run_public_signals_ingest(config)
    except Exception as exc:
        logging.warning(
            "Public signals stage failed; continuing with OSHA outputs only: %s",
            exc,
        )

    logging.info("Stage 5/8: Pull FDA signals (registration + 510(k) + PMA).")
    try:
        run_fda_signals_ingest(config)
    except Exception as exc:
        logging.warning(
            "FDA signals stage failed; continuing with OSHA/public outputs only: %s",
            exc,
        )

    logging.info("Stage 6/8: Pull EPA ECHO facility signals.")
    try:
        run_epa_signals_ingest(config)
    except Exception as exc:
        logging.warning(
            "EPA signals stage failed; continuing with current outputs only: %s",
            exc,
        )

    logging.info("Stage 7/8: Pull NIH RePORTER research signals.")
    try:
        run_nih_signals_ingest(config)
    except Exception as exc:
        logging.warning(
            "NIH signals stage failed; continuing with current outputs only: %s",
            exc,
        )

    logging.info("Stage 8/8: California SOS stage.")
    try:
        run_ca_sos_signals_ingest(config)
    except Exception as exc:
        logging.warning(
            "California SOS stage failed; continuing without state-entity enrichment: %s",
            exc,
        )

    elapsed = time.perf_counter() - start
    logging.info("OSHA multi-source sales pipeline finished in %.1fs.", elapsed)
