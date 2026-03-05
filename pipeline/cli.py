from __future__ import annotations

import argparse
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

from pipeline.compliance import ComplianceError
from pipeline.config import ApiSafetyConfig, PipelineConfig, load_pipeline_config
from pipeline.dol_api import DolApiClient
from pipeline.extract import query_endpoint_to_csv, query_inspection_incremental
from pipeline.http_client import RequestBudget, SafeHttpClient
from pipeline.logging_utils import configure_logging
from pipeline.rate_limit import GlobalRateLimiter
from pipeline.workflows import (
    run_enrichment_ingest,
    run_full_pipeline,
    run_inspection_ingest,
    run_public_signals_ingest,
    run_preflight_checks,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _apply_overrides(config: PipelineConfig, args: argparse.Namespace) -> PipelineConfig:
    project_id = getattr(args, "project_id", None) or config.project_id
    dataset = getattr(args, "dataset", None) or config.dataset
    since_date = getattr(args, "since_date", None) or config.since_date
    api_limit = getattr(args, "api_limit", None) or config.api_limit
    api_max_pages = getattr(args, "api_max_pages", None)
    if api_max_pages is None:
        api_max_pages = config.api_max_pages

    max_requests = getattr(args, "max_requests_per_run", None)
    if max_requests is None:
        max_requests = config.api_safety.max_requests_per_run

    user_agent = getattr(args, "user_agent", None) or config.api_safety.user_agent
    api_safety = replace(
        config.api_safety,
        max_requests_per_run=max_requests,
        user_agent=user_agent,
    )

    return replace(
        config,
        project_id=project_id,
        dataset=dataset,
        since_date=since_date,
        api_limit=api_limit,
        api_max_pages=api_max_pages,
        api_safety=api_safety,
    )


def _build_client(config: PipelineConfig) -> DolApiClient:
    limiter = GlobalRateLimiter(
        min_interval_seconds=config.api_safety.min_interval_seconds,
        state_file=config.api_safety.state_file,
        lock_file=config.api_safety.lock_file,
    )
    budget = RequestBudget(max_requests=config.api_safety.max_requests_per_run)
    http = SafeHttpClient(
        rate_limiter=limiter,
        timeout_seconds=config.api_safety.timeout_seconds,
        max_retries=config.api_safety.max_retries,
        base_backoff_seconds=config.api_safety.base_backoff_seconds,
        request_budget=budget,
        user_agent=config.api_safety.user_agent,
    )
    return DolApiClient(api_key=config.api_key, http_client=http)


def _add_common_run_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--project-id", default=None)
    parser.add_argument("--dataset", default=None)
    parser.add_argument("--since-date", default=None)
    parser.add_argument("--api-limit", type=int, default=None)
    parser.add_argument("--api-max-pages", type=int, default=None)
    parser.add_argument("--max-requests-per-run", type=int, default=None)
    parser.add_argument("--user-agent", default=None)


def _add_global_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--verbose", action="store_true")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OSHA Python pipeline CLI")
    _add_global_args(parser)
    sub = parser.add_subparsers(dest="command", required=True)

    run_full = sub.add_parser("run-full", help="Run full 3-stage daily pipeline")
    _add_common_run_args(run_full)

    ingest_socal = sub.add_parser("ingest-socal", help="Run SoCal inspection ingest")
    _add_common_run_args(ingest_socal)

    ingest_bay = sub.add_parser("ingest-bayarea", help="Run Bay Area inspection ingest")
    _add_common_run_args(ingest_bay)

    ingest_enrich = sub.add_parser(
        "ingest-enrichment",
        help="Run enrichment endpoint ingest + SQL refresh",
    )
    _add_common_run_args(ingest_enrich)

    ingest_public = sub.add_parser(
        "ingest-public-signals",
        help="Pull Census/BLS/USAspending public signals and load BigQuery tables",
    )
    _add_common_run_args(ingest_public)

    q_inspection = sub.add_parser(
        "query-inspection",
        help="Pull inspection endpoint incrementally to CSV",
    )
    q_inspection.add_argument("--geo-profile", choices=["socal", "bay_area"], required=True)
    q_inspection.add_argument("--out-csv", required=True)
    q_inspection.add_argument("--checkpoint-path", required=True)
    q_inspection.add_argument("--since-date", default=None)
    q_inspection.add_argument("--api-limit", type=int, default=5000)
    q_inspection.add_argument("--api-max-pages", type=int, default=0)
    q_inspection.add_argument("--reset-checkpoint", action="store_true")
    q_inspection.add_argument("--max-requests-per-run", type=int, default=None)
    q_inspection.add_argument("--user-agent", default=None)

    q_endpoint = sub.add_parser("query-endpoint", help="Pull a generic OSHA endpoint to CSV")
    q_endpoint.add_argument("--endpoint", required=True)
    q_endpoint.add_argument("--out-csv", required=True)
    q_endpoint.add_argument("--api-limit", type=int, default=5000)
    q_endpoint.add_argument("--api-max-pages", type=int, default=0)
    q_endpoint.add_argument("--sort", default="desc")
    q_endpoint.add_argument("--sort-by", default="load_dt")
    q_endpoint.add_argument("--filter-object-json", default="")
    q_endpoint.add_argument("--append", action="store_true")
    q_endpoint.add_argument("--max-requests-per-run", type=int, default=None)
    q_endpoint.add_argument("--user-agent", default=None)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    configure_logging(verbose=args.verbose)

    config = load_pipeline_config(_repo_root())
    config = _apply_overrides(config, args)
    client = _build_client(config)

    dol_commands = {
        "run-full",
        "ingest-socal",
        "ingest-bayarea",
        "ingest-enrichment",
        "query-inspection",
        "query-endpoint",
    }
    if args.command in dol_commands:
        try:
            run_preflight_checks(config)
        except ComplianceError:
            raise
    else:
        config.paths.data_dir.mkdir(parents=True, exist_ok=True)

    if args.command == "run-full":
        run_full_pipeline(config, client)
        return 0

    if args.command == "ingest-socal":
        run_inspection_ingest(
            config=config,
            client=client,
            geo_profile="socal",
            table="inspection_socal_incremental",
            csv_file="inspection_socal_incremental.csv",
            checkpoint_file="inspection_checkpoint.json",
            max_pages=1,
        )
        return 0

    if args.command == "ingest-bayarea":
        run_inspection_ingest(
            config=config,
            client=client,
            geo_profile="bay_area",
            table="inspection_bayarea_incremental",
            csv_file="inspection_bayarea_incremental.csv",
            checkpoint_file="inspection_bayarea_checkpoint.json",
            max_pages=1,
        )
        return 0

    if args.command == "ingest-enrichment":
        run_enrichment_ingest(config=config, client=client)
        return 0

    if args.command == "ingest-public-signals":
        run_public_signals_ingest(config)
        return 0

    if args.command == "query-inspection":
        query_inspection_incremental(
            client=client,
            geo_profile=args.geo_profile,
            out_csv=Path(args.out_csv),
            checkpoint_path=Path(args.checkpoint_path),
            since_date=args.since_date or config.since_date,
            limit=args.api_limit,
            max_pages=args.api_max_pages,
            reset_checkpoint=args.reset_checkpoint,
        )
        return 0

    if args.command == "query-endpoint":
        filter_object: dict[str, Any] | None = None
        if args.filter_object_json.strip():
            filter_object = json.loads(args.filter_object_json)
        query_endpoint_to_csv(
            client=client,
            endpoint=args.endpoint,
            out_csv=Path(args.out_csv),
            limit=args.api_limit,
            max_pages=args.api_max_pages,
            sort=args.sort,
            sort_by=args.sort_by,
            filter_object=filter_object,
            append=args.append,
        )
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

