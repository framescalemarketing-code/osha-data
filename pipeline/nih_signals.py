from __future__ import annotations

import csv
import json
import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from pipeline.bigquery import bq_load_csv, bq_query_sql
from pipeline.config import PipelineConfig
from pipeline.regions import region_label_from_zip
from pipeline.sql_refresh import run_sql_refresh


NIH_SCHEMA = (
    "region_label:STRING,appl_id:STRING,project_num:STRING,project_title:STRING,org_name:STRING,"
    "org_city:STRING,org_state:STRING,org_zipcode:STRING,org_country:STRING,org_type:STRING,"
    "activity_code:STRING,fiscal_year:STRING,award_amount:STRING,direct_cost_amt:STRING,"
    "indirect_cost_amt:STRING,award_notice_date:STRING,project_start_date:STRING,"
    "project_end_date:STRING,principal_investigators:STRING,agency_ic_admin:STRING,"
    "project_terms:STRING,abstract_text:STRING,load_dt:STRING"
)

_NIH_QUERY_URL = "https://api.reporter.nih.gov/v2/projects/search"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _http_json(
    *,
    body: dict[str, Any],
    timeout_seconds: int = 90,
    max_retries: int = 4,
    base_backoff_seconds: float = 1.0,
) -> Any:
    payload = json.dumps(body).encode("utf-8")
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "osha-nih-signals/1.0",
    }
    for attempt in range(1, max_retries + 2):
        req = Request(url=_NIH_QUERY_URL, method="POST", data=payload, headers=headers)
        try:
            with urlopen(req, timeout=timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8", errors="replace"))
        except HTTPError as exc:
            if attempt > max_retries:
                raise RuntimeError(f"HTTP {exc.code} calling {_NIH_QUERY_URL}") from exc
            sleep_s = base_backoff_seconds * (2 ** (attempt - 1))
            logging.warning(
                "HTTP %s calling NIH RePORTER (attempt %s/%s), retrying in %.1fs",
                exc.code,
                attempt,
                max_retries + 1,
                sleep_s,
            )
            time.sleep(sleep_s)
        except URLError as exc:
            if attempt > max_retries:
                raise RuntimeError(f"Network error calling {_NIH_QUERY_URL}: {exc}") from exc
            sleep_s = base_backoff_seconds * (2 ** (attempt - 1))
            logging.warning(
                "Network error calling NIH RePORTER (attempt %s/%s), retrying in %.1fs: %s",
                attempt,
                max_retries + 1,
                sleep_s,
                exc,
            )
            time.sleep(sleep_s)
    raise RuntimeError("Failed to call NIH RePORTER API")


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)
    return len(rows)


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _join_text_list(value: Any) -> str:
    if isinstance(value, list):
        items = [_text(item) for item in value if _text(item)]
        return " | ".join(items)
    return _text(value)


def _org(result: dict[str, Any]) -> dict[str, Any]:
    org = result.get("organization", {})
    return org if isinstance(org, dict) else {}


def _organization_type(result: dict[str, Any], organization: dict[str, Any]) -> str:
    org_type = result.get("organization_type")
    if isinstance(org_type, dict):
        return _text(org_type.get("name") or org_type.get("code"))
    return _text(
        organization.get("org_type")
        or organization.get("org_class")
        or org_type
    )


def _principal_investigators(result: dict[str, Any]) -> str:
    investigators = result.get("principal_investigators", [])
    if not isinstance(investigators, list):
        return ""
    names = []
    for item in investigators:
        if isinstance(item, dict):
            full_name = _text(item.get("full_name") or item.get("name"))
            if full_name:
                names.append(full_name)
    return " | ".join(names[:6])


def _project_terms(result: dict[str, Any]) -> str:
    terms = result.get("project_terms", [])
    if not isinstance(terms, list):
        return _text(terms)
    names = []
    for item in terms:
        if isinstance(item, dict):
            term = _text(item.get("term"))
            if term:
                names.append(term)
        else:
            term = _text(item)
            if term:
                names.append(term)
    return " | ".join(names[:20])


def fetch_nih_projects(*, out_csv: Path, lookback_years: int, since_date: str) -> int:
    current_year = date.today().year
    try:
        min_year = date.fromisoformat(since_date).year
    except (TypeError, ValueError):
        min_year = current_year - max(lookback_years, 1) + 1
    min_year = max(min_year, 1900)
    fiscal_years = list(range(min_year, current_year + 1))
    limit = 250
    load_dt = _utc_now_iso()
    out_rows: list[list[str]] = []

    for fiscal_year in reversed(fiscal_years):
        offset = 0
        while True:
            body = {
                "criteria": {
                    "org_states": ["CA"],
                    "fiscal_years": [fiscal_year],
                    "sub_project_only": False,
                },
                "offset": offset,
                "limit": limit,
                "sort_field": "award_notice_date",
                "sort_order": "desc",
            }
            payload = _http_json(body=body)
            results = payload.get("results", []) if isinstance(payload, dict) else []
            if not isinstance(results, list) or not results:
                break

            for result in results:
                if not isinstance(result, dict):
                    continue
                organization = _org(result)
                zip_code = _text(
                    organization.get("org_zipcode")
                    or organization.get("zip_code")
                    or organization.get("zipcode")
                )
                region_label = region_label_from_zip(zip_code)
                if not region_label:
                    continue

                out_rows.append(
                    [
                        region_label,
                        _text(result.get("appl_id")),
                        _text(result.get("project_num") or result.get("core_project_num")),
                        _text(result.get("project_title")),
                        _text(organization.get("org_name") or organization.get("name")),
                        _text(organization.get("org_city") or organization.get("city")),
                        _text(organization.get("org_state") or organization.get("state")),
                        zip_code,
                        _text(organization.get("org_country") or organization.get("country")),
                        _organization_type(result, organization),
                        _text(result.get("activity_code")),
                        _text(result.get("fiscal_year")),
                        _text(result.get("award_amount")),
                        _text(result.get("direct_cost_amt")),
                        _text(result.get("indirect_cost_amt")),
                        _text(result.get("award_notice_date")),
                        _text(result.get("project_start_date") or result.get("budget_start")),
                        _text(result.get("project_end_date") or result.get("budget_end")),
                        _principal_investigators(result),
                        _join_text_list(result.get("agency_ic_admin")),
                        _project_terms(result) or _project_terms({"project_terms": result.get("pref_terms")}),
                        _text(result.get("abstract_text") or result.get("phr_text")),
                        load_dt,
                    ]
                )

            if len(results) < limit:
                break
            offset += limit
            time.sleep(1.1)

    return _write_csv(
        out_csv,
        [
            "region_label",
            "appl_id",
            "project_num",
            "project_title",
            "org_name",
            "org_city",
            "org_state",
            "org_zipcode",
            "org_country",
            "org_type",
            "activity_code",
            "fiscal_year",
            "award_amount",
            "direct_cost_amt",
            "indirect_cost_amt",
            "award_notice_date",
            "project_start_date",
            "project_end_date",
            "principal_investigators",
            "agency_ic_admin",
            "project_terms",
            "abstract_text",
            "load_dt",
        ],
        out_rows,
    )


def run_nih_signals_ingest(config: PipelineConfig) -> None:
    data_dir = config.paths.data_dir
    out_csv = data_dir / "nih_project_raw.csv"

    logging.info("Pulling NIH RePORTER project signals for California organizations...")
    row_count = fetch_nih_projects(
        out_csv=out_csv,
        lookback_years=config.nih_lookback_years,
        since_date=config.since_date,
    )
    logging.info("NIH project rows written: %s", row_count)

    bq_query_sql(
        repo_root=config.paths.repo_root,
        project_id=config.nih_project_id,
        sql_text=f"CREATE SCHEMA IF NOT EXISTS `{config.nih_project_id}.{config.nih_dataset}`;",
    )

    if row_count > 0:
        bq_load_csv(
            repo_root=config.paths.repo_root,
            project_id=config.nih_project_id,
            dataset=config.nih_dataset,
            table="nih_project_raw",
            csv_path=out_csv,
            autodetect=False,
            schema=NIH_SCHEMA,
            allow_quoted_newlines=True,
        )

    logging.info("Refreshing NIH source scoring tables...")
    run_sql_refresh(
        config=config,
        sql_filename="refresh_nih_followup.sql",
        project_id=config.nih_project_id,
    )

    logging.info("Refreshing cross-source sales priority outputs...")
    run_sql_refresh(
        config=config,
        sql_filename="refresh_sales_priority_outputs.sql",
        project_id=config.project_id,
    )
