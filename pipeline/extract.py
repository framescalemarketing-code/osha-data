from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from pipeline.checkpoints import read_last_load_dt, write_checkpoint
from pipeline.csv_utils import ensure_header_only, write_rows
from pipeline.dol_api import DolApiClient, Pagination


INSPECTION_COLUMNS = [
    "activity_nr",
    "reporting_id",
    "state_flag",
    "estab_name",
    "site_address",
    "site_city",
    "site_state",
    "site_zip",
    "owner_type",
    "owner_code",
    "adv_notice",
    "safety_hlth",
    "sic_code",
    "naics_code",
    "insp_type",
    "insp_scope",
    "why_no_insp",
    "union_status",
    "safety_manuf",
    "safety_const",
    "safety_marit",
    "health_manuf",
    "health_const",
    "health_marit",
    "migrant",
    "mail_street",
    "mail_city",
    "mail_state",
    "mail_zip",
    "host_est_key",
    "nr_in_estab",
    "open_date",
    "case_mod_date",
    "close_conf_date",
    "close_case_date",
    "load_dt",
]


def _geo_conditions(geo_profile: str) -> list[dict[str, Any]]:
    if geo_profile == "socal":
        return [
            {"field": "site_state", "operator": "eq", "value": "CA"},
            {"field": "site_zip", "operator": "gt", "value": "89999"},
            {"field": "site_zip", "operator": "lt", "value": "93600"},
        ]
    if geo_profile == "bay_area":
        return [
            {"field": "site_state", "operator": "eq", "value": "CA"},
            {
                "or": [
                    {
                        "and": [
                            {"field": "site_zip", "operator": "gt", "value": "93999"},
                            {"field": "site_zip", "operator": "lt", "value": "94200"},
                        ]
                    },
                    {
                        "and": [
                            {"field": "site_zip", "operator": "gt", "value": "94299"},
                            {"field": "site_zip", "operator": "lt", "value": "95200"},
                        ]
                    },
                    {
                        "and": [
                            {"field": "site_zip", "operator": "gt", "value": "95399"},
                            {"field": "site_zip", "operator": "lt", "value": "95500"},
                        ]
                    },
                ]
            },
        ]
    raise ValueError("geo_profile must be one of: socal, bay_area")


def build_inspection_filter(
    *, geo_profile: str, since_date: str, checkpoint_load_dt: str | None
) -> dict[str, Any]:
    conditions = _geo_conditions(geo_profile)
    conditions.append({"field": "close_case_date", "operator": "gt", "value": since_date})
    if checkpoint_load_dt:
        conditions.append({"field": "load_dt", "operator": "gt", "value": checkpoint_load_dt})
    return {"and": conditions}


def query_inspection_incremental(
    *,
    client: DolApiClient,
    geo_profile: str,
    out_csv: Path,
    checkpoint_path: Path,
    since_date: str,
    limit: int,
    max_pages: int,
    reset_checkpoint: bool = False,
) -> int:
    if reset_checkpoint and checkpoint_path.exists():
        checkpoint_path.unlink()

    checkpoint = read_last_load_dt(checkpoint_path)
    filter_object = build_inspection_filter(
        geo_profile=geo_profile, since_date=since_date, checkpoint_load_dt=checkpoint
    )

    logging.info(
        "Inspection pull start: profile=%s limit=%s max_pages=%s since=%s checkpoint=%s",
        geo_profile,
        limit,
        max_pages,
        since_date,
        checkpoint,
    )

    pages = client.iter_pages(
        "inspection",
        pagination=Pagination(limit=limit, max_pages=max_pages),
        sort="asc",
        sort_by="load_dt",
        filter_object=filter_object,
        label=f"inspection {geo_profile}",
    )
    if not pages:
        logging.info("Inspection pull returned no new rows.")
        return 0

    latest_load_dt = checkpoint
    total_rows = 0
    for page in pages:
        write_rows(
            out_csv,
            rows=page,
            columns=INSPECTION_COLUMNS,
            append=True,
        )
        total_rows += len(page)
        for row in page:
            load_dt = str(row.get("load_dt", "")).strip()
            if not load_dt:
                continue
            if latest_load_dt is None or load_dt > latest_load_dt:
                latest_load_dt = load_dt

        if latest_load_dt:
            write_checkpoint(
                checkpoint_path,
                last_load_dt=latest_load_dt,
                close_case_since=since_date,
                limit=limit,
                sort_by="load_dt",
            )

    logging.info("Inspection pull complete: %s new rows written to %s", total_rows, out_csv)
    return total_rows


def query_endpoint_to_csv(
    *,
    client: DolApiClient,
    endpoint: str,
    out_csv: Path,
    limit: int,
    max_pages: int,
    sort: str = "desc",
    sort_by: str = "load_dt",
    filter_object: dict[str, Any] | None = None,
    append: bool = False,
) -> int:
    columns = client.get_metadata_columns(endpoint)
    if not append and out_csv.exists():
        out_csv.unlink()

    pages = client.iter_pages(
        endpoint,
        pagination=Pagination(limit=limit, max_pages=max_pages),
        sort=sort,
        sort_by=sort_by,
        filter_object=filter_object,
        label=f"{endpoint} data",
    )

    total_rows = 0
    wrote_any = False
    for page in pages:
        wrote = write_rows(out_csv, rows=page, columns=columns, append=True)
        wrote_any = True
        total_rows += wrote

    if not wrote_any:
        ensure_header_only(out_csv, columns)

    logging.info("[%s] extraction complete rows=%s path=%s", endpoint, total_rows, out_csv)
    return total_rows

