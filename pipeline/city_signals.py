from __future__ import annotations

import csv
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pipeline.bigquery import bq_load_csv
from pipeline.config import PipelineConfig
from pipeline.sql_refresh import run_sql_refresh


CITY_BIZ_LICENSE_SCHEMA = (
    "source:STRING,business_name:STRING,dba_name:STRING,"
    "address:STRING,city:STRING,state:STRING,zip_code:STRING,"
    "naics_code:STRING,naics_description:STRING,start_date:STRING,load_dt:STRING"
)

# Confirmed live Socrata endpoints (no key required)
SF_BIZ_LICENSE_URL = "https://data.sfgov.org/resource/g8m3-pdis.json"
LA_BIZ_LICENSE_URL = "https://data.lacity.org/resource/r4uk-afju.json"

# All active businesses: includes hazard-intensive industries + pharma/biotech/healthcare/professional services
# NAICS coverage: Food (20), Utilities (22), Chemical (25), Machinery (28), Computer/Electronics (34),
# Transportation (35-36), Wholesale (42), Information (51), Finance (52), Professional Services (54),
# Admin/Waste (56), Education (61), Healthcare/Social (62), Arts (71), Hospitality (72), Other (81)
_SF_SELECT = (
    "dba_name,ownership_name,full_business_address,city,state,"
    "business_zip,naic_code,naic_code_description,dba_start_date"
)
_SF_WHERE = (
    "dba_end_date IS NULL AND administratively_closed IS NULL"
)

_LA_SELECT = (
    "business_name,dba_name,street_address,city,zip_code,"
    "naics,primary_naics_description,location_start_date"
)
_LA_WHERE = (
    "location_end_date IS NULL"
)

_PAGE_SIZE = 2000
_MAX_PAGES = 50  # up to 100000 records per city to capture all industries


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _fetch_socrata_page(
    *,
    base_url: str,
    select: str,
    where: str,
    offset: int,
    limit: int,
    app_token: str,
    timeout_seconds: int = 60,
) -> list[dict[str, Any]]:
    params: dict[str, str] = {
        "$select": select,
        "$where": where,
        "$limit": str(limit),
        "$offset": str(offset),
        "$order": ":id",
    }
    if app_token:
        params["$$app_token"] = app_token
    url = f"{base_url}?{urlencode(params)}"
    req = Request(
        url=url,
        method="GET",
        headers={"Accept": "application/json", "User-Agent": "osha-city-signals/1.0"},
    )
    with urlopen(req, timeout=timeout_seconds) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
        return json.loads(raw)


def _fetch_all(
    *,
    source_name: str,
    base_url: str,
    select: str,
    where: str,
    app_token: str,
    max_pages: int,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for page in range(max_pages):
        offset = page * _PAGE_SIZE
        try:
            batch = _fetch_socrata_page(
                base_url=base_url,
                select=select,
                where=where,
                offset=offset,
                limit=_PAGE_SIZE,
                app_token=app_token,
            )
        except (HTTPError, URLError, OSError) as exc:
            logging.warning("[city_signals/%s] page %s fetch error: %s", source_name, page, exc)
            break
        records.extend(batch)
        logging.info(
            "[city_signals/%s] page %s: %s records (running total=%s)",
            source_name, page, len(batch), len(records),
        )
        if len(batch) < _PAGE_SIZE:
            break
    return records


def _normalize_sf(rec: dict[str, Any], load_dt: str) -> dict[str, str]:
    return {
        "source": "sf_biz_license",
        "business_name": str(rec.get("ownership_name") or "").strip(),
        "dba_name": str(rec.get("dba_name") or "").strip(),
        "address": str(rec.get("full_business_address") or "").strip(),
        "city": str(rec.get("city") or "").strip(),
        "state": str(rec.get("state") or "CA").strip(),
        "zip_code": str(rec.get("business_zip") or "").strip()[:5],
        "naics_code": str(rec.get("naic_code") or "").strip(),
        "naics_description": str(rec.get("naic_code_description") or "").strip(),
        "start_date": str(rec.get("dba_start_date") or "")[:10],
        "load_dt": load_dt,
    }


def _normalize_la(rec: dict[str, Any], load_dt: str) -> dict[str, str]:
    return {
        "source": "la_biz_license",
        "business_name": str(rec.get("business_name") or "").strip(),
        "dba_name": str(rec.get("dba_name") or "").strip(),
        "address": str(rec.get("street_address") or "").strip(),
        "city": str(rec.get("city") or "").strip(),
        "state": "CA",
        "zip_code": str(rec.get("zip_code") or "").strip().rstrip("-")[:5],
        "naics_code": str(rec.get("naics") or "").strip(),
        "naics_description": str(rec.get("primary_naics_description") or "").strip(),
        "start_date": str(rec.get("location_start_date") or "")[:10],
        "load_dt": load_dt,
    }


def run_city_signals_ingest(config: PipelineConfig) -> None:
    app_token: str = getattr(config, "socrata_app_token", "")
    load_dt = _utc_now_iso()
    out_csv = config.paths.data_dir / "city_biz_license_raw.csv"
    all_records: list[dict[str, str]] = []

    logging.info("[city_signals] Fetching SF business licenses (all active industries)...")
    sf_raw = _fetch_all(
        source_name="sf",
        base_url=SF_BIZ_LICENSE_URL,
        select=_SF_SELECT,
        where=_SF_WHERE,
        app_token=app_token,
        max_pages=_MAX_PAGES,
    )
    all_records.extend(_normalize_sf(r, load_dt) for r in sf_raw)
    logging.info("[city_signals] SF total: %s records", len(sf_raw))

    logging.info("[city_signals] Fetching LA business licenses (all active industries)...")
    la_raw = _fetch_all(
        source_name="la",
        base_url=LA_BIZ_LICENSE_URL,
        select=_LA_SELECT,
        where=_LA_WHERE,
        app_token=app_token,
        max_pages=_MAX_PAGES,
    )
    all_records.extend(_normalize_la(r, load_dt) for r in la_raw)
    logging.info("[city_signals] LA total: %s records", len(la_raw))

    if not all_records:
        logging.warning("[city_signals] No records fetched; skipping BQ load.")
        return

    fieldnames = [
        "source", "business_name", "dba_name", "address", "city", "state",
        "zip_code", "naics_code", "naics_description", "start_date", "load_dt",
    ]
    with out_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_records)

    logging.info("[city_signals] Wrote %s total rows to %s", len(all_records), out_csv)

    bq_load_csv(
        repo_root=config.paths.repo_root,
        project_id=config.city_signals_project_id,
        dataset=config.city_signals_dataset,
        table="city_biz_license_raw",
        csv_path=out_csv,
        autodetect=False,
        schema=CITY_BIZ_LICENSE_SCHEMA,
        allow_quoted_newlines=True,
    )

    run_sql_refresh(
        config=config,
        sql_filename="refresh_city_signals.sql",
        project_id=config.city_signals_project_id,
    )
