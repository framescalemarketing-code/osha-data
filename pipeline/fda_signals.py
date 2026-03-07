from __future__ import annotations

import csv
import json
import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pipeline.bigquery import bq_load_csv, bq_query_sql
from pipeline.config import PipelineConfig
from pipeline.regions import BAY_AREA_ZIP_PREFIXES, SAN_DIEGO_ZIP_PREFIXES
from pipeline.sql_refresh import run_sql_refresh


REGISTRATION_SCHEMA = (
    "region_label:STRING,registration_number:STRING,fei_number:STRING,status_code:STRING,"
    "reg_expiry_year:STRING,initial_importer_flag:STRING,facility_name:STRING,"
    "address_line_1:STRING,address_line_2:STRING,city:STRING,state_code:STRING,"
    "zip_code:STRING,country_code:STRING,owner_operator_number:STRING,"
    "owner_operator_firm_name:STRING,establishment_types:STRING,proprietary_names:STRING,"
    "product_code:STRING,product_created_date:STRING,device_name:STRING,medical_specialty:STRING,"
    "regulation_number:STRING,device_class:STRING,k_number:STRING,pma_number:STRING,"
    "source_last_updated:STRING,load_dt:STRING"
)

K510_SCHEMA = (
    "k_number:STRING,applicant:STRING,city:STRING,state:STRING,zip_code:STRING,country_code:STRING,"
    "decision_date:STRING,date_received:STRING,decision_code:STRING,decision_description:STRING,"
    "clearance_type:STRING,product_code:STRING,device_name:STRING,medical_specialty:STRING,"
    "device_class:STRING,advisory_committee_description:STRING,join_key_type:STRING,"
    "join_key:STRING,source_last_updated:STRING,load_dt:STRING"
)

PMA_SCHEMA = (
    "pma_number:STRING,supplement_number:STRING,applicant:STRING,city:STRING,state:STRING,zip:STRING,"
    "decision_date:STRING,date_received:STRING,decision_code:STRING,supplement_type:STRING,"
    "supplement_reason:STRING,product_code:STRING,trade_name:STRING,generic_name:STRING,"
    "medical_specialty:STRING,device_class:STRING,advisory_committee_description:STRING,"
    "join_key_type:STRING,join_key:STRING,source_last_updated:STRING,load_dt:STRING"
)

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _http_json(
    *,
    url: str,
    timeout_seconds: int = 60,
    max_retries: int = 4,
    base_backoff_seconds: float = 1.0,
) -> Any:
    headers = {"Accept": "application/json", "User-Agent": "osha-fda-signals/1.0"}
    for attempt in range(1, max_retries + 2):
        req = Request(url=url, method="GET", headers=headers)
        try:
            with urlopen(req, timeout=timeout_seconds) as response:
                raw = response.read().decode("utf-8", errors="replace")
                return json.loads(raw)
        except HTTPError as exc:
            if attempt > max_retries:
                raise RuntimeError(f"HTTP {exc.code} calling {url}") from exc
            sleep_s = base_backoff_seconds * (2 ** (attempt - 1))
            logging.warning(
                "HTTP %s calling %s (attempt %s/%s), retrying in %.1fs",
                exc.code,
                url,
                attempt,
                max_retries + 1,
                sleep_s,
            )
            time.sleep(sleep_s)
        except URLError as exc:
            if attempt > max_retries:
                raise RuntimeError(f"Network error calling {url}: {exc}") from exc
            sleep_s = base_backoff_seconds * (2 ** (attempt - 1))
            logging.warning(
                "Network error calling %s (attempt %s/%s), retrying in %.1fs: %s",
                url,
                attempt,
                max_retries + 1,
                sleep_s,
                exc,
            )
            time.sleep(sleep_s)

    raise RuntimeError(f"Failed to call {url}")


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


def _text_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        cleaned = [_text(v) for v in value]
        return [v for v in cleaned if v]
    single = _text(value)
    return [single] if single else []


def _join(values: Any, sep: str = " | ") -> str:
    items = _text_list(values)
    if not items:
        return ""
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return sep.join(out)


def _with_api_key(params: dict[str, Any], api_key: str) -> dict[str, Any]:
    merged = dict(params)
    if api_key.strip():
        merged["api_key"] = api_key.strip()
    return merged


def _openfda_search_all(
    *,
    endpoint: str,
    search: str,
    api_key: str,
    limit: int = 100,
    max_pages: int = 500,
) -> tuple[list[dict[str, Any]], str]:
    all_rows: list[dict[str, Any]] = []
    skip = 0
    pages = 0
    last_updated = ""

    while True:
        params = _with_api_key(
            {
                "search": search,
                "limit": min(max(limit, 1), 100),
                "skip": skip,
            },
            api_key,
        )
        url = f"https://api.fda.gov/{endpoint}.json?{urlencode(params)}"
        payload = _http_json(url=url, timeout_seconds=90)
        meta = payload.get("meta", {}) if isinstance(payload, dict) else {}
        results_meta = meta.get("results", {}) if isinstance(meta, dict) else {}
        last_updated = _text(meta.get("last_updated"))
        rows = payload.get("results", []) if isinstance(payload, dict) else []
        if not isinstance(rows, list) or not rows:
            break

        all_rows.extend(rows)
        pages += 1
        if pages >= max_pages:
            logging.warning(
                "OpenFDA %s pagination stopped at page=%s (max_pages cap reached).",
                endpoint,
                pages,
            )
            break

        returned = len(rows)
        total = int(results_meta.get("total", 0) or 0)
        skip += returned
        if returned < limit:
            break
        if total > 0 and skip >= total:
            break

    return all_rows, last_updated


def _zip_prefix_query(prefixes: tuple[str, ...]) -> str:
    zip_terms = " OR ".join(f"registration.zip_code:{prefix}*" for prefix in prefixes)
    return f"registration.state_code:CA AND ({zip_terms})"


def _extract_join_keys(openfda_obj: Any) -> list[tuple[str, str]]:
    if not isinstance(openfda_obj, dict):
        return [("none", "")]
    registrations = [v for v in _text_list(openfda_obj.get("registration_number")) if v]
    fei_numbers = [v for v in _text_list(openfda_obj.get("fei_number")) if v]

    out: list[tuple[str, str]] = []
    out.extend([("registration", v) for v in registrations])
    out.extend([("fei", v) for v in fei_numbers])
    if not out:
        return [("none", "")]

    seen: set[tuple[str, str]] = set()
    deduped: list[tuple[str, str]] = []
    for item in out:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def fetch_device_registration_regions(
    *,
    api_key: str,
    out_csv: Path,
) -> int:
    load_dt = _utc_now_iso()
    out_rows: list[list[str]] = []

    region_searches = [
        ("San Diego", _zip_prefix_query(SAN_DIEGO_ZIP_PREFIXES)),
        ("Bay Area", _zip_prefix_query(BAY_AREA_ZIP_PREFIXES)),
    ]

    for region_label, search in region_searches:
        rows, source_last_updated = _openfda_search_all(
            endpoint="device/registrationlisting",
            search=search,
            api_key=api_key,
        )
        logging.info(
            "OpenFDA device registration rows pulled for %s: %s",
            region_label,
            len(rows),
        )

        for rec in rows:
            if not isinstance(rec, dict):
                continue
            reg = rec.get("registration", {})
            if not isinstance(reg, dict):
                reg = {}
            owner = reg.get("owner_operator", {})
            if not isinstance(owner, dict):
                owner = {}
            products = rec.get("products", [])
            if not isinstance(products, list) or not products:
                products = [{}]

            for product in products:
                if not isinstance(product, dict):
                    product = {}
                openfda = product.get("openfda", {})
                if not isinstance(openfda, dict):
                    openfda = {}
                out_rows.append(
                    [
                        region_label,
                        _text(reg.get("registration_number")),
                        _text(reg.get("fei_number")),
                        _text(reg.get("status_code")),
                        _text(reg.get("reg_expiry_date_year")),
                        _text(reg.get("initial_importer_flag")),
                        _text(reg.get("name")),
                        _text(reg.get("address_line_1")),
                        _text(reg.get("address_line_2")),
                        _text(reg.get("city")),
                        _text(reg.get("state_code")),
                        _text(reg.get("zip_code")),
                        _text(reg.get("iso_country_code")),
                        _text(owner.get("owner_operator_number")),
                        _text(owner.get("firm_name")),
                        _join(rec.get("establishment_type")),
                        _join(rec.get("proprietary_name")),
                        _text(product.get("product_code")),
                        _text(product.get("created_date")),
                        _join(openfda.get("device_name")),
                        _join(openfda.get("medical_specialty_description")),
                        _join(openfda.get("regulation_number")),
                        _join(openfda.get("device_class")),
                        _text(rec.get("k_number")),
                        _text(rec.get("pma_number")),
                        source_last_updated,
                        load_dt,
                    ]
                )

    return _write_csv(
        out_csv,
        [
            "region_label",
            "registration_number",
            "fei_number",
            "status_code",
            "reg_expiry_year",
            "initial_importer_flag",
            "facility_name",
            "address_line_1",
            "address_line_2",
            "city",
            "state_code",
            "zip_code",
            "country_code",
            "owner_operator_number",
            "owner_operator_firm_name",
            "establishment_types",
            "proprietary_names",
            "product_code",
            "product_created_date",
            "device_name",
            "medical_specialty",
            "regulation_number",
            "device_class",
            "k_number",
            "pma_number",
            "source_last_updated",
            "load_dt",
        ],
        out_rows,
    )


def fetch_device_510k_ca(
    *,
    api_key: str,
    out_csv: Path,
    lookback_years: int,
) -> int:
    today = date.today()
    start_date = date(today.year - max(lookback_years, 1), 1, 1)
    search = f"state:CA AND decision_date:[{start_date.isoformat()} TO {today.isoformat()}]"
    rows, source_last_updated = _openfda_search_all(
        endpoint="device/510k",
        search=search,
        api_key=api_key,
    )
    load_dt = _utc_now_iso()
    out_rows: list[list[str]] = []

    for rec in rows:
        if not isinstance(rec, dict):
            continue
        openfda = rec.get("openfda", {})
        join_keys = _extract_join_keys(openfda)
        for join_key_type, join_key in join_keys:
            out_rows.append(
                [
                    _text(rec.get("k_number")),
                    _text(rec.get("applicant")),
                    _text(rec.get("city")),
                    _text(rec.get("state")),
                    _text(rec.get("zip_code")),
                    _text(rec.get("country_code")),
                    _text(rec.get("decision_date")),
                    _text(rec.get("date_received")),
                    _text(rec.get("decision_code")),
                    _text(rec.get("decision_description")),
                    _text(rec.get("clearance_type")),
                    _text(rec.get("product_code")),
                    _text(rec.get("device_name")),
                    _join(openfda.get("medical_specialty_description")),
                    _join(openfda.get("device_class")),
                    _text(rec.get("advisory_committee_description")),
                    join_key_type,
                    join_key,
                    source_last_updated,
                    load_dt,
                ]
            )

    return _write_csv(
        out_csv,
        [
            "k_number",
            "applicant",
            "city",
            "state",
            "zip_code",
            "country_code",
            "decision_date",
            "date_received",
            "decision_code",
            "decision_description",
            "clearance_type",
            "product_code",
            "device_name",
            "medical_specialty",
            "device_class",
            "advisory_committee_description",
            "join_key_type",
            "join_key",
            "source_last_updated",
            "load_dt",
        ],
        out_rows,
    )


def fetch_device_pma_ca(
    *,
    api_key: str,
    out_csv: Path,
    lookback_years: int,
) -> int:
    today = date.today()
    start_date = date(today.year - max(lookback_years, 1), 1, 1)
    search = f"state:CA AND decision_date:[{start_date.isoformat()} TO {today.isoformat()}]"
    rows, source_last_updated = _openfda_search_all(
        endpoint="device/pma",
        search=search,
        api_key=api_key,
    )
    load_dt = _utc_now_iso()
    out_rows: list[list[str]] = []

    for rec in rows:
        if not isinstance(rec, dict):
            continue
        openfda = rec.get("openfda", {})
        join_keys = _extract_join_keys(openfda)
        for join_key_type, join_key in join_keys:
            out_rows.append(
                [
                    _text(rec.get("pma_number")),
                    _text(rec.get("supplement_number")),
                    _text(rec.get("applicant")),
                    _text(rec.get("city")),
                    _text(rec.get("state")),
                    _text(rec.get("zip")),
                    _text(rec.get("decision_date")),
                    _text(rec.get("date_received")),
                    _text(rec.get("decision_code")),
                    _text(rec.get("supplement_type")),
                    _text(rec.get("supplement_reason")),
                    _text(rec.get("product_code")),
                    _text(rec.get("trade_name")),
                    _text(rec.get("generic_name")),
                    _join(openfda.get("medical_specialty_description")),
                    _join(openfda.get("device_class")),
                    _text(rec.get("advisory_committee_description")),
                    join_key_type,
                    join_key,
                    source_last_updated,
                    load_dt,
                ]
            )

    return _write_csv(
        out_csv,
        [
            "pma_number",
            "supplement_number",
            "applicant",
            "city",
            "state",
            "zip",
            "decision_date",
            "date_received",
            "decision_code",
            "supplement_type",
            "supplement_reason",
            "product_code",
            "trade_name",
            "generic_name",
            "medical_specialty",
            "device_class",
            "advisory_committee_description",
            "join_key_type",
            "join_key",
            "source_last_updated",
            "load_dt",
        ],
        out_rows,
    )

def run_fda_signals_ingest(config: PipelineConfig) -> None:
    data_dir = config.paths.data_dir
    reg_csv = data_dir / "fda_device_registration_raw.csv"
    k510_csv = data_dir / "fda_device_510k_raw.csv"
    pma_csv = data_dir / "fda_device_pma_raw.csv"

    logging.info("Pulling OpenFDA device registration listings for target regions...")
    reg_rows = fetch_device_registration_regions(
        api_key=config.openfda_api_key,
        out_csv=reg_csv,
    )
    logging.info("OpenFDA registration rows written: %s", reg_rows)

    logging.info("Pulling OpenFDA 510(k) activity for CA...")
    k510_rows = fetch_device_510k_ca(
        api_key=config.openfda_api_key,
        out_csv=k510_csv,
        lookback_years=config.fda_lookback_years,
    )
    logging.info("OpenFDA 510(k) rows written: %s", k510_rows)

    logging.info("Pulling OpenFDA PMA activity for CA...")
    pma_rows = fetch_device_pma_ca(
        api_key=config.openfda_api_key,
        out_csv=pma_csv,
        lookback_years=config.fda_lookback_years,
    )
    logging.info("OpenFDA PMA rows written: %s", pma_rows)

    bq_query_sql(
        repo_root=config.paths.repo_root,
        project_id=config.fda_project_id,
        sql_text=f"CREATE SCHEMA IF NOT EXISTS `{config.fda_project_id}.{config.fda_dataset}`;",
    )

    if reg_rows > 0:
        bq_load_csv(
            repo_root=config.paths.repo_root,
            project_id=config.fda_project_id,
            dataset=config.fda_dataset,
            table="fda_device_registration_raw",
            csv_path=reg_csv,
            autodetect=False,
            schema=REGISTRATION_SCHEMA,
        )

    if k510_rows > 0:
        bq_load_csv(
            repo_root=config.paths.repo_root,
            project_id=config.fda_project_id,
            dataset=config.fda_dataset,
            table="fda_device_510k_raw",
            csv_path=k510_csv,
            autodetect=False,
            schema=K510_SCHEMA,
        )

    if pma_rows > 0:
        bq_load_csv(
            repo_root=config.paths.repo_root,
            project_id=config.fda_project_id,
            dataset=config.fda_dataset,
            table="fda_device_pma_raw",
            csv_path=pma_csv,
            autodetect=False,
            schema=PMA_SCHEMA,
        )

    logging.info("Refreshing FDA source scoring tables...")
    run_sql_refresh(
        config=config,
        sql_filename="refresh_fda_followup.sql",
        project_id=config.fda_project_id,
    )

    logging.info("Refreshing cross-source sales priority outputs...")
    run_sql_refresh(
        config=config,
        sql_filename="refresh_sales_priority_outputs.sql",
        project_id=config.project_id,
    )
