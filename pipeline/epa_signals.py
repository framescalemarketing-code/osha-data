from __future__ import annotations

import csv
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pipeline.bigquery import bq_load_csv, bq_query_sql
from pipeline.config import PipelineConfig
from pipeline.regions import BAY_AREA_COUNTIES, SAN_DIEGO_COUNTIES
from pipeline.sql_refresh import run_sql_refresh


EPA_SCHEMA = (
    "region_label:STRING,registry_id:STRING,fac_name:STRING,fac_street:STRING,fac_city:STRING,"
    "fac_state:STRING,fac_zip:STRING,fac_county:STRING,fac_federal_flg:STRING,fac_active_flag:STRING,"
    "fac_programs_in_snc:STRING,fac_qtrs_in_nc:STRING,fac_curr_compliance_status:STRING,"
    "fac_curr_snc_flag:STRING,air_flag:STRING,npdes_flag:STRING,sdwis_flag:STRING,rcra_flag:STRING,"
    "caa_qtrs_in_nc:STRING,caa_curr_compliance_status:STRING,caa_curr_hpv_flag:STRING,"
    "cwa_inspection_count:STRING,cwa_formal_action_count:STRING,cwa_qtrs_in_nc:STRING,"
    "cwa_curr_compliance_status:STRING,cwa_curr_snc_flag:STRING,rcra_inspection_count:STRING,"
    "rcra_formal_action_count:STRING,rcra_qtrs_in_nc:STRING,rcra_curr_compliance_status:STRING,"
    "rcra_curr_snc_flag:STRING,sdwa_formal_action_count:STRING,sdwa_curr_compliance_status:STRING,"
    "sdwa_curr_snc_flag:STRING,tri_ids:STRING,tri_releases_transfers:STRING,"
    "tri_on_site_releases:STRING,tri_reporter_in_past:STRING,fec_number_of_cases:STRING,"
    "fec_total_penalties:STRING,fac_naics_codes:STRING,fac_sic_codes:STRING,"
    "fac_date_last_inspection_epa:STRING,fac_date_last_inspection_state:STRING,"
    "fac_date_last_formal_act_epa:STRING,fac_date_last_formal_act_state:STRING,"
    "fac_federal_agency:STRING,dfr_url:STRING,load_dt:STRING"
)

_EPA_QUERY_URL = "https://echogeo.epa.gov/arcgis/rest/services/ECHO/Facilities/MapServer/0/query"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _http_json(
    *,
    url: str,
    timeout_seconds: int = 90,
    max_retries: int = 4,
    base_backoff_seconds: float = 1.0,
) -> Any:
    headers = {"Accept": "application/json", "User-Agent": "osha-epa-signals/1.0"}
    for attempt in range(1, max_retries + 2):
        req = Request(url=url, method="GET", headers=headers)
        try:
            with urlopen(req, timeout=timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8", errors="replace"))
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


def _attr(attrs: dict[str, Any], *keys: str) -> str:
    for key in keys:
        if key in attrs and attrs[key] not in (None, ""):
            return _text(attrs[key])
    return ""


def _county_where_clause(counties: tuple[str, ...]) -> str:
    county_list = ", ".join(f"'{county}'" for county in counties)
    return f"FAC_STATE = 'CA' AND FAC_COUNTY IN ({county_list})"


def _fetch_region_facilities(*, region_label: str, counties: tuple[str, ...]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    page_size = 1000

    while True:
        params = {
            "where": _county_where_clause(counties),
            "outFields": "*",
            "returnGeometry": "false",
            "f": "json",
            "resultOffset": str(offset),
            "resultRecordCount": str(page_size),
            "orderByFields": "OBJECTID ASC",
        }
        payload = _http_json(url=f"{_EPA_QUERY_URL}?{urlencode(params)}")
        features = payload.get("features", []) if isinstance(payload, dict) else []
        if not isinstance(features, list) or not features:
            break

        for feature in features:
            attrs = feature.get("attributes", {}) if isinstance(feature, dict) else {}
            if not isinstance(attrs, dict):
                continue
            attrs["region_label"] = region_label
            rows.append(attrs)

        if len(features) < page_size:
            break
        offset += page_size

    return rows


def fetch_epa_facilities(out_csv: Path) -> int:
    load_dt = _utc_now_iso()
    source_rows: list[dict[str, Any]] = []
    source_rows.extend(
        _fetch_region_facilities(region_label="San Diego", counties=SAN_DIEGO_COUNTIES)
    )
    source_rows.extend(
        _fetch_region_facilities(region_label="Bay Area", counties=BAY_AREA_COUNTIES)
    )

    out_rows: list[list[str]] = []
    for attrs in source_rows:
        out_rows.append(
            [
                _text(attrs.get("region_label")),
                _attr(attrs, "REGISTRY_ID", "RegistryID"),
                _attr(attrs, "FAC_NAME", "FacName"),
                _attr(attrs, "FAC_STREET", "FacStreet"),
                _attr(attrs, "FAC_CITY", "FacCity"),
                _attr(attrs, "FAC_STATE", "FacState"),
                _attr(attrs, "FAC_ZIP", "FacZip"),
                _attr(attrs, "FAC_COUNTY", "FacCounty"),
                _attr(attrs, "FAC_FEDERAL_FLG", "FacFederalFlg"),
                _attr(attrs, "FAC_ACTIVE_FLAG", "FAC_ACTIVE_FLG", "FacActiveFlag"),
                _attr(attrs, "FAC_PROGRAMS_IN_SNC", "FacProgramsInSNC"),
                _attr(attrs, "FAC_QTRS_IN_NC", "FacQtrsInNC"),
                _attr(attrs, "FAC_CURR_COMPLIANCE_STATUS", "FAC_CURR_COMPL_STATUS"),
                _attr(attrs, "FAC_CURR_SNC_FLAG", "FAC_CURR_SNC_FLG"),
                _attr(attrs, "AIR_FLAG", "AirFlag"),
                _attr(attrs, "NPDES_FLAG", "NpdesFlag"),
                _attr(attrs, "SDWIS_FLAG", "SdwisFlag"),
                _attr(attrs, "RCRA_FLAG", "RcraFlag"),
                _attr(attrs, "CAA_QTRS_IN_NC", "CAAQtrsInNC"),
                _attr(attrs, "CAA_CURR_COMPLIANCE_STATUS", "CAA_CURR_COMPL_STATUS"),
                _attr(attrs, "CAA_CURR_HPV_FLAG", "CAACurrHpvFlag"),
                _attr(attrs, "CWA_INSP_COUNT", "CWAInspectionCount"),
                _attr(attrs, "CWA_FORMAL_ACTION_COUNT", "CWAFormalActionCount"),
                _attr(attrs, "CWA_QTRS_IN_NC", "CWAQtrsInNC"),
                _attr(attrs, "CWA_CURR_COMPLIANCE_STATUS", "CWA_CURR_COMPL_STATUS"),
                _attr(attrs, "CWA_CURR_SNC_FLAG", "CWACurrSNCFlag"),
                _attr(attrs, "RCRA_INSP_COUNT", "RCRAInspectionCount"),
                _attr(attrs, "RCRA_FORMAL_ACTION_COUNT", "RCRAFormalActionCount"),
                _attr(attrs, "RCRA_QTRS_IN_NC", "RCRAQtrsInNC"),
                _attr(attrs, "RCRA_CURR_COMPLIANCE_STATUS", "RCRA_CURR_COMPL_STATUS"),
                _attr(attrs, "RCRA_CURR_SNC_FLAG", "RCRACurrSNCFlag"),
                _attr(attrs, "SDWA_FORMAL_ACTION_COUNT", "SDWAFormalActionCount"),
                _attr(attrs, "SDWA_CURR_COMPLIANCE_STATUS", "SDWA_CURR_COMPL_STATUS"),
                _attr(attrs, "SDWA_CURR_SNC_FLAG", "SDWACurrSNCFlag"),
                _attr(attrs, "TRI_IDS", "TRIIDs"),
                _attr(attrs, "TRI_RELEASES_TRANSFERS", "TRIReleasesTransfers"),
                _attr(attrs, "TRI_ON_SITE_RELEASES", "TRIOnSiteReleases"),
                _attr(attrs, "TRI_REPORTER_IN_PAST", "TRIReporterInPast"),
                _attr(attrs, "FEC_NUMBER_OF_CASES", "FecNumberOfCases"),
                _attr(attrs, "FEC_TOTAL_PENALTIES", "FecTotalPenalties"),
                _attr(attrs, "FAC_NAICS_CODES", "FacNaicsCodes"),
                _attr(attrs, "FAC_SIC_CODES", "FacSicCodes"),
                _attr(attrs, "FAC_DATE_LAST_INSPECTION_EPA", "FacDateLastInspectionEPA"),
                _attr(attrs, "FAC_DATE_LAST_INSPECTION_STATE", "FacDateLastInspectionState"),
                _attr(attrs, "FAC_DATE_LAST_FORMAL_ACTION_EPA", "FacDateLastFormalActEPA"),
                _attr(attrs, "FAC_DATE_LAST_FORMAL_ACTION_STATE", "FacDateLastFormalActSt"),
                _attr(attrs, "FAC_FEDERAL_AGENCY", "FacFederalAgency"),
                _attr(attrs, "DFR_URL", "DfrUrl"),
                load_dt,
            ]
        )

    return _write_csv(
        out_csv,
        [
            "region_label",
            "registry_id",
            "fac_name",
            "fac_street",
            "fac_city",
            "fac_state",
            "fac_zip",
            "fac_county",
            "fac_federal_flg",
            "fac_active_flag",
            "fac_programs_in_snc",
            "fac_qtrs_in_nc",
            "fac_curr_compliance_status",
            "fac_curr_snc_flag",
            "air_flag",
            "npdes_flag",
            "sdwis_flag",
            "rcra_flag",
            "caa_qtrs_in_nc",
            "caa_curr_compliance_status",
            "caa_curr_hpv_flag",
            "cwa_inspection_count",
            "cwa_formal_action_count",
            "cwa_qtrs_in_nc",
            "cwa_curr_compliance_status",
            "cwa_curr_snc_flag",
            "rcra_inspection_count",
            "rcra_formal_action_count",
            "rcra_qtrs_in_nc",
            "rcra_curr_compliance_status",
            "rcra_curr_snc_flag",
            "sdwa_formal_action_count",
            "sdwa_curr_compliance_status",
            "sdwa_curr_snc_flag",
            "tri_ids",
            "tri_releases_transfers",
            "tri_on_site_releases",
            "tri_reporter_in_past",
            "fec_number_of_cases",
            "fec_total_penalties",
            "fac_naics_codes",
            "fac_sic_codes",
            "fac_date_last_inspection_epa",
            "fac_date_last_inspection_state",
            "fac_date_last_formal_act_epa",
            "fac_date_last_formal_act_state",
            "fac_federal_agency",
            "dfr_url",
            "load_dt",
        ],
        out_rows,
    )


def run_epa_signals_ingest(config: PipelineConfig) -> None:
    data_dir = config.paths.data_dir
    out_csv = data_dir / "epa_facility_raw.csv"

    logging.info("Pulling EPA ECHO facility signals for target California regions...")
    row_count = fetch_epa_facilities(out_csv)
    logging.info("EPA facility rows written: %s", row_count)

    bq_query_sql(
        repo_root=config.paths.repo_root,
        project_id=config.epa_project_id,
        sql_text=f"CREATE SCHEMA IF NOT EXISTS `{config.epa_project_id}.{config.epa_dataset}`;",
    )

    if row_count > 0:
        bq_load_csv(
            repo_root=config.paths.repo_root,
            project_id=config.epa_project_id,
            dataset=config.epa_dataset,
            table="epa_facility_raw",
            csv_path=out_csv,
            autodetect=False,
            schema=EPA_SCHEMA,
        )

    logging.info("Refreshing EPA source scoring tables...")
    run_sql_refresh(
        config=config,
        sql_filename="refresh_epa_followup.sql",
        project_id=config.epa_project_id,
    )

    logging.info("Refreshing cross-source sales priority outputs...")
    run_sql_refresh(
        config=config,
        sql_filename="refresh_sales_priority_outputs.sql",
        project_id=config.project_id,
    )
