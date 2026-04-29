from __future__ import annotations

import csv
import json
import logging
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pipeline.bigquery import bq_load_csv
from pipeline.config import PipelineConfig
from pipeline.sql_refresh import run_sql_refresh


CENSUS_CBP_SCHEMA = (
    "name:STRING,naics2017:STRING,estab:STRING,emp:STRING,payann:STRING,"
    "state:STRING,county:STRING,load_dt:STRING"
)

BLS_SERIES_SCHEMA = (
    "series_id:STRING,segment:STRING,year:STRING,period:STRING,period_name:STRING,"
    "value:STRING,latest:STRING,footnotes:STRING,load_dt:STRING"
)

USASPENDING_NAICS_SCHEMA = (
    "naics_code:STRING,naics_name:STRING,amount:STRING,total_outlays:STRING,load_dt:STRING"
)

BLS_SERIES_MAP = {
    "SMU06000000000000001": "Total Nonfarm",
    "SMU06000002000000001": "Construction",
    "SMU06000003000000001": "Manufacturing",
    "SMU06000004000000001": "Trade/Transport/Utilities",
    "SMU06000006000000001": "Education/Health Services",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _http_json(
    *,
    url: str,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    timeout_seconds: int = 60,
    max_retries: int = 4,
    base_backoff_seconds: float = 1.0,
) -> Any:
    payload = None
    headers = {"Accept": "application/json", "User-Agent": "osha-public-signals/1.0"}
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    for attempt in range(1, max_retries + 2):
        req = Request(url=url, method=method, data=payload, headers=headers)
        try:
            with urlopen(req, timeout=timeout_seconds) as response:
                raw = response.read().decode("utf-8", errors="replace")
                try:
                    return json.loads(raw)
                except json.JSONDecodeError as exc:
                    excerpt = raw[:240].replace("\n", " ")
                    raise RuntimeError(
                        f"Non-JSON response from {url}. Excerpt: {excerpt}"
                    ) from exc
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


def fetch_census_cbp_ca(*, api_key: str, out_csv: Path, year: int) -> int:
    base_params: dict[str, str] = {
        "get": "NAME,NAICS2017,ESTAB,EMP,PAYANN",
        "for": "county:*",
        "in": "state:06",
    }
    params = dict(base_params)
    if api_key.strip():
        params["key"] = api_key.strip()

    url = f"https://api.census.gov/data/{year}/cbp?{urlencode(params, safe=':*')}"
    try:
        payload = _http_json(url=url, timeout_seconds=90)
    except Exception as exc:
        if api_key.strip():
            logging.warning(
                "Census call with key failed; retrying without key (public rate mode): %s",
                exc,
            )
            fallback_url = (
                f"https://api.census.gov/data/{year}/cbp?"
                f"{urlencode(base_params, safe=':*')}"
            )
            payload = _http_json(url=fallback_url, timeout_seconds=90)
        else:
            raise
    if not isinstance(payload, list) or not payload:
        raise RuntimeError("Unexpected Census payload shape.")

    header = payload[0]
    rows_in = payload[1:]
    idx = {name: i for i, name in enumerate(header)}
    required = ["NAME", "NAICS2017", "ESTAB", "EMP", "PAYANN", "state", "county"]
    for field in required:
        if field not in idx:
            raise RuntimeError(f"Census payload missing field: {field}")

    load_dt = _utc_now_iso()
    out_rows: list[list[str]] = []
    for row in rows_in:
        naics = row[idx["NAICS2017"]]
        if not re.fullmatch(r"\d{2}", naics):
            continue
        out_rows.append(
            [
                row[idx["NAME"]],
                naics,
                row[idx["ESTAB"]],
                row[idx["EMP"]],
                row[idx["PAYANN"]],
                row[idx["state"]],
                row[idx["county"]],
                load_dt,
            ]
        )

    return _write_csv(
        out_csv,
        ["name", "naics2017", "estab", "emp", "payann", "state", "county", "load_dt"],
        out_rows,
    )


def fetch_bls_ca_series(*, api_key: str, out_csv: Path) -> int:
    end_year = date.today().year
    start_year = end_year - 2
    body: dict[str, Any] = {
        "seriesid": list(BLS_SERIES_MAP.keys()),
        "startyear": str(start_year),
        "endyear": str(end_year),
    }
    if api_key.strip():
        body["registrationkey"] = api_key.strip()

    payload = _http_json(
        url="https://api.bls.gov/publicAPI/v2/timeseries/data/",
        method="POST",
        body=body,
        timeout_seconds=90,
    )
    if str(payload.get("status", "")).upper() != "REQUEST_SUCCEEDED" and api_key.strip():
        logging.warning(
            "BLS call with registration key failed; retrying without key (public rate mode)."
        )
        body.pop("registrationkey", None)
        payload = _http_json(
            url="https://api.bls.gov/publicAPI/v2/timeseries/data/",
            method="POST",
            body=body,
            timeout_seconds=90,
        )
    results = payload.get("Results", {})
    series_list = results.get("series", [])
    if not isinstance(series_list, list):
        raise RuntimeError("Unexpected BLS payload shape.")

    load_dt = _utc_now_iso()
    out_rows: list[list[str]] = []
    for series in series_list:
        series_id = str(series.get("seriesID", ""))
        segment = BLS_SERIES_MAP.get(series_id, "Unknown")
        data = series.get("data", [])
        if not isinstance(data, list):
            continue
        for item in data:
            period = str(item.get("period", ""))
            if not re.fullmatch(r"M\d{2}", period):
                continue
            out_rows.append(
                [
                    series_id,
                    segment,
                    str(item.get("year", "")),
                    period,
                    str(item.get("periodName", "")),
                    str(item.get("value", "")),
                    str(item.get("latest", "")),
                    json.dumps(item.get("footnotes", []), ensure_ascii=True),
                    load_dt,
                ]
            )

    return _write_csv(
        out_csv,
        [
            "series_id",
            "segment",
            "year",
            "period",
            "period_name",
            "value",
            "latest",
            "footnotes",
            "load_dt",
        ],
        out_rows,
    )


def fetch_usaspending_naics_ca(*, out_csv: Path, start_date: str, end_date: str) -> int:
    url = "https://api.usaspending.gov/api/v2/search/spending_by_category/naics/"
    page = 1
    load_dt = _utc_now_iso()
    out_rows: list[list[str]] = []

    while True:
        body = {
            "limit": 100,
            "page": page,
            "filters": {
                "time_period": [{"start_date": start_date, "end_date": end_date}],
                "place_of_performance_locations": [{"country": "USA", "state": "CA"}],
            },
        }
        payload = _http_json(url=url, method="POST", body=body, timeout_seconds=90)
        results = payload.get("results", [])
        if not isinstance(results, list):
            break

        for item in results:
            out_rows.append(
                [
                    str(item.get("code", "")),
                    str(item.get("name", "")),
                    str(item.get("amount", "")),
                    str(item.get("total_outlays", "")),
                    load_dt,
                ]
            )

        meta = payload.get("page_metadata", {})
        has_next = bool(meta.get("hasNext"))
        if not has_next:
            break
        page += 1
        if page > 50:
            logging.warning("USAspending page limit reached at page=%s", page)
            break

    return _write_csv(
        out_csv,
        ["naics_code", "naics_name", "amount", "total_outlays", "load_dt"],
        out_rows,
    )


def run_public_signals_ingest(config: PipelineConfig) -> None:
    data_dir = config.paths.data_dir
    census_csv = data_dir / "census_cbp_ca_raw.csv"
    bls_csv = data_dir / "bls_ca_series_raw.csv"
    usaspending_csv = data_dir / "usaspending_naics_ca_raw.csv"

    census_year = int(getattr(config, "census_cbp_year", 2022))
    usaspending_end = date.today().isoformat()
    try:
        parsed_since = date.fromisoformat(str(config.since_date))
        usaspending_start = parsed_since.isoformat()
    except (TypeError, ValueError):
        usaspending_start = (date.today() - timedelta(days=365 * 2)).isoformat()

    logging.info("Pulling Census CBP data (year=%s)...", census_year)
    census_rows = fetch_census_cbp_ca(
        api_key=config.census_api_key,
        out_csv=census_csv,
        year=census_year,
    )
    logging.info("Census CBP rows written: %s", census_rows)

    logging.info("Pulling BLS CA industry series...")
    bls_rows = fetch_bls_ca_series(api_key=config.bls_api_key, out_csv=bls_csv)
    logging.info("BLS rows written: %s", bls_rows)

    logging.info(
        "Pulling USAspending NAICS summary for CA (%s to %s)...",
        usaspending_start,
        usaspending_end,
    )
    usaspending_rows = fetch_usaspending_naics_ca(
        out_csv=usaspending_csv,
        start_date=usaspending_start,
        end_date=usaspending_end,
    )
    logging.info("USAspending rows written: %s", usaspending_rows)

    if census_rows > 0:
        bq_load_csv(
            repo_root=config.paths.repo_root,
            project_id=config.public_project_id,
            dataset=config.public_dataset,
            table="census_cbp_ca_raw",
            csv_path=census_csv,
            autodetect=False,
            schema=CENSUS_CBP_SCHEMA,
        )

    if bls_rows > 0:
        bq_load_csv(
            repo_root=config.paths.repo_root,
            project_id=config.public_project_id,
            dataset=config.public_dataset,
            table="bls_ca_series_raw",
            csv_path=bls_csv,
            autodetect=False,
            schema=BLS_SERIES_SCHEMA,
        )

    if usaspending_rows > 0:
        bq_load_csv(
            repo_root=config.paths.repo_root,
            project_id=config.public_project_id,
            dataset=config.public_dataset,
            table="usaspending_naics_ca_raw",
            csv_path=usaspending_csv,
            autodetect=False,
            schema=USASPENDING_NAICS_SCHEMA,
        )

    public_sql_file = config.paths.sql_dir / "refresh_public_signals.sql"
    if public_sql_file.exists():
        logging.info("Refreshing derived public enrichment tables...")
        run_sql_refresh(
            config=config,
            sql_filename="refresh_public_signals.sql",
            project_id=config.public_project_id,
        )
    else:
        logging.warning("Public signals SQL file not found: %s", public_sql_file)
