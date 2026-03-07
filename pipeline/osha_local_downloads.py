from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path

from pipeline.bigquery import bq_load_csv, bq_query_sql
from pipeline.config import PipelineConfig
from pipeline.sql_refresh import run_sql_refresh


@dataclass(frozen=True)
class LocalDownloadSpec:
    label: str
    table: str
    relative_path: str
    allow_quoted_newlines: bool = False
    field_delimiter: str | None = None
    sanitize_numeric_columns: tuple[str, ...] = ()
    normalize_to_csv: bool = False


def _source_specs() -> list[LocalDownloadSpec]:
    return [
        LocalDownloadSpec(
            label="ITA case detail",
            table="ita_case_detail_2023_raw",
            relative_path=(
                "data/downloads/osha/extracted/ita_case_detail_2023/"
                "ITA Case Detail Data 2023 through 12-31-2023OIICS.csv"
            ),
            allow_quoted_newlines=True,
            sanitize_numeric_columns=("ein",),
        ),
        LocalDownloadSpec(
            label="ITA 300A summary",
            table="ita_300a_summary_2024_2025_raw",
            relative_path=(
                "data/downloads/osha/extracted/ita_300a_summary_2024_2025/"
                "ITA 300A Summary Data 2024 through 08-31-2025.csv"
            ),
            sanitize_numeric_columns=("ein",),
        ),
        LocalDownloadSpec(
            label="Severe injury reports",
            table="severe_injury_2015_2025_raw",
            relative_path="data/downloads/osha/extracted/severe_injury_2015_2025/January2015toJuly2025.csv",
            allow_quoted_newlines=True,
            sanitize_numeric_columns=("Primary NAICS",),
        ),
        LocalDownloadSpec(
            label="Health samples sample",
            table="health_samples_sample_raw",
            relative_path="data/downloads/osha/health_samples_sample.txt",
            field_delimiter="|",
            normalize_to_csv=True,
        ),
    ]


def _sanitize_digits_value(value: str) -> str:
    stripped = value.strip()
    if not stripped:
        return ""
    if stripped.lower() in {"enter ein", "n/a", "na"}:
        return ""
    digits = "".join(ch for ch in stripped if ch.isdigit())
    return digits


def _prepare_load_file(
    config: PipelineConfig, spec: LocalDownloadSpec, source_path: Path
) -> tuple[Path, str | None]:
    if not spec.sanitize_numeric_columns and not spec.normalize_to_csv:
        return source_path, spec.field_delimiter

    temp_dir = config.paths.data_dir / "staged"
    temp_dir.mkdir(parents=True, exist_ok=True)
    staged_path = temp_dir / f"{spec.table}_staged.csv"

    csv.field_size_limit(1024 * 1024 * 64)
    with source_path.open("r", encoding="utf-8-sig", newline="", errors="replace") as src_handle:
        reader = csv.reader(src_handle, delimiter=spec.field_delimiter or ",")
        try:
            header = next(reader)
        except StopIteration:
            raise RuntimeError(f"Source file is empty: {source_path}")
        header_lookup = {name.strip().lower(): idx for idx, name in enumerate(header)}
        indexes_to_clean = [
            header_lookup[name.strip().lower()]
            for name in spec.sanitize_numeric_columns
            if name.strip().lower() in header_lookup
        ]
        if spec.sanitize_numeric_columns and not indexes_to_clean:
            logging.info("No target columns found in %s; staging without numeric cleanup.", source_path)

        with staged_path.open("w", encoding="utf-8", newline="") as dst_handle:
            writer = csv.writer(dst_handle)
            writer.writerow(header)
            for row in reader:
                for idx in indexes_to_clean:
                    if idx < len(row):
                        row[idx] = _sanitize_digits_value(row[idx])
                writer.writerow(row)

    return staged_path, None

def run_osha_local_downloads_ingest(config: PipelineConfig) -> None:
    repo_root = config.paths.repo_root
    loaded_specs: list[LocalDownloadSpec] = []

    for spec in _source_specs():
        csv_path = repo_root / Path(spec.relative_path)
        if not csv_path.exists():
            logging.info("Local OSHA download not found, skipping %s: %s", spec.label, csv_path)
            continue

        staged_path, field_delimiter = _prepare_load_file(config, spec, csv_path)
        logging.info("Loading local OSHA download: %s", spec.label)
        bq_load_csv(
            repo_root=repo_root,
            project_id=config.project_id,
            dataset=config.dataset,
            table=spec.table,
            csv_path=staged_path,
            autodetect=True,
            allow_quoted_newlines=spec.allow_quoted_newlines,
            field_delimiter=field_delimiter,
        )
        loaded_specs.append(spec)

    if not loaded_specs:
        logging.warning("No local OSHA download files were found; skipping local download refresh.")
        return

    logging.info(
        "Refreshing local OSHA download helper tables for %s source files.",
        len(loaded_specs),
    )
    run_sql_refresh(
        config=config,
        sql_filename="refresh_osha_local_downloads.sql",
        project_id=config.project_id,
    )

    logging.info("Refreshing final sales priority outputs with local OSHA signals.")
    run_sql_refresh(
        config=config,
        sql_filename="refresh_sales_priority_outputs.sql",
        project_id=config.project_id,
    )
