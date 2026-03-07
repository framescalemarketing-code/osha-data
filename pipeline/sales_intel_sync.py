from __future__ import annotations

import json
import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any

from pipeline.config import PipelineConfig, env_value, load_dotenv
from pipeline.sales_intel_common import company_key, json_dumps, stable_id
from pipeline.sales_intel_store import SalesIntelStore


def _resolve_bq_command() -> str:
    for candidate in ("bq", "bq.cmd", "bq.exe", "bq.bat"):
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    return "bq"


def _current_limit(config: PipelineConfig) -> int:
    dotenv_values = load_dotenv(config.paths.dotenv_path)
    value = env_value("SALES_INTEL_CURRENT_LIMIT", dotenv_values, "500")
    return int(value)


def _query_current_signal_rows(config: PipelineConfig) -> list[dict[str, Any]]:
    table = f"`{config.project_id}.{config.dataset}.sales_call_now_current`"
    sql = (
        "SELECT * "
        f"FROM {table} "
        "ORDER BY SAFE_CAST(`Overall Sales Score` AS FLOAT64) DESC, `Account Name` "
        f"LIMIT {_current_limit(config)}"
    )
    command = [
        _resolve_bq_command(),
        "query",
        f"--project_id={config.project_id}",
        "--use_legacy_sql=false",
        "--format=json",
        sql,
    ]
    proc = subprocess.run(
        command,
        cwd=str(config.paths.repo_root),
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"Current sales snapshot query failed ({proc.returncode}): {proc.stderr.strip()}"
        )
    raw = proc.stdout.strip() or "[]"
    payload = json.loads(raw)
    if not isinstance(payload, list):
        raise RuntimeError("Unexpected BigQuery JSON payload for current sales snapshot.")
    return payload


def _map_current_signal(row: dict[str, Any]) -> dict[str, Any]:
    company_name = str(row.get("Account Name", "")).strip()
    site_zip = str(row.get("Site ZIP", "")).strip()
    site_address = str(row.get("Site Address", "")).strip()
    return {
        "id": stable_id(company_name, site_zip, site_address),
        "company_name": company_name,
        "company_name_normalized": company_key(company_name),
        "region": str(row.get("Region", "")).strip(),
        "site_address": site_address,
        "site_city": str(row.get("Site City", "")).strip(),
        "site_state": str(row.get("Site State", "")).strip(),
        "site_zip": site_zip,
        "industry_segment": str(row.get("Industry Segment", "")).strip(),
        "current_priority": str(
            row.get("Overall Sales Priority") or row.get("OSHA Follow-up Priority") or ""
        ).strip(),
        "current_action": str(
            row.get("Should Look At Now") or row.get("OSHA Suggested Action") or ""
        ).strip(),
        "overall_sales_score": float(row.get("Overall Sales Score") or 0.0),
        "matched_sources": str(row.get("Matched Sources", "")).strip(),
        "reason_to_contact": str(row.get("Reason To Contact", "")).strip(),
        "reason_to_call_now": str(row.get("Reason To Call Now", "")).strip(),
        "why_fit": str(row.get("Why Fit", "")).strip(),
        "why_now": str(row.get("Why Now", "")).strip(),
        "raw_payload": json_dumps(row),
    }


def sync_current_pipeline_signals(
    config: PipelineConfig,
    *,
    store: SalesIntelStore | None = None,
    export_snapshots: bool = True,
) -> int:
    target_store = store or SalesIntelStore.from_config(config)
    target_store.initialize()
    rows: list[dict[str, Any]]
    try:
        rows = _query_current_signal_rows(config)
    except Exception as exc:
        logging.warning(
            "Current pipeline sync could not query BigQuery sales outputs. "
            "Writing empty current snapshot instead: %s",
            exc,
        )
        rows = []

    mapped = [_map_current_signal(row) for row in rows if str(row.get("Account Name", "")).strip()]
    target_store.replace_current_signals(mapped)
    target_store.rebuild_intersections()
    if export_snapshots:
        target_store.export_snapshots()
    return len(mapped)
